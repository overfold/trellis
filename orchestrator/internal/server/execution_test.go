package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	"github.com/overfold/trellis/orchestrator/internal/spec"
	"github.com/overfold/trellis/orchestrator/internal/state"
)

func executionFixture(t *testing.T) (*Server, *Node, *Allocation) {
	t.Helper()
	s, agent := newTestServerWithAgent()
	t.Cleanup(agent.server.Close)
	store, err := state.NewBoltStore(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	s.state = NewStateController(store, "test")
	s.controlEpoch, s.nodeID = 1, uuid.New()
	node := planTestNode(1, NodeStatusHealthy)
	addTestNode(s, node, s.now())
	job := canonicalTestSpec(&spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "app", Count: 1, Tasks: []spec.TaskSpec{{Name: "app", Image: "app", Networking: &spec.TaskNetworkingSpec{Mode: "none"}}}}}})
	s.jobs[jobKey("default", "web")] = &Job{Spec: job, Revision: 1, Incarnation: "incarnation"}
	allocation := &Allocation{ID: "allocation", Namespace: "default", JobName: "web", TaskGroupName: "app", Node: node, Tasks: job.TaskGroups[0].Tasks, Generation: 1, JobRevision: 1, JobIncarnation: "incarnation", Phase: lifecycle.PhasePlaced, Health: lifecycle.HealthUnknown}
	s.allocations = []*Allocation{allocation}
	s.rebuildAllocationNodeIndexLocked()
	if err := s.Execute(t.Context(), &Action{Type: ActionStart, Allocation: allocation}); err != nil {
		t.Fatal(err)
	}
	return s, node, allocation
}

func fetchExecutionCredentials(t *testing.T, s *Server, node uuid.UUID, version uint64, request nodeapi.AllocationRequest) nodeapi.ExecutionCredentials {
	t.Helper()
	e := echo.New()
	NewHandler(s).Register(e)
	path := fmt.Sprintf("/v1/nodes/%s/execution/%s/credentials?version=%d&generation=%d&epoch=%d&attempt=%d", node, request.AllocationID, version, request.Generation, request.Epoch, request.Attempt)
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req = req.WithContext(context.WithValue(req.Context(), NodeContextKey, node))
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("credentials response=%d: %s", rec.Code, rec.Body.String())
	}
	var credentials nodeapi.ExecutionCredentials
	if err := json.Unmarshal(rec.Body.Bytes(), &credentials); err != nil {
		t.Fatal(err)
	}
	return credentials
}

func committedExecutionRequest(t *testing.T, s *Server, node uuid.UUID, allocation string, generation uint64) nodeapi.AllocationRequest {
	t.Helper()
	doc, err := s.ExecutionIntent(t.Context(), node, 0)
	if err != nil || doc == nil {
		t.Fatalf("read committed intent: %v", err)
	}
	for _, entry := range doc.Allocations {
		if entry.Request.AllocationID == allocation && entry.Request.Generation == generation {
			return entry.Request
		}
	}
	t.Fatalf("missing execution %s generation %d", allocation, generation)
	return nodeapi.AllocationRequest{}
}

func committedAllocationIntent(t *testing.T, s *Server, node uuid.UUID, id string, generation uint64) nodeapi.AllocationIntent {
	t.Helper()
	doc, err := s.ExecutionIntent(t.Context(), node, 0)
	if err != nil || doc == nil {
		t.Fatalf("execution intent: %v", err)
	}
	for _, entry := range doc.Allocations {
		if entry.Request.AllocationID == id && entry.Request.Generation == generation {
			return entry
		}
	}
	t.Fatalf("missing intent for %s/%d", id, generation)
	return nodeapi.AllocationIntent{}
}

func acknowledgeTestCleanup(t *testing.T, s *Server, allocation *Allocation) {
	t.Helper()
	node := allocation.Node
	if node == nil {
		t.Fatal("cleanup requires an assigned node")
	}
	statuses := []nodeapi.AllocationStatus{{ID: allocation.ID, Generation: allocation.Generation, Phase: lifecycle.PhaseStopped, Health: lifecycle.HealthUnknown, CleanupComplete: true}}
	// Heartbeats are complete inventories. Preserve other executions rather
	// than accidentally reporting their absence while acknowledging one stop.
	for _, other := range s.allocations {
		if other == allocation || other.Node == nil || other.Node.ID != node.ID || other.Phase == lifecycle.PhasePending {
			continue
		}
		if other.CleanupComplete {
			statuses = append(statuses, nodeapi.AllocationStatus{ID: other.ID, Generation: other.Generation, Phase: lifecycle.PhaseStopped, Health: lifecycle.HealthUnknown, CleanupComplete: true})
			continue
		}
		for _, task := range other.Tasks {
			health := other.Health
			if health == "" {
				health = lifecycle.HealthUnknown
			}
			statuses = append(statuses, nodeapi.AllocationStatus{ID: other.ID, Generation: other.Generation, Task: task.Name, Phase: other.Phase, Health: health})
		}
	}
	if err := heartbeatAndApply(t, s, node.ID, statuses, "test", nodeResourceObservation{CPUCapacity: max(node.CPUCapacity, node.CPUAllocatable), CPUAllocatable: node.CPUAllocatable, MemoryCapacity: max(node.MemoryCapacity, node.MemoryAllocatable), MemoryAllocatable: node.MemoryAllocatable}); err != nil {
		t.Fatal(err)
	}
	if !allocation.CleanupComplete {
		t.Fatal("exact-generation cleanup proof did not commit")
	}
}

func TestExecutionCleanupProofFailureKeepsInventoryAndReservations(t *testing.T) {
	s, node, allocation := executionFixture(t)
	allocation.Phase = lifecycle.PhaseLost
	node.observedAllocations = []observedAllocation{{ID: allocation.ID, Generation: allocation.Generation, Phase: lifecycle.PhaseRunning}}
	store := &auditStore{memoryStore: memoryStore{}, failBatch: true}
	s.state = NewStateController(store, "test")
	if err := heartbeatAndApply(t, s, node.ID, []nodeapi.AllocationStatus{{ID: allocation.ID, Generation: allocation.Generation, Phase: lifecycle.PhaseStopped, Health: lifecycle.HealthUnknown, CleanupComplete: true}}, "test", nodeResourceObservation{}); err != nil {
		t.Fatal(err)
	}
	complete, err := s.state.executionCleaned(t.Context(), node.ID, allocation.ID, allocation.Generation)
	if err != nil || complete || allocation.CleanupComplete || allocation.Phase != lifecycle.PhaseLost || len(node.observedAllocations) != 1 || node.observedAllocations[0].CleanupComplete {
		t.Fatal("failed proof commit released inventory or reservations")
	}
	store.failBatch = false
	acknowledgeTestCleanup(t, s, allocation)
}

func TestExecutionRecoveryGraceAndPrunedAuthorityConverge(t *testing.T) {
	s, node, allocation := executionFixture(t)
	now := s.now()
	s.now = func() time.Time { return now }
	s.leaderSince = now
	allocation.Phase = lifecycle.PhaseLost
	if err := s.publishReconcileIntent(t.Context()); err != nil {
		t.Fatal(err)
	}
	doc, _ := s.ExecutionIntent(t.Context(), node.ID, 0)
	if doc.CleanupAbsent || doc.Allocations[0].Mode != nodeapi.ExecutionRetain {
		t.Fatal("recovery grace destroyed original execution")
	}
	now = now.Add(leaderRecoveryGrace)
	decision := executionCleanupDecision{Node: node.ID, ID: allocation.ID, Generation: allocation.Generation, RetainLogs: false}
	if err := s.state.CommitReconciliation(t.Context(), &ReconciliationCommit{DeleteAllocations: []string{allocation.ID}, ExecutionCleanup: []executionCleanupDecision{decision}}); err != nil {
		t.Fatal(err)
	}
	s.allocations = nil
	if err := s.publishReconcileIntent(t.Context()); err != nil {
		t.Fatal(err)
	}
	doc, _ = s.ExecutionIntent(t.Context(), node.ID, 0)
	if !doc.CleanupAbsent || doc.Allocations[0].Mode != nodeapi.ExecutionCleanup || doc.Allocations[0].RetainLogs {
		t.Fatal("pruned execution remained retained after recovery")
	}
	if err := heartbeatAndApply(t, s, node.ID, []nodeapi.AllocationStatus{{ID: allocation.ID, Generation: allocation.Generation, Phase: lifecycle.PhaseStopped, Health: lifecycle.HealthUnknown, CleanupComplete: true}}, "test", nodeResourceObservation{}); err != nil {
		t.Fatal(err)
	}
	if err := s.publishReconcileIntent(t.Context()); err != nil {
		t.Fatal(err)
	}
	doc, _ = s.ExecutionIntent(t.Context(), node.ID, 0)
	if !doc.CleanupAbsent || len(doc.Allocations) != 0 {
		t.Fatal("cleaned pruned authority did not converge to absence")
	}
}

func TestExecutionCommittedCleanupSurvivesProjectionFailureAndFailover(t *testing.T) {
	s, node, allocation := executionFixture(t)
	decision := executionCleanupDecision{Node: node.ID, ID: allocation.ID, Generation: allocation.Generation, RetainLogs: false}
	keepLogs := decision
	keepLogs.RetainLogs = true
	// Model the leader stopping after the atomic pruning/cleanup commit but
	// before publishing its node document. No renewable inventory is available.
	if err := s.state.CommitReconciliation(t.Context(), &ReconciliationCommit{DeleteAllocations: []string{allocation.ID}, ExecutionCleanup: []executionCleanupDecision{decision, keepLogs}}); err != nil {
		t.Fatal(err)
	}
	// Neither a duplicate in the same batch nor a later cleanup request can
	// revoke removal of logs, even without a previous response document.
	if err := s.state.CommitReconciliation(t.Context(), &ReconciliationCommit{ExecutionCleanup: []executionCleanupDecision{keepLogs}}); err != nil {
		t.Fatal(err)
	}
	s.allocations, s.executionWatch = nil, nil
	s.controlEpoch++
	s.leaderSince = s.now()
	s.executionIntents = nil
	if err := s.publishReconcileIntent(t.Context()); err != nil {
		t.Fatal(err)
	}
	entry := committedAllocationIntent(t, s, node.ID, allocation.ID, allocation.Generation)
	if entry.Mode != nodeapi.ExecutionCleanup || entry.RetainLogs || entry.Request.Epoch != s.controlEpoch {
		t.Fatal("failover lost durable cleanup authority")
	}
}

func TestExecutionCleanupProofReleasesOnlyExactGenerationAdmission(t *testing.T) {
	s, node, allocation := executionFixture(t)
	allocation.Phase = lifecycle.PhaseLost
	replacement := allocation.cloneRecord()
	replacement.ID, replacement.Generation, replacement.Phase = "replacement", 2, lifecycle.PhasePlaced
	s.allocations = append(s.allocations, replacement)
	s.rebuildAllocationNodeIndexLocked()
	actions := []Action{{Type: ActionStopObserved, Node: node, ID: allocation.ID, Generation: allocation.Generation}, {Type: ActionStart, Allocation: replacement}}
	for _, action := range actions {
		if err := s.Execute(t.Context(), &action); err != nil {
			t.Fatal(err)
		}
	}
	if committedAllocationIntent(t, s, node.ID, replacement.ID, 2).Mode != nodeapi.ExecutionRetain {
		t.Fatal("cleanup publication prematurely admitted replacement")
	}
	if err := heartbeatAndApply(t, s, node.ID, []nodeapi.AllocationStatus{{ID: allocation.ID, Generation: 2, Phase: lifecycle.PhaseStopped, Health: lifecycle.HealthUnknown, CleanupComplete: true}}, "test", nodeResourceObservation{}); err != nil {
		t.Fatal(err)
	}
	if err := s.publishReconcileIntent(t.Context()); err != nil {
		t.Fatal(err)
	}
	if committedAllocationIntent(t, s, node.ID, replacement.ID, 2).Mode != nodeapi.ExecutionRetain {
		t.Fatal("wrong generation released admission")
	}
	acknowledgeTestCleanup(t, s, allocation)
	if err := s.publishReconcileIntent(t.Context()); err != nil {
		t.Fatal(err)
	}
	if committedAllocationIntent(t, s, node.ID, replacement.ID, 2).Mode != nodeapi.ExecutionRun {
		t.Fatal("exact cleanup proof did not release admission")
	}
}

func TestExecutionIntentCommittedVersionsWakeAndSurviveLeaderChange(t *testing.T) {
	s, node, allocation := executionFixture(t)
	first, err := s.ExecutionIntent(t.Context(), node.ID, 0)
	if err != nil || first == nil || first.Version != 1 || !first.Complete || first.CleanupAbsent || first.Allocations[0].Mode != nodeapi.ExecutionRun {
		t.Fatalf("first intent = %+v, %v", first, err)
	}
	if err := s.Execute(t.Context(), &Action{Type: ActionStart, Allocation: allocation}); err != nil {
		t.Fatal(err)
	}
	unchanged, _ := s.ExecutionIntent(t.Context(), node.ID, 0)
	if unchanged.Version != first.Version {
		t.Fatal("unchanged desired execution advanced version")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	woken := make(chan *nodeapi.ExecutionIntent, 1)
	go func() { doc, _ := s.ExecutionIntent(ctx, node.ID, first.Version); woken <- doc }()
	if err := s.Execute(t.Context(), &Action{Type: ActionStop, Allocation: allocation}); err != nil {
		t.Fatal(err)
	}
	second := <-woken
	if second == nil || second.Version != 2 || second.Allocations[0].Mode != nodeapi.ExecutionCleanup || allocation.Phase != lifecycle.PhaseStopping {
		t.Fatalf("cleanup intent = %+v; phase=%s", second, allocation.Phase)
	}
	// A new publisher rebuilds from canonical state with a new epoch and a
	// fresh counter, preserving an already committed destruction decision.
	s.controlEpoch = 2
	s.executionWatch = nil
	s.executionIntents = nil
	allocation.Phase = lifecycle.PhaseLost
	if err := s.Execute(t.Context(), &Action{Type: ActionDrain, Allocation: allocation}); err != nil {
		t.Fatal(err)
	}
	third, _ := s.ExecutionIntent(t.Context(), node.ID, 0)
	if third.Version != 1 || third.Epoch != 2 || third.Allocations[0].Mode != nodeapi.ExecutionCleanup {
		t.Fatalf("new leader intent = %+v", third)
	}
	// The old epoch must not long-poll just because its version was reused.
	e := echo.New()
	NewHandler(s).Register(e)
	request := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/v1/nodes/%s/execution?epoch=%d&version=%d", node.ID, first.Epoch, first.Version), nil)
	request = request.WithContext(context.WithValue(ctx, NodeContextKey, node.ID))
	response := httptest.NewRecorder()
	e.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("old epoch pull blocked or failed: %d %s", response.Code, response.Body.String())
	}
	var pulled nodeapi.ExecutionIntent
	if err := json.Unmarshal(response.Body.Bytes(), &pulled); err != nil || pulled.Epoch != 2 || pulled.Version != 1 {
		t.Fatalf("old epoch pull returned %+v %v", pulled, err)
	}
}

func TestExecutionIntentLostRetainsAndRetryWaits(t *testing.T) {
	s, node, allocation := executionFixture(t)
	allocation.Phase = lifecycle.PhaseLost
	if err := s.Execute(t.Context(), &Action{Type: ActionStart, Allocation: allocation}); err != nil {
		t.Fatal(err)
	}
	doc, _ := s.ExecutionIntent(t.Context(), node.ID, 0)
	if doc.Allocations[0].Mode != nodeapi.ExecutionRetain {
		t.Fatal("lost generation authorized relaunch")
	}
	allocation.Phase = lifecycle.PhaseStarting
	allocation.Attempt = 1
	due := s.now().Add(time.Minute)
	allocation.NextRetryAt = &due
	if err := s.Execute(t.Context(), &Action{Type: ActionStart, Allocation: allocation}); err != nil {
		t.Fatal(err)
	}
	doc, _ = s.ExecutionIntent(t.Context(), node.ID, 0)
	if doc.Allocations[0].Mode != nodeapi.ExecutionRetain || doc.Allocations[0].Request.Attempt != 1 {
		t.Fatal("retry admitted before backoff elapsed")
	}
	s.now = func() time.Time { return due }
	if err := s.Execute(t.Context(), &Action{Type: ActionStart, Allocation: allocation}); err != nil {
		t.Fatal(err)
	}
	doc, _ = s.ExecutionIntent(t.Context(), node.ID, 0)
	if doc.Allocations[0].Mode != nodeapi.ExecutionRun {
		t.Fatal("due retry not authorized")
	}
}

func TestExecutionJobApplyPublishesWithoutTicker(t *testing.T) {
	s, node, allocation := executionFixture(t)
	first, _ := s.ExecutionIntent(t.Context(), node.ID, 0)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	changed := make(chan *nodeapi.ExecutionIntent, 1)
	go func() { doc, _ := s.ExecutionIntent(ctx, node.ID, first.Version); changed <- doc }()
	tasks := spec.CloneTasks(allocation.Tasks)
	tasks[0].Image = "replacement"
	desired := canonicalTestSpec(&spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "app", Count: 1, Tasks: tasks, Update: &spec.UpdateSpec{Strategy: spec.UpdateRecreate}}}})
	if _, err := s.RegisterJob(t.Context(), "default", desired, nil, nil); err != nil {
		t.Fatal(err)
	}
	doc := <-changed
	if doc == nil || doc.Version <= first.Version || doc.Allocations[0].Mode != nodeapi.ExecutionCleanup || allocation.Phase != lifecycle.PhaseStopping {
		t.Fatalf("job apply did not immediately publish cleanup: %+v phase=%s", doc, allocation.Phase)
	}
}

func TestExecutionCredentialsRequireExactCommittedRunIdentity(t *testing.T) {
	s, node, allocation := executionFixture(t)
	e := echo.New()
	NewHandler(s).Register(e)
	for _, tc := range []struct {
		name, query string
		node        uuid.UUID
		status      int
	}{
		{"authorized", "version=1&generation=1&epoch=1&attempt=0", node.ID, http.StatusOK},
		{"wrong node", "version=1&generation=1&epoch=1&attempt=0", uuid.New(), http.StatusForbidden},
		{"old version", "version=0&generation=1&epoch=1&attempt=0", node.ID, http.StatusConflict},
		{"wrong generation", "version=1&generation=2&epoch=1&attempt=0", node.ID, http.StatusForbidden},
		{"wrong attempt", "version=1&generation=1&epoch=1&attempt=1", node.ID, http.StatusForbidden},
		{"wrong epoch", "version=1&generation=1&epoch=2&attempt=0", node.ID, http.StatusConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/v1/nodes/%s/execution/%s/credentials?%s", node.ID, allocation.ID, tc.query), nil)
			request = request.WithContext(context.WithValue(request.Context(), NodeContextKey, tc.node))
			response := httptest.NewRecorder()
			e.ServeHTTP(response, request)
			if response.Code != tc.status {
				t.Fatalf("status=%d want=%d: %s", response.Code, tc.status, response.Body.String())
			}
			if response.Code == http.StatusOK {
				var credentials nodeapi.ExecutionCredentials
				if err := json.Unmarshal(response.Body.Bytes(), &credentials); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestExecutionCleanupProofGenerationFencedAndDurable(t *testing.T) {
	s, node, allocation := executionFixture(t)
	allocation.Phase = lifecycle.PhaseLost
	for _, generation := range []uint64{2, 1} {
		if err := heartbeatAndApply(t, s, node.ID, []nodeapi.AllocationStatus{{ID: allocation.ID, Generation: generation, Phase: lifecycle.PhaseStopped, Health: lifecycle.HealthUnknown, CleanupComplete: true}}, "", nodeResourceObservation{}); err != nil {
			t.Fatal(err)
		}
		if generation == 2 && (allocation.CleanupComplete || allocation.Phase != lifecycle.PhaseLost) {
			t.Fatal("another generation released reservations")
		}
		if generation == 1 {
			if !allocation.CleanupComplete || allocation.Phase != lifecycle.PhaseStopped {
				t.Fatal("complete cleanup did not release lost generation")
			}
			stored, err := s.state.ListAllocations(t.Context())
			if err != nil || !stored[allocation.ID].CleanupComplete || stored[allocation.ID].Node.ID != node.ID {
				t.Fatalf("cleanup proof not durable: %+v %v", stored, err)
			}
		}
	}
}

func TestExecutionRetryAuthorityDoesNotBypassBackoff(t *testing.T) {
	s, node, allocation := executionFixture(t)
	first := committedAllocationIntent(t, s, node.ID, allocation.ID, 1)
	if err := heartbeatAndApply(t, s, node.ID, startFailure(allocation, 0), "test", nodeResourceObservation{}); err != nil {
		t.Fatal(err)
	}
	if allocation.Attempt != 1 || allocation.NextRetryAt == nil {
		t.Fatal("failed attempt did not enter backoff")
	}
	due := *allocation.NextRetryAt
	for range 4 {
		if err := s.Execute(t.Context(), &Action{Type: ActionStart, Allocation: allocation}); err != nil {
			t.Fatal(err)
		}
		entry := committedAllocationIntent(t, s, node.ID, allocation.ID, 1)
		if entry.Mode != nodeapi.ExecutionRetain || entry.Request.Attempt != 1 || allocation.Attempt != 1 {
			t.Fatal("repeated publication bypassed retry authority")
		}
	}
	s.now = func() time.Time { return due }
	s.Reconcile(t.Context())
	retry := committedAllocationIntent(t, s, node.ID, allocation.ID, 1)
	if retry.Mode != nodeapi.ExecutionRun || retry.Request.Attempt != 1 || retry.Request.ExecutionHash != first.Request.ExecutionHash {
		t.Fatal("backoff lost the next explicit attempt's authority")
	}
	if err := heartbeatAndApply(t, s, node.ID, startFailure(allocation, 0), "test", nodeResourceObservation{}); err != nil {
		t.Fatal(err)
	}
	if allocation.Attempt != 1 {
		t.Fatal("old failure consumed a new attempt")
	}
}

func TestExecutionStaleStartCannotAuthorizeDeletedIncarnation(t *testing.T) {
	s, node, allocation := executionFixture(t)
	delete(s.jobs, jobKey(allocation.Namespace, allocation.JobName))
	if err := s.Execute(t.Context(), &Action{Type: ActionStart, Allocation: allocation}); err != nil {
		t.Fatal(err)
	}
	if committedAllocationIntent(t, s, node.ID, allocation.ID, 1).Mode != nodeapi.ExecutionRetain {
		t.Fatal("stale start restored authority after job deletion")
	}
}

func TestReconcilePublicationSerializesUndrain(t *testing.T) {
	s, node, allocation := executionFixture(t)
	node.Status = NodeStatusDraining
	allocation.Phase, allocation.Health = lifecycle.PhaseRunning, lifecycle.HealthHealthy
	allocation.Draining, allocation.DrainReason, allocation.DrainSequence = true, "node", 1
	other := planTestNode(2, NodeStatusHealthy)
	addTestNode(s, other, s.now())
	replacement := allocation.cloneRecord()
	replacement.ID, replacement.Node, replacement.Draining, replacement.DrainReason = "replacement", other, false, ""
	s.allocations = append(s.allocations, replacement)
	s.rebuildAllocationNodeIndexLocked()
	values, err := s.state.store.List(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	store := &auditStore{memoryStore: memoryStore(values), blockBatch: make(chan struct{}), batchStarted: make(chan struct{})}
	s.state = NewStateController(store, "test")
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	pass := make(chan struct{})
	go func() { s.Reconcile(ctx); close(pass) }()
	select {
	case <-store.batchStarted:
	case <-ctx.Done():
		t.Fatal("pass did not reach execution authority commit")
	}
	undrain := make(chan error, 1)
	go func() { undrain <- s.UndrainNode(ctx, node.ID) }()
	select {
	case err := <-undrain:
		t.Fatalf("undrain crossed unpublished cleanup decision: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(store.blockBatch)
	select {
	case <-pass:
	case <-ctx.Done():
		t.Fatal("publication did not complete")
	}
	select {
	case err := <-undrain:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("undrain did not complete")
	}
	entry := committedAllocationIntent(t, s, node.ID, allocation.ID, 1)
	if entry.Mode != nodeapi.ExecutionCleanup || !allocation.Draining || allocation.Phase != lifecycle.PhaseStopping {
		t.Fatal("undrain revoked the committed cleanup generation")
	}
}

func TestReconcileCommitsAuthorityWithoutNodeDocuments(t *testing.T) {
	s, node, _ := executionFixture(t)
	store := &auditStore{memoryStore: memoryStore{}}
	s.state = NewStateController(store, "test")
	s.allocations = nil
	s.rebuildAllocationNodeIndexLocked()
	s.executionIntents = nil
	job := s.jobs[jobKey("default", "web")]
	job.Spec.TaskGroups[0].Count = 3
	job.Spec.TaskGroups[0].Restart = &spec.RestartPolicySpec{MaxRestarts: 3, Window: time.Minute}
	if err := s.state.PutJob(t.Context(), jobKey("default", "web"), job); err != nil {
		t.Fatal(err)
	}
	if err := s.state.PutNode(t.Context(), node.ID.String(), nodeSummary(node)); err != nil {
		t.Fatal(err)
	}
	store.puts = 0
	s.Reconcile(t.Context())
	stored, err := s.state.ListAllocations(t.Context())
	if err != nil || len(stored) != 3 {
		t.Fatalf("committed allocations: %v %v", stored, err)
	}
	for _, allocation := range stored {
		if allocation.Phase != lifecycle.PhaseStarting || allocation.Restart == nil || allocation.Restart.MaxRestarts != 3 {
			t.Fatalf("admission was not committed with placement: %+v", allocation)
		}
	}
	if store.puts != 0 || len(store.batchSizes) != 1 {
		t.Fatalf("three starts required extra writes: puts=%d batches=%v", store.puts, store.batchSizes)
	}
	doc, err := s.ExecutionIntent(t.Context(), node.ID, 0)
	if err != nil || doc == nil || len(doc.Allocations) != 3 {
		t.Fatalf("projected intent: %+v %v", doc, err)
	}
	first := doc.Allocations[0].Request
	documents, err := store.List(t.Context(), "trellis/test/node-execution/")
	if err != nil || len(documents) != 0 {
		t.Fatalf("persisted response documents: %v %v", documents, err)
	}
	s.Reconcile(t.Context())
	if store.puts != 0 || len(store.batchSizes) != 1 {
		t.Fatal("unchanged reconciliation wrote derived intent")
	}
	// A changed/deleted source job must not redefine an existing generation.
	job.Spec.TaskGroups[0].Restart.MaxRestarts = 20
	job.Spec.TaskGroups[0].Tasks[0].Image = "changed"
	if err := s.state.PutJob(t.Context(), jobKey("default", "web"), job); err != nil {
		t.Fatal(err)
	}
	s.controlEpoch++
	s.executionIntents = nil
	if err := s.Reload(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := s.Execute(t.Context(), &Action{Type: ActionDrain}); err != nil {
		t.Fatal(err)
	}
	rebuilt := committedExecutionRequest(t, s, node.ID, first.AllocationID, first.Generation)
	if rebuilt.ExecutionHash != first.ExecutionHash || rebuilt.Restart.MaxRestarts != 3 || rebuilt.Tasks[0].Image != first.Tasks[0].Image {
		t.Fatalf("cache-free recovery changed execution identity: %+v", rebuilt)
	}
}
