package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	"github.com/overfold/trellis/orchestrator/internal/spec"
	"github.com/overfold/trellis/orchestrator/internal/state"
)

type undrainFailingStore struct{ memoryStore }

type memoryStoreWrapper func(memoryStore) state.Store

func (undrainFailingStore) Put(context.Context, string, []byte) error {
	return errors.New("storage unavailable")
}

func (undrainFailingStore) Batch(context.Context, []state.Mutation) error {
	return errors.New("storage unavailable")
}

type nodeWriteFailingStore struct{ memoryStore }

func (s nodeWriteFailingStore) Put(ctx context.Context, key string, value []byte) error {
	if strings.Contains(key, "/nodes/") {
		return errors.New("storage unavailable")
	}
	return s.memoryStore.Put(ctx, key, value)
}

func (s nodeWriteFailingStore) Batch(ctx context.Context, mutations []state.Mutation) error {
	for _, mutation := range mutations {
		if strings.Contains(mutation.Key, "/nodes/") {
			return errors.New("storage unavailable")
		}
	}
	return s.memoryStore.Batch(ctx, mutations)
}

func TestHandleUndrainNodeReportsStateFailure(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusDraining}
	s.nodes[node.ID] = node
	jobSpec := &spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "api", Tasks: []spec.TaskSpec{{Name: "server", Image: "app"}}}}}
	s.jobs[jobKey("default", "web")] = &Job{Spec: canonicalTestSpec(jobSpec), Revision: 1}
	s.allocations = []*Allocation{{ID: "original", Namespace: "default", JobName: "web", TaskGroupName: "api", Node: node, Generation: 1, JobRevision: 1, Phase: lifecycle.PhaseRunning, Draining: true, DrainReason: "node"}}
	e := echo.New()
	NewHandler(s).Register(e)
	req := httptest.NewRequest(http.MethodDelete, "/v1/nodes/"+uuid.NewString()+"/drain", nil)
	req = req.WithContext(context.WithValue(req.Context(), AdminContextKey, true))
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound || !strings.Contains(rec.Body.String(), "node not found") {
		t.Fatalf("undrain unknown node: status %d, body %s", rec.Code, rec.Body.String())
	}
	stateStore := s.state
	s.state = NewStateController(undrainFailingStore{memoryStore{}}, "test")
	req = httptest.NewRequest(http.MethodDelete, "/v1/nodes/"+node.ID.String()+"/drain", nil)
	req = req.WithContext(context.WithValue(req.Context(), AdminContextKey, true))
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "put nodes and allocations") {
		t.Fatalf("persistence failure: status %d, body %s", rec.Code, rec.Body.String())
	}
	// Once the undrain is durable, publication does not depend on connectivity.
	s.state = stateStore
	node.Host, node.Port = "192.0.2.1", 1
	req = httptest.NewRequest(http.MethodDelete, "/v1/nodes/"+node.ID.String()+"/drain", nil)
	req = req.WithContext(context.WithValue(req.Context(), AdminContextKey, true))
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent || s.allocations[0].Draining {
		t.Fatalf("durable undrain: status %d, body %s", rec.Code, rec.Body.String())
	}
}

func TestReconcileRollingDoesNotReuseHealthyReplacement(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()

	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy}
	addTestNode(s, node, s.now())
	s.leaderSince = s.now().Add(-time.Minute)

	newSpec := &spec.JobSpec{
		Namespace: "default",
		Name:      "web",
		TaskGroups: []spec.TaskGroupSpec{{
			Name:   "api",
			Count:  3,
			Update: &spec.UpdateSpec{Strategy: spec.UpdateRolling, MaxParallel: 1},
			Tasks:  []spec.TaskSpec{{Name: "server", Image: "app:v2"}},
		}},
	}
	s.jobs[jobKey("default", "web")] = &Job{
		Spec:          canonicalTestSpec(newSpec),
		Revision:      2,
		ContentHashes: map[string]string{"api": spec.TaskGroupContentHash(&newSpec.TaskGroups[0])},
	}

	now := s.now()
	makeAllocation := func(id string, revision int, image string, health lifecycle.Health, draining bool) *Allocation {
		a := &Allocation{
			ID:            id,
			Namespace:     "default",
			JobName:       "web",
			TaskGroupName: "api",
			Tasks:         []spec.TaskSpec{{Name: "server", Image: image}},
			Node:          node,
			Generation:    1,
			JobRevision:   revision,
			Phase:         lifecycle.PhaseRunning,
			Health:        health,
			Draining:      draining,
			Diagnostic: lifecycle.Diagnostic{
				CreatedAt:      now,
				TransitionedAt: now,
			}}
		return a
	}

	oldA := makeAllocation("old-a", 1, "app:v1", lifecycle.HealthHealthy, true)
	oldB := makeAllocation("old-b", 1, "app:v1", lifecycle.HealthHealthy, true)
	newHealthy := makeAllocation("new-healthy", 2, "app:v2", lifecycle.HealthHealthy, false)
	newStarting := makeAllocation("new-starting", 2, "app:v2", lifecycle.HealthUnknown, false)
	s.allocations = []*Allocation{oldA, oldB, newHealthy, newStarting}

	s.Reconcile(context.Background())

	for _, old := range []*Allocation{oldA, oldB} {
		old.mu.Lock()
		phase := old.Phase
		old.mu.Unlock()
		if phase == lifecycle.PhaseStopping || phase == lifecycle.PhaseStopped {
			t.Fatalf("draining allocation %s stopped before the in-flight replacement became healthy", old.ID)
		}
	}
}

func TestReconcileStopsPendingFromOldRevision(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy}
	addTestNode(s, node, s.now())
	jobSpec := &spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "api", Count: 1, Tasks: []spec.TaskSpec{{Name: "server", Image: "app:v2"}}}}}
	s.jobs[jobKey("default", "web")] = &Job{Spec: canonicalTestSpec(jobSpec), Revision: 2}
	old := &Allocation{ID: "old", Namespace: "default", JobName: "web", TaskGroupName: "api", Tasks: []spec.TaskSpec{{Name: "server", Image: "app:v1"}}, Generation: 1, JobRevision: 1, Phase: lifecycle.PhasePending}
	s.allocations = []*Allocation{old}

	s.Reconcile(context.Background())

	if old.Phase != lifecycle.PhaseStopped || old.Node != nil {
		t.Fatalf("old pending allocation: phase=%s node=%v", old.Phase, old.Node)
	}
	if len(s.allocations) != 2 {
		t.Fatalf("allocation count = %d, want 2", len(s.allocations))
	}
	replacement := s.allocations[1]
	if replacement.JobRevision != 2 || replacement.Tasks[0].Image != "app:v2" || replacement.Node != node {
		t.Fatalf("replacement = %+v, want current revision and task on node", replacement)
	}
}

func TestReconcileChargesAllocationsQueuedForStop(t *testing.T) {
	tests := []struct {
		name              string
		setup             func(*testing.T, *Server, *Node, []spec.TaskSpec) *Allocation
		tasks             []spec.TaskSpec
		cpu               int
		memoryAllocatable int64
	}{
		{
			name: "deleted job CPU",
			setup: func(_ *testing.T, s *Server, node *Node, tasks []spec.TaskSpec) *Allocation {
				s.jobs[jobKey("default", "wanted")] = &Job{Spec: canonicalTestSpec(&spec.JobSpec{Namespace: "default", Name: "wanted", TaskGroups: []spec.TaskGroupSpec{{Name: "app", Count: 1, Tasks: tasks}}}), Revision: 1}
				return &Allocation{ID: "obsolete", Namespace: "default", JobName: "deleted", TaskGroupName: "app", Tasks: tasks, Node: node, Generation: 1, JobRevision: 1, Phase: lifecycle.PhaseRunning}
			},
			tasks:             []spec.TaskSpec{{Name: "app", Image: "app", Resources: &spec.ResourcesSpec{CPU: 1000, Memory: 128 << 20}}},
			cpu:               1000,
			memoryAllocatable: 1 << 30,
		},
		{
			name: "namespace-unadmitted memory",
			setup: func(_ *testing.T, s *Server, node *Node, tasks []spec.TaskSpec) *Allocation {
				limits := spec.DefaultLimits()
				limits.MaxDesiredAllocationsPerNamespace = 1
				s.jobLimits = limits
				s.jobs[jobKey("default", "admitted")] = &Job{Spec: canonicalTestSpec(&spec.JobSpec{Namespace: "default", Name: "admitted", TaskGroups: []spec.TaskGroupSpec{{Name: "app", Count: 1, Tasks: tasks}}}), Revision: 1}
				s.jobs[jobKey("default", "unadmitted")] = &Job{Spec: canonicalTestSpec(&spec.JobSpec{Namespace: "default", Name: "unadmitted", TaskGroups: []spec.TaskGroupSpec{{Name: "app", Count: 1, Tasks: tasks}}}), Revision: 1}
				return &Allocation{ID: "obsolete", Namespace: "default", JobName: "unadmitted", TaskGroupName: "app", Tasks: tasks, Node: node, Generation: 1, JobRevision: 1, Phase: lifecycle.PhaseRunning}
			},
			tasks:             []spec.TaskSpec{{Name: "app", Image: "app", Resources: &spec.ResourcesSpec{CPU: 100, Memory: 1 << 30}}},
			cpu:               1000,
			memoryAllocatable: 1 << 30,
		},
		{
			name: "recreate-obsolete static host port",
			setup: func(_ *testing.T, s *Server, node *Node, tasks []spec.TaskSpec) *Allocation {
				s.jobs[jobKey("default", "web")] = &Job{Spec: canonicalTestSpec(&spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "app", Count: 1, Tasks: tasks}}}), Revision: 2}
				return &Allocation{ID: "obsolete", Namespace: "default", JobName: "web", TaskGroupName: "app", Tasks: tasks, Node: node, Generation: 1, JobRevision: 1, Phase: lifecycle.PhaseRunning}
			},
			tasks:             []spec.TaskSpec{{Name: "app", Image: "app:v2", Networking: &spec.TaskNetworkingSpec{Mode: spec.TaskNetworkHost, Ports: []spec.PortSpec{{Port: 8080}}}}},
			cpu:               1000,
			memoryAllocatable: 1 << 30,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, agent := newTestServerWithAgent()
			defer agent.server.Close()
			node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy, CPUAllocatable: tt.cpu, MemoryAllocatable: tt.memoryAllocatable}
			addTestNode(s, node, s.now())
			obsolete := tt.setup(t, s, node, tt.tasks)
			s.allocations = []*Allocation{obsolete}

			s.Reconcile(context.Background())

			if obsolete.Phase != lifecycle.PhaseStopping || obsolete.CleanupComplete {
				t.Fatalf("obsolete allocation after failed stop: phase=%s retry=%v", obsolete.Phase, obsolete.NextRetryAt)
			}
			if tt.name == "recreate-obsolete static host port" {
				if len(s.allocations) != 1 {
					t.Fatal("recreate admitted replacement before stop completed")
				}
			} else if len(s.allocations) != 2 || s.allocations[1].Phase != lifecycle.PhasePending || s.allocations[1].Reason == "" {
				t.Fatalf("allocations = %#v, want the obsolete allocation and one pending placement diagnostic", s.allocations)
			}
			entry := committedAllocationIntent(t, s, node.ID, obsolete.ID, obsolete.Generation)
			if entry.Mode != nodeapi.ExecutionCleanup {
				t.Fatal("obsolete cleanup not published")
			}
			for _, allocation := range s.allocations {
				if allocation != obsolete && allocation.Node != nil && committedAllocationIntent(t, s, allocation.Node.ID, allocation.ID, allocation.Generation).Mode == nodeapi.ExecutionRun {
					t.Fatal("start admitted before old resource cleanup")
				}
			}
			s.Reconcile(t.Context())
			if obsolete.CleanupComplete {
				t.Fatal("repeated publication released occupancy")
			}
			acknowledgeTestCleanup(t, s, obsolete)
			s.Reconcile(context.Background())
			observeStarted(t, s, node.ID)
			var replacement *Allocation
			for _, allocation := range s.allocations {
				if allocation.ID != obsolete.ID {
					replacement = allocation
					break
				}
			}
			if replacement == nil || replacement.Phase != lifecycle.PhaseRunning {
				var states []string
				for _, allocation := range s.allocations {
					states = append(states, allocation.ID+":"+string(allocation.Phase))
				}
				t.Fatalf("post-stop allocations = %v calls=%#v, want a running replacement after occupancy is released", states, agent.recordedCalls())
			}
		})
	}
}

func TestReconcileStopsObsoletePendingGroupsAndReplicas(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	jobSpec := &spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "api", Count: 1, Tasks: []spec.TaskSpec{{Name: "server", Image: "app"}}}}}
	s.jobs[jobKey("default", "web")] = &Job{Spec: canonicalTestSpec(jobSpec), Revision: 2}
	makePending := func(id, group string) *Allocation {
		return &Allocation{ID: id, Namespace: "default", JobName: "web", TaskGroupName: group, Generation: 1, JobRevision: 2, Phase: lifecycle.PhasePending}
	}
	removed := makePending("removed", "worker")
	first := makePending("first", "api")
	surplus := makePending("surplus", "api")
	s.allocations = []*Allocation{removed, first, surplus}

	s.Reconcile(context.Background())

	if removed.Phase != lifecycle.PhaseStopped || first.Phase != lifecycle.PhasePending || surplus.Phase != lifecycle.PhaseStopped {
		t.Fatalf("pending phases: removed=%s first=%s surplus=%s", removed.Phase, first.Phase, surplus.Phase)
	}
}

func TestDrainNodeStopsAllocationAfterReplacementHealthy(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	drainingNode := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy}
	replacementNode := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy}
	addTestNode(s, drainingNode, s.now())
	addTestNode(s, replacementNode, s.now())
	s.leaderSince = s.now().Add(-time.Minute)

	jobSpec := &spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "api", Count: 1, Tasks: []spec.TaskSpec{{Name: "server", Image: "app"}}}}}
	s.jobs[jobKey("default", "web")] = &Job{Spec: canonicalTestSpec(jobSpec), Revision: 1}
	original := &Allocation{ID: "original", Namespace: "default", JobName: "web", TaskGroupName: "api", Tasks: jobSpec.TaskGroups[0].Tasks, Node: drainingNode, Generation: 1, JobRevision: 1, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy, Diagnostic: lifecycle.Diagnostic{CreatedAt: s.now(), TransitionedAt: s.now()}}
	s.allocations = []*Allocation{original}

	if err := s.DrainNode(context.Background(), drainingNode.ID); err != nil {
		t.Fatal(err)
	}
	if !original.Draining || original.Phase != lifecycle.PhaseRunning {
		t.Fatalf("original after drain = draining %t, phase %s; want draining and running", original.Draining, original.Phase)
	}
	if len(s.allocations) != 2 {
		t.Fatalf("allocations after drain = %d, want original and replacement", len(s.allocations))
	}
	replacement := s.allocations[1]
	if replacement.Node != replacementNode {
		t.Fatalf("replacement node = %v, want %v", replacement.Node, replacementNode)
	}
	observeStarted(t, s, replacementNode.ID)
	s.Reconcile(context.Background())
	if original.Phase != lifecycle.PhaseStopping || committedAllocationIntent(t, s, drainingNode.ID, original.ID, 1).Mode != nodeapi.ExecutionCleanup {
		t.Fatalf("original cleanup after replacement healthy = %s", original.Phase)
	}
}

func TestUndrainNodeRetainsCurrentAllocation(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy}
	addTestNode(s, node, s.now())
	jobSpec := &spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "api", Count: 1, Tasks: []spec.TaskSpec{{Name: "server", Image: "app"}}}}}
	s.jobs[jobKey("default", "web")] = &Job{Spec: canonicalTestSpec(jobSpec), Revision: 2}
	allocation := &Allocation{ID: "original", Namespace: "default", JobName: "web", TaskGroupName: "api", Tasks: jobSpec.TaskGroups[0].Tasks, Node: node, Generation: 1, JobRevision: 2, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy, Diagnostic: lifecycle.Diagnostic{CreatedAt: s.now(), TransitionedAt: s.now()}}
	s.allocations = []*Allocation{allocation}

	if err := s.DrainNode(context.Background(), node.ID); err != nil {
		t.Fatal(err)
	}
	if !allocation.Draining {
		t.Fatal("allocation was not marked draining")
	}
	if err := s.UndrainNode(context.Background(), node.ID); err != nil {
		t.Fatal(err)
	}
	if allocation.Draining || allocation.Phase != lifecycle.PhaseRunning || len(s.allocations) != 2 || s.allocations[1].Phase != lifecycle.PhaseStopped {
		t.Fatalf("allocation after undrain: draining=%t phase=%s count=%d", allocation.Draining, allocation.Phase, len(s.allocations))
	}
	entry := committedAllocationIntent(t, s, node.ID, allocation.ID, 1)
	if entry.Mode != nodeapi.ExecutionRun || entry.Request.Draining || entry.Request.DrainSequence != 2 {
		t.Fatal("undrain did not publish saved control sequence")
	}

}

func TestUndrainNodeRetriesRecoveredStartingAllocation(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusDraining}
	addTestNode(s, node, s.now())
	jobSpec := &spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "api", Count: 1, Tasks: []spec.TaskSpec{{Name: "server", Image: "app"}}}}}
	s.jobs[jobKey("default", "web")] = &Job{Spec: canonicalTestSpec(jobSpec), Revision: 1}
	allocation := &Allocation{ID: "original", Namespace: "default", JobName: "web", TaskGroupName: "api", Tasks: jobSpec.TaskGroups[0].Tasks, Node: node, Generation: 1, JobRevision: 1, Phase: lifecycle.PhaseStarting, Health: lifecycle.HealthUnknown, Draining: true, DrainReason: "node", Diagnostic: lifecycle.Diagnostic{CreatedAt: s.now(), TransitionedAt: s.now()}}
	s.allocations = []*Allocation{allocation}

	if err := s.UndrainNode(context.Background(), node.ID); err != nil {
		t.Fatalf("undrain starting allocation: %v", err)
	}
	if node.Status != NodeStatusHealthy || allocation.Draining || allocation.Phase != lifecycle.PhaseStarting {
		t.Fatalf("after undrain: node=%s draining=%t phase=%s", node.Status, allocation.Draining, allocation.Phase)
	}
	entry := committedAllocationIntent(t, s, node.ID, allocation.ID, 1)
	if entry.Mode != nodeapi.ExecutionRun || entry.Request.Draining || entry.Request.DrainSequence != allocation.DrainSequence {
		t.Fatal("recovered start not authorized with saved undrain state")
	}

}

func TestUndrainNodePreservesRestartIntent(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy}
	addTestNode(s, node, s.now())
	jobSpec := &spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "api", Count: 1, Tasks: []spec.TaskSpec{{Name: "server", Image: "app"}}}}}
	s.jobs[jobKey("default", "web")] = &Job{Spec: canonicalTestSpec(jobSpec), Revision: 1}
	allocation := &Allocation{ID: "original", Namespace: "default", JobName: "web", TaskGroupName: "api", Tasks: jobSpec.TaskGroups[0].Tasks, Node: node, Generation: 1, JobRevision: 1, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy}
	s.allocations = []*Allocation{allocation}

	if err := s.DrainNode(context.Background(), node.ID); err != nil {
		t.Fatal(err)
	}
	if allocation.DrainReason != "node" {
		t.Fatalf("drain reason = %q, want node", allocation.DrainReason)
	}
	if err := s.RestartJob(context.Background(), "default", "web"); err != nil {
		t.Fatal(err)
	}
	if allocation.DrainReason != "restart" {
		t.Fatalf("drain reason = %q, want restart", allocation.DrainReason)
	}
	if err := s.UndrainNode(context.Background(), node.ID); err != nil {
		t.Fatal(err)
	}
	if !allocation.Draining || allocation.DrainReason != "restart" || len(s.allocations) != 2 {
		t.Fatalf("restart intent after undrain: draining=%t reason=%q allocations=%d", allocation.Draining, allocation.DrainReason, len(s.allocations))
	}
	entry := committedAllocationIntent(t, s, node.ID, allocation.ID, 1)
	if !entry.Request.Draining || entry.Request.DrainSequence != allocation.DrainSequence {
		t.Fatal("undrain overwrote restart control state")
	}

}

func newDrainedNodeFixture(t *testing.T) (*Server, *testAgent, *Node, *Allocation) {
	t.Helper()
	s, agent := newTestServerWithAgent()
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy}
	addTestNode(s, node, s.now())
	s.controlEpoch = 1
	jobSpec := &spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "api", Count: 1, Tasks: []spec.TaskSpec{{Name: "server", Image: "app"}}}}}
	s.jobs[jobKey("default", "web")] = &Job{Spec: canonicalTestSpec(jobSpec), Revision: 1}
	allocation := &Allocation{ID: "original", Namespace: "default", JobName: "web", TaskGroupName: "api", Tasks: jobSpec.TaskGroups[0].Tasks, Node: node, Generation: 1, JobRevision: 1, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy}
	s.allocations = []*Allocation{allocation}
	if err := s.DrainNode(context.Background(), node.ID); err != nil {
		t.Fatal(err)
	}
	if !allocation.Draining || allocation.DrainSequence != 1 {
		t.Fatalf("drained allocation: draining=%t sequence=%d", allocation.Draining, allocation.DrainSequence)
	}
	return s, agent, node, allocation
}

func TestUndrainNodeSaveFailureSendsNoResume(t *testing.T) {
	for _, tc := range []struct {
		name  string
		store memoryStoreWrapper
	}{
		{name: "allocation save", store: func(m memoryStore) state.Store { return undrainFailingStore{m} }},
		{name: "node save", store: func(m memoryStore) state.Store { return nodeWriteFailingStore{m} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, agent, node, allocation := newDrainedNodeFixture(t)
			defer agent.server.Close()
			persisted := memoryStore{}
			s.state = NewStateController(tc.store(persisted), "test")
			if err := s.UndrainNode(context.Background(), node.ID); err == nil {
				t.Fatal("undrain succeeded despite a failed save")
			}
			if node.Status != NodeStatusDraining {
				t.Fatalf("node status after failed save = %s, want draining", node.Status)
			}
			// Server and agent must agree: whatever was saved, the next pass
			// converges on a drain newer than anything the agent was sent.
			s.state = NewStateController(persisted, "test")
			s.Reconcile(context.Background())
			if !allocation.Draining {
				t.Fatal("allocation not draining after reconciliation of a draining node")
			}
			entry := committedAllocationIntent(t, s, node.ID, allocation.ID, 1)
			if !entry.Request.Draining || entry.Request.DrainSequence != allocation.DrainSequence {
				t.Fatal("failed undrain published a newer resume")
			}

		})
	}
}

func TestUndrainAuthorityPersistsAcrossDisconnectedAgentAndLeadership(t *testing.T) {
	s, agent, node, allocation := newDrainedNodeFixture(t)
	t.Cleanup(agent.server.Close)
	node.Host, node.Port = "192.0.2.1", 1
	if err := s.UndrainNode(t.Context(), node.ID); err != nil {
		t.Fatal(err)
	}
	entry := committedAllocationIntent(t, s, node.ID, allocation.ID, 1)
	if entry.Mode != nodeapi.ExecutionRun || entry.Request.Draining || entry.Request.DrainSequence != 2 {
		t.Fatal("disconnection blocked undrain publication")
	}
	s.controlEpoch = 2
	s.Reconcile(t.Context())
	entry = committedAllocationIntent(t, s, node.ID, allocation.ID, 1)
	if entry.Request.Epoch != 2 || entry.Request.Draining || entry.Request.DrainSequence != 2 {
		t.Fatal("failover lost saved undrain authority")
	}
}

func TestReconcileStopsRemovedGroupOnDrainingNode(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()

	drainingNode := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusDraining}
	healthyNode := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy}
	addTestNode(s, drainingNode, s.now())
	addTestNode(s, healthyNode, s.now())

	jobSpec := &spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "api", Count: 1, Tasks: []spec.TaskSpec{{Name: "server", Image: "app:v2"}}}}}
	s.jobs[jobKey("default", "web")] = &Job{Spec: canonicalTestSpec(jobSpec), Revision: 2}
	now := s.now()
	removed := &Allocation{ID: "removed", Namespace: "default", JobName: "web", TaskGroupName: "worker", Tasks: []spec.TaskSpec{{Name: "worker", Image: "app:v1"}}, Node: drainingNode, Generation: 1, JobRevision: 1, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy, Diagnostic: lifecycle.Diagnostic{CreatedAt: now, TransitionedAt: now}}
	retained := &Allocation{ID: "retained", Namespace: "default", JobName: "web", TaskGroupName: "api", Tasks: jobSpec.TaskGroups[0].Tasks, Node: healthyNode, Generation: 1, JobRevision: 2, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy, Diagnostic: lifecycle.Diagnostic{CreatedAt: now, TransitionedAt: now}}
	s.allocations = []*Allocation{removed, retained}

	s.Reconcile(context.Background())

	if removed.Phase != lifecycle.PhaseStopping || committedAllocationIntent(t, s, drainingNode.ID, removed.ID, 1).Mode != nodeapi.ExecutionCleanup {
		t.Fatalf("removed group allocation cleanup = %s", removed.Phase)
	}
	if retained.Phase != lifecycle.PhaseRunning {
		t.Fatalf("retained group allocation phase = %s, want running", retained.Phase)
	}
}
