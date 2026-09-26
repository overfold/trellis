package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/lifecycle"
	"github.com/clofour/trellis/internal/spec"
	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

type undrainFailingStore struct{ memoryStore }

func (undrainFailingStore) Put(context.Context, string, []byte) error {
	return errors.New("storage unavailable")
}

func TestHandleUndrainNodeReportsResumeFailure(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusDraining}
	s.nodes[node.ID] = node
	jobSpec := &spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "api", Tasks: []spec.TaskSpec{{Name: "server", Image: "app"}}}}}
	s.jobs[jobKey("default", "web")] = &Job{Spec: jobSpec, Revision: 1}
	s.allocations = []*Allocation{{ID: "original", Namespace: "default", JobName: "web", TaskGroupName: "api", Node: node, Generation: 1, JobRevision: 1, Phase: lifecycle.PhaseRunning, Draining: true, DrainReason: "node"}}
	agent.mu.Lock()
	agent.failResume = true
	agent.mu.Unlock()
	e := echo.New()
	NewHandler(s).Register(e)
	for _, tc := range []struct {
		id      string
		status  int
		message string
	}{
		{uuid.NewString(), http.StatusNotFound, "node not found"},
		{node.ID.String(), http.StatusInternalServerError, "resume allocation original"},
	} {
		req := httptest.NewRequest(http.MethodDelete, "/v1/nodes/"+tc.id+"/drain", nil)
		req = req.WithContext(context.WithValue(req.Context(), AdminContextKey, true))
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		if rec.Code != tc.status || !strings.Contains(rec.Body.String(), tc.message) {
			t.Fatalf("undrain %s: status %d, body %s", tc.id, rec.Code, rec.Body.String())
		}
	}
	agent.mu.Lock()
	agent.failResume = false
	agent.mu.Unlock()
	s.state = NewStateController(undrainFailingStore{memoryStore{}}, "test")
	req := httptest.NewRequest(http.MethodDelete, "/v1/nodes/"+node.ID.String()+"/drain", nil)
	req = req.WithContext(context.WithValue(req.Context(), AdminContextKey, true))
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), "persist resumed allocation original") {
		t.Fatalf("persistence failure: status %d, body %s", rec.Code, rec.Body.String())
	}
}

func TestReconcileRollingDoesNotReuseHealthyReplacement(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()

	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy, LastHeartbeat: s.now()}
	s.nodes[node.ID] = node
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
		Spec:          newSpec,
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

func TestDrainNodeStopsAllocationAfterReplacementHealthy(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	drainingNode := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy, LastHeartbeat: s.now()}
	replacementNode := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy, LastHeartbeat: s.now()}
	s.nodes[drainingNode.ID] = drainingNode
	s.nodes[replacementNode.ID] = replacementNode
	s.leaderSince = s.now().Add(-time.Minute)

	jobSpec := &spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "api", Count: 1, Tasks: []spec.TaskSpec{{Name: "server", Image: "app"}}}}}
	s.jobs[jobKey("default", "web")] = &Job{Spec: jobSpec, Revision: 1}
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
	replacement.Health = lifecycle.HealthHealthy
	s.Reconcile(context.Background())
	if original.Phase != lifecycle.PhaseStopped {
		t.Fatalf("original phase after replacement healthy = %s, want stopped", original.Phase)
	}
}

func TestUndrainNodeRetainsCurrentAllocation(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy, LastHeartbeat: s.now()}
	s.nodes[node.ID] = node
	jobSpec := &spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "api", Count: 1, Tasks: []spec.TaskSpec{{Name: "server", Image: "app"}}}}}
	s.jobs[jobKey("default", "web")] = &Job{Spec: jobSpec, Revision: 2}
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
	if allocation.Draining || allocation.Phase != lifecycle.PhaseRunning || len(s.allocations) != 1 {
		t.Fatalf("allocation after undrain: draining=%t phase=%s count=%d", allocation.Draining, allocation.Phase, len(s.allocations))
	}
	resumed := false
	for _, call := range agent.recordedCalls() {
		if call.path == "/v1/allocations/original/drain" {
			var request api.DrainAllocationRequest
			if err := json.Unmarshal(call.body, &request); err != nil {
				t.Fatal(err)
			}
			want := uint64(1)
			if call.method == http.MethodDelete {
				want = 2
			}
			if request.Sequence != want {
				t.Fatalf("%s sequence = %d, want %d", call.method, request.Sequence, want)
			}
		}
		if call.method == "DELETE" && call.path == "/v1/allocations/original/drain" {
			resumed = true
		}
	}
	if !resumed {
		t.Fatal("agent did not receive allocation resume")
	}
}

func TestUndrainNodeRetriesRecoveredStartingAllocation(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusDraining, LastHeartbeat: s.now()}
	s.nodes[node.ID] = node
	jobSpec := &spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "api", Count: 1, Tasks: []spec.TaskSpec{{Name: "server", Image: "app"}}}}}
	s.jobs[jobKey("default", "web")] = &Job{Spec: jobSpec, Revision: 1}
	allocation := &Allocation{ID: "original", Namespace: "default", JobName: "web", TaskGroupName: "api", Tasks: jobSpec.TaskGroups[0].Tasks, Node: node, Generation: 1, JobRevision: 1, Phase: lifecycle.PhaseStarting, Health: lifecycle.HealthUnknown, Draining: true, DrainReason: "node", Diagnostic: lifecycle.Diagnostic{CreatedAt: s.now(), TransitionedAt: s.now()}}
	s.allocations = []*Allocation{allocation}

	if err := s.UndrainNode(context.Background(), node.ID); err != nil {
		t.Fatalf("undrain starting allocation: %v", err)
	}
	if node.Status != NodeStatusHealthy || allocation.Draining || allocation.Phase != lifecycle.PhaseRunning {
		t.Fatalf("after undrain: node=%s draining=%t phase=%s", node.Status, allocation.Draining, allocation.Phase)
	}
	var resumed, started bool
	for _, call := range agent.recordedCalls() {
		if call.method == "DELETE" && call.path == "/v1/allocations/original/drain" {
			resumed = true
		}
		if call.method == "POST" && call.path == "/v1/allocations" {
			started = true
		}
	}
	if !resumed || !started {
		t.Fatalf("agent calls after undrain: resumed=%t started=%t", resumed, started)
	}
}

func TestUndrainNodePreservesRestartIntent(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy, LastHeartbeat: s.now()}
	s.nodes[node.ID] = node
	jobSpec := &spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "api", Count: 1, Tasks: []spec.TaskSpec{{Name: "server", Image: "app"}}}}}
	s.jobs[jobKey("default", "web")] = &Job{Spec: jobSpec, Revision: 1}
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
	for _, call := range agent.recordedCalls() {
		if call.method == "DELETE" && call.path == "/v1/allocations/original/drain" {
			t.Fatal("undrain resumed an allocation marked for restart")
		}
	}
	persisted, err := s.state.ListAllocations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !persisted[allocation.ID].Draining || persisted[allocation.ID].DrainReason != "restart" {
		t.Fatalf("persisted restart intent = %#v", persisted[allocation.ID])
	}
}

func TestUndrainNodeResumeFailureLeavesNodeDraining(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy, LastHeartbeat: s.now()}
	s.nodes[node.ID] = node
	jobSpec := &spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "api", Count: 1, Tasks: []spec.TaskSpec{{Name: "server", Image: "app"}}}}}
	s.jobs[jobKey("default", "web")] = &Job{Spec: jobSpec, Revision: 1}
	allocation := &Allocation{ID: "original", Namespace: "default", JobName: "web", TaskGroupName: "api", Tasks: jobSpec.TaskGroups[0].Tasks, Node: node, Generation: 1, JobRevision: 1, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy}
	s.allocations = []*Allocation{allocation}

	if err := s.DrainNode(context.Background(), node.ID); err != nil {
		t.Fatal(err)
	}
	agent.mu.Lock()
	agent.failResume = true
	agent.mu.Unlock()
	if err := s.UndrainNode(context.Background(), node.ID); err == nil {
		t.Fatal("undrain succeeded despite agent resume failure")
	}
	if node.Status != NodeStatusDraining || !allocation.Draining {
		t.Fatalf("after failed resume: node=%s allocation draining=%t", node.Status, allocation.Draining)
	}
	nodes, err := s.state.ListNodes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if nodes[node.ID.String()].Status != NodeStatusDraining {
		t.Fatalf("persisted node status = %s, want draining", nodes[node.ID.String()].Status)
	}
	agent.mu.Lock()
	agent.failResume = false
	agent.mu.Unlock()
	if err := s.UndrainNode(context.Background(), node.ID); err != nil {
		t.Fatal(err)
	}
	if node.Status != NodeStatusHealthy || allocation.Draining {
		t.Fatalf("after retry: node=%s allocation draining=%t", node.Status, allocation.Draining)
	}
}

func TestReconcileStopsRemovedGroupOnDrainingNode(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()

	drainingNode := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusDraining, LastHeartbeat: s.now()}
	healthyNode := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy, LastHeartbeat: s.now()}
	s.nodes[drainingNode.ID] = drainingNode
	s.nodes[healthyNode.ID] = healthyNode

	jobSpec := &spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "api", Count: 1, Tasks: []spec.TaskSpec{{Name: "server", Image: "app:v2"}}}}}
	s.jobs[jobKey("default", "web")] = &Job{Spec: jobSpec, Revision: 2}
	now := s.now()
	removed := &Allocation{ID: "removed", Namespace: "default", JobName: "web", TaskGroupName: "worker", Tasks: []spec.TaskSpec{{Name: "worker", Image: "app:v1"}}, Node: drainingNode, Generation: 1, JobRevision: 1, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy, Diagnostic: lifecycle.Diagnostic{CreatedAt: now, TransitionedAt: now}}
	retained := &Allocation{ID: "retained", Namespace: "default", JobName: "web", TaskGroupName: "api", Tasks: jobSpec.TaskGroups[0].Tasks, Node: healthyNode, Generation: 1, JobRevision: 2, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy, Diagnostic: lifecycle.Diagnostic{CreatedAt: now, TransitionedAt: now}}
	s.allocations = []*Allocation{removed, retained}

	s.Reconcile(context.Background())

	if removed.Phase != lifecycle.PhaseStopped {
		t.Fatalf("removed group allocation phase = %s, want stopped", removed.Phase)
	}
	if retained.Phase != lifecycle.PhaseRunning {
		t.Fatalf("retained group allocation phase = %s, want running", retained.Phase)
	}
}
