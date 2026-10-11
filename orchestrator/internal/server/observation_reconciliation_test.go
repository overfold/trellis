package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	"github.com/overfold/trellis/orchestrator/internal/spec"
)

func TestHeartbeatReturnsControlPlaneTopology(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	node := &Node{ID: uuid.New(), Status: NodeStatusHealthy}
	addTestNode(s, node, time.Time{})
	usage := 0.25
	used, available := int64(2<<30), int64(6<<30)
	metricsAt := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	body, err := json.Marshal(nodeapi.HeartbeatRequest{
		NodeID: node.ID, Version: "test",
		CPUCapacity: 2000, MemoryCapacity: 8 << 30,
		CPUAllocatable: 1900, MemoryAllocatable: 7 << 30,
		CPUUsage: &usage, MemoryUsed: &used, MemoryAvailable: &available, MetricsAt: &metricsAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	e := echo.New()
	NewHandler(s).Register(e)
	request := httptest.NewRequest(http.MethodPost, "/v1/nodes/"+node.ID.String()+"/heartbeat", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request = request.WithContext(context.WithValue(request.Context(), NodeContextKey, node.ID))
	recorder := httptest.NewRecorder()
	e.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || recorder.Body.Len() == 0 {
		t.Fatalf("heartbeat response = status %d body %q, want topology JSON with 200", recorder.Code, recorder.Body.String())
	}
	applyTestObservations(s)
	if node.CPUCapacity != 2000 || node.CPUAllocatable != 1900 || node.CPUUsage == nil || *node.CPUUsage != usage {
		t.Fatalf("node CPU observation = %#v", node)
	}
	if node.MemoryCapacity != 8<<30 || node.MemoryAllocatable != 7<<30 || node.MemoryUsed == nil || *node.MemoryUsed != used || node.MemoryAvailable == nil || *node.MemoryAvailable != available {
		t.Fatalf("node memory observation = %#v", node)
	}
	if node.MetricsAt == nil || !node.MetricsAt.Equal(metricsAt) {
		t.Fatalf("node metrics time = %v, want %v", node.MetricsAt, metricsAt)
	}
}

func TestReconcileStopsHeartbeatObservedOrphanAfterRecoveryGrace(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy}
	addTestNode(s, node, s.now())
	s.leaderSince = s.now().Add(-leaderRecoveryGrace - time.Second)
	if err := heartbeatAndApply(t, s, node.ID, []nodeapi.AllocationStatus{{
		ID: "orphan", Generation: 3, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy,
	}}, "test", nodeResourceObservation{}); err != nil {
		t.Fatal(err)
	}

	s.Reconcile(context.Background())
	entry := committedAllocationIntent(t, s, node.ID, "orphan", 3)
	if entry.Mode != nodeapi.ExecutionCleanup {
		t.Fatal("orphan generation 3 cleanup not published")
	}

}

func TestReconcileStopsTerminalAllocationReportedByReturningNode(t *testing.T) {
	for _, phase := range []lifecycle.Phase{lifecycle.PhaseLost, lifecycle.PhaseStopped} {
		t.Run(string(phase), func(t *testing.T) {
			s, agent := newTestServerWithAgent()
			defer agent.server.Close()
			node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy}
			addTestNode(s, node, s.now())
			s.leaderSince = s.now().Add(-leaderRecoveryGrace - time.Second)
			tasks := []spec.TaskSpec{{Name: "app", Image: "app"}}
			s.jobs[jobKey("default", "web")] = &Job{Spec: canonicalTestSpec(&spec.JobSpec{
				Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "app", Count: 1, Tasks: tasks}},
			}), Revision: 1}
			old := &Allocation{
				ID: "old", Namespace: "default", JobName: "web", TaskGroupName: "app", Tasks: tasks,
				Node: node, Generation: 1, JobRevision: 1, Phase: phase, Health: lifecycle.HealthUnknown,
				Diagnostic: lifecycle.Diagnostic{CreatedAt: s.now(), TransitionedAt: s.now()},
			}
			replacement := &Allocation{
				ID: "replacement", Namespace: "default", JobName: "web", TaskGroupName: "app", Tasks: tasks,
				Node: node, Generation: 1, JobRevision: 1, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy,
				Diagnostic: lifecycle.Diagnostic{CreatedAt: s.now(), TransitionedAt: s.now()},
			}
			s.allocations = []*Allocation{old, replacement}
			if err := heartbeatAndApply(t, s, node.ID, []nodeapi.AllocationStatus{
				{ID: "old", Generation: 1, Task: "app", Phase: lifecycle.PhaseStarting, Health: lifecycle.HealthUnknown},
				{ID: "replacement", Generation: 1, Task: "app", Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy},
			}, "test", nodeResourceObservation{}); err != nil {
				t.Fatal(err)
			}
			if old.Phase != phase {
				t.Fatalf("reported %s allocation phase = %s, want %s", phase, old.Phase, phase)
			}

			s.Reconcile(context.Background())
			entry := committedAllocationIntent(t, s, node.ID, "old", 1)
			if entry.Mode != nodeapi.ExecutionCleanup {
				t.Fatal("returning terminal generation cleanup not published")
			}
			if old.CleanupComplete {
				t.Fatal("publication fabricated cleanup proof")
			}
			acknowledgeTestCleanup(t, s, old)
			if old.Phase != lifecycle.PhaseStopped || replacement.Phase != lifecycle.PhaseRunning {
				t.Fatalf("phases after reconcile: old=%s replacement=%s, want stopped/running", old.Phase, replacement.Phase)
			}
		})
	}
}

func TestReconcileProtectsRecoveredObservationDuringLeaderGrace(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy}
	addTestNode(s, node, s.now())
	s.leaderSince = s.now()
	if err := heartbeatAndApply(t, s, node.ID, []nodeapi.AllocationStatus{{
		ID: "recovered", Generation: 1, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy,
	}}, "test", nodeResourceObservation{}); err != nil {
		t.Fatal(err)
	}

	s.Reconcile(context.Background())
	doc, err := s.ExecutionIntent(t.Context(), node.ID, 0)
	if err != nil || doc == nil || doc.CleanupAbsent || len(doc.Allocations) != 0 {
		t.Fatalf("recovery grace authority: %+v %v", doc, err)
	}

}

func TestReconcileStopsStaleObservedGeneration(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy}
	addTestNode(s, node, s.now())
	s.leaderSince = s.now().Add(-leaderRecoveryGrace - time.Second)
	tasks := []spec.TaskSpec{{Name: "app", Image: "app"}}
	s.jobs[jobKey("default", "web")] = &Job{Spec: canonicalTestSpec(&spec.JobSpec{
		Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "app", Count: 1, Tasks: tasks}},
	}), Revision: 2}
	s.allocations = []*Allocation{{
		ID: "alloc", Namespace: "default", JobName: "web", TaskGroupName: "app", Tasks: tasks,
		Node: node, Generation: 2, JobRevision: 2, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy,
		Diagnostic: lifecycle.Diagnostic{CreatedAt: s.now(), TransitionedAt: s.now()},
	}}
	if err := heartbeatAndApply(t, s, node.ID, []nodeapi.AllocationStatus{
		{ID: "alloc", Generation: 1, Task: "app", Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy},
		{ID: "alloc", Generation: 2, Task: "app", Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy},
	}, "test", nodeResourceObservation{}); err != nil {
		t.Fatal(err)
	}

	s.Reconcile(context.Background())
	old := committedAllocationIntent(t, s, node.ID, "alloc", 1)
	current := committedAllocationIntent(t, s, node.ID, "alloc", 2)
	if old.Mode != nodeapi.ExecutionCleanup || current.Mode != nodeapi.ExecutionRun {
		t.Fatal("cleanup was not fenced to stale generation")
	}

}
