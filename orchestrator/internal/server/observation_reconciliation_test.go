package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/lifecycle"
	"github.com/clofour/trellis/internal/spec"
	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

func TestHeartbeatReturnsNoContent(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	node := &Node{ID: uuid.New(), Status: NodeStatusHealthy}
	s.nodes[node.ID] = node
	body, err := json.Marshal(api.HeartbeatRequest{NodeID: node.ID, Version: "test"})
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
	if recorder.Code != http.StatusNoContent || recorder.Body.Len() != 0 {
		t.Fatalf("heartbeat response = status %d body %q, want empty 204", recorder.Code, recorder.Body.String())
	}
}

func TestReconcileStopsHeartbeatObservedOrphanAfterRecoveryGrace(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy, LastHeartbeat: s.now()}
	s.nodes[node.ID] = node
	s.leaderSince = s.now().Add(-leaderRecoveryGrace - time.Second)
	if err := s.Heartbeat(context.Background(), node.ID, []api.AllocationStatus{{
		ID: "orphan", Generation: 3, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy,
	}}, "test", nil, nil); err != nil {
		t.Fatal(err)
	}

	s.Reconcile(context.Background())
	calls := agent.recordedCalls()
	if len(calls) != 1 || calls[0].method != http.MethodDelete || calls[0].path != "/v1/allocations/orphan" {
		t.Fatalf("agent calls = %#v, want one orphan stop", calls)
	}
	var request api.StopAllocationRequest
	if err := json.Unmarshal(calls[0].body, &request); err != nil {
		t.Fatal(err)
	}
	if request.AllocationID != "orphan" || request.Generation != 3 {
		t.Fatalf("stop request = %#v, want orphan generation 3", request)
	}
}

func TestReconcileProtectsRecoveredObservationDuringLeaderGrace(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy, LastHeartbeat: s.now()}
	s.nodes[node.ID] = node
	s.leaderSince = s.now()
	if err := s.Heartbeat(context.Background(), node.ID, []api.AllocationStatus{{
		ID: "recovered", Generation: 1, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy,
	}}, "test", nil, nil); err != nil {
		t.Fatal(err)
	}

	s.Reconcile(context.Background())
	if calls := agent.recordedCalls(); len(calls) != 0 {
		t.Fatalf("agent calls during leader recovery grace = %#v, want none", calls)
	}
}

func TestReconcileStopsStaleObservedGeneration(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy, LastHeartbeat: s.now()}
	s.nodes[node.ID] = node
	s.leaderSince = s.now().Add(-leaderRecoveryGrace - time.Second)
	tasks := []spec.TaskSpec{{Name: "app", Image: "app"}}
	s.jobs[jobKey("default", "web")] = &Job{Spec: &spec.JobSpec{
		Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "app", Count: 1, Tasks: tasks}},
	}, Revision: 2}
	s.allocations = []*Allocation{{
		ID: "alloc", Namespace: "default", JobName: "web", TaskGroupName: "app", Tasks: tasks,
		Node: node, Generation: 2, JobRevision: 2, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy,
		Diagnostic: lifecycle.Diagnostic{CreatedAt: s.now(), TransitionedAt: s.now()},
	}}
	if err := s.Heartbeat(context.Background(), node.ID, []api.AllocationStatus{
		{ID: "alloc", Generation: 1, Task: "app", Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy},
		{ID: "alloc", Generation: 2, Task: "app", Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy},
	}, "test", nil, nil); err != nil {
		t.Fatal(err)
	}

	s.Reconcile(context.Background())
	calls := agent.recordedCalls()
	if len(calls) != 1 || calls[0].method != http.MethodDelete {
		t.Fatalf("agent calls = %#v, want one stale-generation stop", calls)
	}
	var request api.StopAllocationRequest
	if err := json.Unmarshal(calls[0].body, &request); err != nil {
		t.Fatal(err)
	}
	if request.AllocationID != "alloc" || request.Generation != 1 {
		t.Fatalf("stop request = %#v, want alloc generation 1", request)
	}
}
