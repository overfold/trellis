package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/catalog"
	"github.com/clofour/trellis/internal/lifecycle"
	"github.com/clofour/trellis/internal/spec"
	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

func heartbeatReasonTestServer(phase lifecycle.Phase) (*Server, *Node, *Allocation) {
	node := &Node{ID: uuid.MustParse("99999999-9999-9999-9999-999999999999"), Host: "node-a", Status: NodeStatusHealthy}
	allocation := &Allocation{
		ID: "demo-web-1", Node: node, Generation: 1,
		Tasks: []spec.TaskSpec{{Name: "app"}, {Name: "sidecar"}},
		Phase: phase, Health: lifecycle.HealthHealthy,
	}
	s := &Server{
		state: NewStateController(memoryStore{}, "test"),
		nodes: map[uuid.UUID]*Node{node.ID: node}, allocations: []*Allocation{allocation},
		catalog: catalog.New(),
	}
	return s, node, allocation
}

func postHeartbeat(t *testing.T, s *Server, nodeID uuid.UUID, allocations []api.AllocationStatus) int {
	t.Helper()
	body, err := json.Marshal(api.HeartbeatRequest{NodeID: nodeID, Version: "test", Allocations: allocations})
	if err != nil {
		t.Fatal(err)
	}
	e := echo.New()
	NewHandler(s).Register(e)
	request := httptest.NewRequest(http.MethodPost, "/v1/nodes/"+nodeID.String()+"/heartbeat", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request = request.WithContext(context.WithValue(request.Context(), NodeContextKey, nodeID))
	recorder := httptest.NewRecorder()
	e.ServeHTTP(recorder, request)
	return recorder.Code
}

func TestHeartbeatRecordsReportedFailureReason(t *testing.T) {
	failed := api.AllocationStatus{ID: "demo-web-1", Generation: 1, Task: "app", Phase: lifecycle.PhaseFailed, Health: lifecycle.HealthUnhealthy, Reason: api.OperationRestartExhausted}
	running := api.AllocationStatus{ID: "demo-web-1", Generation: 1, Task: "sidecar", Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy}
	for _, actual := range [][]api.AllocationStatus{{failed, running}, {running, failed}} {
		s, node, allocation := heartbeatReasonTestServer(lifecycle.PhaseRunning)
		if code := postHeartbeat(t, s, node.ID, actual); code != http.StatusNoContent {
			t.Fatalf("heartbeat status = %d, want 204", code)
		}
		if allocation.Phase != lifecycle.PhaseFailed || allocation.Reason != string(api.OperationRestartExhausted) {
			t.Fatalf("allocation = %s reason %q, want failed with %q", allocation.Phase, allocation.Reason, api.OperationRestartExhausted)
		}
		events := allocation.Events.Entries()
		if len(events) == 0 || events[len(events)-1].Phase != lifecycle.PhaseFailed || events[len(events)-1].Reason != string(api.OperationRestartExhausted) {
			t.Fatalf("events = %+v, want a failed event with the reported reason", events)
		}
		if response := s.allocationResponseLocked(allocation); response.Reason != string(api.OperationRestartExhausted) {
			t.Fatalf("allocation response reason = %q", response.Reason)
		}
	}
}

func TestHeartbeatKeepsExistingFailureReason(t *testing.T) {
	s, node, allocation := heartbeatReasonTestServer(lifecycle.PhaseFailed)
	allocation.Reason, allocation.Message = "retry_limit", "agent unavailable"
	actual := []api.AllocationStatus{{ID: allocation.ID, Generation: 1, Task: "app", Phase: lifecycle.PhaseFailed, Health: lifecycle.HealthUnhealthy, Reason: api.OperationRestartExhausted}}
	if code := postHeartbeat(t, s, node.ID, actual); code != http.StatusNoContent {
		t.Fatalf("heartbeat status = %d, want 204", code)
	}
	if allocation.Reason != "retry_limit" || allocation.Message != "agent unavailable" {
		t.Fatalf("failed allocation reason = %q/%q, want the recorded failure kept", allocation.Reason, allocation.Message)
	}
}

func TestHeartbeatRejectsInvalidFailureReason(t *testing.T) {
	for _, status := range []api.AllocationStatus{
		{Phase: lifecycle.PhaseFailed, Reason: "unknown"},
		{Phase: lifecycle.PhaseRunning, Reason: api.OperationRestartExhausted},
	} {
		s, node, allocation := heartbeatReasonTestServer(lifecycle.PhaseRunning)
		status.ID, status.Generation, status.Task, status.Health = allocation.ID, 1, "app", lifecycle.HealthUnhealthy
		if code := postHeartbeat(t, s, node.ID, []api.AllocationStatus{status}); code != http.StatusInternalServerError {
			t.Fatalf("heartbeat with %s/%q status = %d, want rejection", status.Phase, status.Reason, code)
		}
		if allocation.Phase != lifecycle.PhaseRunning || allocation.Reason != "" {
			t.Fatalf("rejected heartbeat changed allocation to %s/%q", allocation.Phase, allocation.Reason)
		}
	}
}
