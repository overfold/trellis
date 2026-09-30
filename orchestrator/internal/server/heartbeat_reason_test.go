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
	"github.com/overfold/trellis/orchestrator/internal/catalog"
	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	"github.com/overfold/trellis/orchestrator/internal/spec"
)

func heartbeatReasonTestServer(phase lifecycle.Phase) (*Server, *Node, *Allocation) {
	node := &Node{ID: uuid.MustParse("99999999-9999-9999-9999-999999999999"), Host: "node-a", Status: NodeStatusHealthy}
	allocation := &Allocation{
		ID: "demo-web-1", Node: node, Generation: 1,
		Tasks: []spec.TaskSpec{{Name: "app"}, {Name: "sidecar"}},
		Phase: phase, Health: lifecycle.HealthHealthy,
	}
	s := &Server{
		now:   time.Now,
		state: NewStateController(memoryStore{}, "test"),
		nodes: map[uuid.UUID]*Node{node.ID: node}, allocations: []*Allocation{allocation},
		catalog: catalog.New(),
	}
	return s, node, allocation
}

func postHeartbeat(t *testing.T, s *Server, nodeID uuid.UUID, allocations []nodeapi.AllocationStatus) int {
	t.Helper()
	body, err := json.Marshal(nodeapi.HeartbeatRequest{NodeID: nodeID, Version: "test", Allocations: allocations})
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
	failed := nodeapi.AllocationStatus{ID: "demo-web-1", Generation: 1, Task: "app", Phase: lifecycle.PhaseFailed, Health: lifecycle.HealthUnhealthy, Reason: nodeapi.OperationRestartExhausted}
	running := nodeapi.AllocationStatus{ID: "demo-web-1", Generation: 1, Task: "sidecar", Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy}
	for _, actual := range [][]nodeapi.AllocationStatus{{failed, running}, {running, failed}} {
		s, node, allocation := heartbeatReasonTestServer(lifecycle.PhaseRunning)
		if code := postHeartbeat(t, s, node.ID, actual); code != http.StatusNoContent {
			t.Fatalf("heartbeat status = %d, want 204", code)
		}
		if allocation.Phase != lifecycle.PhaseFailed || allocation.Reason != string(nodeapi.OperationRestartExhausted) {
			t.Fatalf("allocation = %s reason %q, want failed with %q", allocation.Phase, allocation.Reason, nodeapi.OperationRestartExhausted)
		}
		events := allocation.Events.Entries()
		if len(events) == 0 || events[len(events)-1].Phase != lifecycle.PhaseFailed || events[len(events)-1].Reason != string(nodeapi.OperationRestartExhausted) {
			t.Fatalf("events = %+v, want a failed event with the reported reason", events)
		}
		if response := s.allocationResponseLocked(allocation); response.Reason != string(nodeapi.OperationRestartExhausted) {
			t.Fatalf("allocation response reason = %q", response.Reason)
		}
	}
}

func TestHeartbeatKeepsExistingFailureReason(t *testing.T) {
	s, node, allocation := heartbeatReasonTestServer(lifecycle.PhaseFailed)
	allocation.Reason, allocation.Message = "retry_limit", "agent unavailable"
	actual := []nodeapi.AllocationStatus{{ID: allocation.ID, Generation: 1, Task: "app", Phase: lifecycle.PhaseFailed, Health: lifecycle.HealthUnhealthy, Reason: nodeapi.OperationRestartExhausted}}
	if code := postHeartbeat(t, s, node.ID, actual); code != http.StatusNoContent {
		t.Fatalf("heartbeat status = %d, want 204", code)
	}
	if allocation.Reason != "retry_limit" || allocation.Message != "agent unavailable" {
		t.Fatalf("failed allocation reason = %q/%q, want the recorded failure kept", allocation.Reason, allocation.Message)
	}
}

func TestHeartbeatRejectsInvalidFailureReason(t *testing.T) {
	for _, status := range []nodeapi.AllocationStatus{
		{Phase: lifecycle.PhaseFailed, Reason: "unknown"},
		{Phase: lifecycle.PhaseRunning, Reason: nodeapi.OperationRestartExhausted},
	} {
		s, node, allocation := heartbeatReasonTestServer(lifecycle.PhaseRunning)
		status.ID, status.Generation, status.Task, status.Health = allocation.ID, 1, "app", lifecycle.HealthUnhealthy
		if code := postHeartbeat(t, s, node.ID, []nodeapi.AllocationStatus{status}); code != http.StatusInternalServerError {
			t.Fatalf("heartbeat with %s/%q status = %d, want rejection", status.Phase, status.Reason, code)
		}
		if allocation.Phase != lifecycle.PhaseRunning || allocation.Reason != "" {
			t.Fatalf("rejected heartbeat changed allocation to %s/%q", allocation.Phase, allocation.Reason)
		}
	}
}

func TestHeartbeatRejectsExcessAllocationReports(t *testing.T) {
	s, node, _ := heartbeatReasonTestServer(lifecycle.PhaseRunning)
	reports := make([]nodeapi.AllocationStatus, maxHeartbeatAllocationStatuses+1)
	if err := s.Heartbeat(context.Background(), node.ID, reports, "test", nil, nil, nodeResourceObservation{}); err == nil {
		t.Fatal("oversized heartbeat succeeded")
	}
}

func TestHeartbeatHandlerRejectsExcessAllocationReports(t *testing.T) {
	s, node, _ := heartbeatReasonTestServer(lifecycle.PhaseRunning)
	body := bytes.NewBufferString(`{"allocations":[`)
	for i := 0; i <= maxHeartbeatAllocationStatuses; i++ {
		if i > 0 {
			body.WriteByte(',')
		}
		body.WriteString(`{}`)
	}
	body.WriteString(`]}`)
	e := echo.New()
	NewHandler(s).Register(e)
	request := httptest.NewRequest(http.MethodPost, "/v1/nodes/"+node.ID.String()+"/heartbeat", body)
	request.Header.Set("Content-Type", "application/json")
	request = request.WithContext(context.WithValue(request.Context(), NodeContextKey, node.ID))
	recorder := httptest.NewRecorder()
	e.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("heartbeat status = %d, want 413", recorder.Code)
	}
}
