package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
)

func TestSendHeartbeatCarriesFailureReason(t *testing.T) {
	var raw map[string]json.RawMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
			t.Error(err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	nodeID := uuid.New()
	client := NewServerClient("token", server.URL, nil)
	err := client.SendHeartbeat(context.Background(), nodeID, &Heartbeat{NodeID: nodeID, Allocations: []nodeapi.AllocationStatus{
		{ID: "failed", Generation: 1, Phase: lifecycle.PhaseFailed, Health: lifecycle.HealthUnhealthy, Reason: nodeapi.OperationRestartExhausted},
		{ID: "running", Generation: 1, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy},
	}})
	if err != nil {
		t.Fatal(err)
	}
	var allocations []map[string]any
	if err := json.Unmarshal(raw["allocations"], &allocations); err != nil {
		t.Fatal(err)
	}
	if len(allocations) != 2 || allocations[0]["reason"] != "restart_budget_exhausted" {
		t.Fatalf("allocations = %v, want the failure reason on the wire", allocations)
	}
	if _, ok := allocations[1]["reason"]; ok {
		t.Fatalf("running allocation carries a reason: %v", allocations[1])
	}
}
