package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
)

func TestRetainedLogInventoryCleanupAfterPruning(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy}
	addTestNode(s, node, s.now())
	s.leaderSince = s.now().Add(-leaderRecoveryGrace - time.Second)
	allocation := failedAllocation("failed", 1, s.now())
	allocation.Node = node
	s.allocations = []*Allocation{allocation}
	ctx := context.Background()
	if err := s.state.PutAllocation(ctx, allocation); err != nil {
		t.Fatal(err)
	}
	status := nodeapi.AllocationStatus{ID: "failed", Generation: 1, Task: "app", Phase: lifecycle.PhaseFailed, Health: lifecycle.HealthUnhealthy}
	if err := heartbeatAndApply(t, s, node.ID, []nodeapi.AllocationStatus{status}, "test", nodeResourceObservation{}); err != nil {
		t.Fatal(err)
	}
	s.Reconcile(ctx)
	calls := agent.recordedCalls()
	if len(calls) != 1 {
		t.Fatalf("cleanup calls=%v", calls)
	}
	var stop nodeapi.StopAllocationRequest
	if err := json.Unmarshal(calls[0].body, &stop); err != nil {
		t.Fatal(err)
	}
	if !stop.RetainLogs {
		t.Fatal("failed allocation cleanup discarded retained logs")
	}

	status.RetainedLogs, status.Phase, status.Health = true, lifecycle.PhaseStopped, lifecycle.HealthUnknown
	if err := heartbeatAndApply(t, s, node.ID, []nodeapi.AllocationStatus{status}, "test", nodeResourceObservation{}); err != nil {
		t.Fatal(err)
	}
	s.Reconcile(ctx)
	if len(agent.recordedCalls()) != 1 {
		t.Fatal("retained logs removed while history still exists")
	}
	if allocation.Phase != lifecycle.PhaseFailed || allocation.Health != lifecycle.HealthUnhealthy {
		t.Fatal("log inventory changed terminal lifecycle/health")
	}

	// History can disappear while the node is unavailable. Its later complete
	// heartbeat must still cause retryable cleanup without a durable tombstone.
	s.allocations = nil
	if err := heartbeatAndApply(t, s, node.ID, []nodeapi.AllocationStatus{status}, "test", nodeResourceObservation{}); err != nil {
		t.Fatal(err)
	}
	s.Reconcile(ctx)
	calls = agent.recordedCalls()
	if len(calls) != 2 {
		t.Fatalf("pruned log cleanup calls=%v", calls)
	}
	stop = nodeapi.StopAllocationRequest{}
	if err := json.Unmarshal(calls[1].body, &stop); err != nil {
		t.Fatal(err)
	}
	if stop.RetainLogs || stop.Generation != 1 {
		t.Fatalf("pruned cleanup=%+v", stop)
	}
	// No acknowledgement yet: the observed inventory drives another retry.
	s.Reconcile(ctx)
	if len(agent.recordedCalls()) != 3 {
		t.Fatal("pruned cleanup was not retried")
	}
}

func TestLogPruningUsesTerminalHistoryRetention(t *testing.T) {
	now := time.Now().UTC()
	node := &Node{ID: uuid.New(), Status: NodeStatusUnhealthy}
	older := failedAllocation("old", 1, now.Add(-time.Minute))
	newer := failedAllocation("new", 1, now)
	older.Node, newer.Node = node, node
	node.observedAllocations = []observedAllocation{
		{ID: "old", Generation: 1, RetainedLogs: true, Phase: lifecycle.PhaseStopped},
		{ID: "new", Generation: 1, RetainedLogs: true, Phase: lifecycle.PhaseStopped},
	}
	policy := DefaultReplacementPolicy()
	policy.RetainTerminal = 1
	in := &reconcilePlanInput{Now: now, LeaderSince: now.Add(-time.Hour), Policy: policy, Nodes: map[uuid.UUID]*Node{node.ID: node}, Allocations: []*Allocation{older, newer}, AllocationLossTimeout: time.Minute}
	plan, err := planReconciliation(in)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Pruned) != 1 || plan.Pruned[0].ID != "old" || len(plan.Actions) != 0 {
		t.Fatalf("offline pruning=%+v actions=%+v", plan.Pruned, plan.Actions)
	}
	// The node returns after the pruning commit; only the absent history's
	// logs should be collected, even though both generations are advertised.
	node.Status = NodeStatusHealthy
	in.Heartbeats = map[uuid.UUID]time.Time{node.ID: now}
	in.Allocations = []*Allocation{newer}
	plan, err = planReconciliation(in)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Actions) != 1 || plan.Actions[0].ID != "old" || plan.Actions[0].RetainLogs {
		t.Fatalf("catch-up cleanup=%+v", plan.Actions)
	}
}
