package server

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
)

func TestRetainedLogInventoryCleanupAfterPruning(t *testing.T) {
	s, node, allocation := executionFixture(t)
	allocation.Phase, allocation.Health = lifecycle.PhaseFailed, lifecycle.HealthUnhealthy
	s.leaderSince = s.now().Add(-leaderRecoveryGrace)
	if err := heartbeatAndApply(t, s, node.ID, []nodeapi.AllocationStatus{{ID: allocation.ID, Generation: 1, Task: "app", Phase: lifecycle.PhaseFailed, Health: lifecycle.HealthUnhealthy}}, "test", nodeResourceObservation{}); err != nil {
		t.Fatal(err)
	}
	s.Reconcile(t.Context())
	first := committedAllocationIntent(t, s, node.ID, allocation.ID, 1)
	if first.Mode != nodeapi.ExecutionCleanup || !first.RetainLogs {
		t.Fatal("terminal cleanup discarded retained history logs")
	}
	// Logs alone must not release reservations or rewrite the terminal result.
	status := nodeapi.AllocationStatus{ID: allocation.ID, Generation: 1, Task: "app", Phase: lifecycle.PhaseStopped, Health: lifecycle.HealthUnknown, RetainedLogs: true}
	if err := heartbeatAndApply(t, s, node.ID, []nodeapi.AllocationStatus{status}, "test", nodeResourceObservation{}); err != nil {
		t.Fatal(err)
	}
	s.Reconcile(t.Context())
	if allocation.CleanupComplete || allocation.Phase != lifecycle.PhaseFailed || !committedAllocationIntent(t, s, node.ID, allocation.ID, 1).RetainLogs {
		t.Fatal("log inventory changed terminal execution")
	}
	acknowledgeTestCleanup(t, s, allocation)
	// Pruning commits irreversible log removal, rather than treating resource
	// release as acknowledgement that the retained history's logs are gone.
	if err := s.state.CommitReconciliation(t.Context(), &ReconciliationCommit{DeleteAllocations: []string{allocation.ID}}); err != nil {
		t.Fatal(err)
	}
	s.allocations = nil
	s.rebuildAllocationNodeIndexLocked()
	s.Reconcile(t.Context())
	if committedAllocationIntent(t, s, node.ID, allocation.ID, 1).RetainLogs {
		t.Fatal("pruning did not publish irreversible log removal")
	}
	receipt := nodeapi.AllocationStatus{ID: allocation.ID, Generation: 1, CleanupComplete: true, Phase: lifecycle.PhaseStopped, Health: lifecycle.HealthUnknown}
	// Receipt and task-log entries aggregate in either arrival order.
	for _, report := range [][]nodeapi.AllocationStatus{{status, receipt}, {receipt, status}} {
		if err := heartbeatAndApply(t, s, node.ID, report, "test", nodeResourceObservation{}); err != nil {
			t.Fatal(err)
		}
		s.Reconcile(t.Context())
		if committedAllocationIntent(t, s, node.ID, allocation.ID, 1).RetainLogs {
			t.Fatal("cleanup receipt hid retained logs")
		}
	}
	if err := heartbeatAndApply(t, s, node.ID, []nodeapi.AllocationStatus{receipt}, "test", nodeResourceObservation{}); err != nil {
		t.Fatal(err)
	}
	s.Reconcile(t.Context())
	doc, err := s.ExecutionIntent(t.Context(), node.ID, 0)
	if err != nil || !doc.CleanupAbsent || len(doc.Allocations) != 0 {
		t.Fatalf("pruned log authority=%+v %v", doc, err)
	}
	decisions, err := s.state.executionDecisions(t.Context(), node.ID)
	if err != nil || len(decisions) != 0 {
		t.Fatalf("retired decisions = %+v, %v", decisions, err)
	}
	// A delayed duplicate receipt must not recreate the retired proof.
	if err := heartbeatAndApply(t, s, node.ID, []nodeapi.AllocationStatus{receipt}, "test", nodeResourceObservation{}); err != nil {
		t.Fatal(err)
	}
	complete, err := s.state.executionCleaned(t.Context(), node.ID, allocation.ID, 1)
	if err != nil || complete {
		t.Fatalf("retired proof recreated = %t, %v", complete, err)
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
