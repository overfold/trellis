package server

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	"github.com/overfold/trellis/orchestrator/internal/spec"
)

func newStartingTestServer(t *testing.T, attempt int) (*Server, *Node, *Allocation) {
	t.Helper()
	s, agent := newTestServerWithAgent()
	t.Cleanup(agent.server.Close)
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy}
	addTestNode(s, node, s.now())
	tasks := []spec.TaskSpec{{Name: "app", Image: "app"}}
	s.jobs[jobKey("default", "web")] = &Job{Spec: canonicalTestSpec(&spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "web", Count: 1, Tasks: tasks}}}), Revision: 1}
	allocation := &Allocation{
		ID: "default-web-1", Namespace: "default", JobName: "web", TaskGroupName: "web",
		Node: node, Tasks: tasks, Generation: 1, JobRevision: 1,
		Phase: lifecycle.PhaseStarting, Health: lifecycle.HealthUnknown,
	}
	allocation.Attempt = attempt
	s.allocations = []*Allocation{allocation}
	return s, node, allocation
}

func startFailure(allocation *Allocation, attempt int) []nodeapi.AllocationStatus {
	return []nodeapi.AllocationStatus{{ID: allocation.ID, Generation: allocation.Generation, Task: "app", Phase: lifecycle.PhaseStarting, Health: lifecycle.HealthUnknown, StartFailure: &nodeapi.StartFailure{Attempt: attempt, Message: "pull image app: registry unavailable"}}}
}

func TestPublishedStartWaitsForObservedState(t *testing.T) {
	s, node, allocation := newStartingTestServer(t, 2)
	if err := s.Execute(t.Context(), &Action{Type: ActionStart, Allocation: allocation}); err != nil {
		t.Fatal(err)
	}
	first := committedAllocationIntent(t, s, node.ID, allocation.ID, allocation.Generation)
	if first.Mode != nodeapi.ExecutionRun || first.Request.Attempt != 2 || allocation.Phase != lifecycle.PhaseStarting {
		t.Fatalf("published start=%+v", first)
	}
	allocation.Attempt = 3
	if err := s.Execute(t.Context(), &Action{Type: ActionStart, Allocation: allocation}); err != nil {
		t.Fatal(err)
	}
	retry := committedAllocationIntent(t, s, node.ID, allocation.ID, allocation.Generation)
	if retry.Request.Attempt != 3 || retry.Request.ExecutionHash != first.Request.ExecutionHash {
		t.Fatal("retry changed frozen execution identity")
	}
}

func TestHeartbeatCountsStartFailureOnce(t *testing.T) {
	s, node, allocation := newStartingTestServer(t, 0)
	ctx := context.Background()
	for range 2 {
		if err := heartbeatAndApply(t, s, node.ID, startFailure(allocation, 0), "test", nodeResourceObservation{}); err != nil {
			t.Fatal(err)
		}
	}
	if allocation.Phase != lifecycle.PhaseStarting || allocation.Attempt != 1 || allocation.NextRetryAt == nil || allocation.Reason != "agent_start_failed" || !strings.Contains(allocation.Message, "registry unavailable") {
		t.Fatalf("after failure: phase=%s attempt=%d retry=%v reason=%s message=%q", allocation.Phase, allocation.Attempt, allocation.NextRetryAt, allocation.Reason, allocation.Message)
	}
	persisted, err := s.state.ListAllocations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := persisted[allocation.ID]; got == nil || got.Attempt != 1 || got.NextRetryAt == nil {
		t.Fatalf("persisted failure count = %+v", got)
	}
	// A report for an attempt the control plane already counted, or has not
	// sent, is ignored.
	if err := heartbeatAndApply(t, s, node.ID, startFailure(allocation, 5), "test", nodeResourceObservation{}); err != nil {
		t.Fatal(err)
	}
	if allocation.Attempt != 1 {
		t.Fatalf("unrelated attempt counted: attempt=%d", allocation.Attempt)
	}
	for attempt := 1; attempt < maxExecutionAttempts; attempt++ {
		if err := heartbeatAndApply(t, s, node.ID, startFailure(allocation, attempt), "test", nodeResourceObservation{}); err != nil {
			t.Fatal(err)
		}
	}
	if allocation.Phase != lifecycle.PhaseFailed || allocation.Reason != "retry_limit" || allocation.NextRetryAt != nil {
		t.Fatalf("after exhausted attempts: phase=%s reason=%s retry=%v, want failed retry_limit", allocation.Phase, allocation.Reason, allocation.NextRetryAt)
	}
}

func TestHeartbeatRunningPreservesDurableStartAttempt(t *testing.T) {
	s, node, allocation := newStartingTestServer(t, 3)
	running := []nodeapi.AllocationStatus{{ID: allocation.ID, Generation: 1, Task: "app", Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy}}
	if err := heartbeatAndApply(t, s, node.ID, running, "test", nodeResourceObservation{}); err != nil {
		t.Fatal(err)
	}
	if allocation.Phase != lifecycle.PhaseRunning || allocation.Attempt != 3 || allocation.NextRetryAt != nil {
		t.Fatalf("after running: phase=%s attempt=%d retry=%v", allocation.Phase, allocation.Attempt, allocation.NextRetryAt)
	}
}

func TestHeartbeatRejectsInvalidStartFailure(t *testing.T) {
	s, node, allocation := newStartingTestServer(t, 0)
	for name, status := range map[string]nodeapi.AllocationStatus{
		"running phase": {ID: allocation.ID, Generation: 1, Task: "app", Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy, StartFailure: &nodeapi.StartFailure{}},
		"long message":  {ID: allocation.ID, Generation: 1, Task: "app", Phase: lifecycle.PhaseStarting, Health: lifecycle.HealthUnknown, StartFailure: &nodeapi.StartFailure{Message: strings.Repeat("x", nodeapi.MaxStartFailureMessageBytes+1)}},
	} {
		if err := heartbeatAndApply(t, s, node.ID, []nodeapi.AllocationStatus{status}, "test", nodeResourceObservation{}); err == nil {
			t.Fatalf("%s: heartbeat accepted invalid start failure", name)
		}
	}
}

func TestHeartbeatTerminalStartFailureFailsAllocation(t *testing.T) {
	s, node, allocation := newStartingTestServer(t, 1)
	statuses := startFailure(allocation, 1)
	statuses[0].StartFailure.Code = nodeapi.OperationRestartExhausted
	if err := heartbeatAndApply(t, s, node.ID, statuses, "test", nodeResourceObservation{}); err != nil {
		t.Fatal(err)
	}
	if allocation.Phase != lifecycle.PhaseFailed || allocation.Reason != string(nodeapi.OperationRestartExhausted) || allocation.NextRetryAt != nil {
		t.Fatalf("after terminal failure: phase=%s reason=%s retry=%v", allocation.Phase, allocation.Reason, allocation.NextRetryAt)
	}
	statuses[0].StartFailure.Code = nodeapi.OperationFailed
	if err := heartbeatAndApply(t, s, node.ID, statuses, "test", nodeResourceObservation{}); err == nil {
		t.Fatal("heartbeat accepted a start failure with a non-terminal code")
	}
}

func TestDisconnectedAgentDoesNotConsumeExecutionBudget(t *testing.T) {
	s, node, allocation := newStartingTestServer(t, 2)
	node.Host, node.Port = "192.0.2.1", 1
	for range 4 {
		if err := s.Execute(t.Context(), &Action{Type: ActionStart, Allocation: allocation}); err != nil {
			t.Fatal(err)
		}
		if allocation.Attempt != 2 || allocation.NextRetryAt != nil || allocation.Phase != lifecycle.PhaseStarting {
			t.Fatal("publication consumed execution budget")
		}
	}
	if committedAllocationIntent(t, s, node.ID, allocation.ID, allocation.Generation).Mode != nodeapi.ExecutionRun {
		t.Fatal("disconnection revoked explicit run authority")
	}
}
