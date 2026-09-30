package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/overfold/trellis/internal/api"
	"github.com/overfold/trellis/internal/lifecycle"
	"github.com/overfold/trellis/internal/spec"
)

func newStartingTestServer(t *testing.T, attempt int) (*Server, *testAgent, *Node, *Allocation) {
	t.Helper()
	s, agent := newTestServerWithAgent()
	t.Cleanup(agent.server.Close)
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy, LastHeartbeat: s.now()}
	s.nodes[node.ID] = node
	tasks := []spec.TaskSpec{{Name: "app", Image: "app"}}
	s.jobs[jobKey("default", "web")] = &Job{Spec: canonicalTestSpec(&spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "web", Count: 1, Tasks: tasks}}}), Revision: 1}
	allocation := &Allocation{
		ID: "default-web-1", Namespace: "default", JobName: "web", TaskGroupName: "web",
		Node: node, Tasks: tasks, Generation: 1, JobRevision: 1,
		Phase: lifecycle.PhaseStarting, Health: lifecycle.HealthUnknown,
	}
	allocation.Attempt = attempt
	s.allocations = []*Allocation{allocation}
	return s, agent, node, allocation
}

func startFailure(allocation *Allocation, attempt int) []api.AllocationStatus {
	return []api.AllocationStatus{{ID: allocation.ID, Generation: allocation.Generation, Task: "app", Phase: lifecycle.PhaseStarting, Health: lifecycle.HealthUnknown, StartFailure: &api.StartFailure{Attempt: attempt, Message: "pull image app: registry unavailable"}}}
}

func TestExecuteAcceptedStartWaitsForObservedState(t *testing.T) {
	s, agent, _, allocation := newStartingTestServer(t, 2)
	if err := s.Execute(context.Background(), &Action{Type: ActionStart, Allocation: allocation}); err != nil {
		t.Fatal(err)
	}
	if allocation.Phase != lifecycle.PhaseStarting || allocation.Attempt != 2 {
		t.Fatalf("after accepted start: phase=%s attempt=%d, want starting at attempt 2", allocation.Phase, allocation.Attempt)
	}
	var starts []api.AllocationRequest
	for _, call := range agent.recordedCalls() {
		if call.method == http.MethodPost && call.path == "/v1/allocations" {
			var request api.AllocationRequest
			if err := json.Unmarshal(call.body, &request); err != nil {
				t.Fatal(err)
			}
			starts = append(starts, request)
		}
	}
	if len(starts) != 1 || starts[0].Attempt != 2 {
		t.Fatalf("start requests = %+v, want one at attempt 2", starts)
	}
	// The attempt is not part of the execution.
	allocation.Attempt = 3
	if err := s.Execute(context.Background(), &Action{Type: ActionStart, Allocation: allocation}); err != nil {
		t.Fatal(err)
	}
	var retried api.AllocationRequest
	calls := agent.recordedCalls()
	if err := json.Unmarshal(calls[len(calls)-1].body, &retried); err != nil {
		t.Fatal(err)
	}
	if retried.Attempt != 3 || retried.ExecutionHash != starts[0].ExecutionHash {
		t.Fatalf("retry attempt=%d hash=%s, want attempt 3 with hash %s", retried.Attempt, retried.ExecutionHash, starts[0].ExecutionHash)
	}
}

func TestHeartbeatCountsStartFailureOnce(t *testing.T) {
	s, _, node, allocation := newStartingTestServer(t, 0)
	ctx := context.Background()
	for range 2 {
		if err := s.Heartbeat(ctx, node.ID, startFailure(allocation, 0), "test", nil, nil, nodeResourceObservation{}); err != nil {
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
	if err := s.Heartbeat(ctx, node.ID, startFailure(allocation, 5), "test", nil, nil, nodeResourceObservation{}); err != nil {
		t.Fatal(err)
	}
	if allocation.Attempt != 1 {
		t.Fatalf("unrelated attempt counted: attempt=%d", allocation.Attempt)
	}
	for attempt := 1; attempt < maxExecutionAttempts; attempt++ {
		if err := s.Heartbeat(ctx, node.ID, startFailure(allocation, attempt), "test", nil, nil, nodeResourceObservation{}); err != nil {
			t.Fatal(err)
		}
	}
	if allocation.Phase != lifecycle.PhaseFailed || allocation.Reason != "retry_limit" || allocation.NextRetryAt != nil {
		t.Fatalf("after exhausted attempts: phase=%s reason=%s retry=%v, want failed retry_limit", allocation.Phase, allocation.Reason, allocation.NextRetryAt)
	}
}

func TestHeartbeatRunningResetsStartAttempts(t *testing.T) {
	s, _, node, allocation := newStartingTestServer(t, 3)
	running := []api.AllocationStatus{{ID: allocation.ID, Generation: 1, Task: "app", Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy}}
	if err := s.Heartbeat(context.Background(), node.ID, running, "test", nil, nil, nodeResourceObservation{}); err != nil {
		t.Fatal(err)
	}
	if allocation.Phase != lifecycle.PhaseRunning || allocation.Attempt != 0 || allocation.NextRetryAt != nil {
		t.Fatalf("after running: phase=%s attempt=%d retry=%v", allocation.Phase, allocation.Attempt, allocation.NextRetryAt)
	}
}

func TestHeartbeatRejectsInvalidStartFailure(t *testing.T) {
	s, _, node, allocation := newStartingTestServer(t, 0)
	for name, status := range map[string]api.AllocationStatus{
		"running phase": {ID: allocation.ID, Generation: 1, Task: "app", Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy, StartFailure: &api.StartFailure{}},
		"long message":  {ID: allocation.ID, Generation: 1, Task: "app", Phase: lifecycle.PhaseStarting, Health: lifecycle.HealthUnknown, StartFailure: &api.StartFailure{Message: strings.Repeat("x", api.MaxStartFailureMessageBytes+1)}},
	} {
		if err := s.Heartbeat(context.Background(), node.ID, []api.AllocationStatus{status}, "test", nil, nil, nodeResourceObservation{}); err == nil {
			t.Fatalf("%s: heartbeat accepted invalid start failure", name)
		}
	}
}

func TestHeartbeatTerminalStartFailureFailsAllocation(t *testing.T) {
	s, _, node, allocation := newStartingTestServer(t, 1)
	statuses := startFailure(allocation, 1)
	statuses[0].StartFailure.Code = api.OperationRestartExhausted
	if err := s.Heartbeat(context.Background(), node.ID, statuses, "test", nil, nil, nodeResourceObservation{}); err != nil {
		t.Fatal(err)
	}
	if allocation.Phase != lifecycle.PhaseFailed || allocation.Reason != string(api.OperationRestartExhausted) || allocation.NextRetryAt != nil {
		t.Fatalf("after terminal failure: phase=%s reason=%s retry=%v", allocation.Phase, allocation.Reason, allocation.NextRetryAt)
	}
	statuses[0].StartFailure.Code = api.OperationFailed
	if err := s.Heartbeat(context.Background(), node.ID, statuses, "test", nil, nil, nodeResourceObservation{}); err == nil {
		t.Fatal("heartbeat accepted a start failure with a non-terminal code")
	}
}

func TestStartRequestFailureAfterObservedRunningIsNotCounted(t *testing.T) {
	s, agent, _, allocation := newStartingTestServer(t, 0)
	agent.mu.Lock()
	agent.failRun = true
	agent.mu.Unlock()
	// The heartbeat observing the start complete lands while the start
	// request is in flight.
	allocation.mu.Lock()
	allocation.Phase = lifecycle.PhaseRunning
	allocation.mu.Unlock()
	if err := s.Execute(context.Background(), &Action{Type: ActionStart, Allocation: allocation}); err == nil {
		t.Fatal("start request failure not returned")
	}
	if allocation.Phase != lifecycle.PhaseRunning || allocation.Attempt != 0 || allocation.NextRetryAt != nil {
		t.Fatalf("running allocation after failed request: phase=%s attempt=%d retry=%v", allocation.Phase, allocation.Attempt, allocation.NextRetryAt)
	}
}
