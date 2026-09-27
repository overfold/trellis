package agent

import (
	"context"
	"testing"
	"time"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/lifecycle"
	"github.com/clofour/trellis/internal/runtime"
	"github.com/clofour/trellis/internal/spec"
	"github.com/clofour/trellis/internal/storage"
)

func TestAllocationStatusReportsRestartExhaustionReason(t *testing.T) {
	agent := newOperationTestAgent(t, &reconcilerRuntime{status: runtime.StatusStopped})
	agent.allocations["failed"] = &Allocation{ID: "failed", AllocationID: "failed", Generation: 1, TaskName: "task", Status: "failed", Health: "unhealthy", RestartExhausted: true}
	agent.allocations["running"] = &Allocation{ID: "running", AllocationID: "running", Generation: 1, TaskName: "task", Status: "running", Health: "healthy"}
	for _, status := range agent.allocationStatuses() {
		switch status.ID {
		case "failed":
			if status.Phase != lifecycle.PhaseFailed || status.Reason != api.OperationRestartExhausted {
				t.Fatalf("failed status = %+v, want reason %q", status, api.OperationRestartExhausted)
			}
		case "running":
			if status.Reason != "" {
				t.Fatalf("running status reason = %q, want empty", status.Reason)
			}
		}
	}
}

// restartBudgetTestAgent returns an agent whose only record is a task of
// allocation generation 1 that already used restarts of its budget. The record
// is stored but not loaded, so a test chooses how the agent learns of it.
func restartBudgetTestAgent(t *testing.T, rt runtime.ContainerRuntime, status string, attempts int, window time.Time) (*Agent, *storage.LocalStorage, *Allocation) {
	t.Helper()
	record := recoveryTestAllocation(0)
	record.Ports = nil
	record.Status = status
	record.RestartAttempts, record.RestartWindow = attempts, window
	agent, local := newRecoveryTestAgent(t, rt, record)
	agent.reconciler.Subscriber = agent
	return agent, local, record
}

func runRestartBudgetTestTask(agent *Agent, policy *spec.RestartPolicySpec) error {
	task := &spec.TaskSpec{Name: "task", Image: "image"}
	return agent.RunAllocation(context.Background(), "task", "allocation", 1, 1, "hash", "default", "job", "group", "task", task, "", nil, nil, nil, policy, false, 0)
}

func TestStartRetryKeepsRestartAttemptsForSameGeneration(t *testing.T) {
	policy := &spec.RestartPolicySpec{MaxRestarts: 1, Window: time.Hour}
	window := time.Now().Add(-time.Minute).Round(0)
	rt := &reconcilerRuntime{status: runtime.StatusRunning}
	// A start that failed after recording its task leaves it for a retry.
	agent, local, record := restartBudgetTestAgent(t, rt, "starting", 1, window)
	agent.allocations[record.ID] = record

	if err := runRestartBudgetTestTask(agent, policy); err != nil {
		t.Fatalf("start retry: %v", err)
	}
	started := agent.allocations["task"]
	if started == nil || started.RestartAttempts != 1 || !started.RestartWindow.Equal(window) {
		t.Fatalf("restarted record = %+v, want attempts 1 in window %v", started, window)
	}
	var stored Allocation
	if err := local.Get(allocationRecordKey("task"), &stored); err != nil {
		t.Fatal(err)
	}
	if stored.RestartAttempts != 1 || !stored.RestartWindow.Equal(window) {
		t.Fatalf("stored restart state = %d/%v, want 1/%v", stored.RestartAttempts, stored.RestartWindow, window)
	}

	// The retry did not refill the budget: the next stop exhausts it.
	rt.status = runtime.StatusStopped
	if err := agent.reconciler.Reconcile(context.Background(), "task"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if rt.restartCount != 0 {
		t.Fatalf("restart count = %d, want the carried budget to be exhausted", rt.restartCount)
	}
	if got := agent.allocations["task"]; got.Status != "failed" || !got.RestartExhausted {
		t.Fatalf("allocation = %s exhausted=%v, want failed with exhausted budget", got.Status, got.RestartExhausted)
	}
}

func TestRecoveredStoppedTaskKeepsRestartAttemptsOnStartRetry(t *testing.T) {
	policy := &spec.RestartPolicySpec{MaxRestarts: 1, Window: time.Hour}
	window := time.Now().Add(-time.Minute).Round(0)
	rt := &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{status: runtime.StatusStopped}}
	agent, _, _ := restartBudgetTestAgent(t, rt, "running", 1, window)
	rt.containers = []runtime.ContainerInfo{{ID: "task", Status: runtime.StatusStopped, Labels: recoveryTestLabels(recoveryTestAllocation(0))}}
	if err := agent.recover(context.Background()); err != nil {
		t.Fatalf("recover: %v", err)
	}
	if got := agent.allocations["task"]; got == nil || got.Status != "starting" {
		t.Fatalf("recovered allocation = %+v, want starting for the server start retry", got)
	}

	if err := runRestartBudgetTestTask(agent, policy); err != nil {
		t.Fatalf("start retry: %v", err)
	}
	if got := agent.allocations["task"]; got.RestartAttempts != 1 || !got.RestartWindow.Equal(window) {
		t.Fatalf("restarted record = %d/%v, want 1/%v", got.RestartAttempts, got.RestartWindow, window)
	}
	if err := agent.reconciler.Reconcile(context.Background(), "task"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if rt.restartCount != 0 || agent.allocations["task"].Status != "failed" {
		t.Fatalf("restart=%d status=%q, want the carried budget exhausted", rt.restartCount, agent.allocations["task"].Status)
	}
}

func TestStartRetryKeepsFixedRestartWindow(t *testing.T) {
	// A carried window that already elapsed starts a new fixed window on the
	// next restart instead of counting the old attempts against it.
	policy := &spec.RestartPolicySpec{MaxRestarts: 1, Window: time.Minute}
	rt := &reconcilerRuntime{status: runtime.StatusRunning}
	agent, _, record := restartBudgetTestAgent(t, rt, "starting", 1, time.Now().Add(-time.Hour))
	agent.allocations[record.ID] = record
	if err := runRestartBudgetTestTask(agent, policy); err != nil {
		t.Fatalf("start retry: %v", err)
	}
	rt.status = runtime.StatusStopped
	before := time.Now()
	if err := agent.reconciler.Reconcile(context.Background(), "task"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := agent.allocations["task"]
	if rt.restartCount != 1 || got.RestartAttempts != 1 || got.RestartWindow.Before(before) || got.RestartExhausted {
		t.Fatalf("restart=%d record=%d/%v exhausted=%v, want one restart in a new window", rt.restartCount, got.RestartAttempts, got.RestartWindow, got.RestartExhausted)
	}
}

func TestNewGenerationStartsWithFreshRestartBudget(t *testing.T) {
	rt := &reconcilerRuntime{status: runtime.StatusRunning}
	agent := newOperationTestAgent(t, rt)
	request := operationTestRequest()
	request.Tasks = request.Tasks[:1]
	request.Restart = &spec.RestartPolicySpec{MaxRestarts: 1, Window: time.Hour}
	previous := &Allocation{
		ID: "allocation-g1-first", ContainerID: "allocation-g1-first", AllocationID: request.AllocationID, Generation: request.Generation - 1,
		JobRevision: request.JobRevision, ExecutionHash: "previous", TaskName: "first",
		Spec: &spec.TaskSpec{Name: "first", Image: "image"}, Status: "running", Health: "healthy",
		RestartAttempts: 1, RestartWindow: time.Now(),
	}
	agent.allocations[previous.ID] = previous
	if err := agent.RunGroup(context.Background(), request); err != nil {
		t.Fatalf("run group: %v", err)
	}
	current := agent.allocations["allocation-g2-first"]
	if current == nil || current.RestartAttempts != 0 || !current.RestartWindow.IsZero() {
		t.Fatalf("new generation record = %+v, want a fresh restart budget", current)
	}
	if agent.allocations[previous.ID] != nil {
		t.Fatal("older generation was not replaced")
	}
	rt.status = runtime.StatusStopped
	if err := agent.reconciler.Reconcile(context.Background(), current.ID); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if rt.restartCount != 1 {
		t.Fatalf("restart count = %d, want the new generation to use its own budget", rt.restartCount)
	}
}
