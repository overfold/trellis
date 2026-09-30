package agent

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/overfold/trellis/orchestrator/internal/api"
	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	"github.com/overfold/trellis/orchestrator/internal/runtime"
	"github.com/overfold/trellis/orchestrator/internal/spec"
	"github.com/overfold/trellis/orchestrator/internal/storage"
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
	return agent.startTask(context.Background(), &taskStart{ID: "task", AllocationID: "allocation", Generation: 1, JobRevision: 1, ExecutionHash: "hash", Namespace: "default", JobName: "job", GroupName: "group", Spec: task, Restart: policy})
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

func TestRestartPersistenceFailureLeavesRuntimeAndDurableBudgetUnchanged(t *testing.T) {
	policy := &spec.RestartPolicySpec{MaxRestarts: 1, Window: time.Hour}
	rt := &reconcilerRuntime{status: runtime.StatusStopped}
	agent := newOperationTestAgent(t, rt)
	root := t.TempDir()
	local := storage.NewLocalStorage(root)
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	agent.ConfigureDurability(local, "test")
	record := recoveryTestAllocation(0)
	record.Ports = nil
	record.Restart = policy
	agent.allocations[record.ID] = record
	if err := agent.persistAllocation(record); err != nil {
		t.Fatal(err)
	}
	agent.reconciler.Subscriber = agent
	agent.reconciler.TrackRecovered(record.ID, false, policy, 0, time.Time{}, false)

	recordDir := filepath.Join(root, "agent", "allocations")
	savedRecordDir := filepath.Join(root, "saved-allocations")
	if err := os.Rename(recordDir, savedRecordDir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(recordDir, []byte("blocked"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := agent.reconciler.Reconcile(context.Background(), record.ID); err == nil {
		t.Fatal("reconcile succeeded although the restart attempt could not be persisted")
	}
	if rt.restartCount != 0 || record.RestartAttempts != 0 {
		t.Fatalf("restart count = %d, in-memory attempts = %d; want neither changed", rt.restartCount, record.RestartAttempts)
	}

	if err := os.Remove(recordDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(savedRecordDir, recordDir); err != nil {
		t.Fatal(err)
	}
	var stored Allocation
	if err := local.Get(allocationRecordKey(record.ID), &stored); err != nil {
		t.Fatal(err)
	}
	if stored.RestartAttempts != 0 {
		t.Fatalf("durable restart attempts = %d, want unchanged", stored.RestartAttempts)
	}
	if err := agent.reconciler.Reconcile(context.Background(), record.ID); err != nil {
		t.Fatalf("retry after storage recovery: %v", err)
	}
	if rt.restartCount != 1 || record.RestartAttempts != 1 {
		t.Fatalf("restart count = %d, attempts = %d; want one recorded restart", rt.restartCount, record.RestartAttempts)
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
	if err := runGroup(context.Background(), agent, request); err != nil {
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
