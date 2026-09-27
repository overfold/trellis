package agent

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/runtime"
)

// newPausedInjectedRuntime returns an injected runtime holding one running
// container for allocation that was then paused out of band.
func newPausedInjectedRuntime(t *testing.T, allocation *Allocation) *runtime.InjectedRuntime {
	t.Helper()
	dir := t.TempDir()
	rt, err := runtime.NewInjectedRuntime(filepath.Join(dir, "runtime.json"), filepath.Join(dir, "fault.json"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := rt.Create(ctx, runtime.CreateOptions{ID: allocation.ContainerID, Labels: recoveryTestLabels(allocation)}); err != nil {
		t.Fatal(err)
	}
	if err := rt.Start(ctx, allocation.ContainerID); err != nil {
		t.Fatal(err)
	}
	if err := rt.Pause(ctx, allocation.ContainerID); err != nil {
		t.Fatal(err)
	}
	return rt
}

func assertInjectedStatus(t *testing.T, rt *runtime.InjectedRuntime, id string, want runtime.ContainerStatus) {
	t.Helper()
	observed, err := rt.Inspect(context.Background(), id)
	if err != nil {
		t.Fatalf("inspect %s: %v", id, err)
	}
	if observed.Status != want {
		t.Fatalf("container %s status = %q, want %q", id, observed.Status, want)
	}
}

func assertReportedPhaseHealth(t *testing.T, agent *Agent, allocationID, phase, health string) {
	t.Helper()
	for _, reported := range agent.allocationStatuses() {
		if reported.ID == allocationID {
			if string(reported.Phase) != phase || string(reported.Health) != health {
				t.Fatalf("heartbeat reports %s/%s, want %s/%s", reported.Phase, reported.Health, phase, health)
			}
			return
		}
	}
	t.Fatalf("allocation %s is not reported in heartbeats", allocationID)
}

// A paused container exists: recovery must observe it rather than retry the
// listing, keep its resources, and neither restart nor replace it. Its frozen
// processes cannot serve, so it is reported running but unhealthy.
func TestRecoverPausedContainerIsObservedRunningAndUnhealthy(t *testing.T) {
	record := recoveryTestAllocation(18090)
	rt := newPausedInjectedRuntime(t, record)
	agent, local := newRecoveryTestAgent(t, rt, record)

	if err := agent.recover(context.Background()); err != nil {
		t.Fatalf("recover: %v", err)
	}
	recovered := agent.allocations["task"]
	if recovered == nil || recovered.unobserved || recovered.Status != "running" || recovered.Health != "unhealthy" {
		t.Fatalf("recovered allocation = %+v, want observed running and unhealthy", recovered)
	}
	if agent.retryRecovery(context.Background()) {
		t.Fatal("paused container left recovery work pending")
	}
	if !portClaimed(agent, 18090) {
		t.Fatal("paused allocation lost its port claim")
	}
	assertReportedPhaseHealth(t, agent, "allocation", "running", "unhealthy")
	var persisted Allocation
	if err := local.Get(allocationRecordKey("task"), &persisted); err != nil || persisted.Status != "running" || persisted.Health != "unhealthy" {
		t.Fatalf("persisted allocation = %+v (%v), want running and unhealthy", persisted, err)
	}

	// The container is still present, so local reconciliation must not
	// restart it.
	if state := agent.reconciler.states["task"]; state == nil || state.stopping {
		t.Fatal("paused allocation is not tracked for local reconciliation")
	}
	if err := agent.reconciler.Reconcile(context.Background(), "task"); err != nil {
		t.Fatalf("reconcile paused allocation: %v", err)
	}
	assertInjectedStatus(t, rt, "task", runtime.StatusPaused)
	if _, err := agent.selectRunningExecTarget(context.Background(), "allocation", ""); err == nil {
		t.Fatal("exec targeted a paused container")
	}

	// A control-plane stop still reaches the runtime.
	if err := agent.StopGroup(context.Background(), &api.StopAllocationRequest{AllocationID: "allocation", Generation: 1, Epoch: 1}); err != nil {
		t.Fatalf("stop paused allocation: %v", err)
	}
	if agent.allocations["task"] != nil || portClaimed(agent, 18090) {
		t.Fatal("stop left the paused allocation or its port claim")
	}
	if _, err := rt.Inspect(context.Background(), "task"); err == nil {
		t.Fatal("stop left the paused container behind")
	}
}

// The unhealthy observation recorded for a paused task must not outlive the
// pause: once the container runs again, recovery reports it healthy.
func TestRecoverResumedContainerIsHealthyAgain(t *testing.T) {
	record := recoveryTestAllocation(18093)
	rt := newPausedInjectedRuntime(t, record)
	paused, local := newRecoveryTestAgent(t, rt, record)
	if err := paused.recover(context.Background()); err != nil {
		t.Fatalf("recover paused: %v", err)
	}
	var persisted Allocation
	if err := local.Get(allocationRecordKey("task"), &persisted); err != nil || persisted.Health != "unhealthy" {
		t.Fatalf("persisted allocation = %+v (%v), want unhealthy while paused", persisted, err)
	}

	if err := rt.Start(context.Background(), "task"); err != nil {
		t.Fatal(err)
	}
	resumed := newOperationTestAgent(t, rt)
	resumed.ConfigureDurability(local, "test")
	if err := resumed.recover(context.Background()); err != nil {
		t.Fatalf("recover resumed: %v", err)
	}
	recovered := resumed.allocations["task"]
	if recovered == nil || recovered.Status != "running" || recovered.Health != "healthy" {
		t.Fatalf("recovered allocation = %+v, want running and healthy", recovered)
	}
	assertReportedPhaseHealth(t, resumed, "allocation", "running", "healthy")
}

func TestRecoverPausedStoppingContainerStaysRestartSuppressed(t *testing.T) {
	record := recoveryTestAllocation(18091)
	record.Status = "stopping"
	rt := newPausedInjectedRuntime(t, record)
	agent, _ := newRecoveryTestAgent(t, rt, record)

	if err := agent.recover(context.Background()); err != nil {
		t.Fatalf("recover: %v", err)
	}
	recovered := agent.allocations["task"]
	if recovered == nil || recovered.unobserved || recovered.Status != "stopping" {
		t.Fatalf("recovered allocation = %+v, want observed stopping", recovered)
	}
	if state := agent.reconciler.states["task"]; state == nil || !state.stopping {
		t.Fatal("paused stopping allocation is not restart-suppressed")
	}
	assertInjectedStatus(t, rt, "task", runtime.StatusPaused)
}

// After an initial listing failure, a labelled container without a record is
// adopted on retry; a paused one is observed rather than left pending.
func TestRecoverRetryAdoptsUnrecordedPausedContainer(t *testing.T) {
	rt := &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{}, listErr: errors.New("containerd unavailable")}
	agent, _ := newRecoveryTestAgent(t, rt)
	if err := agent.recover(context.Background()); err == nil {
		t.Fatal("recover succeeded despite listing failure")
	}

	rt.listErr = nil
	rt.status = runtime.StatusPaused
	rt.containers = []runtime.ContainerInfo{{ID: "task", Status: runtime.StatusPaused, Labels: recoveryTestLabels(recoveryTestAllocation(0))}}
	if agent.retryRecovery(context.Background()) {
		t.Fatal("adopted paused container left recovery work pending")
	}
	adopted := agent.allocations["task"]
	if adopted == nil || adopted.unobserved || adopted.AllocationID != "allocation" || adopted.Status != "running" || adopted.Health != "unhealthy" {
		t.Fatalf("adopted allocation = %+v, want observed running and unhealthy", adopted)
	}
	if rt.stopCount != 0 || rt.removeCount != 0 || rt.restartCount != 0 {
		t.Fatalf("paused container was acted on: stops=%d removes=%d restarts=%d", rt.stopCount, rt.removeCount, rt.restartCount)
	}
}

// An allocation left unobserved by an unreadable listing is classified once a
// later listing reports its container paused, ending the retry loop.
func TestRecoverRetryObservesPausedContainer(t *testing.T) {
	rt := &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{status: runtime.StatusUnknown}}
	record := recoveryTestAllocation(18092)
	rt.containers = []runtime.ContainerInfo{{ID: "task", Status: runtime.StatusUnknown, Labels: recoveryTestLabels(record)}}
	agent, _ := newRecoveryTestAgent(t, rt, record)
	if err := agent.recover(context.Background()); err != nil {
		t.Fatalf("recover: %v", err)
	}
	if allocation := agent.allocations["task"]; allocation == nil || !allocation.unobserved {
		t.Fatalf("allocation with unknown container state = %+v, want unobserved", allocation)
	}

	rt.status = runtime.StatusPaused
	rt.containers = []runtime.ContainerInfo{{ID: "task", Status: runtime.StatusPaused, Labels: recoveryTestLabels(record)}}
	if agent.retryRecovery(context.Background()) {
		t.Fatal("retry left recovery work pending after observing a paused container")
	}
	recovered := agent.allocations["task"]
	if recovered == nil || recovered.unobserved || recovered.Status != "running" || recovered.Health != "unhealthy" {
		t.Fatalf("allocation after retry = %+v, want observed running and unhealthy", recovered)
	}
	if rt.stopCount != 0 || rt.removeCount != 0 || rt.restartCount != 0 {
		t.Fatalf("paused container was acted on: stops=%d removes=%d restarts=%d", rt.stopCount, rt.removeCount, rt.restartCount)
	}
}
