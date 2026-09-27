package agent

import (
	"context"
	"errors"
	"testing"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/runtime"
	"github.com/clofour/trellis/internal/spec"
	"github.com/clofour/trellis/internal/storage"
)

type listingRecoveryRuntime struct {
	*reconcilerRuntime
	listErr     error
	containers  []runtime.ContainerInfo
	stopErr     error
	stopCount   int
	removeCount int
}

func (r *listingRecoveryRuntime) ListManaged(context.Context, string) ([]runtime.ContainerInfo, error) {
	if r.listErr != nil {
		return nil, r.listErr
	}
	return r.containers, nil
}

func (r *listingRecoveryRuntime) Stop(context.Context, string) error {
	r.stopCount++
	return r.stopErr
}

func (r *listingRecoveryRuntime) Remove(context.Context, string) error {
	r.removeCount++
	return nil
}

func recoveryTestAllocation(hostPort int) *Allocation {
	return &Allocation{
		ID: "task", AllocationID: "allocation", ContainerID: "task",
		Generation: 1, JobRevision: 1, ExecutionHash: "hash",
		Namespace: "default", JobName: "job", GroupName: "group", TaskName: "task",
		Spec:   &spec.TaskSpec{Name: "task", Image: "image"},
		Ports:  []*runtime.Port{{HostPort: hostPort, ContainerPort: 80}},
		Status: "running", Health: "healthy",
	}
}

func recoveryTestLabels(allocation *Allocation) map[string]string {
	return map[string]string{
		"trellis.cluster":               "test",
		"trellis.allocation-id":         allocation.AllocationID,
		"trellis.allocation-generation": "1",
		"trellis.job-revision":          "1",
		"trellis.execution-hash":        allocation.ExecutionHash,
		"trellis.namespace":             allocation.Namespace,
		"trellis.job":                   allocation.JobName,
		"trellis.task-group":            allocation.GroupName,
		"trellis.task":                  allocation.TaskName,
	}
}

func newRecoveryTestAgent(t *testing.T, rt runtime.ContainerRuntime, records ...*Allocation) (*Agent, *storage.LocalStorage) {
	t.Helper()
	local := storage.NewLocalStorage(t.TempDir())
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	writer := newOperationTestAgent(t, rt)
	writer.ConfigureDurability(local, "test")
	for _, record := range records {
		if err := writer.persistAllocation(record); err != nil {
			t.Fatal(err)
		}
	}
	agent := newOperationTestAgent(t, rt)
	agent.ConfigureDurability(local, "test")
	return agent, local
}

func assertUnobservedAllocation(t *testing.T, agent *Agent, id, status string, hostPort int) {
	t.Helper()
	allocation := agent.allocations[id]
	if allocation == nil {
		t.Fatalf("allocation %s was not recovered", id)
	}
	if !allocation.unobserved || allocation.Status != status || allocation.Health != "unknown" {
		t.Fatalf("recovered allocation = %+v, want unobserved %s with unknown health", allocation, status)
	}
	if !portClaimed(agent, hostPort) {
		t.Fatalf("port %d was not adopted", hostPort)
	}
	if state := agent.reconciler.states[id]; state == nil || !state.stopping {
		t.Fatal("unobserved allocation is not restart-suppressed")
	}
	reported := false
	for _, status := range agent.allocationStatuses() {
		reported = reported || status.ID == allocation.AllocationID
	}
	if !reported {
		t.Fatal("unobserved allocation is not reported in heartbeats")
	}
}

func TestRecoverListingFailurePreservesStoredAllocations(t *testing.T) {
	rt := &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{}, listErr: errors.New("containerd unavailable")}
	record := recoveryTestAllocation(18080)
	agent, local := newRecoveryTestAgent(t, rt, record)

	if err := agent.recover(context.Background()); err == nil {
		t.Fatal("recover succeeded despite listing failure")
	}
	assertUnobservedAllocation(t, agent, "task", "running", 18080)
	var persisted Allocation
	if err := local.Get(allocationRecordKey("task"), &persisted); err != nil {
		t.Fatalf("allocation record after failed listing: %v", err)
	}
	if !agent.retryRecovery(context.Background()) {
		t.Fatal("retry reported no pending work while listing still fails")
	}

	rt.listErr = nil
	rt.status = runtime.StatusRunning
	rt.containers = []runtime.ContainerInfo{{ID: "task", Status: runtime.StatusRunning, Labels: recoveryTestLabels(record)}}
	if agent.retryRecovery(context.Background()) {
		t.Fatal("retry left recovery work pending after a successful observation")
	}
	recovered := agent.allocations["task"]
	if recovered == nil || recovered.unobserved || recovered.Status != "running" {
		t.Fatalf("allocation after retry = %+v, want observed running", recovered)
	}
	if state := agent.reconciler.states["task"]; state == nil || state.stopping {
		t.Fatal("observed running allocation did not regain local restarts")
	}
	if !portClaimed(agent, 18080) {
		t.Fatal("observed running allocation lost its port claim")
	}
}

func TestRecoverListingFailureRetryCleansUpConfirmedMissingContainer(t *testing.T) {
	rt := &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{}, listErr: errors.New("containerd unavailable")}
	agent, local := newRecoveryTestAgent(t, rt, recoveryTestAllocation(18081))
	if err := agent.recover(context.Background()); err == nil {
		t.Fatal("recover succeeded despite listing failure")
	}

	rt.listErr = nil
	if agent.retryRecovery(context.Background()) {
		t.Fatal("retry left recovery work pending")
	}
	if agent.allocations["task"] != nil {
		t.Fatal("missing allocation remains in memory after successful listing")
	}
	if portClaimed(agent, 18081) {
		t.Fatal("missing allocation kept its port claim after cleanup")
	}
	var persisted Allocation
	if err := local.Get(allocationRecordKey("task"), &persisted); err == nil {
		t.Fatal("missing allocation record survived cleanup")
	}
}

func TestRecoverListingFailureRetryAdoptsUnrecordedContainer(t *testing.T) {
	rt := &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{}, listErr: errors.New("containerd unavailable")}
	agent, _ := newRecoveryTestAgent(t, rt)
	if err := agent.recover(context.Background()); err == nil {
		t.Fatal("recover succeeded despite listing failure")
	}
	if !agent.retryRecovery(context.Background()) {
		t.Fatal("retry reported no pending work before any listing succeeded")
	}

	rt.listErr = nil
	labels := recoveryTestLabels(recoveryTestAllocation(0))
	rt.containers = []runtime.ContainerInfo{{ID: "task", Status: runtime.StatusRunning, Labels: labels}}
	if agent.retryRecovery(context.Background()) {
		t.Fatal("retry left recovery work pending")
	}
	if got := agent.allocations["task"]; got == nil || got.Status != "running" || got.AllocationID != "allocation" {
		t.Fatalf("adopted allocation = %+v, want running allocation", got)
	}
	if agent.retryRecovery(context.Background()) {
		t.Fatal("completed recovery reported pending work")
	}
}

func TestRecoverUnknownStatusContainerIsPreservedAndStopFailsHonestly(t *testing.T) {
	stopErr := errors.New("task status unavailable")
	rt := &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{status: runtime.StatusUnknown}, stopErr: stopErr}
	record := recoveryTestAllocation(18082)
	rt.containers = []runtime.ContainerInfo{{ID: "task", Status: runtime.StatusUnknown, Labels: recoveryTestLabels(record)}}
	agent, local := newRecoveryTestAgent(t, rt, record)

	if err := agent.recover(context.Background()); err != nil {
		t.Fatalf("recover: %v", err)
	}
	assertUnobservedAllocation(t, agent, "task", "running", 18082)
	if err := agent.reconciler.Reconcile(context.Background(), "task"); err != nil {
		t.Fatalf("reconcile unknown allocation: %v", err)
	}
	if rt.restartCount != 0 {
		t.Fatalf("unknown allocation restarted %d times", rt.restartCount)
	}

	stop := &api.StopAllocationRequest{AllocationID: "allocation", Generation: 1}
	if err := agent.StopGroup(context.Background(), stop); !errors.Is(err, stopErr) {
		t.Fatalf("stop error = %v, want %v", err, stopErr)
	}
	if rt.stopCount != 1 {
		t.Fatalf("runtime stop calls = %d, want 1", rt.stopCount)
	}
	if agent.allocations["task"] == nil || !portClaimed(agent, 18082) {
		t.Fatal("failed stop released the unknown allocation")
	}

	rt.stopErr = nil
	if err := agent.StopGroup(context.Background(), stop); err != nil {
		t.Fatalf("stop retry: %v", err)
	}
	if agent.allocations["task"] != nil || portClaimed(agent, 18082) {
		t.Fatal("successful stop left the allocation or its port claim")
	}
	var persisted Allocation
	if err := local.Get(allocationRecordKey("task"), &persisted); err == nil {
		t.Fatal("stopped allocation record survived")
	}
}

func TestRecoverUnreadableContainerKeepsRecordAndResources(t *testing.T) {
	rt := &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{}}
	record := recoveryTestAllocation(18083)
	record.Status = "stopping"
	// An unreadable container is listed without labels; it must not be
	// confused with an absent container.
	rt.containers = []runtime.ContainerInfo{
		{ID: "task", Status: runtime.StatusUnknown},
		{ID: "foreign", Status: runtime.StatusUnknown},
	}
	agent, _ := newRecoveryTestAgent(t, rt, record)

	if err := agent.recover(context.Background()); err != nil {
		t.Fatalf("recover: %v", err)
	}
	assertUnobservedAllocation(t, agent, "task", "stopping", 18083)
	if agent.allocations["foreign"] != nil {
		t.Fatal("recovery adopted an unidentifiable container")
	}

	// Once readable, a stopping record stays stopping and restart-suppressed.
	rt.containers = []runtime.ContainerInfo{{ID: "task", Status: runtime.StatusRunning, Labels: recoveryTestLabels(record)}}
	if agent.retryRecovery(context.Background()) {
		t.Fatal("retry left recovery work pending")
	}
	recovered := agent.allocations["task"]
	if recovered == nil || recovered.unobserved || recovered.Status != "stopping" {
		t.Fatalf("allocation after retry = %+v, want observed stopping", recovered)
	}
	if state := agent.reconciler.states["task"]; state == nil || !state.stopping {
		t.Fatal("recovered stopping allocation regained local restarts")
	}
}

func portClaimed(agent *Agent, hostPort int) bool {
	agent.ports.mu.Lock()
	defer agent.ports.mu.Unlock()
	return agent.ports.claims[hostPort] != nil
}
