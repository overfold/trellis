package agent

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/runtime"
	"github.com/clofour/trellis/internal/spec"
	"github.com/clofour/trellis/internal/storage"
	"github.com/containerd/errdefs"
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
	if !allocation.unobserved || allocation.Status != status {
		t.Fatalf("recovered allocation = %+v, want unobserved %s", allocation, status)
	}
	if !portClaimed(agent, hostPort) {
		t.Fatalf("port %d was not adopted", hostPort)
	}
	if state := agent.reconciler.states[id]; state == nil || !state.stopping {
		t.Fatal("unobserved allocation is not restart-suppressed")
	}
	reported := false
	for _, reportedStatus := range agent.allocationStatuses() {
		if reportedStatus.ID == allocation.AllocationID {
			reported = true
			if reportedStatus.Health != "unknown" {
				t.Fatalf("heartbeat health = %q, want unknown", reportedStatus.Health)
			}
		}
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
	if recovered == nil || recovered.unobserved || recovered.Status != "running" || recovered.Health != "healthy" {
		t.Fatalf("allocation after retry = %+v, want observed running with its recorded health", recovered)
	}
	if err := local.Get(allocationRecordKey("task"), &persisted); err != nil || persisted.Health != "healthy" {
		t.Fatalf("persisted allocation = %+v (%v), want recorded health kept", persisted, err)
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
	rt.status = runtime.StatusRunning
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

	request := operationTestRequest()
	request.AllocationID, request.Generation, request.JobRevision, request.ExecutionHash = "allocation", 1, 1, "hash"
	request.Tasks = []spec.TaskSpec{{Name: "task", Image: "image"}}
	if err := agent.RunAllocation(context.Background(), "task", "allocation", 1, 1, "hash", "default", "job", "group", "task", &request.Tasks[0], "", nil, nil, nil, nil); err == nil {
		t.Fatal("start retry acknowledged an allocation whose container state is unknown")
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

type staleListingRuntime struct {
	*listingRecoveryRuntime
	inspectErr error
}

func (r *staleListingRuntime) Inspect(context.Context, string) (*runtime.ContainerInfo, error) {
	return nil, r.inspectErr
}

func TestRecoverRetryDoesNotResurrectContainerRemovedAfterListing(t *testing.T) {
	rt := &staleListingRuntime{
		listingRecoveryRuntime: &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{}, listErr: errors.New("containerd unavailable")},
		inspectErr:             errors.New("container task not found"),
	}
	agent, local := newRecoveryTestAgent(t, rt)
	if err := agent.recover(context.Background()); err == nil {
		t.Fatal("recover succeeded despite listing failure")
	}

	// The listing still names a container that was removed before the
	// allocation lock was taken.
	rt.listErr = nil
	rt.containers = []runtime.ContainerInfo{{ID: "task", Status: runtime.StatusRunning, Labels: recoveryTestLabels(recoveryTestAllocation(0))}}
	agent.retryRecovery(context.Background())
	if agent.allocations["task"] != nil {
		t.Fatal("retry adopted a container that could not be re-inspected")
	}
	var persisted Allocation
	if err := local.Get(allocationRecordKey("task"), &persisted); err == nil {
		t.Fatal("retry recorded a container that could not be re-inspected")
	}
}

func TestRecoverRetryKeepsUnknownContainerWithoutRewritingRecord(t *testing.T) {
	rt := &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{}}
	record := recoveryTestAllocation(18084)
	rt.containers = []runtime.ContainerInfo{{ID: "task", Status: runtime.StatusUnknown, Labels: recoveryTestLabels(record)}}
	agent, local := newRecoveryTestAgent(t, rt, record)
	if err := agent.recover(context.Background()); err != nil {
		t.Fatalf("recover: %v", err)
	}
	var persisted Allocation
	if err := local.Get(allocationRecordKey("task"), &persisted); err != nil || persisted.Health != "healthy" {
		t.Fatalf("persisted allocation = %+v (%v), want record unchanged", persisted, err)
	}
	before := agent.allocations["task"]
	if !agent.retryRecovery(context.Background()) {
		t.Fatal("retry reported no pending work for an unknown container")
	}
	if agent.allocations["task"] != before {
		t.Fatal("retry replaced an allocation whose container is still unknown")
	}
}

func TestRunAllocationObservesRecoveredAllocationOnDemand(t *testing.T) {
	rt := &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{}, listErr: errors.New("containerd unavailable")}
	record := recoveryTestAllocation(18085)
	agent, _ := newRecoveryTestAgent(t, rt, record)
	if err := agent.recover(context.Background()); err == nil {
		t.Fatal("recover succeeded despite listing failure")
	}

	rt.status = runtime.StatusRunning
	task := &spec.TaskSpec{Name: "task", Image: "image"}
	if err := agent.RunAllocation(context.Background(), "task", "allocation", 1, 1, "hash", "default", "job", "group", "task", task, "", nil, nil, nil, nil); err != nil {
		t.Fatalf("start retry for observed running allocation: %v", err)
	}
	recovered := agent.allocations["task"]
	if recovered == nil || recovered.unobserved || recovered.Status != "running" || recovered.Health != "healthy" {
		t.Fatalf("allocation after on-demand observation = %+v, want observed running", recovered)
	}
	if rt.stopCount != 0 {
		t.Fatalf("start retry stopped a running allocation %d times", rt.stopCount)
	}
}

func TestRecoverRetryAdoptsOlderGenerationAsStopping(t *testing.T) {
	rt := &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{status: runtime.StatusRunning}, listErr: errors.New("containerd unavailable")}
	agent, _ := newRecoveryTestAgent(t, rt)
	if err := agent.recover(context.Background()); err == nil {
		t.Fatal("recover succeeded despite listing failure")
	}
	newer := recoveryTestAllocation(0)
	newer.ID, newer.ContainerID, newer.Generation = "task-g2", "task-g2", 2
	agent.allocations[newer.ID] = newer

	rt.listErr = nil
	rt.containers = []runtime.ContainerInfo{{ID: "task", Status: runtime.StatusRunning, Labels: recoveryTestLabels(recoveryTestAllocation(0))}}
	agent.retryRecovery(context.Background())
	older := agent.allocations["task"]
	if older == nil || older.Status != "stopping" {
		t.Fatalf("older generation = %+v, want stopping", older)
	}
	if state := agent.reconciler.states["task"]; state == nil || !state.stopping {
		t.Fatal("older generation regained local restarts")
	}
}

func TestRecoverMissingKeepsPortClaimSharedWithLiveAllocation(t *testing.T) {
	rt := &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{}, listErr: errors.New("containerd unavailable")}
	stale := recoveryTestAllocation(18086)
	stale.Status = "stopping"
	live := recoveryTestAllocation(18086)
	live.ID, live.ContainerID, live.AllocationID = "live", "live", "other"
	agent, _ := newRecoveryTestAgent(t, rt, stale, live)
	if err := agent.recover(context.Background()); err == nil {
		t.Fatal("recover succeeded despite listing failure")
	}

	rt.listErr = nil
	labels := recoveryTestLabels(live)
	rt.containers = []runtime.ContainerInfo{{ID: "live", Status: runtime.StatusRunning, Labels: labels}}
	agent.retryRecovery(context.Background())
	if agent.allocations["task"] != nil || agent.allocations["live"] == nil {
		t.Fatal("retry did not resolve the stale and live allocations")
	}
	if !portClaimed(agent, 18086) {
		t.Fatal("cleanup of a missing allocation released a live allocation's port")
	}
}

type missingInspectRuntime struct {
	*listingRecoveryRuntime
	created int
}

func (r *missingInspectRuntime) Inspect(context.Context, string) (*runtime.ContainerInfo, error) {
	if r.created > 0 {
		return &runtime.ContainerInfo{Status: runtime.StatusRunning}, nil
	}
	return nil, fmt.Errorf("loading container: %w", errdefs.ErrNotFound)
}

func (r *missingInspectRuntime) Create(_ context.Context, options runtime.CreateOptions) (string, error) {
	r.created++
	return options.ID, nil
}

func TestRunAllocationReplacesRecoveredAllocationConfirmedMissing(t *testing.T) {
	rt := &missingInspectRuntime{listingRecoveryRuntime: &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{}, listErr: errors.New("containerd unavailable")}}
	record := recoveryTestAllocation(18087)
	record.Status = "starting"
	agent, _ := newRecoveryTestAgent(t, rt, record)
	if err := agent.recover(context.Background()); err == nil {
		t.Fatal("recover succeeded despite listing failure")
	}

	rt.listErr = nil
	task := &spec.TaskSpec{Name: "task", Image: "image"}
	if err := agent.RunAllocation(context.Background(), "task", "allocation", 1, 1, "hash", "default", "job", "group", "task", task, "", nil, nil, nil, nil); err != nil {
		t.Fatalf("start retry after confirmed missing container: %v", err)
	}
	if rt.created != 1 || rt.stopCount != 0 {
		t.Fatalf("create=%d stop=%d, want 1/0", rt.created, rt.stopCount)
	}
	if got := agent.allocations["task"]; got == nil || got.unobserved || got.Status != "running" {
		t.Fatalf("allocation after start retry = %+v, want running", got)
	}
}

func TestRecoverRetryKeepsListingUntilUnrecordedContainerIsInspected(t *testing.T) {
	rt := &staleListingRuntime{
		listingRecoveryRuntime: &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{}, listErr: errors.New("containerd unavailable")},
		inspectErr:             errors.New("shim unresponsive"),
	}
	agent, _ := newRecoveryTestAgent(t, rt)
	if err := agent.recover(context.Background()); err == nil {
		t.Fatal("recover succeeded despite listing failure")
	}
	rt.listErr = nil
	rt.containers = []runtime.ContainerInfo{{ID: "task", Status: runtime.StatusRunning, Labels: recoveryTestLabels(recoveryTestAllocation(0))}}
	if !agent.retryRecovery(context.Background()) {
		t.Fatal("retry stopped listing while an unrecorded container was uninspected")
	}
	rt.inspectErr = errdefs.ErrNotFound
	if agent.retryRecovery(context.Background()) {
		t.Fatal("retry kept listing after the unrecorded container was confirmed gone")
	}
}
