package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/containerd/errdefs"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	"github.com/overfold/trellis/orchestrator/internal/runtime"
	"github.com/overfold/trellis/orchestrator/internal/spec"
	"github.com/overfold/trellis/orchestrator/internal/storage"
)

type listingRecoveryRuntime struct {
	*reconcilerRuntime
	listErr     error
	containers  []runtime.ContainerInfo
	stopErr     error
	stopCount   int
	removeCount int
}

func TestAllocationRecoveryJSONPreservesFencingAndResources(t *testing.T) {
	const stored = `{"ID":"task","AllocationID":"alloc","Generation":7,"JobRevision":11,"ExecutionHash":"hash","DrainSequence":13,"Ports":[{"HostPort":18080,"ContainerPort":8080}],"Mounts":[{"HostPath":"/data","ContainerPath":"/app","ReadOnly":true,"Secret":false,"SecretEnv":false}],"NetworkIntent":{"AllocationID":"alloc","Namespace":"team","Network":"team"}}`
	var allocation Allocation
	if err := json.Unmarshal([]byte(stored), &allocation); err != nil {
		t.Fatal(err)
	}
	if allocation.Generation != 7 || allocation.JobRevision != 11 || allocation.ExecutionHash != "hash" || allocation.DrainSequence != 13 {
		t.Fatalf("recovered fencing = %+v", allocation)
	}
	if len(allocation.Ports) != 1 || allocation.Ports[0].HostPort != 18080 || len(allocation.Mounts) != 1 || !allocation.Mounts[0].ReadOnly || allocation.NetworkIntent == nil || allocation.NetworkIntent.Namespace != "team" {
		t.Fatalf("recovered resources = %+v", allocation)
	}
	raw, err := json.Marshal(&allocation)
	if err != nil {
		t.Fatal(err)
	}
	var encoded map[string]json.RawMessage
	if err := json.Unmarshal(raw, &encoded); err != nil {
		t.Fatal(err)
	}
	var original map[string]json.RawMessage
	if err := json.Unmarshal([]byte(stored), &original); err != nil {
		t.Fatal(err)
	}
	for key, want := range original {
		if string(encoded[key]) != string(want) {
			t.Errorf("persisted %s = %s, want %s", key, encoded[key], want)
		}
	}
	// Fields without omitempty must still be present when zero-valued.
	if string(encoded["RestartAttempts"]) != "0" || string(encoded["Network"]) != "null" {
		t.Fatalf("zero-valued recovery fields were omitted: %s", raw)
	}
}

func (r *listingRecoveryRuntime) ListManaged(context.Context, string) ([]runtime.ContainerInfo, error) {
	if r.listErr != nil {
		return nil, r.listErr
	}
	return r.containers, nil
}

func (r *listingRecoveryRuntime) Inspect(ctx context.Context, id string) (*runtime.ContainerInfo, error) {
	for _, container := range r.containers {
		if container.ID == id {
			container.Status = r.status
			return &container, nil
		}
	}
	return r.reconcilerRuntime.Inspect(ctx, id)
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
		"trellis.allocation-generation": strconv.FormatUint(allocation.Generation, 10),
		"trellis.job-revision":          strconv.Itoa(allocation.JobRevision),
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
	if err := local.Put("agent/control-epoch", uint64(0)); err != nil {
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

	if err := agent.recover(context.Background()); err != nil {
		t.Fatalf("recover with failed listing: %v", err)
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
	if err := agent.recover(context.Background()); err != nil {
		t.Fatalf("recover with failed listing: %v", err)
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

func TestRecoverListingFailureRetryRefusesUnrecordedContainer(t *testing.T) {
	for _, status := range []runtime.ContainerStatus{runtime.StatusRunning, runtime.StatusPaused} {
		t.Run(string(status), func(t *testing.T) {
			rt := &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{}, listErr: errors.New("containerd unavailable")}
			agent, local := newRecoveryTestAgent(t, rt)
			if err := agent.recover(context.Background()); err != nil {
				t.Fatalf("recover with failed listing: %v", err)
			}
			if !agent.retryRecovery(context.Background()) {
				t.Fatal("retry reported no pending work before any listing succeeded")
			}

			rt.listErr = nil
			rt.status = status
			labels := recoveryTestLabels(recoveryTestAllocation(0))
			rt.containers = []runtime.ContainerInfo{{ID: "stray", Status: status, Labels: labels}}
			if !agent.retryRecovery(context.Background()) {
				t.Fatal("retry reported recovery complete with an unrecorded container")
			}
			assertRecoveryRefused(t, agent, local, "stray")
			if rt.stopCount != 0 || rt.removeCount != 0 || rt.restartCount != 0 {
				t.Fatalf("unrecorded container was acted on: stops=%d removes=%d restarts=%d", rt.stopCount, rt.removeCount, rt.restartCount)
			}
			// Recovery stays stopped; a later listing does not adopt it either.
			if !agent.retryRecovery(context.Background()) || agent.allocations["stray"] != nil {
				t.Fatal("recovery continued after refusing an unrecorded container")
			}
		})
	}
}

// assertRecoveryRefused checks that recovery failed on an unrecorded container
// without adopting or recording it.
func assertRecoveryRefused(t *testing.T, agent *Agent, local *storage.LocalStorage, containerID string) {
	t.Helper()
	select {
	case err := <-agent.Failed():
		for _, want := range []string{fmt.Sprintf("%q", containerID), "no durable allocation record", "Agent recovery refused"} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("recovery error = %v, want it to contain %s", err, want)
			}
		}
	default:
		t.Fatal("recovery did not fail on an unrecorded container")
	}
	if agent.allocations[containerID] != nil {
		t.Fatalf("unrecorded container was adopted: %+v", agent.allocations[containerID])
	}
	var persisted Allocation
	if err := local.Get(allocationRecordKey(containerID), &persisted); err == nil {
		t.Fatal("unrecorded container was recorded from its labels")
	}
	if !agent.recoveryPending() {
		t.Fatal("refused recovery reported no pending work")
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
	if err := agent.startTask(context.Background(), &taskStart{ID: "task", AllocationID: "allocation", Generation: 1, JobRevision: 1, ExecutionHash: "hash", Namespace: "default", JobName: "job", GroupName: "group", Spec: &request.Tasks[0]}); err == nil {
		t.Fatal("start retry acknowledged an allocation whose container state is unknown")
	}

	stop := &nodeapi.StopAllocationRequest{AllocationID: "allocation", Generation: 1, Epoch: 1}
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
	// An unreadable recorded container is listed without labels; it must not
	// be confused with an absent container.
	rt.containers = []runtime.ContainerInfo{{ID: "task", Status: runtime.StatusUnknown}}
	agent, _ := newRecoveryTestAgent(t, rt, record)

	if err := agent.recover(context.Background()); err != nil {
		t.Fatalf("recover: %v", err)
	}
	assertUnobservedAllocation(t, agent, "task", "stopping", 18083)

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
		inspectErr:             fmt.Errorf("loading container: %w", errdefs.ErrNotFound),
	}
	agent, local := newRecoveryTestAgent(t, rt)
	if err := agent.recover(context.Background()); err != nil {
		t.Fatalf("recover with failed listing: %v", err)
	}

	// The listing still names a container that was removed before the
	// allocation lock was taken.
	rt.listErr = nil
	rt.containers = []runtime.ContainerInfo{{ID: "task", Status: runtime.StatusRunning, Labels: recoveryTestLabels(recoveryTestAllocation(0))}}
	if !agent.retryRecovery(context.Background()) {
		t.Fatal("retry completed listing before a later listing confirmed the container gone")
	}
	select {
	case err := <-agent.Failed():
		t.Fatalf("recovery failed on a container removed after listing: %v", err)
	default:
	}
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
	if err := agent.recover(context.Background()); err != nil {
		t.Fatalf("recover with failed listing: %v", err)
	}

	rt.status = runtime.StatusRunning
	rt.containers = []runtime.ContainerInfo{{ID: "task", Labels: recoveryTestLabels(record)}}
	task := &spec.TaskSpec{Name: "task", Image: "image"}
	if err := agent.startTask(context.Background(), &taskStart{ID: "task", AllocationID: "allocation", Generation: 1, JobRevision: 1, ExecutionHash: "hash", Namespace: "default", JobName: "job", GroupName: "group", Spec: task}); err != nil {
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

func TestStopAllocationKeepsPortClaimSharedWithRetainedAllocation(t *testing.T) {
	rt := &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{}, listErr: errors.New("containerd unavailable")}
	stale := recoveryTestAllocation(18088)
	stale.Status = "stopping"
	live := recoveryTestAllocation(18088)
	live.ID, live.ContainerID, live.AllocationID = "live", "live", "other"
	agent, _ := newRecoveryTestAgent(t, rt, stale, live)
	if err := agent.recover(context.Background()); err != nil {
		t.Fatalf("recover with failed listing: %v", err)
	}
	rt.listErr = nil
	if err := agent.StopGroup(context.Background(), &nodeapi.StopAllocationRequest{AllocationID: "allocation", Generation: 1, Epoch: 1}); err != nil {
		t.Fatalf("stop stale allocation: %v", err)
	}
	if agent.allocations["task"] != nil || !portClaimed(agent, 18088) {
		t.Fatal("stopping a stale allocation released a live allocation's port")
	}
}

func TestRecoverMissingKeepsPortClaimSharedWithLiveAllocation(t *testing.T) {
	rt := &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{}, listErr: errors.New("containerd unavailable")}
	stale := recoveryTestAllocation(18086)
	stale.Status = "stopping"
	live := recoveryTestAllocation(18086)
	live.ID, live.ContainerID, live.AllocationID = "live", "live", "other"
	agent, _ := newRecoveryTestAgent(t, rt, stale, live)
	if err := agent.recover(context.Background()); err != nil {
		t.Fatalf("recover with failed listing: %v", err)
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
	if err := agent.recover(context.Background()); err != nil {
		t.Fatalf("recover with failed listing: %v", err)
	}

	rt.listErr = nil
	task := &spec.TaskSpec{Name: "task", Image: "image"}
	if err := agent.startTask(context.Background(), &taskStart{ID: "task", AllocationID: "allocation", Generation: 1, JobRevision: 1, ExecutionHash: "hash", Namespace: "default", JobName: "job", GroupName: "group", Spec: task}); err != nil {
		t.Fatalf("start retry after confirmed missing container: %v", err)
	}
	if rt.created != 1 || rt.stopCount != 0 {
		t.Fatalf("create=%d stop=%d, want 1/0", rt.created, rt.stopCount)
	}
	if got := agent.allocations["task"]; got == nil || got.unobserved || got.Status != "running" {
		t.Fatalf("allocation after start retry = %+v, want running", got)
	}
}

func TestRecoverRetryRefusesUninspectableUnrecordedContainer(t *testing.T) {
	rt := &staleListingRuntime{
		listingRecoveryRuntime: &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{}, listErr: errors.New("containerd unavailable")},
		inspectErr:             errors.New("shim unresponsive"),
	}
	agent, local := newRecoveryTestAgent(t, rt)
	if err := agent.recover(context.Background()); err != nil {
		t.Fatalf("recover with failed listing: %v", err)
	}
	rt.listErr = nil
	rt.containers = []runtime.ContainerInfo{{ID: "task", Status: runtime.StatusRunning, Labels: recoveryTestLabels(recoveryTestAllocation(0))}}
	agent.retryRecovery(context.Background())
	assertRecoveryRefused(t, agent, local, "task")
}

func TestRecoverRejectsUnreadableContainerWithoutRecord(t *testing.T) {
	rt := &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{}}
	rt.containers = []runtime.ContainerInfo{{ID: "unreadable", Status: runtime.StatusUnknown}}
	agent, _ := newRecoveryTestAgent(t, rt)
	if err := agent.recover(context.Background()); err == nil {
		t.Fatal("recover accepted an unreadable container without durable state")
	}
}

func TestRecoverRetriesSupersededStopAfterRestart(t *testing.T) {
	rt := &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{status: runtime.StatusRunning}}
	older := recoveryTestAllocation(0)
	older.Status = "stopping"
	newer := recoveryTestAllocation(0)
	newer.ID, newer.ContainerID, newer.Generation = "task-g2", "task-g2", 2
	newerLabels := recoveryTestLabels(newer)
	newerLabels["trellis.allocation-generation"] = "2"
	rt.containers = []runtime.ContainerInfo{
		{ID: "task", Status: runtime.StatusRunning, Labels: recoveryTestLabels(older)},
		{ID: "task-g2", Status: runtime.StatusRunning, Labels: newerLabels},
	}
	agent, _ := newRecoveryTestAgent(t, rt, older, newer)
	if err := agent.recover(context.Background()); err != nil {
		t.Fatalf("recover: %v", err)
	}
	if !agent.recoveryPending() {
		t.Fatal("retained superseded generation is not scheduled for a stop")
	}
	if agent.retryRecovery(context.Background()) {
		t.Fatal("retry left recovery work pending after stopping the superseded generation")
	}
	if rt.stopCount != 1 || agent.allocations["task"] != nil || agent.allocations["task-g2"] == nil {
		t.Fatalf("stop calls = %d, older = %+v, newer present = %v; want only the older generation stopped", rt.stopCount, agent.allocations["task"], agent.allocations["task-g2"] != nil)
	}
}

func TestRecoverRetryQueuesSupersededStopAfterFailedListing(t *testing.T) {
	rt := &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{status: runtime.StatusRunning}, listErr: errors.New("containerd unavailable")}
	older := recoveryTestAllocation(0)
	older.Status = "stopping"
	newer := recoveryTestAllocation(0)
	newer.ID, newer.ContainerID, newer.Generation = "task-g2", "task-g2", 2
	newerLabels := recoveryTestLabels(newer)
	newerLabels["trellis.allocation-generation"] = "2"
	agent, _ := newRecoveryTestAgent(t, rt, older, newer)
	if err := agent.recover(context.Background()); err != nil {
		t.Fatalf("recover with failed listing: %v", err)
	}

	rt.listErr = nil
	rt.containers = []runtime.ContainerInfo{
		{ID: "task", Status: runtime.StatusRunning, Labels: recoveryTestLabels(older)},
		{ID: "task-g2", Status: runtime.StatusRunning, Labels: newerLabels},
	}
	if agent.retryRecovery(context.Background()) {
		t.Fatal("retry left recovery work pending")
	}
	if rt.stopCount != 1 || agent.allocations["task"] != nil || agent.allocations["task-g2"] == nil {
		t.Fatalf("stop calls = %d, older = %+v; want only the superseded generation stopped", rt.stopCount, agent.allocations["task"])
	}
}

func TestStopGroupStopsUnrecordedContainerWhileListingIncomplete(t *testing.T) {
	rt := &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{status: runtime.StatusRunning}, listErr: errors.New("containerd unavailable")}
	agent, _ := newRecoveryTestAgent(t, rt)
	if err := agent.recover(context.Background()); err != nil {
		t.Fatalf("recover with failed listing: %v", err)
	}
	stop := &nodeapi.StopAllocationRequest{AllocationID: "allocation", Generation: 1, Epoch: 1}
	if err := agent.StopGroup(context.Background(), stop); err == nil {
		t.Fatal("stop acknowledged an allocation that recovery could not list")
	}

	rt.listErr = nil
	rt.containers = []runtime.ContainerInfo{{ID: "task", Status: runtime.StatusRunning, Labels: recoveryTestLabels(recoveryTestAllocation(0))}}
	if err := agent.StopGroup(context.Background(), stop); err != nil {
		t.Fatalf("stop unrecorded allocation: %v", err)
	}
	if rt.stopCount != 1 || rt.removeCount != 1 || agent.allocations["task"] != nil {
		t.Fatalf("stop=%d remove=%d allocation=%+v; want the unrecorded container stopped", rt.stopCount, rt.removeCount, agent.allocations["task"])
	}
}

func TestStopGroupChecksUnrecordedSiblingsWhileListingIncomplete(t *testing.T) {
	rt := &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{status: runtime.StatusRunning}, listErr: errors.New("containerd unavailable")}
	recorded := recoveryTestAllocation(0)
	agent, _ := newRecoveryTestAgent(t, rt, recorded)
	if err := agent.recover(context.Background()); err != nil {
		t.Fatalf("recover with failed listing: %v", err)
	}

	sibling := recoveryTestAllocation(0)
	sibling.ID, sibling.ContainerID, sibling.TaskName = "sibling", "sibling", "sibling"
	rt.listErr = nil
	rt.containers = []runtime.ContainerInfo{
		{ID: "sibling", Status: runtime.StatusRunning, Labels: recoveryTestLabels(sibling)},
		{ID: "unreadable", Status: runtime.StatusUnknown},
	}
	stop := &nodeapi.StopAllocationRequest{AllocationID: "allocation", Generation: 1, Epoch: 1}
	if err := agent.StopGroup(context.Background(), stop); err == nil {
		t.Fatal("stop succeeded while an unreadable container could belong to the allocation")
	}
	if rt.stopCount != 2 || agent.allocations["task"] != nil || agent.allocations["sibling"] != nil {
		t.Fatalf("stop calls = %d; want the recorded task and its unrecorded sibling stopped", rt.stopCount)
	}

	rt.containers = nil
	if err := agent.StopGroup(context.Background(), stop); err != nil {
		t.Fatalf("stop after every container is accounted for: %v", err)
	}
}

func TestRecoverRetryKeepsExhaustedRestartBudgetTerminal(t *testing.T) {
	rt := &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{status: runtime.StatusStopped}, listErr: errors.New("containerd unavailable")}
	record := recoveryTestAllocation(0)
	record.RestartExhausted = true
	agent, _ := newRecoveryTestAgent(t, rt, record)
	if err := agent.recover(context.Background()); err != nil {
		t.Fatalf("recover with failed listing: %v", err)
	}
	task := &spec.TaskSpec{Name: "task", Image: "image"}
	if err := agent.startTask(context.Background(), &taskStart{ID: "task", AllocationID: "allocation", Generation: 1, JobRevision: 1, ExecutionHash: "hash", Namespace: "default", JobName: "job", GroupName: "group", Spec: task}); !errors.Is(err, ErrRestartBudgetExhausted) {
		t.Fatalf("start retry error = %v, want exhausted restart budget", err)
	}

	rt.listErr = nil
	rt.containers = []runtime.ContainerInfo{{ID: "task", Status: runtime.StatusStopped, Labels: recoveryTestLabels(record)}}
	if agent.retryRecovery(context.Background()) {
		t.Fatal("retry left recovery work pending")
	}
	recovered := agent.allocations["task"]
	if recovered == nil || recovered.Status != "failed" || recovered.Health != "unhealthy" {
		t.Fatalf("allocation after retry = %+v, want terminal failed", recovered)
	}
	if rt.restartCount != 0 || rt.stopCount != 0 {
		t.Fatalf("restart=%d stop=%d; exhausted allocation must not be restarted or replaced", rt.restartCount, rt.stopCount)
	}
}

func TestStopGroupStopsOlderUnrecordedGenerationsWhileListingIncomplete(t *testing.T) {
	rt := &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{status: runtime.StatusRunning}, listErr: errors.New("containerd unavailable")}
	agent, _ := newRecoveryTestAgent(t, rt)
	if err := agent.recover(context.Background()); err != nil {
		t.Fatalf("recover with failed listing: %v", err)
	}
	older := recoveryTestAllocation(0)
	newerLabels := recoveryTestLabels(older)
	newerLabels["trellis.allocation-generation"] = "2"
	rt.listErr = nil
	rt.containers = []runtime.ContainerInfo{
		{ID: "task", Status: runtime.StatusRunning, Labels: recoveryTestLabels(older)},
		{ID: "task-g2", Status: runtime.StatusRunning, Labels: newerLabels},
	}
	if err := agent.StopGroup(context.Background(), &nodeapi.StopAllocationRequest{AllocationID: "allocation", Generation: 2, Epoch: 1}); err != nil {
		t.Fatalf("stop newer generation: %v", err)
	}
	if rt.stopCount != 2 || agent.allocations["task"] != nil || agent.allocations["task-g2"] != nil {
		t.Fatalf("stop calls = %d; want both unrecorded generations stopped", rt.stopCount)
	}
}

func TestStopGroupRejectsStaleGenerationWhenNewerUnrecordedIsListed(t *testing.T) {
	rt := &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{status: runtime.StatusRunning}, listErr: errors.New("containerd unavailable")}
	agent, _ := newRecoveryTestAgent(t, rt)
	if err := agent.recover(context.Background()); err != nil {
		t.Fatalf("recover with failed listing: %v", err)
	}
	labels := recoveryTestLabels(recoveryTestAllocation(0))
	labels["trellis.allocation-generation"] = "2"
	rt.listErr = nil
	rt.containers = []runtime.ContainerInfo{{ID: "task-g2", Status: runtime.StatusRunning, Labels: labels}}
	err := agent.StopGroup(context.Background(), &nodeapi.StopAllocationRequest{AllocationID: "allocation", Generation: 1, Epoch: 1})
	if !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("stop error = %v, want stale generation", err)
	}
	if rt.stopCount != 0 {
		t.Fatalf("stale stop stopped %d containers", rt.stopCount)
	}
}

func TestStopGroupDoesNotReStopRecordedTasksWhileListingIncomplete(t *testing.T) {
	rt := &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{status: runtime.StatusRunning}, listErr: errors.New("containerd unavailable")}
	record := recoveryTestAllocation(0)
	agent, local := newRecoveryTestAgent(t, rt, record)
	if err := agent.recover(context.Background()); err != nil {
		t.Fatalf("recover with failed listing: %v", err)
	}
	rt.listErr = nil
	rt.containers = []runtime.ContainerInfo{{ID: "task", Status: runtime.StatusRunning, Labels: recoveryTestLabels(record)}}
	if err := agent.StopGroup(context.Background(), &nodeapi.StopAllocationRequest{AllocationID: "allocation", Generation: 1, Epoch: 1}); err != nil {
		t.Fatalf("stop recorded allocation: %v", err)
	}
	if rt.stopCount != 1 || agent.allocations["task"] != nil {
		t.Fatalf("stop calls = %d, allocation = %+v; want one stop and no retained record", rt.stopCount, agent.allocations["task"])
	}
	var persisted Allocation
	if err := local.Get(allocationRecordKey("task"), &persisted); err == nil {
		t.Fatal("stop rewrote a record for the stopped task")
	}
}

func TestRecoverVerifiesEveryContainerBeforeRuntimeOperations(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mutate  func(map[string]string)
		cleared bool
	}{
		{name: "matching labels", cleared: true},
		{name: "different allocation", mutate: func(labels map[string]string) { labels["trellis.allocation-id"] = "other" }},
		{name: "different generation", mutate: func(labels map[string]string) { labels["trellis.allocation-generation"] = "2" }},
		{name: "different task", mutate: func(labels map[string]string) { labels["trellis.task"] = "other" }},
	} {
		for _, initiallyUnverified := range []bool{false, true} {
			for _, retry := range []bool{false, true} {
				name := tc.name
				if initiallyUnverified {
					name += " with ambiguous create"
				} else {
					name += " with ordinary durable record"
				}
				if retry {
					name += " after failed listing"
				}
				t.Run(name, func(t *testing.T) {
					rt := &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{}}
					record := recoveryTestAllocation(18090)
					record.ContainerOwnershipUnverified = initiallyUnverified
					labels := recoveryTestLabels(record)
					if tc.mutate != nil {
						tc.mutate(labels)
					}
					rt.status = runtime.StatusRunning
					rt.containers = []runtime.ContainerInfo{{ID: "task", Status: runtime.StatusRunning, Labels: labels}}
					if retry {
						rt.listErr = errors.New("containerd unavailable")
					}
					agent, local := newRecoveryTestAgent(t, rt, record)
					err := agent.recover(context.Background())
					if retry {
						if err != nil {
							t.Fatalf("recover with failed listing: %v", err)
						}
						var persisted Allocation
						if err := local.Get(allocationRecordKey("task"), &persisted); err != nil || persisted.ContainerOwnershipUnverified != initiallyUnverified {
							t.Fatalf("unobserved record = %+v (%v), want ownership flag %t", persisted, err, initiallyUnverified)
						}
						rt.listErr = nil
						agent.retryRecovery(context.Background())
					} else if err != nil {
						t.Fatalf("recover: %v", err)
					}

					recovered := agent.allocations["task"]
					if recovered == nil || recovered.ContainerOwnershipUnverified == tc.cleared {
						t.Fatalf("recovered allocation = %+v, want ownership verified %t", recovered, tc.cleared)
					}
					var persisted Allocation
					if err := local.Get(allocationRecordKey("task"), &persisted); err != nil || persisted.ContainerOwnershipUnverified == tc.cleared {
						t.Fatalf("persisted allocation = %+v (%v), want ownership verified %t", persisted, err, tc.cleared)
					}
					agent.mu.RLock()
					targets, _ := agent.execTargetsLocked("allocation")
					agent.mu.RUnlock()
					if tc.cleared && (recovered.Status != "running" || len(targets) != 1 || targets[0].ContainerID != "task") {
						t.Fatalf("exec targets = %#v for %s allocation, want the recovered container", targets, recovered.Status)
					}
					if !tc.cleared && (recovered.Status != "stopping" || len(targets) != 0) {
						t.Fatalf("mismatched container: status %s, exec targets %#v; want stopping with none", recovered.Status, targets)
					}
					if !tc.cleared {
						if _, tracked := agent.reconciler.states["task"]; tracked {
							t.Fatal("mismatched container was tracked for health or restart reconciliation")
						}
						err := agent.StopAllocation(context.Background(), "task")
						if !errors.Is(err, ErrExecutionConflict) {
							t.Fatalf("stop mismatched container: %v, want execution conflict", err)
						}
						if rt.stopCount != 0 || rt.removeCount != 0 || !portClaimed(agent, 18090) {
							t.Fatalf("stop=%d remove=%d port retained=%t; want no runtime cleanup and retained resources", rt.stopCount, rt.removeCount, portClaimed(agent, 18090))
						}
					}
				})
			}
		}
	}
}
