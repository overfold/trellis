package agent

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/health"
	"github.com/clofour/trellis/internal/network"
	"github.com/clofour/trellis/internal/runtime"
	"github.com/clofour/trellis/internal/spec"
	"github.com/clofour/trellis/internal/storage"
	"github.com/google/uuid"
)

type blockingStartRuntime struct {
	*reconcilerRuntime
	started chan string
	release chan struct{}
	labels  map[string]map[string]string
	created map[string]runtime.CreateOptions
}

type failingStopRuntime struct {
	*reconcilerRuntime
	stopErr     error
	stopCount   int
	removeCount int
}

type failingRemoveRuntime struct {
	*ambiguousStartRuntime
	removeErr error
}

func (r *failingRemoveRuntime) Remove(context.Context, string) error { return r.removeErr }

type removedStartRuntime struct {
	*ambiguousStartRuntime
	removeCount int
}

func (r *removedStartRuntime) Remove(context.Context, string) error {
	r.removeCount++
	return nil
}

func (r *removedStartRuntime) ListManaged(context.Context, string) ([]runtime.ContainerInfo, error) {
	return nil, nil
}

type failingDetachNetworkManager struct {
	countingNetworkManager
	detachErr error
}

func (m *failingDetachNetworkManager) Attach(ctx context.Context, request network.AttachRequest) (*network.Attachment, error) {
	return staticNetworkManager{}.Attach(ctx, request)
}

func (m *failingDetachNetworkManager) Detach(ctx context.Context, attachment *network.Attachment) error {
	m.countingNetworkManager.Detach(ctx, attachment)
	return m.detachErr
}

type stoppedWithErrorRuntime struct {
	*reconcilerRuntime
	stopErr    error
	startCount int
	managedID  string
}

type createdRecoveryRuntime struct {
	*reconcilerRuntime
	managedID   string
	labels      map[string]string
	stopCount   int
	startCount  int
	removeCount int
}

type recoveryProbeRuntime struct {
	*createdRecoveryRuntime
	commands chan []string
}

type firstHealthProbeRuntime struct {
	*reconcilerRuntime
	started chan struct{}
	probed  chan struct{}
}

func (r *firstHealthProbeRuntime) Start(context.Context, string) error {
	close(r.started)
	return nil
}

func (r *firstHealthProbeRuntime) Exec(context.Context, string, []string) (int, error) {
	select {
	case r.probed <- struct{}{}:
	default:
	}
	return 0, nil
}

type blockingRecoveryDetach struct {
	network.DisabledManager
	entered chan struct{}
	release chan struct{}
}

func (m *blockingRecoveryDetach) Detach(context.Context, *network.Attachment) error {
	close(m.entered)
	<-m.release
	return nil
}

func (r *recoveryProbeRuntime) Exec(_ context.Context, _ string, command []string) (int, error) {
	select {
	case r.commands <- command:
	default:
	}
	return 0, nil
}

func (r *createdRecoveryRuntime) ListManaged(context.Context, string) ([]runtime.ContainerInfo, error) {
	return []runtime.ContainerInfo{{ID: r.managedID, Status: r.status, Labels: r.labels}}, nil
}

func (r *createdRecoveryRuntime) Stop(context.Context, string) error {
	r.stopCount++
	r.status = runtime.StatusStopped
	return nil
}

func (r *createdRecoveryRuntime) Remove(context.Context, string) error {
	r.removeCount++
	return nil
}

func (r *createdRecoveryRuntime) Create(_ context.Context, options runtime.CreateOptions) (string, error) {
	r.status = runtime.StatusCreated
	return options.ID, nil
}

func (r *createdRecoveryRuntime) Start(context.Context, string) error {
	r.startCount++
	r.status = runtime.StatusRunning
	return nil
}

func (r *stoppedWithErrorRuntime) Stop(context.Context, string) error {
	r.status = runtime.StatusStopped
	return r.stopErr
}

func (r *stoppedWithErrorRuntime) Start(context.Context, string) error {
	r.startCount++
	r.status = runtime.StatusRunning
	return nil
}

func (r *stoppedWithErrorRuntime) ListManaged(context.Context, string) ([]runtime.ContainerInfo, error) {
	if r.managedID == "" {
		return nil, nil
	}
	return []runtime.ContainerInfo{{ID: r.managedID, Status: r.status}}, nil
}

type blockingStopRuntime struct {
	*reconcilerRuntime
	stopped chan struct{}
	release chan struct{}
}

func (r *blockingStopRuntime) Stop(context.Context, string) error {
	r.status = runtime.StatusStopped
	close(r.stopped)
	<-r.release
	return nil
}

type startHookRuntime struct {
	*failingStopRuntime
	onStart func() error
}

func (r *startHookRuntime) Start(context.Context, string) error { return r.onStart() }

type ambiguousStartRuntime struct {
	*reconcilerRuntime
	stopCount int
	stopErr   error
	onStop    func()
}

func (r *ambiguousStartRuntime) Start(context.Context, string) error {
	r.status = runtime.StatusRunning
	return errors.New("start response lost")
}
func (r *ambiguousStartRuntime) Inspect(context.Context, string) (*runtime.ContainerInfo, error) {
	return nil, errors.New("inspect unavailable")
}
func (r *ambiguousStartRuntime) Stop(context.Context, string) error {
	r.stopCount++
	if r.onStop != nil {
		r.onStop()
	}
	if r.stopErr != nil {
		return r.stopErr
	}
	r.status = runtime.StatusStopped
	return nil
}

func (r *failingStopRuntime) Stop(context.Context, string) error {
	r.stopCount++
	if r.stopErr != nil {
		return r.stopErr
	}
	r.status = runtime.StatusStopped
	return nil
}
func (r *failingStopRuntime) Remove(context.Context, string) error {
	r.removeCount++
	return nil
}

type countingNetworkManager struct{ detachCount int }

func (*countingNetworkManager) Attach(context.Context, network.AttachRequest) (*network.Attachment, error) {
	return nil, nil
}
func (m *countingNetworkManager) Detach(context.Context, *network.Attachment) error {
	m.detachCount++
	return nil
}

func (r *blockingStartRuntime) Create(_ context.Context, options runtime.CreateOptions) (string, error) {
	r.labels[options.ID] = options.Labels
	if r.created != nil {
		r.created[options.ID] = options
	}
	return options.ID, nil
}

func (r *blockingStartRuntime) Start(_ context.Context, id string) error {
	r.started <- id
	<-r.release
	return nil
}

type staticNetworkManager struct{}

func (staticNetworkManager) Attach(_ context.Context, request network.AttachRequest) (*network.Attachment, error) {
	return &network.Attachment{AllocationID: request.AllocationID, NetworkNamespace: "/var/run/netns/" + request.AllocationID}, nil
}

func (staticNetworkManager) Detach(context.Context, *network.Attachment) error { return nil }

func newOperationTestAgent(t *testing.T, rt runtime.ContainerRuntime) *Agent {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reconciler := NewAllocationReconciler(rt, nil)
	return NewAgent(log, rt, health.NewHealthManager(log, rt, nil), reconciler, NewPortManager(rt, 0, 0, 0), NewVolumeManager(t.TempDir()), nil, uuid.New())
}

func operationTestRequest() *api.AllocationRequest {
	return &api.AllocationRequest{
		AllocationID: "allocation", Generation: 2, JobRevision: 7, ExecutionHash: "execution-hash",
		Namespace: "default", JobName: "job", GroupName: "group",
		Tasks: []spec.TaskSpec{{Name: "first", Image: "image"}, {Name: "second", Image: "image"}},
	}
}

func TestRunAllocationMountsHealthProbeForEveryNetworkAndRuntime(t *testing.T) {
	for _, mode := range []spec.TaskNetworkMode{spec.TaskNetworkHost, spec.TaskNetworkIsolated, spec.TaskNetworkWireGuard} {
		for _, taskRuntime := range []string{"runc", "runsc"} {
			t.Run(string(mode)+"/"+taskRuntime, func(t *testing.T) {
				rt := &blockingStartRuntime{
					reconcilerRuntime: &reconcilerRuntime{},
					started:           make(chan string, 1),
					release:           make(chan struct{}, 1),
					labels:            map[string]map[string]string{},
					created:           map[string]runtime.CreateOptions{},
				}
				rt.release <- struct{}{}
				agent := newOperationTestAgent(t, rt)
				agent.SetNetworkManager(staticNetworkManager{})
				task := &spec.TaskSpec{Name: "web", Image: "image", Networking: &spec.TaskNetworkingSpec{Mode: mode}}
				var plan *network.Plan
				if mode == spec.TaskNetworkWireGuard {
					plan = &network.Plan{}
				}
				if err := agent.RunAllocation(context.Background(), "alloc", "scheduler", 1, 1, "hash", "default", "job", "group", "web", task, taskRuntime, plan, nil, nil, nil); err != nil {
					t.Fatal(err)
				}

				options := rt.created["alloc"]
				if options.Runtime != taskRuntime {
					t.Fatalf("runtime = %q, want %q", options.Runtime, taskRuntime)
				}
				var probeMount *runtime.Mount
				for _, mount := range options.Mounts {
					if mount.ContainerPath == health.ProbeContainerPath {
						probeMount = mount
						break
					}
				}
				if probeMount == nil || !probeMount.ReadOnly || filepath.Base(probeMount.HostPath) != "trellis-health-probe" {
					t.Fatalf("health probe mount = %#v", probeMount)
				}
			})
		}
	}
}

func TestRunAllocationRegistersHealthAfterStoringRunningAllocation(t *testing.T) {
	rt := &firstHealthProbeRuntime{
		reconcilerRuntime: &reconcilerRuntime{},
		started:           make(chan struct{}),
		probed:            make(chan struct{}, 1),
	}
	agent := newOperationTestAgent(t, rt)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	agent.health.SetContext(ctx)
	callbackDone := make(chan struct{})
	agent.health.Subscriber = &healthCallbackRecorder{agent: agent, done: callbackDone}
	task := &spec.TaskSpec{Name: "web", Image: "image", HealthCheck: &spec.HealthCheckSpec{
		Type: "script", Command: []string{"true"}, Interval: time.Millisecond, Threshold: 1,
	}}

	// Hold tracking so the old registration order gives a fast probe time to
	// update the starting allocation before RunAllocation replaces it.
	agent.reconciler.mu.Lock()
	runDone := make(chan error, 1)
	go func() {
		runDone <- agent.RunAllocation(ctx, "alloc", "scheduler", 1, 1, "hash", "default", "job", "group", "web", task, "", nil, nil, nil, nil)
	}()
	select {
	case <-rt.started:
	case <-time.After(time.Second):
		agent.reconciler.mu.Unlock()
		t.Fatal("allocation did not start")
	}
	var probedBeforeReady bool
	select {
	case <-rt.probed:
		probedBeforeReady = true
	case <-time.After(50 * time.Millisecond):
	}
	agent.reconciler.mu.Unlock()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("allocation did not finish starting")
	}
	if probedBeforeReady {
		t.Fatal("health probe ran before the running allocation was stored")
	}
	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("first health probe did not publish its result")
	}
	agent.mu.RLock()
	got := agent.allocations["alloc"].Health
	agent.mu.RUnlock()
	if got != "healthy" {
		t.Fatalf("health after first probe = %q, want healthy", got)
	}
}

func TestConcurrentDuplicateRunWaitsForEveryTask(t *testing.T) {
	rt := &blockingStartRuntime{reconcilerRuntime: &reconcilerRuntime{}, started: make(chan string, 2), release: make(chan struct{}), labels: map[string]map[string]string{}}
	agent := newOperationTestAgent(t, rt)
	request := operationTestRequest()
	firstDone := make(chan error, 1)
	duplicateDone := make(chan error, 1)
	go func() { firstDone <- agent.RunGroup(context.Background(), request) }()
	<-rt.started
	go func() { duplicateDone <- agent.RunGroup(context.Background(), request) }()
	select {
	case err := <-duplicateDone:
		t.Fatalf("duplicate returned before the original completed: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	rt.release <- struct{}{}
	<-rt.started
	select {
	case err := <-duplicateDone:
		t.Fatalf("duplicate returned before the second task completed: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	rt.release <- struct{}{}
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-duplicateDone; err != nil {
		t.Fatal(err)
	}
}

func TestStopGroupWaitsForInProgressRun(t *testing.T) {
	rt := &blockingStartRuntime{reconcilerRuntime: &reconcilerRuntime{}, started: make(chan string, 2), release: make(chan struct{}), labels: map[string]map[string]string{}}
	agent := newOperationTestAgent(t, rt)
	request := operationTestRequest()
	runDone := make(chan error, 1)
	stopDone := make(chan error, 1)
	go func() { runDone <- agent.RunGroup(context.Background(), request) }()
	<-rt.started
	go func() {
		stopDone <- agent.StopGroup(context.Background(), &api.StopAllocationRequest{AllocationID: request.AllocationID, Generation: request.Generation})
	}()
	select {
	case err := <-stopDone:
		t.Fatalf("stop returned during start: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	rt.release <- struct{}{}
	<-rt.started
	rt.release <- struct{}{}
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
	if err := <-stopDone; err != nil {
		t.Fatal(err)
	}
	if got := len(agent.GetAllocations()); got != 0 {
		t.Fatalf("allocations after stop = %d, want 0", got)
	}
}

func TestStopAttemptsRuntimeWhenStoppingPersistenceFails(t *testing.T) {
	stopErr := errors.New("stop failed")
	rt := &failingStopRuntime{reconcilerRuntime: &reconcilerRuntime{status: runtime.StatusRunning}, stopErr: stopErr}
	agent := newOperationTestAgent(t, rt)
	root := t.TempDir()
	local := storage.NewLocalStorage(root)
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	agent.ConfigureDurability(local, "test")
	agent.allocations["task"] = &Allocation{
		ID: "task", AllocationID: "allocation", ContainerID: "task", Status: "running",
	}
	agent.reconciler.Track("task", false, nil)
	if err := agent.persistAllocation(agent.allocations["task"]); err != nil {
		t.Fatal(err)
	}

	recordDir := filepath.Join(root, "agent", "allocations")
	if err := os.RemoveAll(recordDir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(recordDir, []byte("blocked"), 0o600); err != nil {
		t.Fatal(err)
	}

	err := agent.StopAllocation(context.Background(), "task")
	if !errors.Is(err, stopErr) || !strings.Contains(err.Error(), "persist stopping allocation") {
		t.Fatalf("stop error = %v, want persistence and runtime stop failures", err)
	}
	if rt.stopCount != 1 {
		t.Fatalf("stop attempts = %d, want 1 despite persistence failure", rt.stopCount)
	}
	if got := agent.allocations["task"].Status; got != "stopping" {
		t.Fatalf("allocation status = %q, want stopping", got)
	}
	if _, ok := agent.reconciler.states["task"]; !ok {
		t.Fatal("allocation was untracked after failed runtime stop")
	}
}

func TestFailedStopKeepsAllocationResourcesAndObservation(t *testing.T) {
	rt := &failingStopRuntime{reconcilerRuntime: &reconcilerRuntime{status: runtime.StatusRunning}, stopErr: errors.New("stop failed")}
	agent := newOperationTestAgent(t, rt)
	networkManager := &countingNetworkManager{}
	agent.SetNetworkManager(networkManager)
	port, err := agent.ports.Claim(spec.PortSpec{})
	if err != nil {
		t.Fatal(err)
	}
	secretDir := filepath.Join(t.TempDir(), "secrets")
	if err := os.Mkdir(secretDir, 0o700); err != nil {
		t.Fatal(err)
	}
	agent.allocations["task"] = &Allocation{
		ID: "task", AllocationID: "allocation", ContainerID: "task",
		Ports: []*runtime.Port{port}, SecretDir: secretDir, Network: &network.Attachment{},
	}
	agent.reconciler.Track("task", false, nil)

	if err := agent.StopAllocation(context.Background(), "task"); !errors.Is(err, rt.stopErr) {
		t.Fatalf("stop error = %v, want %v", err, rt.stopErr)
	}
	if networkManager.detachCount != 0 || rt.removeCount != 0 {
		t.Fatalf("failed stop detached network %d times and removed container %d times", networkManager.detachCount, rt.removeCount)
	}
	if _, err := os.Stat(secretDir); err != nil {
		t.Fatalf("secret directory after failed stop: %v", err)
	}
	if _, ok := agent.ports.claims[port.HostPort]; !ok {
		t.Fatal("port claim released after failed stop")
	}
	if _, ok := agent.reconciler.states["task"]; !ok {
		t.Fatal("allocation untracked after failed stop")
	}
	if len(agent.GetAllocations()) != 1 {
		t.Fatal("allocation removed after failed stop")
	}

	rt.stopErr = nil
	if err := agent.StopAllocation(context.Background(), "task"); err != nil {
		t.Fatalf("retry stop: %v", err)
	}
	if networkManager.detachCount != 1 || rt.removeCount != 1 {
		t.Fatalf("retry detached network %d times and removed container %d times", networkManager.detachCount, rt.removeCount)
	}
	if _, err := os.Stat(secretDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("secret directory after retry: %v", err)
	}
	if _, ok := agent.ports.claims[port.HostPort]; ok {
		t.Fatal("port claim retained after retry")
	}
}

func TestConcurrentStopCannotRestartBeforeCleanup(t *testing.T) {
	rt := &blockingStopRuntime{reconcilerRuntime: &reconcilerRuntime{status: runtime.StatusRunning}, stopped: make(chan struct{}), release: make(chan struct{})}
	agent := newOperationTestAgent(t, rt)
	agent.allocations["task"] = &Allocation{ID: "task", AllocationID: "allocation", ContainerID: "task"}
	agent.reconciler.Track("task", false, nil)
	stopDone := make(chan error, 1)
	go func() { stopDone <- agent.StopAllocation(context.Background(), "task") }()
	<-rt.stopped
	if err := agent.reconciler.Reconcile(context.Background(), "task"); err != nil {
		t.Fatalf("reconcile during stop: %v", err)
	}
	close(rt.release)
	if err := <-stopDone; err != nil {
		t.Fatal(err)
	}
	if rt.restartCount != 0 {
		t.Fatalf("container restarted %d times during stop", rt.restartCount)
	}
}

func TestFailedRunStopPreservesStartedAllocationResources(t *testing.T) {
	stopErr := errors.New("stop failed")
	rt := &startHookRuntime{failingStopRuntime: &failingStopRuntime{reconcilerRuntime: &reconcilerRuntime{status: runtime.StatusRunning}, stopErr: stopErr}}
	agent := newOperationTestAgent(t, rt)
	manager := &countingNetworkManager{}
	agent.SetNetworkManager(manager)
	root := t.TempDir()
	local := storage.NewLocalStorage(root)
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	agent.ConfigureDurability(local, "test")
	request := operationTestRequest()
	request.Tasks = []spec.TaskSpec{{Name: "first", Image: "image", Networking: &spec.TaskNetworkingSpec{Ports: []spec.PortSpec{{}}}}}
	request.Secrets = []api.DeliveredSecret{{Task: "first", Name: "key", Target: spec.SecretTargetFile, Path: "/run/trellis-secrets/key", Value: []byte("secret")}}
	id := "allocation-g2-first"
	recordDir := filepath.Join(root, "agent", "allocations")
	rt.onStart = func() error {
		if err := os.RemoveAll(recordDir); err != nil {
			return err
		}
		return os.WriteFile(recordDir, []byte("blocked"), 0o600)
	}
	err := agent.RunGroup(context.Background(), request)
	if !errors.Is(err, stopErr) || !strings.Contains(err.Error(), "persist allocation") || !strings.Contains(err.Error(), "persist stopping allocation") {
		t.Fatalf("run error = %v, want persistence and stop failures", err)
	}
	if rt.stopCount != 1 {
		t.Fatalf("stop attempts after persistence failure = %d, want 1", rt.stopCount)
	}
	alloc := agent.allocations[id]
	if alloc == nil || len(alloc.Ports) != 1 || alloc.SecretDir == "" {
		t.Fatalf("started allocation or port claim lost: %+v", alloc)
	}
	if _, err := os.Stat(alloc.SecretDir); err != nil {
		t.Fatalf("secret directory after failed stop: %v", err)
	}
	if rt.removeCount != 0 || manager.detachCount != 0 {
		t.Fatalf("failed stop removed container %d times, detached network %d times", rt.removeCount, manager.detachCount)
	}
	if _, ok := agent.ports.claims[alloc.Ports[0].HostPort]; !ok {
		t.Fatal("port claim released after failed stop")
	}
	if _, ok := agent.reconciler.states[id]; !ok {
		t.Fatal("started allocation untracked after failed stop")
	}
	if err := os.Remove(recordDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(recordDir, 0o750); err != nil {
		t.Fatal(err)
	}

	err = agent.RunGroup(context.Background(), request)
	if !errors.Is(err, stopErr) || !strings.Contains(err.Error(), "clean up incomplete allocation") {
		t.Fatalf("retry run error = %v, want retryable cleanup failure", err)
	}
	if errors.Is(err, ErrAllocationExists) {
		t.Fatalf("retry surfaced terminal allocation conflict: %v", err)
	}
	if got := agent.allocations[id].Status; got != "stopping" {
		t.Fatalf("retained allocation status = %q, want stopping", got)
	}

	rt.stopErr = nil
	rt.onStart = func() error {
		rt.status = runtime.StatusRunning
		return nil
	}
	if err := agent.RunGroup(context.Background(), request); err != nil {
		t.Fatalf("retry after cleanup became possible: %v", err)
	}
	if current := agent.allocations[id]; current == nil || current.Status != "running" {
		t.Fatalf("allocation after successful retry = %+v, want running", current)
	}
	if rt.removeCount != 1 || manager.detachCount != 1 {
		t.Fatalf("successful retry removed old container %d times, detached old network %d times", rt.removeCount, manager.detachCount)
	}
}

func TestAmbiguousStartStopsBeforeReleasingResources(t *testing.T) {
	rt := &ambiguousStartRuntime{reconcilerRuntime: &reconcilerRuntime{}}
	agent := newOperationTestAgent(t, rt)
	manager := &countingNetworkManager{}
	agent.SetNetworkManager(manager)
	request := operationTestRequest()
	request.Tasks = []spec.TaskSpec{{Name: "first", Image: "image", Networking: &spec.TaskNetworkingSpec{Ports: []spec.PortSpec{{}}}}}
	request.Secrets = []api.DeliveredSecret{{Task: "first", Name: "key", Target: spec.SecretTargetFile, Path: "/run/trellis-secrets/key", Value: []byte("secret")}}
	id := "allocation-g2-first"
	rt.onStop = func() {
		alloc := agent.allocations[id]
		if alloc == nil || len(alloc.Ports) != 1 || alloc.SecretDir == "" {
			t.Fatalf("resources missing before stop: %+v", alloc)
		}
		if _, ok := agent.ports.claims[alloc.Ports[0].HostPort]; !ok {
			t.Fatal("port released before stop")
		}
		if _, err := os.Stat(alloc.SecretDir); err != nil {
			t.Fatalf("secret files removed before stop: %v", err)
		}
		if manager.detachCount != 0 {
			t.Fatal("network detached before stop")
		}
	}
	if err := agent.RunGroup(context.Background(), request); err == nil || !strings.Contains(err.Error(), "start container") {
		t.Fatalf("run error = %v, want start failure", err)
	}
	if rt.stopCount != 1 {
		t.Fatalf("stop count = %d, want 1", rt.stopCount)
	}
	if len(agent.GetAllocations()) != 0 || len(agent.ports.claims) != 0 || manager.detachCount != 1 {
		t.Fatalf("resources retained after successful stop: allocations=%d ports=%d detach=%d", len(agent.GetAllocations()), len(agent.ports.claims), manager.detachCount)
	}
}

func TestAmbiguousStartFailedStopRemainsTracked(t *testing.T) {
	stopErr := errors.New("stop failed")
	rt := &ambiguousStartRuntime{reconcilerRuntime: &reconcilerRuntime{}, stopErr: stopErr}
	agent := newOperationTestAgent(t, rt)
	request := operationTestRequest()
	request.Tasks = []spec.TaskSpec{{Name: "first", Image: "image"}}
	id := "allocation-g2-first"

	err := agent.RunGroup(context.Background(), request)
	if !errors.Is(err, stopErr) || !strings.Contains(err.Error(), "start container") {
		t.Fatalf("run error = %v, want ambiguous start and failed cleanup", err)
	}
	if agent.allocations[id] == nil {
		t.Fatal("ambiguous started allocation was discarded after failed stop")
	}
	if _, ok := agent.reconciler.states[id]; !ok {
		t.Fatal("ambiguous started allocation was left untracked after failed stop")
	}
	if got := agent.allocations[id].Status; got != "stopping" {
		t.Fatalf("ambiguous allocation status = %q, want stopping", got)
	}

	rt.stopErr = nil
	if err := agent.StopAllocation(context.Background(), id); err != nil {
		t.Fatalf("retry stop: %v", err)
	}
	if _, ok := agent.reconciler.states[id]; ok {
		t.Fatal("allocation remained tracked after successful retry stop")
	}
}

func TestFailedStartCleanupRetainsAllocationForRetry(t *testing.T) {
	removeErr := errors.New("remove failed")
	detachErr := errors.New("detach failed")
	rt := &failingRemoveRuntime{ambiguousStartRuntime: &ambiguousStartRuntime{reconcilerRuntime: &reconcilerRuntime{}}, removeErr: removeErr}
	agent := newOperationTestAgent(t, rt)
	manager := &failingDetachNetworkManager{detachErr: detachErr}
	agent.SetNetworkManager(manager)
	local := storage.NewLocalStorage(t.TempDir())
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	agent.ConfigureDurability(local, "test")
	request := operationTestRequest()
	request.Tasks = request.Tasks[:1]
	id := "allocation-g2-first"

	err := agent.RunGroup(context.Background(), request)
	if !errors.Is(err, removeErr) || !errors.Is(err, detachErr) {
		t.Fatalf("run error = %v, want removal and detach errors", err)
	}
	if allocation := agent.allocations[id]; allocation == nil || allocation.Status != "stopping" {
		t.Fatalf("retained allocation = %+v, want stopping", allocation)
	}
	var recorded Allocation
	if err := local.Get(allocationRecordKey(id), &recorded); err != nil || recorded.Status != "stopping" {
		t.Fatalf("recorded allocation = %+v, error = %v, want stopping", recorded, err)
	}

	rt.removeErr = nil
	manager.detachErr = nil
	if err := agent.StopAllocation(context.Background(), id); err != nil {
		t.Fatalf("retry cleanup: %v", err)
	}
	if agent.allocations[id] != nil {
		t.Fatal("allocation retained after successful cleanup")
	}
	if err := local.Get(allocationRecordKey(id), &recorded); err == nil {
		t.Fatal("allocation record retained after successful cleanup")
	}
}

func TestRecoverRetainsFailedStartRecordUntilNetworkDetachSucceeds(t *testing.T) {
	detachErr := errors.New("detach failed")
	rt := &removedStartRuntime{ambiguousStartRuntime: &ambiguousStartRuntime{reconcilerRuntime: &reconcilerRuntime{}}}
	local := storage.NewLocalStorage(t.TempDir())
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	manager := &failingDetachNetworkManager{detachErr: detachErr}
	first := newOperationTestAgent(t, rt)
	first.SetNetworkManager(manager)
	first.ConfigureDurability(local, "test")
	request := operationTestRequest()
	request.Tasks = []spec.TaskSpec{{Name: "first", Image: "image", Networking: &spec.TaskNetworkingSpec{Mode: spec.TaskNetworkWireGuard}}}
	request.NetworkPlan = &network.Plan{}
	id := "allocation-g2-first"

	if err := first.RunGroup(context.Background(), request); !errors.Is(err, detachErr) {
		t.Fatalf("failed start error = %v, want detach failure", err)
	}
	if rt.removeCount != 1 {
		t.Fatalf("container removals = %d, want 1", rt.removeCount)
	}
	var recorded Allocation
	if err := local.Get(allocationRecordKey(id), &recorded); err != nil || recorded.Network == nil {
		t.Fatalf("failed start record = %+v, error = %v, want network attachment", recorded, err)
	}

	second := newOperationTestAgent(t, rt)
	second.SetNetworkManager(manager)
	second.ConfigureDurability(local, "test")
	if err := second.recover(context.Background()); err != nil {
		t.Fatalf("recover with failed detach: %v", err)
	}
	if err := local.Get(allocationRecordKey(id), &recorded); err != nil {
		t.Fatalf("record removed after failed recovery detach: %v", err)
	}
	if recovered := second.allocations[id]; recovered == nil || recovered.Status != "stopping" {
		t.Fatalf("failed cleanup is not reachable after recovery: %+v", recovered)
	}
	if err := second.RunGroup(context.Background(), request); !errors.Is(err, detachErr) {
		t.Fatalf("retry start error = %v, want cleanup failure", err)
	}
	if err := local.Get(allocationRecordKey(id), &recorded); err != nil || recorded.Network == nil {
		t.Fatalf("retry start overwrote cleanup record: %+v, error = %v", recorded, err)
	}

	manager.detachErr = nil
	if err := second.StopGroup(context.Background(), &api.StopAllocationRequest{AllocationID: request.AllocationID, Generation: request.Generation}); err != nil {
		t.Fatalf("retry stop after recovery: %v", err)
	}
	if err := local.Get(allocationRecordKey(id), &recorded); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("record after successful retry stop: %v, want not found", err)
	}
	if manager.detachCount != 4 {
		t.Fatalf("network detaches = %d, want initial cleanup, recovery, retry start, and retry stop", manager.detachCount)
	}
}

func TestRecoverMissingContainerRemovesSecretDirectoryBeforeRecord(t *testing.T) {
	rt := &removedStartRuntime{ambiguousStartRuntime: &ambiguousStartRuntime{reconcilerRuntime: &reconcilerRuntime{}}}
	local := storage.NewLocalStorage(t.TempDir())
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	secretDir := filepath.Join(t.TempDir(), "blocked", "secrets")
	if err := os.WriteFile(filepath.Dir(secretDir), []byte("blocked"), 0o600); err != nil {
		t.Fatal(err)
	}
	id := "allocation-g2-first"
	allocation := &Allocation{ID: id, ContainerID: id, AllocationID: "allocation", Generation: 2, Status: "stopping", SecretDir: secretDir}
	first := newOperationTestAgent(t, rt)
	first.ConfigureDurability(local, "test")
	if err := first.persistAllocation(allocation); err != nil {
		t.Fatal(err)
	}

	second := newOperationTestAgent(t, rt)
	second.ConfigureDurability(local, "test")
	if err := second.recover(context.Background()); err != nil {
		t.Fatalf("recover with failed secret removal: %v", err)
	}
	var recorded Allocation
	if err := local.Get(allocationRecordKey(id), &recorded); err != nil || recorded.SecretDir != secretDir {
		t.Fatalf("record after failed secret removal = %+v, error = %v", recorded, err)
	}
	if second.allocations[id] == nil {
		t.Fatal("failed secret cleanup is not reachable after recovery")
	}
	if err := os.Remove(filepath.Dir(secretDir)); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(secretDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(secretDir, "key"), []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	third := newOperationTestAgent(t, rt)
	third.ConfigureDurability(local, "test")
	if err := third.recover(context.Background()); err != nil {
		t.Fatalf("recover after secret removal became possible: %v", err)
	}
	if _, err := os.Stat(secretDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("secret directory after recovery retry: %v", err)
	}
	if err := local.Get(allocationRecordKey(id), &recorded); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("record after recovery retry: %v", err)
	}
}

func TestStopErrorAfterExitDoesNotRestartAllocation(t *testing.T) {
	stopErr := errors.New("task delete failed")
	rt := &stoppedWithErrorRuntime{
		reconcilerRuntime: &reconcilerRuntime{status: runtime.StatusRunning},
		stopErr:           stopErr,
	}
	agent := newOperationTestAgent(t, rt)
	agent.allocations["task"] = &Allocation{
		ID: "task", AllocationID: "allocation", ContainerID: "task", Status: "running",
	}
	agent.reconciler.Track("task", false, nil)

	if err := agent.StopAllocation(context.Background(), "task"); !errors.Is(err, stopErr) {
		t.Fatalf("stop error = %v, want %v", err, stopErr)
	}
	if got := agent.allocations["task"].Status; got != "stopping" {
		t.Fatalf("allocation status = %q, want stopping", got)
	}
	if err := agent.reconciler.Reconcile(context.Background(), "task"); err != nil {
		t.Fatalf("reconcile after ambiguous stop: %v", err)
	}
	if rt.restartCount != 0 {
		t.Fatalf("allocation restarted %d times after stop intent", rt.restartCount)
	}
}

func TestRecoverCreatedAllocationCanBeRetriedByControlPlane(t *testing.T) {
	id := "allocation-g2-first"
	rt := &createdRecoveryRuntime{
		reconcilerRuntime: &reconcilerRuntime{status: runtime.StatusCreated},
		managedID:         id,
	}
	local := storage.NewLocalStorage(t.TempDir())
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	request := operationTestRequest()
	request.Tasks = request.Tasks[:1]
	stale := &Allocation{
		ID: id, AllocationID: request.AllocationID, ContainerID: id,
		Generation: request.Generation, JobRevision: request.JobRevision, ExecutionHash: request.ExecutionHash,
		Spec: &request.Tasks[0], Status: "starting", Health: "unknown", Draining: true,
	}

	first := newOperationTestAgent(t, rt)
	first.ConfigureDurability(local, "test")
	if err := first.persistAllocation(stale); err != nil {
		t.Fatal(err)
	}

	second := newOperationTestAgent(t, rt)
	second.ConfigureDurability(local, "test")
	if err := second.recover(context.Background()); err != nil {
		t.Fatalf("recover: %v", err)
	}
	if rt.startCount != 0 {
		t.Fatalf("recovery started created task %d times, want 0", rt.startCount)
	}
	if got := second.allocations[id]; got == nil || got.Status != "starting" {
		t.Fatalf("recovered allocation = %+v, want starting", got)
	}
	if state := second.reconciler.states[id]; state == nil || !state.stopping {
		t.Fatal("recovered draining allocation is not restart-suppressed")
	}
	drain := &api.DrainAllocationRequest{AllocationID: request.AllocationID, Generation: request.Generation}
	if err := second.ResumeGroup(drain); err != nil {
		t.Fatalf("undrain recovered allocation: %v", err)
	}
	if second.allocations[id].Draining {
		t.Fatal("recovered allocation remains draining after undrain")
	}
	if state := second.reconciler.states[id]; state == nil || !state.stopping {
		t.Fatal("created allocation enabled local restart before start retry")
	}

	// The server's normal reconciliation reissues ActionStart, which reaches
	// RunGroup. The old Created task must be cleanly stopped/removed before
	// recreating and starting the allocation.
	if err := second.RunGroup(context.Background(), request); err != nil {
		t.Fatalf("control-plane start retry: %v", err)
	}
	if rt.stopCount != 1 || rt.removeCount != 1 || rt.startCount != 1 {
		t.Fatalf("retry operations stop=%d remove=%d start=%d, want 1/1/1", rt.stopCount, rt.removeCount, rt.startCount)
	}
	if rt.status != runtime.StatusRunning {
		t.Fatalf("runtime status after retry = %q, want running", rt.status)
	}
	if got := second.allocations[id]; got == nil || got.Status != "running" {
		t.Fatalf("allocation after retry = %+v, want running", got)
	}
}

func TestRecoverNonRunningAllocationDefersRestartToServer(t *testing.T) {
	for _, durableStatus := range []string{"running", "starting"} {
		t.Run(durableStatus, func(t *testing.T) {
			rt := &stoppedWithErrorRuntime{
				reconcilerRuntime: &reconcilerRuntime{status: runtime.StatusStopped},
				managedID:         "task",
			}
			local := storage.NewLocalStorage(t.TempDir())
			if err := local.Init(); err != nil {
				t.Fatal(err)
			}
			stale := &Allocation{
				ID: "task", AllocationID: "allocation", ContainerID: "task",
				Generation: 1, JobRevision: 1, ExecutionHash: "hash",
				Spec: &spec.TaskSpec{Name: "task", Image: "image"},
				Status: durableStatus, Health: "healthy",
			}
			first := newOperationTestAgent(t, rt)
			first.ConfigureDurability(local, "test")
			if err := first.persistAllocation(stale); err != nil {
				t.Fatal(err)
			}

			second := newOperationTestAgent(t, rt)
			second.ConfigureDurability(local, "test")
			if err := second.recover(context.Background()); err != nil {
				t.Fatalf("recover: %v", err)
			}

			recovered := second.allocations["task"]
			if recovered == nil || recovered.Status != "starting" || recovered.Health != "unknown" {
				t.Fatalf("recovered allocation = %+v, want starting/unknown observation", recovered)
			}
			if rt.startCount != 0 || rt.restartCount != 0 {
				t.Fatalf("recovery performed start=%d restart=%d, want no local resurrection", rt.startCount, rt.restartCount)
			}
			if _, ok := second.reconciler.states["task"]; ok {
				t.Fatal("non-running recovered allocation entered local restart reconciliation")
			}
			request := &api.DrainAllocationRequest{AllocationID: "allocation", Generation: 1}
			if err := second.DrainGroup(request); err != nil {
				t.Fatalf("drain recovered allocation: %v", err)
			}
			if err := second.ResumeGroup(request); err != nil {
				t.Fatalf("undrain recovered allocation: %v", err)
			}
			if recovered.Draining {
				t.Fatal("recovered allocation remains draining after undrain")
			}
			if _, ok := second.reconciler.states["task"]; ok {
				t.Fatal("undrain started local restart reconciliation before start retry")
			}
		})
	}
}

func TestRecoverRunningAllocationResetsHealthUntilProbe(t *testing.T) {
	rt := &createdRecoveryRuntime{
		reconcilerRuntime: &reconcilerRuntime{status: runtime.StatusRunning},
		managedID:         "task",
	}
	local := storage.NewLocalStorage(t.TempDir())
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	check := &spec.HealthCheckSpec{Type: "script", Interval: time.Hour}
	first := newOperationTestAgent(t, rt)
	first.ConfigureDurability(local, "test")
	if err := first.persistAllocation(&Allocation{
		ID: "task", AllocationID: "allocation", ContainerID: "task",
		Spec: &spec.TaskSpec{Name: "task", HealthCheck: check},
		Status: "running", Health: "healthy",
	}); err != nil {
		t.Fatal(err)
	}
	second := newOperationTestAgent(t, rt)
	second.ConfigureDurability(local, "test")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	second.health.SetContext(ctx)
	if err := second.recover(ctx); err != nil {
		t.Fatalf("recover: %v", err)
	}
	if got := second.allocations["task"].Health; got != "unknown" {
		t.Fatalf("recovered health = %q, want unknown", got)
	}
	var persisted Allocation
	if err := local.Get(allocationRecordKey("task"), &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Health != "unknown" {
		t.Fatalf("persisted health = %q, want unknown", persisted.Health)
	}
}

func TestRecoverPersistsProbeResultBeforeReturning(t *testing.T) {
	rt := &createdRecoveryRuntime{
		reconcilerRuntime: &reconcilerRuntime{status: runtime.StatusRunning},
		managedID:         "task",
	}
	local := storage.NewLocalStorage(t.TempDir())
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	first := newOperationTestAgent(t, rt)
	first.ConfigureDurability(local, "test")
	if err := first.persistAllocation(&Allocation{
		ID: "task", AllocationID: "allocation", ContainerID: "task",
		Spec: &spec.TaskSpec{Name: "task", HealthCheck: &spec.HealthCheckSpec{
			Type: "script", Command: []string{"true"}, Interval: time.Millisecond, Threshold: 1,
		}},
		Status: "running", Health: "healthy",
	}); err != nil {
		t.Fatal(err)
	}
	if err := first.persistAllocation(&Allocation{
		ID: "stale", AllocationID: "stale", ContainerID: "stale",
		Network: &network.Attachment{AllocationID: "stale"},
	}); err != nil {
		t.Fatal(err)
	}

	second := newOperationTestAgent(t, rt)
	second.ConfigureDurability(local, "test")
	second.health.Subscriber = second
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	second.health.SetContext(ctx)
	detach := &blockingRecoveryDetach{entered: make(chan struct{}), release: make(chan struct{})}
	second.SetNetworkManager(detach)
	done := make(chan error, 1)
	go func() { done <- second.recover(ctx) }()
	defer func() {
		select {
		case <-detach.release:
		default:
			close(detach.release)
		}
	}()

	select {
	case <-detach.entered:
	case err := <-done:
		t.Fatalf("recovery finished before stale cleanup: %v", err)
	case <-time.After(time.Second):
		t.Fatal("recovery did not reach stale cleanup")
	}

	deadline := time.After(time.Second)
	for {
		var persisted Allocation
		if err := local.Get(allocationRecordKey("task"), &persisted); err != nil {
			t.Fatal(err)
		}
		if persisted.Health == "healthy" {
			break
		}
		select {
		case <-deadline:
			t.Fatal("probe did not persist healthy during recovery")
		case <-time.After(time.Millisecond):
		}
	}
	second.mu.RLock()
	got := second.allocations["task"].Health
	second.mu.RUnlock()
	if got != "healthy" {
		t.Fatalf("recovered health = %q, want healthy", got)
	}
	close(detach.release)
	if err := <-done; err != nil {
		t.Fatalf("recover: %v", err)
	}
}

func TestRecoverRunningAllocationProbesContainerPort(t *testing.T) {
	for _, checkType := range []spec.HealthCheckType{"http", "tcp"} {
		t.Run(string(checkType), func(t *testing.T) {
			rt := &recoveryProbeRuntime{
				createdRecoveryRuntime: &createdRecoveryRuntime{
					reconcilerRuntime: &reconcilerRuntime{status: runtime.StatusRunning},
					managedID:         "task",
				},
				commands: make(chan []string, 1),
			}
			local := storage.NewLocalStorage(t.TempDir())
			if err := local.Init(); err != nil {
				t.Fatal(err)
			}
			check := &spec.HealthCheckSpec{Type: checkType, Port: 8080, Path: "/health", Interval: 10 * time.Millisecond, Threshold: 1000}
			first := newOperationTestAgent(t, rt)
			first.ConfigureDurability(local, "test")
			if err := first.persistAllocation(&Allocation{
				ID: "task", AllocationID: "allocation", ContainerID: "task",
				Spec:   &spec.TaskSpec{Name: "task", HealthCheck: check},
				Ports:  []*runtime.Port{{HostPort: 32080, ContainerPort: 8080}},
				Status: "running", Health: "healthy",
			}); err != nil {
				t.Fatal(err)
			}
			second := newOperationTestAgent(t, rt)
			second.ConfigureDurability(local, "test")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			second.health.SetContext(ctx)
			if err := second.recover(ctx); err != nil {
				t.Fatalf("recover: %v", err)
			}
			select {
			case command := <-rt.commands:
				if len(command) < 3 || command[0] != health.ProbeContainerPath || command[1] != string(checkType) || command[2] != "8080" {
					t.Fatalf("recovered probe command = %v, want %s on container port 8080", command, checkType)
				}
			case <-time.After(time.Second):
				t.Fatal("recovered health probe did not run")
			}
		})
	}
}

func TestResumeGroupRecoveredAllocationWithoutSpec(t *testing.T) {
	rt := &createdRecoveryRuntime{
		reconcilerRuntime: &reconcilerRuntime{status: runtime.StatusRunning},
		managedID:         "task",
		labels: map[string]string{
			"trellis.allocation-id":         "allocation",
			"trellis.allocation-generation": "1",
			"trellis.job-revision":         "1",
			"trellis.execution-hash":       "hash",
		},
	}
	local := storage.NewLocalStorage(t.TempDir())
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	agent := newOperationTestAgent(t, rt)
	agent.ConfigureDurability(local, "test")
	if err := agent.recover(context.Background()); err != nil {
		t.Fatalf("recover: %v", err)
	}
	recovered := agent.allocations["task"]
	if recovered == nil || recovered.Spec != nil || recovered.Status != "running" {
		t.Fatalf("recovered allocation = %+v, want running with nil spec", recovered)
	}
	request := &api.DrainAllocationRequest{AllocationID: "allocation", Generation: 1}
	if err := agent.DrainGroup(request); err != nil {
		t.Fatalf("drain recovered allocation: %v", err)
	}
	if err := agent.ResumeGroup(request); err != nil {
		t.Fatalf("resume recovered allocation: %v", err)
	}
	if recovered.Draining || agent.reconciler.states["task"].stopping {
		t.Fatal("recovered allocation remains restart-suppressed after resume")
	}
}

func TestRecoverStoppingAllocationDoesNotStartOrRestart(t *testing.T) {
	stopErr := errors.New("task delete failed")
	rt := &stoppedWithErrorRuntime{
		reconcilerRuntime: &reconcilerRuntime{status: runtime.StatusRunning},
		stopErr:           stopErr,
		managedID:         "task",
	}
	local := storage.NewLocalStorage(t.TempDir())
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}

	first := newOperationTestAgent(t, rt)
	first.ConfigureDurability(local, "test")
	first.allocations["task"] = &Allocation{
		ID: "task", AllocationID: "allocation", ContainerID: "task",
		Generation: 1, JobRevision: 1, ExecutionHash: "hash",
		Spec: &spec.TaskSpec{Name: "task", Image: "image"},
		Status: "running", Health: "healthy",
	}
	first.reconciler.Track("task", false, nil)
	if err := first.persistAllocation(first.allocations["task"]); err != nil {
		t.Fatal(err)
	}
	if err := first.StopAllocation(context.Background(), "task"); !errors.Is(err, stopErr) {
		t.Fatalf("stop error = %v, want %v", err, stopErr)
	}
	if rt.status != runtime.StatusStopped {
		t.Fatalf("runtime status after failed stop = %q, want stopped", rt.status)
	}

	second := newOperationTestAgent(t, rt)
	second.ConfigureDurability(local, "test")
	if err := second.recover(context.Background()); err != nil {
		t.Fatalf("recover: %v", err)
	}
	recovered := second.allocations["task"]
	if recovered == nil || recovered.Status != "stopping" {
		t.Fatalf("recovered allocation = %+v, want stopping", recovered)
	}
	if rt.startCount != 0 {
		t.Fatalf("recovery started stopping allocation %d times", rt.startCount)
	}
	if err := second.reconciler.Reconcile(context.Background(), "task"); err != nil {
		t.Fatalf("reconcile recovered stopping allocation: %v", err)
	}
	if rt.restartCount != 0 {
		t.Fatalf("recovered stopping allocation restarted %d times", rt.restartCount)
	}
}

func TestRuntimeRecoveryMetadataRoundTrip(t *testing.T) {
	rt := &blockingStartRuntime{reconcilerRuntime: &reconcilerRuntime{}, started: make(chan string, 1), release: make(chan struct{}, 1), labels: map[string]map[string]string{}}
	agent := newOperationTestAgent(t, rt)
	request := operationTestRequest()
	request.Tasks = request.Tasks[:1]
	rt.release <- struct{}{}
	if err := agent.RunGroup(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	id := "allocation-g2-first"
	recovered := allocationFromRuntime(runtime.ContainerInfo{ID: id, Labels: rt.labels[id]})
	if recovered == nil || recovered.ExecutionHash != request.ExecutionHash || recovered.JobRevision != request.JobRevision {
		t.Fatalf("recovered metadata = %+v", recovered)
	}
	delete(rt.labels[id], "trellis.execution-hash")
	if allocationFromRuntime(runtime.ContainerInfo{ID: id, Labels: rt.labels[id]}) != nil {
		t.Fatal("container with insufficient fencing metadata was adopted")
	}
}
