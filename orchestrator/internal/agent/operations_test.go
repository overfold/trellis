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
}

type failingStopRuntime struct {
	*reconcilerRuntime
	stopErr     error
	removeCount int
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

func (r *failingStopRuntime) Stop(context.Context, string) error { return r.stopErr }
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
	return options.ID, nil
}

func (r *blockingStartRuntime) Start(_ context.Context, id string) error {
	r.started <- id
	<-r.release
	return nil
}

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
	if !errors.Is(err, stopErr) || !strings.Contains(err.Error(), "persist allocation") {
		t.Fatalf("run error = %v, want persistence and stop failures", err)
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
	rt.stopErr = nil
	if err := agent.StopAllocation(context.Background(), id); err != nil {
		t.Fatalf("retry stop: %v", err)
	}
	if rt.removeCount != 1 || manager.detachCount != 1 {
		t.Fatalf("retry removed container %d times, detached network %d times", rt.removeCount, manager.detachCount)
	}
	if _, err := os.Stat(alloc.SecretDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("secret directory after retry: %v", err)
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
