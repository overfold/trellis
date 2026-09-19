package agent

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/health"
	"github.com/clofour/trellis/internal/runtime"
	"github.com/clofour/trellis/internal/spec"
	"github.com/google/uuid"
)

type blockingStartRuntime struct {
	*reconcilerRuntime
	started chan string
	release chan struct{}
	labels  map[string]map[string]string
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
