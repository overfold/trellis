package agent

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	"github.com/overfold/trellis/orchestrator/internal/runtime"
)

type stalledReconcileRuntime struct {
	*reconcilerRuntime
	restart  bool
	entered  chan context.Context
	observed chan string
	release  chan struct{} // non-nil simulates a runtime ignoring cancellation
	calls    atomic.Int32
}

func (r *stalledReconcileRuntime) stall(ctx context.Context) error {
	r.calls.Add(1)
	r.entered <- ctx
	if r.release != nil {
		<-r.release
		return nil
	}
	<-ctx.Done()
	return ctx.Err()
}

func (r *stalledReconcileRuntime) Inspect(ctx context.Context, id string) (*runtime.ContainerInfo, error) {
	if id != "hung" {
		r.observed <- id
		return &runtime.ContainerInfo{Status: runtime.StatusRunning}, nil
	}
	if !r.restart {
		if err := r.stall(ctx); err != nil {
			return nil, err
		}
	}
	return &runtime.ContainerInfo{Status: runtime.StatusStopped}, nil
}

func (r *stalledReconcileRuntime) Restart(ctx context.Context, _ string) error {
	return r.stall(ctx)
}

func newStalledReconcileRuntime(restart bool) *stalledReconcileRuntime {
	return &stalledReconcileRuntime{reconcilerRuntime: &reconcilerRuntime{}, restart: restart,
		entered: make(chan context.Context, 10), observed: make(chan string, 100)}
}

func TestReconcileStallDoesNotBlockPollingOrShutdown(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(map[bool]string{false: "inspect", true: "restart"}[restart], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				rt := newStalledReconcileRuntime(restart)
				r := NewAllocationReconciler(rt, nil)
				r.Track("hung", false, testRestartPolicy())
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				done := make(chan struct{})
				go func() { r.Run(ctx); close(done) }()
				time.Sleep(3 * time.Second)
				attempt := <-rt.entered
				deadline, ok := attempt.Deadline()
				if !ok || time.Until(deadline) != 10*time.Second {
					t.Fatalf("runtime deadline = %v, present = %v", deadline, ok)
				}
				r.Track("other", false, testRestartPolicy())
				time.Sleep(3 * time.Second)
				synctest.Wait()
				select {
				case <-rt.observed:
				default:
					t.Fatal("hung allocation blocked unrelated polling")
				}
				if rt.calls.Load() != 1 {
					t.Fatal("overlapping passes for hung allocation")
				}
				time.Sleep(7 * time.Second)
				synctest.Wait()
				if !errors.Is(attempt.Err(), context.DeadlineExceeded) {
					t.Fatalf("attempt error = %v", attempt.Err())
				}
				// A timed-out allocation remains retryable on the next tick.
				time.Sleep(2 * time.Second)
				second := <-rt.entered
				cancel()
				<-done
				synctest.Wait()
				if !errors.Is(second.Err(), context.Canceled) {
					t.Fatalf("shutdown did not cancel attempt: %v", second.Err())
				}
			})
		})
	}
}

func TestLifecycleCancelsHungReconciliation(t *testing.T) {
	for _, restart := range []bool{false, true} {
		for _, drain := range []bool{false, true} {
			t.Run(map[bool]string{false: "inspect", true: "restart"}[restart]+"/"+map[bool]string{false: "stop", true: "drain"}[drain], func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					rt := newStalledReconcileRuntime(restart)
					a := newOperationTestAgent(t, rt)
					a.allocations["hung"] = &Allocation{ID: "hung", AllocationID: "alloc", Generation: 1, ContainerID: "hung"}
					a.reconciler.Track("hung", false, testRestartPolicy())
					done := make(chan error, 1)
					go func() { done <- a.reconciler.Reconcile(context.Background(), "hung") }()
					attempt := <-rt.entered
					var err error
					if drain {
						err = a.DrainGroup(context.Background(), &nodeapi.DrainAllocationRequest{AllocationID: "alloc", Generation: 1, Epoch: 1, Sequence: 1})
					} else {
						err = a.StopGroup(context.Background(), &nodeapi.StopAllocationRequest{AllocationID: "alloc", Generation: 1, Epoch: 1})
					}
					if err != nil {
						t.Fatal(err)
					}
					if !errors.Is(<-done, context.Canceled) || !errors.Is(attempt.Err(), context.Canceled) {
						t.Fatal("lifecycle did not cancel reconciliation")
					}
				})
			})
		}
	}
}

func TestLateRuntimeCompletionRemainsFenced(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(map[bool]string{false: "inspect", true: "restart"}[restart], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				rt := newStalledReconcileRuntime(restart)
				rt.release = make(chan struct{})
				subscriber := &statusRecorder{}
				a := newOperationTestAgent(t, rt)
				a.allocations["hung"] = &Allocation{ID: "hung", AllocationID: "alloc", Generation: 1, ContainerID: "hung"}
				r := a.reconciler
				r.Subscriber = subscriber
				r.Track("hung", false, testRestartPolicy())
				done := make(chan error, 1)
				go func() { done <- r.Reconcile(context.Background(), "hung") }()
				<-rt.entered
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := a.DrainGroup(ctx, &nodeapi.DrainAllocationRequest{AllocationID: "alloc", Generation: 1, Epoch: 1, Sequence: 1}); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("drain wait = %v", err)
				}
				stopAt := time.Now()
				if err := a.StopGroup(context.Background(), &nodeapi.StopAllocationRequest{AllocationID: "alloc", Generation: 1, Epoch: 1}); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("stop wait = %v", err)
				}
				if time.Since(stopAt) != 10*time.Second {
					t.Fatal("deadline-free stop did not use the default wait budget")
				}
				if a.allocations["hung"] == nil {
					t.Fatal("timed-out stop cleaned up an in-flight runtime operation")
				}
				// Resume cannot revive the cancelled pass or remove its exclusion.
				r.ResumeRestarts("hung", false, testRestartPolicy(), 0, time.Time{}, false)
				wait, cancelWait := context.WithTimeout(context.Background(), time.Second)
				defer cancelWait()
				if err := r.Reconcile(wait, "hung"); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("overlapping pass = %v", err)
				}
				r.Track("other", false, testRestartPolicy())
				if err := r.Reconcile(context.Background(), "other"); err != nil {
					t.Fatal(err)
				}
				close(rt.release)
				if !errors.Is(<-done, context.Canceled) {
					t.Fatal("late success escaped cancellation")
				}
				if len(subscriber.statuses) != 0 {
					t.Fatalf("late statuses = %v", subscriber.statuses)
				}
				if rt.calls.Load() != 1 {
					t.Fatal("late Inspect triggered Restart or overlapping runtime call")
				}
			})
		})
	}
}

func TestCancellationIgnoringRuntimeDoesNotBlockOtherLifecycleOrShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := newStalledReconcileRuntime(true)
		rt.release = make(chan struct{})
		a := newOperationTestAgent(t, rt)
		a.reconciler.Track("hung", false, testRestartPolicy())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan struct{})
		go func() { a.reconciler.Run(ctx); close(done) }()
		time.Sleep(3 * time.Second)
		<-rt.entered
		// The deadline expires, but a deliberately broken runtime keeps the
		// original pass outstanding. Later ticks must not create more calls.
		time.Sleep(20 * time.Second)
		synctest.Wait()
		if rt.calls.Load() != 1 {
			t.Fatal("polling accumulated hung runtime calls")
		}
		a.mu.Lock()
		a.allocations["other"] = &Allocation{ID: "other", AllocationID: "other", Generation: 1, ContainerID: "other"}
		a.mu.Unlock()
		a.reconciler.Track("other", false, testRestartPolicy())
		if err := a.DrainGroup(context.Background(), &nodeapi.DrainAllocationRequest{AllocationID: "other", Generation: 1, Epoch: 1, Sequence: 1}); err != nil {
			t.Fatal(err)
		}
		if err := a.StopGroup(context.Background(), &nodeapi.StopAllocationRequest{AllocationID: "other", Generation: 1, Epoch: 1}); err != nil {
			t.Fatal(err)
		}
		if err := a.StartGroup(context.Background(), singleTaskRequest()); err != nil {
			t.Fatal(err)
		}
		cancel()
		<-done // Run must return before the broken runtime releases.
		close(rt.release)
		synctest.Wait()
	})
}
