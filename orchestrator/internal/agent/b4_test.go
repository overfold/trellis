package agent

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	"github.com/overfold/trellis/orchestrator/internal/network"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	"github.com/overfold/trellis/orchestrator/internal/runtime"
	"github.com/overfold/trellis/orchestrator/internal/spec"
)

// A cooperative runtime must receive a finite deadline even after the start
// context expires. Release also lets the unchanged baseline probe terminate.
type failedStartBudgetRuntime struct {
	*reconcilerRuntime
	stage              string
	entered            chan context.Context
	release            chan struct{}
	ignoreCancellation bool
}

func (r *failedStartBudgetRuntime) block(ctx context.Context) error {
	r.entered <- ctx
	if r.ignoreCancellation {
		<-r.release
		return errors.New("released uncooperative operation")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.release:
		return errors.New("released probe")
	}
}

func (r *failedStartBudgetRuntime) Create(ctx context.Context, o runtime.CreateOptions) (string, error) {
	if r.stage == "create-inspect" {
		<-ctx.Done()
		return "", errors.New("ambiguous create")
	}
	return o.ID, nil
}
func (r *failedStartBudgetRuntime) Start(ctx context.Context, _ string) error {
	if r.stage == "launch" {
		r.entered <- ctx
	}
	<-ctx.Done()
	return ctx.Err()
}
func (r *failedStartBudgetRuntime) Inspect(ctx context.Context, _ string) (*runtime.ContainerInfo, error) {
	if r.stage == "stop" || r.stage == "remove" || r.stage == "launch" || r.stage == "detach" {
		return &runtime.ContainerInfo{Status: runtime.StatusStopped}, nil
	}
	return nil, r.block(ctx)
}
func (r *failedStartBudgetRuntime) Stop(ctx context.Context, _ string) error {
	if r.stage == "stop" {
		return r.block(ctx)
	}
	return nil
}
func (r *failedStartBudgetRuntime) Remove(ctx context.Context, _ string) error {
	if r.stage == "remove" {
		return r.block(ctx)
	}
	return nil
}

type failedStartNetwork struct {
	staticNetworkManager
	runtime *failedStartBudgetRuntime
}

func (m failedStartNetwork) Detach(ctx context.Context, _ *network.Attachment) error {
	return m.runtime.block(ctx)
}

func TestB4FailedStartCleanupBudget(t *testing.T) {
	for _, stage := range []string{"create-inspect", "start-inspect", "stop", "remove", "detach"} {
		t.Run(stage, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				rt := &failedStartBudgetRuntime{reconcilerRuntime: &reconcilerRuntime{}, stage: stage, entered: make(chan context.Context, 4), release: make(chan struct{})}
				a := newOperationTestAgent(t, rt)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				a.lifetime = ctx
				req := singleTaskRequest()
				if stage == "detach" {
					a.SetNetworkManager(failedStartNetwork{runtime: rt})
					req.Tasks[0].Networking = &spec.TaskNetworkingSpec{Mode: spec.TaskNetworkWireGuard}
					req.NetworkPlan = &network.Plan{}
				}
				if err := a.StartGroup(context.Background(), req); err != nil {
					t.Fatal(err)
				}
				attempt := <-rt.entered
				_, bounded := attempt.Deadline()
				if !bounded {
					close(rt.release)
					waitStart(t, a, req.AllocationID)
					t.Fatal("failed-start runtime operation has no deadline")
				}
				time.Sleep(time.Minute)
				start := waitStart(t, a, req.AllocationID)
				if start.err == nil {
					t.Fatal("failed start reported success")
				}
				id := taskRecordID(req.AllocationID, req.Generation, req.Tasks[0].Name)
				if stage != "start-inspect" && a.allocations[id] == nil {
					t.Fatal("uncertain runtime lost resource record")
				}
			})
		})
	}
}

func TestB4CancelledAllocationControlWait(t *testing.T) {
	for _, drain := range []bool{false, true} {
		t.Run(fmt.Sprint("drain=", drain), func(t *testing.T) {
			a := newOperationTestAgent(t, &reconcilerRuntime{})
			unlock := a.lockAllocationOperation("held")
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				if drain {
					done <- a.DrainGroup(ctx, &nodeapi.DrainAllocationRequest{AllocationID: "held", Generation: 1, Epoch: 1, Sequence: 1})
				} else {
					done <- a.StopGroup(ctx, &nodeapi.StopAllocationRequest{AllocationID: "held", Generation: 1, Epoch: 1})
				}
			}()
			select {
			case err := <-done:
				unlock()
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("wait error = %v", err)
				}
			case <-time.After(time.Second):
				unlock()
				<-done
				t.Fatal("cancelled control still waits for allocation lock")
			}
		})
	}
}

func TestB4BackgroundStartAdmission(t *testing.T) {
	rt := newSlowPullRuntime()
	rt.pulling = make(chan string, 16)
	a := newOperationTestAgent(t, rt)
	ctx, cancel := context.WithCancel(context.Background())
	a.lifetime = ctx
	defer cancel()
	post := func(req *nodeapi.AllocationRequest) int {
		t.Helper()
		req.Tasks[0].Resources = &spec.ResourcesSpec{CPU: 100, Memory: 1 << 20}
		req.Tasks[0].Networking = &spec.TaskNetworkingSpec{Mode: spec.TaskNetworkNone}
		err := a.StartGroup(ctx, req)
		if errors.Is(err, ErrStartLimit) {
			return http.StatusServiceUnavailable
		}
		if err != nil {
			return http.StatusConflict
		}
		return http.StatusOK
	}
	committed := singleTaskRequest()
	committed.AllocationID = "committed"
	if status := post(committed); status != http.StatusOK {
		t.Fatalf("initial committed start status = %d", status)
	}
	waitPulling(t, rt)
	a.mu.RLock()
	initial := a.starts[committed.AllocationID]
	a.mu.RUnlock()
	rt.release <- nil
	<-initial.done
	accepted := 0
	for i := range 9 {
		req := singleTaskRequest()
		req.AllocationID = fmt.Sprintf("admission-%d", i)
		if status := post(req); status != http.StatusOK {
			if status != http.StatusServiceUnavailable {
				t.Fatalf("admission status = %d", status)
			}
			break
		}
		accepted++
	}
	if accepted != 8 {
		t.Fatalf("accepted %d distinct starts, want bounded admission of 8", accepted)
	}
	for range 8 {
		waitPulling(t, rt)
	}
	if status := post(committed); status != http.StatusOK {
		t.Fatalf("committed duplicate at capacity status = %d", status)
	}
	req := singleTaskRequest()
	req.AllocationID = "admission-0"
	req.Generation--
	if status := post(req); status != http.StatusConflict {
		t.Fatalf("stale generation at capacity status = %d", status)
	}
	req.Generation++
	req.ExecutionHash = "conflicting-execution"
	if status := post(req); status != http.StatusConflict {
		t.Fatalf("conflicting execution at capacity status = %d", status)
	}
	req.ExecutionHash = committed.ExecutionHash
	if status := post(req); status != http.StatusOK {
		t.Fatalf("duplicate at capacity status = %d", status)
	}
	if err := a.DrainGroup(context.Background(), &nodeapi.DrainAllocationRequest{AllocationID: req.AllocationID, Generation: req.Generation, Epoch: req.Epoch, Sequence: 1}); err != nil {
		t.Fatal(err)
	}
	if err := a.StopGroup(context.Background(), &nodeapi.StopAllocationRequest{AllocationID: req.AllocationID, Generation: req.Generation, Epoch: req.Epoch}); err != nil {
		t.Fatal(err)
	}
	req.AllocationID = "recovered-capacity"
	if status := post(req); status != http.StatusOK {
		t.Fatalf("capacity recovery status = %d", status)
	}
	waitPulling(t, rt)
	cancel()
	a.mu.RLock()
	var done []<-chan struct{}
	for _, start := range a.starts {
		done = append(done, start.done)
	}
	a.mu.RUnlock()
	for _, ch := range done {
		<-ch
	}
}

func TestB4StoppedNoProbeRestartFailureHealth(t *testing.T) {
	rt := &reconcilerRuntime{status: runtime.StatusStopped, restartErr: errors.New("restart unavailable")}
	a := newOperationTestAgent(t, rt)
	a.reconciler.Subscriber = a
	a.allocations["task"] = &Allocation{ID: "task", AllocationID: "allocation", Generation: 1, TaskName: "task", Status: "running", Health: "healthy"}
	a.reconciler.Track("task", false, testRestartPolicy())
	if err := a.reconciler.Reconcile(context.Background(), "task"); err == nil {
		t.Fatal("restart unexpectedly succeeded")
	}
	statuses := statusesFor(a, "allocation")
	if len(statuses) != 1 || statuses[0].Phase != lifecycle.PhaseRunning || statuses[0].Health != lifecycle.HealthUnhealthy {
		t.Fatalf("stopped heartbeat = %+v", statuses)
	}
	rt.restartErr = nil
	if err := a.reconciler.Reconcile(context.Background(), "task"); err != nil {
		t.Fatal(err)
	}
	if statusesFor(a, "allocation")[0].Health != lifecycle.HealthHealthy {
		t.Fatal("successful recovery did not restore health")
	}
	// An ambiguous restart response can leave execution running. A fresh
	// inspection is sufficient evidence, without consuming another attempt.
	rt.restartErr = errors.New("lost restart response")
	if err := a.reconciler.Reconcile(context.Background(), "task"); err == nil {
		t.Fatal("ambiguous restart unexpectedly succeeded")
	}
	before := rt.restartCount
	rt.status = runtime.StatusRunning
	if err := a.reconciler.Reconcile(context.Background(), "task"); err != nil {
		t.Fatal(err)
	}
	if statusesFor(a, "allocation")[0].Health != lifecycle.HealthHealthy || rt.restartCount != before {
		t.Fatal("fresh execution evidence did not restore health without another restart")
	}
}

func TestB4StopCancelsLaunchBeforeLock(t *testing.T) {
	rt := &failedStartBudgetRuntime{reconcilerRuntime: &reconcilerRuntime{}, stage: "launch", entered: make(chan context.Context, 1)}
	a := newOperationTestAgent(t, rt)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a.lifetime = ctx
	req := singleTaskRequest()
	req.Epoch = 2
	if err := a.StartGroup(ctx, req); err != nil {
		t.Fatal(err)
	}
	attempt := <-rt.entered
	done := make(chan error, 1)
	go func() {
		done <- a.StopGroup(ctx, &nodeapi.StopAllocationRequest{AllocationID: req.AllocationID, Generation: req.Generation, Epoch: req.Epoch})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		cancel()
		<-done
		t.Fatal("stop waited for launch instead of cancelling it")
	}
	statuses := statusesFor(a, req.AllocationID)
	if !errors.Is(attempt.Err(), context.Canceled) || len(statuses) != 1 || !statuses[0].CleanupComplete {
		t.Fatal("stop did not cancel launch and finish cleanup")
	}
}

func TestB4UncooperativeCleanupRetainsExclusionAndCapacity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt := &failedStartBudgetRuntime{reconcilerRuntime: &reconcilerRuntime{}, stage: "stop", entered: make(chan context.Context, 16), release: make(chan struct{}), ignoreCancellation: true}
		defer func() {
			select {
			case <-rt.release:
			default:
				close(rt.release)
			}
		}()
		a := newOperationTestAgent(t, rt)
		for i := range 8 {
			req := singleTaskRequest()
			req.AllocationID = fmt.Sprintf("uncertain-%d", i)
			if err := a.StartGroup(context.Background(), req); err != nil {
				t.Fatal(err)
			}
		}
		var attempts []context.Context
		for range 8 {
			attempts = append(attempts, <-rt.entered)
		}
		time.Sleep(time.Minute)
		for _, ctx := range attempts {
			if ctx.Err() == nil {
				t.Fatal("cleanup received no finite budget")
			}
		}
		req := singleTaskRequest()
		req.AllocationID = "overflow"
		if err := a.StartGroup(context.Background(), req); err == nil {
			t.Fatal("expired cleanup freed a still-active start slot")
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := a.StopGroup(ctx, &nodeapi.StopAllocationRequest{AllocationID: "uncertain-0", Generation: req.Generation, Epoch: req.Epoch}); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("stop exclusion = %v", err)
		}
		if len(a.GetAllocations()) != 8 {
			t.Fatal("timed-out control cleaned uncertain runtime resources")
		}
		// Actual completion, not deadline expiry, recovers capacity.
		close(rt.release)
		synctest.Wait()
		req.AllocationID = "capacity-recovered"
		if err := a.StartGroup(context.Background(), req); err != nil {
			t.Fatalf("completion did not recover capacity: %v", err)
		}
		a.mu.RLock()
		start := a.starts[req.AllocationID]
		a.mu.RUnlock()
		start.cancel()
		<-start.done
	})
}
