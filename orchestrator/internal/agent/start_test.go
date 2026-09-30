package agent

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/overfold/trellis/orchestrator/internal/api"
	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	"github.com/overfold/trellis/orchestrator/internal/runtime"
	"github.com/overfold/trellis/orchestrator/internal/storage"
)

// slowPullRuntime blocks image pulls until released and counts pulls and
// creates.
type slowPullRuntime struct {
	*reconcilerRuntime
	mu      sync.Mutex
	pulling chan string
	release chan error
	pulls   int
	creates int
}

func newSlowPullRuntime() *slowPullRuntime {
	return &slowPullRuntime{reconcilerRuntime: &reconcilerRuntime{status: runtime.StatusRunning}, pulling: make(chan string, 8), release: make(chan error)}
}

func (r *slowPullRuntime) Pull(ctx context.Context, image string) error {
	r.mu.Lock()
	r.pulls++
	r.mu.Unlock()
	r.pulling <- image
	select {
	case err := <-r.release:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *slowPullRuntime) Create(_ context.Context, options runtime.CreateOptions) (string, error) {
	r.mu.Lock()
	r.creates++
	r.mu.Unlock()
	return options.ID, nil
}

func (r *slowPullRuntime) counts() (int, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.pulls, r.creates
}

func singleTaskRequest() *api.AllocationRequest {
	request := operationTestRequest()
	request.Tasks = request.Tasks[:1]
	return request
}

func waitPulling(t *testing.T, rt *slowPullRuntime) {
	t.Helper()
	select {
	case <-rt.pulling:
	case <-time.After(time.Second):
		t.Fatal("image pull did not start")
	}
}

func waitStart(t *testing.T, agent *Agent, allocationID string) *groupStart {
	t.Helper()
	agent.mu.RLock()
	start := agent.starts[allocationID]
	agent.mu.RUnlock()
	if start == nil {
		t.Fatal("no start in progress")
	}
	select {
	case <-start.done:
	case <-time.After(time.Second):
		t.Fatal("start did not finish")
	}
	return start
}

func statusesFor(agent *Agent, allocationID string) []api.AllocationStatus {
	var result []api.AllocationStatus
	for _, status := range agent.allocationStatuses() {
		if status.ID == allocationID {
			result = append(result, status)
		}
	}
	return result
}

func TestStartGroupAcceptsBeforeSlowPullAndReportsStarting(t *testing.T) {
	rt := newSlowPullRuntime()
	agent := newOperationTestAgent(t, rt)
	request := singleTaskRequest()
	if err := agent.StartGroup(context.Background(), request); err != nil {
		t.Fatalf("accept start: %v", err)
	}
	waitPulling(t, rt)
	statuses := statusesFor(agent, request.AllocationID)
	if len(statuses) != 1 || statuses[0].Phase != lifecycle.PhaseStarting || statuses[0].Generation != request.Generation || statuses[0].Task != "first" || statuses[0].StartFailure != nil {
		t.Fatalf("heartbeat during pull = %+v, want one starting task", statuses)
	}
	// A retry of the same generation during the pull is accepted without
	// starting again.
	request.Attempt = 1
	if err := agent.StartGroup(context.Background(), request); err != nil {
		t.Fatalf("retry during pull: %v", err)
	}
	rt.release <- nil
	start := waitStart(t, agent, request.AllocationID)
	if start.err != nil {
		t.Fatalf("start error = %v", start.err)
	}
	if pulls, creates := rt.counts(); pulls != 1 || creates != 1 {
		t.Fatalf("pulls=%d creates=%d, want one of each", pulls, creates)
	}
	statuses = statusesFor(agent, request.AllocationID)
	if len(statuses) != 1 || statuses[0].Phase != lifecycle.PhaseRunning {
		t.Fatalf("heartbeat after start = %+v, want running", statuses)
	}
	agent.mu.RLock()
	remaining := agent.starts[request.AllocationID]
	agent.mu.RUnlock()
	if remaining != nil {
		t.Fatal("completed start still tracked")
	}
	// Retrying a running generation starts nothing new.
	if err := runGroup(context.Background(), agent, request); err != nil {
		t.Fatalf("retry after start: %v", err)
	}
	if _, creates := rt.counts(); creates != 1 {
		t.Fatalf("creates after retry = %d, want 1", creates)
	}
}

func TestStartFailureIsReportedUntilNextAttempt(t *testing.T) {
	rt := newSlowPullRuntime()
	agent := newOperationTestAgent(t, rt)
	request := singleTaskRequest()
	request.Attempt = 2
	if err := agent.StartGroup(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	waitPulling(t, rt)
	rt.release <- errors.New("registry unavailable")
	waitStart(t, agent, request.AllocationID)
	statuses := statusesFor(agent, request.AllocationID)
	if len(statuses) != 1 || statuses[0].Phase != lifecycle.PhaseStarting || statuses[0].StartFailure == nil || statuses[0].StartFailure.Attempt != 2 {
		t.Fatalf("heartbeat after failure = %+v, want starting with attempt 2 failure", statuses)
	}
	// The same attempt keeps the failure until the control plane counts it.
	if err := agent.StartGroup(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if pulls, _ := rt.counts(); pulls != 1 {
		t.Fatalf("same-attempt retry pulled again: %d pulls", pulls)
	}
	if statuses := statusesFor(agent, request.AllocationID); len(statuses) != 1 || statuses[0].StartFailure == nil {
		t.Fatalf("failure not kept for same attempt: %+v", statuses)
	}
	// The next attempt starts again.
	request.Attempt = 3
	if err := agent.StartGroup(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	waitPulling(t, rt)
	if statuses := statusesFor(agent, request.AllocationID); len(statuses) != 1 || statuses[0].StartFailure != nil {
		t.Fatalf("new attempt still reports old failure: %+v", statuses)
	}
	rt.release <- nil
	if start := waitStart(t, agent, request.AllocationID); start.err != nil {
		t.Fatalf("second attempt: %v", start.err)
	}
}

func TestStopCancelsStartDuringPull(t *testing.T) {
	rt := newSlowPullRuntime()
	agent := newOperationTestAgent(t, rt)
	request := singleTaskRequest()
	if err := agent.StartGroup(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	waitPulling(t, rt)
	stop := &api.StopAllocationRequest{AllocationID: request.AllocationID, Generation: request.Generation, Epoch: request.Epoch}
	if err := agent.StopGroup(context.Background(), stop); err != nil {
		t.Fatalf("stop during pull: %v", err)
	}
	if _, creates := rt.counts(); creates != 0 {
		t.Fatalf("stopped start created %d containers", creates)
	}
	if statuses := statusesFor(agent, request.AllocationID); len(statuses) != 0 {
		t.Fatalf("heartbeat after stop = %+v, want nothing", statuses)
	}
}

func TestStartFencesAgainstStartInProgress(t *testing.T) {
	rt := newSlowPullRuntime()
	agent := newOperationTestAgent(t, rt)
	request := singleTaskRequest()
	if err := agent.StartGroup(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	waitPulling(t, rt)

	older := singleTaskRequest()
	older.Generation = request.Generation - 1
	if err := agent.StartGroup(context.Background(), older); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("older start = %v, want stale generation", err)
	}
	if err := agent.StopGroup(context.Background(), &api.StopAllocationRequest{AllocationID: request.AllocationID, Generation: older.Generation, Epoch: request.Epoch}); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("older stop = %v, want stale generation", err)
	}
	if err := agent.DrainGroup(&api.DrainAllocationRequest{AllocationID: request.AllocationID, Generation: older.Generation, Epoch: request.Epoch, Sequence: 9}); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("older drain = %v, want stale generation", err)
	}
	conflict := singleTaskRequest()
	conflict.ExecutionHash = "other"
	if err := agent.StartGroup(context.Background(), conflict); !errors.Is(err, ErrExecutionConflict) {
		t.Fatalf("conflicting start = %v, want execution conflict", err)
	}
	stale := singleTaskRequest()
	if err := agent.AcceptEpoch(5); err != nil {
		t.Fatal(err)
	}
	stale.Epoch = 4
	if err := agent.StartGroup(context.Background(), stale); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("stale epoch start = %v, want stale epoch", err)
	}

	// A drain delivered during the pull applies when the task is created.
	if err := agent.DrainGroup(&api.DrainAllocationRequest{AllocationID: request.AllocationID, Generation: request.Generation, Epoch: 5, Sequence: 3}); err != nil {
		t.Fatalf("drain during pull: %v", err)
	}
	// The leader of the new epoch re-sends the start it still wants.
	request.Epoch = 5
	if err := agent.StartGroup(context.Background(), request); err != nil {
		t.Fatalf("retry at new epoch: %v", err)
	}
	rt.release <- nil
	if start := waitStart(t, agent, request.AllocationID); start.err != nil {
		t.Fatalf("start: %v", start.err)
	}
	id := taskRecordID(request.AllocationID, request.Generation, "first")
	agent.mu.RLock()
	task := agent.allocations[id]
	agent.mu.RUnlock()
	if task == nil || !task.Draining || task.DrainSequence != 3 {
		t.Fatalf("task after drained start = %+v, want draining at sequence 3", task)
	}
	if state := agent.reconciler.states[id]; state == nil || !state.stopping {
		t.Fatal("task drained during its pull is not restart-suppressed")
	}
}

func TestNewerGenerationCancelsStartInProgress(t *testing.T) {
	rt := newSlowPullRuntime()
	agent := newOperationTestAgent(t, rt)
	request := singleTaskRequest()
	if err := agent.StartGroup(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	waitPulling(t, rt)
	newer := singleTaskRequest()
	newer.Generation = request.Generation + 1
	if err := agent.StartGroup(context.Background(), newer); err != nil {
		t.Fatalf("newer generation start: %v", err)
	}
	waitPulling(t, rt)
	rt.release <- nil
	if start := waitStart(t, agent, newer.AllocationID); start.err != nil || start.generation != newer.Generation {
		t.Fatalf("newer start generation=%d error=%v", start.generation, start.err)
	}
	statuses := statusesFor(agent, request.AllocationID)
	if len(statuses) != 1 || statuses[0].Generation != newer.Generation || statuses[0].Phase != lifecycle.PhaseRunning {
		t.Fatalf("heartbeat = %+v, want only the newer generation running", statuses)
	}
}

func TestAgentRestartDuringPullRecoversOnRetriedStart(t *testing.T) {
	root := t.TempDir()
	local := storage.NewLocalStorage(root)
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	rt := newSlowPullRuntime()
	first := newOperationTestAgent(t, rt)
	first.ConfigureDurability(local, "test")
	ctx, cancel := context.WithCancel(context.Background())
	first.lifetime = ctx
	request := singleTaskRequest()
	if err := first.StartGroup(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	waitPulling(t, rt)
	// The agent stops mid-pull: nothing durable describes the start.
	first.mu.RLock()
	interrupted := first.starts[request.AllocationID]
	first.mu.RUnlock()
	cancel()
	select {
	case <-interrupted.done:
	case <-time.After(time.Second):
		t.Fatal("interrupted start did not finish")
	}
	if records, errs := local.ListRaw("agent/allocations"); len(errs) != 0 || len(records) != 0 {
		t.Fatalf("records after interrupted pull = %d, errors %v; want none", len(records), errs)
	}

	second := newOperationTestAgent(t, &reconcilerRuntime{status: runtime.StatusRunning})
	second.ConfigureDurability(local, "test")
	if err := second.recover(context.Background()); err != nil {
		t.Fatalf("recover after interrupted pull: %v", err)
	}
	if statuses := statusesFor(second, request.AllocationID); len(statuses) != 0 {
		t.Fatalf("restarted agent reports %+v, want nothing until the start is retried", statuses)
	}
	if err := runGroup(context.Background(), second, request); err != nil {
		t.Fatalf("retried start after restart: %v", err)
	}
	statuses := statusesFor(second, request.AllocationID)
	if len(statuses) != 1 || statuses[0].Phase != lifecycle.PhaseRunning {
		t.Fatalf("heartbeat after retried start = %+v, want running", statuses)
	}
}

func TestTruncateStartFailureKeepsValidUTF8(t *testing.T) {
	message := string(make([]byte, api.MaxStartFailureMessageBytes-1)) + "é\xff"
	got := truncateStartFailure(message)
	if len(got) > api.MaxStartFailureMessageBytes {
		t.Fatalf("length = %d", len(got))
	}
	for _, r := range got {
		if r == '�' {
			t.Fatal("truncated message contains a replacement rune")
		}
	}
}

func TestStartSupersededByNewEpochCreatesNothing(t *testing.T) {
	rt := newSlowPullRuntime()
	agent := newOperationTestAgent(t, rt)
	request := singleTaskRequest()
	if err := agent.StartGroup(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	waitPulling(t, rt)
	agent.mu.RLock()
	start := agent.starts[request.AllocationID]
	agent.mu.RUnlock()
	// A new leader takes over and has not asked for this start.
	if err := agent.AcceptEpoch(request.Epoch + 1); err != nil {
		t.Fatal(err)
	}
	rt.release <- nil
	<-start.done
	if !errors.Is(start.err, errStartSuperseded) {
		t.Fatalf("start error = %v, want superseded", start.err)
	}
	if _, creates := rt.counts(); creates != 0 {
		t.Fatalf("superseded start created %d containers", creates)
	}
	if statuses := statusesFor(agent, request.AllocationID); len(statuses) != 0 {
		t.Fatalf("superseded start reported %+v, want nothing", statuses)
	}
	request.Epoch++
	if err := agent.StartGroup(context.Background(), request); err != nil {
		t.Fatalf("new leader's start: %v", err)
	}
	waitPulling(t, rt)
	rt.release <- nil
	if start := waitStart(t, agent, request.AllocationID); start.err != nil {
		t.Fatalf("new leader's start: %v", start.err)
	}
}

func TestTerminalStartFailureCarriesCode(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code api.OperationCode
	}{
		{ErrRestartBudgetExhausted, api.OperationRestartExhausted},
		{ErrAllocationExists, api.OperationConflict},
		{ErrExecutionConflict, api.OperationConflict},
		{ErrStaleGeneration, api.OperationStaleGeneration},
		{errors.New("pull failed"), ""},
	} {
		if got := terminalStartCode(fmt.Errorf("start: %w", tc.err)); got != tc.code {
			t.Fatalf("code for %v = %q, want %q", tc.err, got, tc.code)
		}
	}
}

func TestFailedNewerStartDoesNotFenceOlderStop(t *testing.T) {
	rt := newSlowPullRuntime()
	agent := newOperationTestAgent(t, rt)
	request := singleTaskRequest()
	if err := agent.StartGroup(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	waitPulling(t, rt)
	rt.release <- errors.New("registry unavailable")
	waitStart(t, agent, request.AllocationID)
	older := &api.StopAllocationRequest{AllocationID: request.AllocationID, Generation: request.Generation - 1, Epoch: request.Epoch}
	if err := agent.StopGroup(context.Background(), older); err != nil {
		t.Fatalf("stop of older generation beside a failed start: %v", err)
	}
	if statuses := statusesFor(agent, request.AllocationID); len(statuses) != 1 || statuses[0].StartFailure == nil {
		t.Fatalf("failure no longer reported after older stop: %+v", statuses)
	}
}
