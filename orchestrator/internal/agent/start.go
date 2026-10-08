package agent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	"github.com/overfold/trellis/orchestrator/internal/spec"
)

// groupStart is an accepted allocation start. The agent pulls its images and
// creates its tasks in the background. Heartbeats report its tasks that have
// no record yet as starting, and report its failure until the control plane
// retries with another attempt. It is not persisted: after an agent restart,
// the control plane's next start request for the generation starts it again.
type groupStart struct {
	generation    uint64
	jobRevision   int
	executionHash string
	tasks         []string
	cancel        context.CancelFunc
	done          chan struct{}

	// The fields below are guarded by Agent.mu. attempt and epoch are the
	// newest start attempt and control epoch that requested this start;
	// draining and drainSequence hold the newest drain state delivered while
	// the start runs; err and failed are set before done closes.
	attempt       int
	epoch         uint64
	draining      bool
	drainSequence uint64
	err           error
	failed        bool
}

// finished reports whether the background start returned.
func (s *groupStart) finished() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

func (s *groupStart) mergeDrain(draining bool, sequence uint64) {
	if sequence > s.drainSequence {
		s.draining, s.drainSequence = draining, sequence
	}
}

// maxStartDuration bounds a background start, so a pull that never completes
// is reported as a failed attempt instead of leaving the allocation starting.
const maxStartDuration = 30 * time.Minute

// errStartSuperseded ends a start whose control epoch was superseded before it
// created any task; the new leader's retry starts it again.
var errStartSuperseded = errors.New("allocation start superseded by a newer control epoch")

func taskRecordID(allocationID string, generation uint64, task string) string {
	return fmt.Sprintf("%s-g%d-%s", allocationID, generation, task)
}

// StartGroup fences an allocation start and runs it in the background. It
// returns once the start is accepted. A retry for the generation and execution
// already starting, running, or awaiting the control plane's view of its
// failure is accepted without starting again.
func (a *Agent) StartGroup(ctx context.Context, request *nodeapi.AllocationRequest) error {
	_, err := a.acceptStart(ctx, request)
	return err
}

func (a *Agent) acceptStart(ctx context.Context, request *nodeapi.AllocationRequest) (*groupStart, error) {
	// A retry of a running start is accepted without the operation lock,
	// which the start holds while it creates tasks.
	if start, err := a.acceptRunningStart(request); start != nil || err != nil {
		return start, err
	}
	for {
		unlock := a.lockAllocationOperation(request.AllocationID)
		start, wait, err := a.acceptStartLocked(request)
		unlock()
		if wait == nil {
			return start, err
		}
		// An older generation is still starting. It was cancelled; let its
		// cleanup finish before this generation replaces it.
		select {
		case <-wait:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// acceptRunningStart accepts a retry of the running start of the same
// generation and execution. It returns nil when there is none.
func (a *Agent) acceptRunningStart(request *nodeapi.AllocationRequest) (*groupStart, error) {
	if err := a.AcceptEpoch(request.Epoch); err != nil {
		return nil, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	current := a.starts[request.AllocationID]
	if current == nil || current.finished() || current.generation != request.Generation || current.jobRevision != request.JobRevision || current.executionHash != request.ExecutionHash {
		return nil, nil
	}
	current.mergeRetry(request)
	return current, nil
}

// mergeRetry records a retry of a running start: its failure is reported
// against the newest attempt, and the newest epoch owns it. The caller holds
// Agent.mu.
func (s *groupStart) mergeRetry(request *nodeapi.AllocationRequest) {
	s.attempt = max(s.attempt, request.Attempt)
	s.epoch = max(s.epoch, request.Epoch)
	s.mergeDrain(request.Draining, request.DrainSequence)
}

// acceptStartLocked registers a start, or returns the channel of an older
// generation's start that must finish first. The caller holds the allocation
// operation lock.
func (a *Agent) acceptStartLocked(request *nodeapi.AllocationRequest) (*groupStart, <-chan struct{}, error) {
	if err := a.AcceptEpoch(request.Epoch); err != nil {
		return nil, nil, err
	}
	if err := a.fenceStart(request); err != nil {
		return nil, nil, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if current := a.starts[request.AllocationID]; current != nil {
		switch {
		case current.generation > request.Generation:
			return nil, nil, fmt.Errorf("%w: current %d, requested %d", ErrStaleGeneration, current.generation, request.Generation)
		case current.generation < request.Generation:
			if !current.finished() {
				current.cancel()
				return nil, current.done, nil
			}
			delete(a.starts, request.AllocationID)
		case current.jobRevision != request.JobRevision || current.executionHash != request.ExecutionHash:
			return nil, nil, fmt.Errorf("%w: allocation %s generation %d", ErrExecutionConflict, request.AllocationID, request.Generation)
		case !current.finished():
			current.mergeRetry(request)
			return current, nil, nil
		case current.attempt == request.Attempt:
			// The control plane has not counted this failure yet; keep
			// reporting it rather than hiding it behind a new attempt.
			return current, nil, nil
		default:
			delete(a.starts, request.AllocationID)
		}
	}
	tasks := make([]string, len(request.Tasks))
	for i := range request.Tasks {
		tasks[i] = request.Tasks[i].Name
		id := taskRecordID(request.AllocationID, request.Generation, tasks[i])
		if a.orphanDetaches[id] {
			return nil, nil, fmt.Errorf("orphaned network cleanup for task %s is still in progress; retry the start", id)
		}
	}
	ctx, cancel := context.WithTimeout(a.lifetimeContext(), maxStartDuration)
	start := &groupStart{
		generation:    request.Generation,
		jobRevision:   request.JobRevision,
		executionHash: request.ExecutionHash,
		tasks:         tasks,
		cancel:        cancel,
		done:          make(chan struct{}),
		attempt:       request.Attempt,
		epoch:         request.Epoch,
		draining:      request.Draining,
		drainSequence: request.DrainSequence,
	}
	if a.starts == nil {
		a.starts = make(map[string]*groupStart)
	}
	a.starts[request.AllocationID] = start
	go a.runStart(ctx, start, cloneStartRequest(request))
	return start, nil, nil
}

func (a *Agent) lifetimeContext() context.Context {
	if a.lifetime != nil {
		return a.lifetime
	}
	return context.Background()
}

// cloneStartRequest copies a start request for its background start. The
// handler clears the delivered secret values when it returns.
func cloneStartRequest(request *nodeapi.AllocationRequest) *nodeapi.AllocationRequest {
	clone := *request
	clone.Tasks = append([]spec.TaskSpec(nil), request.Tasks...)
	clone.EnvOverrides = maps.Clone(request.EnvOverrides)
	clone.Secrets = append([]nodeapi.DeliveredSecret(nil), request.Secrets...)
	for i := range clone.Secrets {
		clone.Secrets[i].Value = bytes.Clone(request.Secrets[i].Value)
	}
	return &clone
}

// runStart pulls the images of a start without holding the allocation
// operation lock, so a slow pull delays neither stops, drains, and retries of
// the allocation nor any other allocation. It then creates and starts the
// tasks under the lock.
func (a *Agent) runStart(ctx context.Context, start *groupStart, request *nodeapi.AllocationRequest) {
	defer func() {
		for i := range request.Secrets {
			clear(request.Secrets[i].Value)
		}
	}()
	err := a.pullImages(ctx, request)
	if err == nil {
		unlock := a.lockAllocationOperation(request.AllocationID)
		if err = ctx.Err(); err == nil {
			err = a.runGroupTasks(ctx, start, request)
		}
		unlock()
	}
	if err != nil {
		a.log.Error("start allocation failed", "allocation", request.AllocationID, "generation", request.Generation, "error", err)
	}
	a.finishStart(ctx, request.AllocationID, start, err)
}

// finishStart publishes a start's result. A failure, including exceeding
// maxStartDuration, is kept for heartbeats unless a stop, a newer generation,
// a newer control epoch, or agent shutdown ended the start.
func (a *Agent) finishStart(ctx context.Context, allocationID string, start *groupStart, err error) {
	cancelled := errors.Is(ctx.Err(), context.Canceled) || errors.Is(err, errStartSuperseded)
	a.mu.Lock()
	start.err = err
	start.failed = err != nil && !cancelled
	if a.starts[allocationID] == start && !start.failed {
		delete(a.starts, allocationID)
	}
	close(start.done)
	a.mu.Unlock()
	start.cancel()
}

// pullImages pulls each image of the tasks that are not already running for
// this generation.
func (a *Agent) pullImages(ctx context.Context, request *nodeapi.AllocationRequest) error {
	pulled := make(map[string]bool, len(request.Tasks))
	for i := range request.Tasks {
		task := &request.Tasks[i]
		if pulled[task.Image] {
			continue
		}
		a.mu.RLock()
		existing := a.allocations[taskRecordID(request.AllocationID, request.Generation, task.Name)]
		running := existing != nil && existing.Status == "running" && !existing.unobserved
		a.mu.RUnlock()
		if running {
			continue
		}
		if err := a.runtime.Pull(ctx, task.Image); err != nil {
			return fmt.Errorf("pull image %s: %w", task.Image, err)
		}
		pulled[task.Image] = true
	}
	return nil
}

// fenceStart rejects a start for a generation older than a recorded one, a
// different execution of a recorded generation, and a generation whose restart
// budget is exhausted. The caller holds the allocation operation lock.
func (a *Agent) fenceStart(request *nodeapi.AllocationRequest) error {
	a.mu.RLock()
	defer a.mu.RUnlock()
	var exhaustedTask string
	for _, allocation := range a.allocations {
		if allocation.AllocationID != request.AllocationID {
			continue
		}
		if allocation.Generation > request.Generation {
			return fmt.Errorf("%w: current %d, requested %d", ErrStaleGeneration, allocation.Generation, request.Generation)
		}
		if allocation.Generation == request.Generation && (allocation.JobRevision != request.JobRevision || allocation.ExecutionHash != request.ExecutionHash) {
			return fmt.Errorf("%w: allocation %s generation %d", ErrExecutionConflict, request.AllocationID, request.Generation)
		}
		if allocation.Generation == request.Generation && allocation.RestartExhausted && (exhaustedTask == "" || allocation.TaskName < exhaustedTask) {
			exhaustedTask = allocation.TaskName
		}
	}
	// Reject after the fencing checks, and before touching any task, so a
	// start retry cannot churn the siblings of a task whose restart budget is
	// terminally exhausted. Pick the task deterministically.
	if exhaustedTask != "" {
		return fmt.Errorf("%w: allocation %s generation %d task %s", ErrRestartBudgetExhausted, request.AllocationID, request.Generation, exhaustedTask)
	}
	return nil
}

// cancelStartLocked cancels a start of the generation or an older one before
// a stop. It returns the channel to wait on while the start is still running,
// and rejects the stop when a newer generation is starting. The caller holds
// the allocation operation lock.
func (a *Agent) cancelStartLocked(allocationID string, generation uint64) (<-chan struct{}, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	start := a.starts[allocationID]
	if start == nil {
		return nil, nil
	}
	if start.generation > generation {
		// A failed newer start never stopped this generation; let the
		// control plane stop it while the failure stays reported.
		if start.finished() {
			return nil, nil
		}
		return nil, fmt.Errorf("%w: current %d, requested %d", ErrStaleGeneration, start.generation, generation)
	}
	if !start.finished() {
		start.cancel()
		return start.done, nil
	}
	delete(a.starts, allocationID)
	return nil, nil
}

// runGroupTasks replaces older generations and starts every task of the
// allocation. The caller holds the allocation operation lock.
func (a *Agent) runGroupTasks(ctx context.Context, start *groupStart, request *nodeapi.AllocationRequest) error {
	// The lock was released while images were pulled; fence again. A start
	// that no leader of the current epoch has requested does not create tasks.
	if err := a.fenceStart(request); err != nil {
		return err
	}
	a.mu.RLock()
	if start.epoch < a.epoch {
		a.mu.RUnlock()
		return fmt.Errorf("%w: requested at %d, accepted %d", errStartSuperseded, start.epoch, a.epoch)
	}
	var oldIDs []string
	for id, allocation := range a.allocations {
		if allocation.AllocationID == request.AllocationID && allocation.Generation < request.Generation {
			oldIDs = append(oldIDs, id)
		}
	}
	// Drains and resumes delivered during the pull update the start.
	request.Draining, request.DrainSequence = start.draining, start.drainSequence
	a.mu.RUnlock()
	for _, id := range oldIDs {
		if err := a.stopAllocation(ctx, id, false); err != nil {
			return fmt.Errorf("replace older generation: %w", err)
		}
	}
	if err := a.removeRetainedLogs(request.AllocationID, request.Generation-1); err != nil {
		return fmt.Errorf("remove logs of superseded generations: %w", err)
	}
	draining, drainSequence := a.startDrainState(request)
	// Tasks that are already running keep their records, so apply the drain
	// state to them before any task starts. Each start reapplies it so a
	// partially applied state converges. Records that are neither running nor
	// starting are rebuilt by startTask with the same state.
	var err error
	if draining {
		err = a.applyDrain(ctx, request.AllocationID, request.Generation, drainSequence)
	} else if drainSequence > 0 {
		err = a.applyResume(request.AllocationID, request.Generation, drainSequence, true)
	}
	if err != nil {
		return err
	}
	for i := range request.Tasks {
		task := &request.Tasks[i]
		if err := a.startTask(ctx, &taskStart{
			ID:            taskRecordID(request.AllocationID, request.Generation, task.Name),
			AllocationID:  request.AllocationID,
			Generation:    request.Generation,
			JobRevision:   request.JobRevision,
			ExecutionHash: request.ExecutionHash,
			Namespace:     request.Namespace,
			JobName:       request.JobName,
			GroupName:     request.GroupName,
			Spec:          task,
			Runtime:       request.Runtime,
			NetworkPlan:   request.NetworkPlan,
			EnvOverrides:  request.EnvOverrides,
			Secrets:       request.Secrets,
			Restart:       request.Restart,
			Draining:      draining,
			DrainSequence: drainSequence,
		}); err != nil {
			return err
		}
	}
	return nil
}

// startingStatusesLocked reports the tasks of accepted starts that have no
// record, as starting. A failed start's tasks carry its failure; a task it
// left behind reports its own record instead. The caller holds a.mu.
func (a *Agent) startingStatusesLocked() []nodeapi.AllocationStatus {
	var statuses []nodeapi.AllocationStatus
	for allocationID, start := range a.starts {
		var failure *nodeapi.StartFailure
		if start.failed {
			failure = &nodeapi.StartFailure{Attempt: start.attempt, Code: terminalStartCode(start.err), Message: truncateStartFailure(start.err.Error())}
		}
		for _, task := range start.tasks {
			if a.allocations[taskRecordID(allocationID, start.generation, task)] != nil {
				continue
			}
			statuses = append(statuses, nodeapi.AllocationStatus{ID: allocationID, Generation: start.generation, Task: task, Phase: lifecycle.PhaseStarting, Health: lifecycle.HealthUnknown, StartFailure: failure})
		}
	}
	return statuses
}

// terminalStartCode identifies a start failure that retrying the same
// generation cannot fix.
func terminalStartCode(err error) nodeapi.OperationCode {
	switch {
	case errors.Is(err, ErrStaleGeneration):
		return nodeapi.OperationStaleGeneration
	case errors.Is(err, ErrExecutionConflict), errors.Is(err, ErrAllocationExists):
		return nodeapi.OperationConflict
	case errors.Is(err, ErrRestartBudgetExhausted):
		return nodeapi.OperationRestartExhausted
	}
	return ""
}

// truncateStartFailure bounds a failure message to valid UTF-8 that fits the
// heartbeat limit after JSON encoding.
func truncateStartFailure(message string) string {
	message = strings.ToValidUTF8(message, "")
	if len(message) > nodeapi.MaxStartFailureMessageBytes {
		message = strings.ToValidUTF8(message[:nodeapi.MaxStartFailureMessageBytes], "")
	}
	return message
}
