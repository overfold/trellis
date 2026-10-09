package agent

import (
	"context"
	"errors"
	"fmt"

	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
)

// startDrainState combines the drain state carried by a start request with the
// newest drain or resume the agent already applied to that generation. The
// higher sequence wins, so a delayed start cannot roll back a later drain.
func (a *Agent) startDrainState(request *nodeapi.AllocationRequest) (bool, uint64) {
	draining, sequence := request.Draining, request.DrainSequence
	a.mu.RLock()
	defer a.mu.RUnlock()
	for _, allocation := range a.allocations {
		if allocation.AllocationID != request.AllocationID || allocation.Generation != request.Generation || allocation.DrainSequence <= request.DrainSequence {
			continue
		}
		// Local records that disagree at one sequence resolve to draining, so
		// the result never depends on map iteration order.
		if allocation.DrainSequence > sequence || (allocation.DrainSequence == sequence && allocation.Draining) {
			draining, sequence = allocation.Draining, allocation.DrainSequence
		}
	}
	return draining, sequence
}

// DrainGroup suppresses automatic restarts for one allocation generation until
// the control plane delivers the normal stop operation.
func (a *Agent) DrainGroup(ctx context.Context, request *nodeapi.DrainAllocationRequest) error {
	unlock := a.lockAllocationOperation(request.AllocationID)
	defer unlock()
	if err := a.AcceptEpoch(request.Epoch); err != nil {
		return err
	}
	return a.applyDrain(ctx, request.AllocationID, request.Generation, request.Sequence)
}

// applyDrain marks one allocation generation draining at sequence. The caller
// must hold the allocation operation lock.
func (a *Agent) applyDrain(ctx context.Context, allocationID string, generation, sequence uint64) error {
	a.mu.Lock()
	if err := a.drainStartLocked(allocationID, generation, true, sequence); err != nil {
		a.mu.Unlock()
		return err
	}
	var ids []string
	var persistErr error
	for _, allocation := range a.allocations {
		if allocation.AllocationID != allocationID {
			continue
		}
		if allocation.Generation > generation {
			a.mu.Unlock()
			return fmt.Errorf("%w: current %d, requested %d", ErrStaleGeneration, allocation.Generation, generation)
		}
		if allocation.Generation != generation {
			continue
		}
		if sequence < allocation.DrainSequence || sequence == allocation.DrainSequence && !allocation.Draining {
			continue
		}
		previousDraining, previousSequence := allocation.Draining, allocation.DrainSequence
		allocation.Draining = true
		allocation.DrainSequence = sequence
		if err := a.persistAllocation(allocation); err != nil {
			allocation.Draining, allocation.DrainSequence = previousDraining, previousSequence
			persistErr = fmt.Errorf("persist draining allocation: %w", err)
			break
		}
		ids = append(ids, allocation.ID)
	}
	a.mu.Unlock()
	for _, id := range ids {
		persistErr = errors.Join(persistErr, a.reconciler.SuppressRestarts(ctx, id))
	}
	return persistErr
}

// drainStartLocked records a drain or resume for a start of the generation
// that runs in the background, which applies it when it creates the tasks. It
// rejects the operation when a newer generation is starting. The caller holds
// a.mu.
func (a *Agent) drainStartLocked(allocationID string, generation uint64, draining bool, sequence uint64) error {
	start := a.starts[allocationID]
	if start == nil {
		return nil
	}
	if start.generation > generation && !start.finished() {
		return fmt.Errorf("%w: current %d, requested %d", ErrStaleGeneration, start.generation, generation)
	}
	if start.generation == generation {
		start.mergeDrain(draining, sequence)
	}
	return nil
}

// ResumeGroup cancels a drain for a retained allocation generation.
func (a *Agent) ResumeGroup(request *nodeapi.DrainAllocationRequest) error {
	unlock := a.lockAllocationOperation(request.AllocationID)
	defer unlock()
	if err := a.AcceptEpoch(request.Epoch); err != nil {
		return err
	}
	return a.applyResume(request.AllocationID, request.Generation, request.Sequence, false)
}

// applyResume cancels a drain for one allocation generation at sequence. A
// record that is neither running nor starting fails the resume unless
// skipInactive is set. The caller must hold the allocation operation lock.
func (a *Agent) applyResume(allocationID string, generation, sequence uint64, skipInactive bool) error {
	a.mu.Lock()
	if err := a.drainStartLocked(allocationID, generation, false, sequence); err != nil {
		a.mu.Unlock()
		return err
	}
	var resumed []*Allocation
	for _, allocation := range a.allocations {
		if allocation.AllocationID != allocationID {
			continue
		}
		if allocation.Generation > generation {
			a.mu.Unlock()
			return fmt.Errorf("%w: current %d, requested %d", ErrStaleGeneration, allocation.Generation, generation)
		}
		if allocation.Generation != generation {
			continue
		}
		if sequence < allocation.DrainSequence || sequence == allocation.DrainSequence && allocation.Draining {
			continue
		}
		if allocation.Status != "running" && allocation.Status != "starting" && allocation.Status != "failed" {
			if skipInactive {
				continue
			}
			a.mu.Unlock()
			return fmt.Errorf("cannot resume allocation %s task %s with status %q", allocationID, allocation.ID, allocation.Status)
		}
		resumed = append(resumed, allocation)
	}
	// Snapshot reconciler inputs under a.mu; restart callbacks update them.
	var running []Allocation
	for _, allocation := range resumed {
		previousDraining, previousSequence := allocation.Draining, allocation.DrainSequence
		allocation.Draining = false
		allocation.DrainSequence = sequence
		if err := a.persistAllocation(allocation); err != nil {
			allocation.Draining, allocation.DrainSequence = previousDraining, previousSequence
			a.mu.Unlock()
			return fmt.Errorf("persist resumed allocation: %w", err)
		}
		// The control plane will retry the start for a recovered starting task.
		// Leave it untracked until that retry resolves its runtime state. A
		// terminally failed task stays restart-suppressed. Recovery retries
		// classify a task whose runtime state is unknown.
		if allocation.Status == "running" && !allocation.unobserved {
			running = append(running, *allocation)
		}
	}
	a.mu.Unlock()
	for _, allocation := range running {
		a.reconciler.ResumeRestarts(allocation.ID, allocation.Spec != nil && allocation.Spec.HealthCheck != nil, allocation.Restart, allocation.RestartAttempts, allocation.RestartWindow, allocation.RestartExhausted)
	}
	return nil
}
