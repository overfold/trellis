package agent

import (
	"context"
	"fmt"
	"io"

	"github.com/overfold/trellis/orchestrator/internal/transport"
)

const (
	// A followed stream holds a goroutine, a pipe, and an open log file for as
	// long as its client keeps reading, so admission is bounded per node and
	// per allocation.
	logFollowGlobalLimit        = 64
	logFollowPerAllocationLimit = 8
)

// TaskLogs opens logs for one task in a scheduler allocation. A followed
// stream is admitted against the node and allocation limits and releases its
// slot when closed.
func (a *Agent) TaskLogs(ctx context.Context, allocationID, task string, follow bool, tail int) (io.ReadCloser, error) {
	a.mu.RLock()
	var match *Allocation
	var retained *retainedTaskLog
	matches := 0
	for _, allocation := range a.allocations {
		if allocation.AllocationID != allocationID {
			continue
		}
		if task != "" && allocation.TaskName != task {
			continue
		}
		match = allocation
		matches++
	}
	if matches == 0 {
		var generation uint64
		for _, candidate := range a.retainedLogs {
			if candidate.AllocationID == allocationID && candidate.Generation > generation {
				generation = candidate.Generation
			}
		}
		for _, candidate := range a.retainedLogs {
			if candidate.AllocationID == allocationID && candidate.Generation == generation && (task == "" || candidate.TaskName == task) {
				retained = candidate
				matches++
			}
		}
	}
	a.mu.RUnlock()

	if matches == 0 {
		if task == "" {
			return nil, fmt.Errorf("%w: %s", ErrAllocationNotFound, allocationID)
		}
		return nil, fmt.Errorf("%w: allocation %s has no task %q", ErrAllocationNotFound, allocationID, task)
	}
	if task == "" && matches > 1 {
		return nil, fmt.Errorf("allocation %s has multiple tasks; specify task", allocationID)
	}
	if retained != nil {
		// A terminal stream has no producer and must finish at the current EOF.
		return a.runtime.Logs(ctx, retained.ContainerID, false, tail)
	}
	if !follow {
		return a.runtime.Logs(ctx, match.ContainerID, false, tail)
	}
	release, ok := a.logStreams.Acquire(allocationID)
	if !ok {
		return nil, fmt.Errorf("%w: followed log streams for allocation %s or on this node are at their maximum (%d per allocation, %d per node)", ErrLogStreamLimit, allocationID, logFollowPerAllocationLimit, logFollowGlobalLimit)
	}
	logs, err := a.runtime.Logs(ctx, match.ContainerID, true, tail)
	if err != nil {
		release()
		return nil, err
	}
	return transport.ReleaseOnClose(logs, release), nil
}
