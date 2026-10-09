package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/overfold/trellis/orchestrator/internal/client"
	"github.com/overfold/trellis/orchestrator/internal/runtime"
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

// logLimitInterval is how often the agent checks task log sizes. A task can
// exceed the log limit by what it writes in one interval.
const logLimitInterval = time.Second

func (a *Agent) removeRetainedLogs(allocationID string, generation uint64) error {
	retainedRuntime, ok := a.runtime.(runtime.RetainedLogRuntime)
	a.mu.RLock()
	var records []*retainedTaskLog
	for _, record := range a.retainedLogs {
		if record.AllocationID == allocationID && record.Generation <= generation {
			records = append(records, record)
		}
	}
	a.mu.RUnlock()
	var errs []error
	for _, record := range records {
		if ok {
			if err := retainedRuntime.RemoveRetainedLogs(record.ContainerID); err != nil {
				errs = append(errs, fmt.Errorf("remove retained logs for %s: %w", record.ContainerID, err))
				continue
			}
		}
		if err := a.deleteRetainedLogRecord(record.ContainerID); err != nil {
			errs = append(errs, fmt.Errorf("delete retained log record for %s: %w", record.ContainerID, err))
			continue
		}
		a.mu.Lock()
		delete(a.retainedLogs, record.ContainerID)
		a.mu.Unlock()
	}
	return errors.Join(errs...)
}

// addTaskLogUsage reports the disk consumed by task logs, which have no size
// limit, so operators can alert before the node's disk fills. The fields are
// omitted when the runtime cannot measure them.
func (a *Agent) addTaskLogUsage(heartbeat *client.Heartbeat) {
	measurer, ok := a.runtime.(runtime.LogUsageRuntime)
	if !ok {
		return
	}
	usage, err := measurer.LogUsage()
	if err != nil {
		a.log.Warn("measure task log usage failed", "error", err)
		return
	}
	heartbeat.TaskLogBytes = &usage.Bytes
	heartbeat.TaskLogFilesystemAvailable = &usage.FilesystemAvailable
	heartbeat.TaskLogFilesystemCapacity = &usage.FilesystemCapacity
}

// runLogLimitLoop rotates task logs that outgrow the node's log limit. Task
// output keeps flowing to the log through the containerd shim while the agent
// is stopped; the limit is enforced again once it restarts.
func (a *Agent) runLogLimitLoop(ctx context.Context) {
	limiter, ok := a.runtime.(runtime.LogLimitRuntime)
	if !ok {
		return
	}
	ticker := time.NewTicker(logLimitInterval)
	defer ticker.Stop()
	var lastErr string
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		err := limiter.EnforceLogLimit(a.taskLogLimit())
		if err == nil {
			lastErr = ""
			continue
		}
		// Log a persistent failure once rather than every interval.
		if err.Error() != lastErr {
			a.log.Warn("enforce task log limit failed", "error", err)
		}
		lastErr = err.Error()
	}
}
