package agent

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/containerd/errdefs"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	"github.com/overfold/trellis/orchestrator/internal/runtime"
)

// StopGroup stops all tasks in an allocation group.
func (a *Agent) StopGroup(ctx context.Context, request *nodeapi.StopAllocationRequest) error {
	if request.Generation == 0 {
		return ErrInvalidGeneration
	}
	var unlock func()
	for {
		unlock = a.lockAllocationOperation(request.AllocationID)
		if err := a.AcceptEpoch(request.Epoch); err != nil {
			unlock()
			return err
		}
		// A start in the background is cancelled; its own cleanup runs
		// before the stop handles whatever records it left.
		wait, err := a.cancelStartLocked(request.AllocationID, request.Generation)
		if err != nil {
			unlock()
			return err
		}
		if wait == nil {
			break
		}
		unlock()
		select {
		case <-wait:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	defer unlock()
	a.mu.RLock()
	var ids []string
	for id, allocation := range a.allocations {
		if allocation.AllocationID != request.AllocationID {
			continue
		}
		if allocation.Generation > request.Generation {
			a.mu.RUnlock()
			return fmt.Errorf("%w: current %d, requested %d", ErrStaleGeneration, allocation.Generation, request.Generation)
		}
		if allocation.Generation == request.Generation {
			ids = append(ids, id)
		}
	}
	for _, record := range a.retainedLogs {
		if record.AllocationID == request.AllocationID && record.Generation > request.Generation {
			a.mu.RUnlock()
			return fmt.Errorf("%w: current %d, requested %d", ErrStaleGeneration, record.Generation, request.Generation)
		}
	}
	listPending := a.recoveryListPending
	a.mu.RUnlock()
	var containers []runtime.ContainerInfo
	var listErr error
	if listPending {
		// Recovery has not listed every container, so tasks of this
		// generation may run without a record. Fence against a newer listed
		// generation before stopping anything.
		containers, listErr = a.listUnrecorded(ctx, request)
		if errors.Is(listErr, ErrStaleGeneration) {
			return listErr
		}
	}
	// Retention must survive a crash immediately after the stop fence, before
	// stopAllocation has touched any task. These records do not stop execution.
	if request.RetainLogs {
		for _, id := range ids {
			allocation, ok := a.snapshotAllocation(id)
			if !ok {
				continue
			}
			record := &retainedTaskLog{AllocationID: allocation.AllocationID, Generation: allocation.Generation, Namespace: allocation.Namespace, TaskName: allocation.TaskName, ContainerID: allocation.ContainerID}
			if err := a.persistRetainedLog(record); err != nil {
				return fmt.Errorf("persist retained log intent before stop fence: %w", err)
			}
			a.mu.Lock()
			a.retainedLogs[record.ContainerID] = record
			a.mu.Unlock()
		}
	}
	// Persist the fence before deleting any task record. A crash during
	// cleanup must not permit a delayed start to undo this stop intent.
	a.mu.Lock()
	if a.stoppedGenerations[request.AllocationID] < request.Generation {
		if a.local != nil {
			key := "agent/stopped-generations/" + allocationFileName(request.AllocationID)
			if err := a.local.Put(key, request.Generation); err != nil {
				a.mu.Unlock()
				return fmt.Errorf("persist stopped generation: %w", err)
			}
		}
		if a.stoppedGenerations == nil {
			a.stoppedGenerations = make(map[string]uint64)
		}
		a.stoppedGenerations[request.AllocationID] = request.Generation
	}
	a.mu.Unlock()
	var errs []error
	for _, id := range ids {
		if err := a.stopAllocation(ctx, id, request.RetainLogs); err != nil {
			errs = append(errs, err)
		}
	}
	if !request.RetainLogs {
		errs = append(errs, a.removeRetainedLogs(request.AllocationID, request.Generation))
	}
	if listPending {
		if listErr != nil {
			errs = append(errs, listErr)
		} else {
			errs = append(errs, a.stopUnrecorded(ctx, request, containers, ids))
		}
	}
	return errors.Join(errs...)
}

// listUnrecorded lists containers for stopUnrecorded and rejects a stop for a
// generation older than a listed unrecorded one.
func (a *Agent) listUnrecorded(ctx context.Context, request *nodeapi.StopAllocationRequest) ([]runtime.ContainerInfo, error) {
	managed, ok := a.runtime.(runtime.ManagedRuntime)
	if !ok {
		return nil, nil
	}
	containers, err := managed.ListManaged(ctx, a.cluster)
	if err != nil {
		return nil, fmt.Errorf("list containers for unrecorded allocation %s: %w", request.AllocationID, err)
	}
	for _, container := range containers {
		if allocation := allocationFromRuntime(container); allocation != nil && allocation.AllocationID == request.AllocationID && allocation.Generation > request.Generation {
			return nil, fmt.Errorf("%w: listed %d, requested %d", ErrStaleGeneration, allocation.Generation, request.Generation)
		}
	}
	return containers, nil
}

// stopUnrecorded stops listed unrecorded containers of an allocation
// generation, and of its older generations, while recovery has not completed a
// listing. handled names the recorded tasks the caller already stopped; the
// listing predates those stops. The caller holds the allocation operation lock.
func (a *Agent) stopUnrecorded(ctx context.Context, request *nodeapi.StopAllocationRequest, containers []runtime.ContainerInfo, handled []string) error {
	skip := make(map[string]bool, len(handled))
	for _, id := range handled {
		skip[id] = true
	}
	var errs []error
	a.mu.RLock()
	unidentified := a.hasUnreadableUnknownLocked(containers)
	a.mu.RUnlock()
	if unidentified {
		errs = append(errs, fmt.Errorf("stop allocation %s: an unreadable container may belong to it", request.AllocationID))
	}
	for _, container := range containers {
		allocation := allocationFromRuntime(container)
		// Older unrecorded generations are stopped too, as a start stops
		// known older generations; otherwise they would be adopted later.
		if allocation == nil || allocation.AllocationID != request.AllocationID || allocation.Generation > request.Generation || skip[allocation.ID] {
			continue
		}
		a.mu.RLock()
		_, known := a.allocations[allocation.ID]
		a.mu.RUnlock()
		if known {
			continue
		}
		allocation.Status = "stopping"
		allocation.SecretDir = a.recoveredSecretDir(allocation.ID)
		a.adoptPorts(allocation)
		a.mu.Lock()
		a.allocations[allocation.ID] = allocation
		persistErr := a.persistAllocation(allocation)
		a.mu.Unlock()
		a.reconciler.TrackStopping(allocation.ID, false, nil, 0, time.Time{}, false)
		if err := a.stopAllocation(ctx, allocation.ID, request.RetainLogs); err != nil {
			errs = append(errs, errors.Join(persistErr, err))
		}
	}
	return errors.Join(errs...)
}

func (a *Agent) stopAllocation(ctx context.Context, allocID string, retainLogs bool) error {
	a.mu.RLock()
	stored, ok := a.allocations[allocID]
	var alloc Allocation
	if ok {
		alloc = *stored
		alloc.Ports = append([]*runtime.Port(nil), stored.Ports...)
	}
	a.mu.RUnlock()
	if !ok {
		return fmt.Errorf("%w: %s", ErrAllocationNotFound, allocID)
	}

	containerID := alloc.ContainerID
	containerMissing := false
	if alloc.ContainerOwnershipUnverified {
		observed, err := a.runtime.Inspect(ctx, containerID)
		if errdefs.IsNotFound(err) {
			containerMissing = true
		} else if err != nil {
			return fmt.Errorf("verify container %s before cleanup: %w", containerID, err)
		} else if !a.containerMatchesAllocation(*observed, &alloc) {
			return fmt.Errorf("%w: container %s has different execution metadata", ErrExecutionConflict, containerID)
		}
	}
	if retainLogs {
		record := &retainedTaskLog{AllocationID: alloc.AllocationID, Generation: alloc.Generation, Namespace: alloc.Namespace, TaskName: alloc.TaskName, ContainerID: containerID}
		if err := a.persistRetainedLog(record); err != nil {
			return fmt.Errorf("persist retained log record before cleanup: %w", err)
		}
		a.mu.Lock()
		a.retainedLogs[containerID] = record
		a.mu.Unlock()
	}
	if err := a.reconciler.BeginStop(ctx, allocID); err != nil {
		return fmt.Errorf("suppress restarts before stop: %w", err)
	}
	persistStopErr := a.markAllocationStopping(allocID)
	a.closeExecSessionsForTask(ctx, allocID, containerID)
	if !containerMissing {
		if err := a.runtime.Stop(ctx, containerID); err != nil {
			return errors.Join(persistStopErr, fmt.Errorf("stop container %s: %w", containerID, err))
		}
	}

	var errs []error
	a.health.DeregisterTask(allocID)
	if err := a.reconciler.Untrack(allocID); err != nil {
		errs = append(errs, fmt.Errorf("untrack allocation %s: %w", allocID, err))
	}

	if err := a.detachAllocationNetwork(ctx, &alloc); err != nil {
		errs = append(errs, fmt.Errorf("detach allocation network: %w", err))
	}
	containerRemoved := true
	if !containerMissing {
		var err error
		if retainedRuntime, ok := a.runtime.(runtime.RetainedLogRuntime); retainLogs && ok {
			err = retainedRuntime.RemoveRetainingLogs(ctx, containerID)
		} else {
			err = a.runtime.Remove(ctx, containerID)
		}
		if err != nil {
			containerRemoved = false
			errs = append(errs, fmt.Errorf("remove container %s: %w", containerID, err))
		}
	}
	if alloc.SecretDir != "" {
		if err := removeSecretDir(alloc.SecretDir); err != nil {
			errs = append(errs, fmt.Errorf("remove secret files: %w", err))
		}
	}

	// Staging mounts remain the OCI mount sources of a container that still
	// exists; a stop retry releases them after removal succeeds.
	if containerRemoved {
		if err := a.volumes.ReleaseStaging(allocID); err != nil {
			errs = append(errs, fmt.Errorf("release volume staging: %w", err))
		}
	}
	if err := errors.Join(errs...); err != nil {
		return errors.Join(persistStopErr, err)
	}
	if err := a.deleteAllocationRecord(allocID); err != nil {
		return errors.Join(persistStopErr, fmt.Errorf("delete allocation record: %w", err))
	}
	a.mu.Lock()
	delete(a.allocations, allocID)
	a.forgetIdleNetworkPlanLocked(alloc.Namespace)
	a.mu.Unlock()
	for _, p := range alloc.Ports {
		// Recovery may retain a stale record sharing a live allocation's port.
		if !a.hostPortInUse(p) {
			_ = a.ports.Release(p)
		}
	}

	return persistStopErr
}
