package agent

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/containerd/errdefs"
	"github.com/overfold/trellis/orchestrator/internal/runtime"
)

const (
	recoveryRetryMinDelay = time.Second
	recoveryRetryMaxDelay = 30 * time.Second
)

// recoveryGuidance points operators at the manual steps for recovery state
// that the agent refuses to start with.
const recoveryGuidance = "see \"Agent recovery refused\" in docs/public/operations.md"

// recover restores allocations from durable records. Unreadable or
// inconsistent durable state fails it; a failed runtime listing does not,
// because it says nothing about any container.
func (a *Agent) recover(ctx context.Context) error {
	if a.local == nil {
		a.cleanupVolumeStaging(nil)
		return nil
	}
	var epoch uint64
	epochErr := a.local.Get(controlEpochKey, &epoch)
	if epochErr != nil && !errors.Is(epochErr, os.ErrNotExist) {
		return fmt.Errorf("read control-plane epoch %s: %w; %s", controlEpochKey, epochErr, recoveryGuidance)
	}
	stops, stopErrs := a.local.ListRaw("agent/stopped-generations")
	if err := errors.Join(stopErrs...); err != nil {
		return fmt.Errorf("read stopped generation watermarks: %w; %s", err, recoveryGuidance)
	}
	a.stoppedGenerations = make(map[string]uint64, len(stops))
	for name, raw := range stops {
		id, err := base64.RawURLEncoding.DecodeString(name)
		var generation uint64
		if err != nil || len(id) == 0 || allocationFileName(string(id)) != name || json.Unmarshal(raw, &generation) != nil || generation == 0 {
			return fmt.Errorf("invalid stopped generation watermark %s; %s", name, recoveryGuidance)
		}
		a.stoppedGenerations[string(id)] = generation
	}
	if epochErr != nil && len(stops) != 0 {
		return fmt.Errorf("control-plane epoch is missing while stopped generation watermarks exist; %s", recoveryGuidance)
	}
	records, recordErrs := a.local.ListRaw("agent/allocations")
	if err := errors.Join(recordErrs...); err != nil {
		return fmt.Errorf("read allocation recovery records: %w; %s", err, recoveryGuidance)
	}
	stored := make(map[string]*Allocation, len(records))
	retained, retainedErrs := a.local.ListRaw("agent/retained-logs")
	if err := errors.Join(retainedErrs...); err != nil {
		return fmt.Errorf("read retained log records: %w; %s", err, recoveryGuidance)
	}
	if epochErr != nil && len(retained) != 0 {
		return fmt.Errorf("control-plane epoch %s is missing while retained logs exist; %s", controlEpochKey, recoveryGuidance)
	}
	for name, raw := range retained {
		var record retainedTaskLog
		if err := json.Unmarshal(raw, &record); err != nil || record.AllocationID == "" || record.Generation == 0 || record.Namespace == "" || record.TaskName == "" || record.ContainerID == "" || record.ContainerID == "." || record.ContainerID == ".." || record.ContainerID != filepath.Base(record.ContainerID) || name != allocationFileName(record.ContainerID) {
			return fmt.Errorf("decode retained log record agent/retained-logs/%s: invalid identity; %s", name, recoveryGuidance)
		}
		a.retainedLogs[record.ContainerID] = &record
	}
	for name, raw := range records {
		var allocation Allocation
		if err := json.Unmarshal(raw, &allocation); err != nil {
			return fmt.Errorf("decode allocation recovery record agent/allocations/%s: %w; %s", name, err, recoveryGuidance)
		}
		if allocation.ID == "" || allocation.ContainerID == "" || allocation.AllocationID == "" {
			return fmt.Errorf("decode allocation recovery record agent/allocations/%s: allocation identity is required; %s", name, recoveryGuidance)
		}
		expectedName := base64.RawURLEncoding.EncodeToString([]byte(allocation.ID))
		if name != expectedName {
			return fmt.Errorf("decode allocation recovery record agent/allocations/%s: record name does not match allocation ID %q; %s", name, allocation.ID, recoveryGuidance)
		}
		if _, exists := stored[allocation.ContainerID]; exists {
			return fmt.Errorf("decode allocation recovery record agent/allocations/%s: duplicate container ID %q; %s", name, allocation.ContainerID, recoveryGuidance)
		}
		stored[allocation.ContainerID] = &allocation
	}
	if epochErr != nil && len(records) != 0 {
		return fmt.Errorf("control-plane epoch %s is missing while %d allocation recovery records exist; %s", controlEpochKey, len(records), recoveryGuidance)
	}
	managed, ok := a.runtime.(runtime.ManagedRuntime)
	if !ok {
		if len(records) != 0 {
			return fmt.Errorf("runtime cannot recover existing allocation records")
		}
		if epochErr != nil {
			if err := a.local.Put(controlEpochKey, epoch); err != nil {
				return fmt.Errorf("initialize control-plane epoch: %w", err)
			}
		}
		a.epoch = epoch
		a.cleanupVolumeStaging(nil)
		return nil
	}
	containers, err := managed.ListManaged(ctx, a.cluster)
	if err != nil {
		if epochErr != nil {
			// Only an empty first boot may start without an epoch, and a
			// failed listing cannot show that no container exists.
			return fmt.Errorf("control-plane epoch %s is missing and managed containers could not be listed to confirm an empty first boot: %w; %s", controlEpochKey, err, recoveryGuidance)
		}
		// A failed listing proves nothing about any container. Keep every
		// record and its resources until the recovery retry observes the
		// runtime again; it also runs the checks and the orphaned resource
		// sweep that a completed listing allows.
		a.log.Warn("list managed containers during recovery; retrying", "error", err)
		a.epoch = epoch
		a.mu.Lock()
		a.recoveryListPending = true
		a.mu.Unlock()
		for _, allocation := range stored {
			a.recoverUnobserved(allocation)
		}
		return nil
	}
	if epochErr != nil {
		if len(containers) != 0 {
			return fmt.Errorf("control-plane epoch %s is missing while managed runtime container %q exists; %s", controlEpochKey, containers[0].ID, recoveryGuidance)
		}
		if err := a.local.Put(controlEpochKey, epoch); err != nil {
			return fmt.Errorf("initialize control-plane epoch: %w", err)
		}
	}
	for _, container := range containers {
		if stored[container.ID] == nil {
			return unrecordedContainerError(container)
		}
	}
	a.epoch = epoch
	// Existing containers still reference their staging mounts as OCI mount
	// sources; keep those so a later restart can create a new task.
	liveContainers := make([]string, 0, len(containers))
	for _, container := range containers {
		liveContainers = append(liveContainers, container.ID)
	}
	a.cleanupVolumeStaging(liveContainers)
	seen := make(map[string]bool, len(containers))
	for _, container := range containers {
		seen[container.ID] = true
		a.recoverContainer(container, stored[container.ID])
	}
	a.mu.Lock()
	a.recoveryListPending = a.hasUnreadableUnknownLocked(containers)
	a.mu.Unlock()
	for containerID, allocation := range stored {
		if containerID == "" || seen[containerID] {
			continue
		}
		a.recoverMissing(ctx, allocation)
	}
	a.queueRecoveryStops()
	return nil
}

// queueRecoveryStops rebuilds recovery's cleanup queue. Durable stop intent
// and interrupted cleanup must finish locally, including older generations
// whose control-plane stops would now be rejected as stale.
func (a *Agent) queueRecoveryStops() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.recoveryStops = nil
	for id, allocation := range a.allocations {
		if a.pendingRecoveryStopLocked(allocation) {
			if a.recoveryStops == nil {
				a.recoveryStops = make(map[string]string)
			}
			a.recoveryStops[id] = allocation.AllocationID
		}
	}
}

func (a *Agent) pendingRecoveryStopLocked(allocation *Allocation) bool {
	// stopAllocation verifies ambiguous ownership on every retry before
	// touching execution resources. Unknown runtime state still needs relisting.
	return allocation.Status == "stopping" && !allocation.unobserved
}

// recoverContainer restores one observed container from its durable record.
func (a *Agent) recoverContainer(container runtime.ContainerInfo, allocation *Allocation) {
	a.mu.RLock()
	stopped := a.stoppedGenerations[allocation.AllocationID] >= allocation.Generation && allocation.Generation != 0
	a.mu.RUnlock()
	if stopped {
		// The fence may have committed before any task record was marked
		// stopping. Apply it before classifying runtime observations.
		allocation.Status = "stopping"
	}
	if !observedStatus(container.Status) {
		a.recoverUnobserved(allocation)
		return
	}
	if allocation.unobserved {
		// Replace the restart suppression applied while state was unknown.
		_ = a.reconciler.Untrack(allocation.ID)
		allocation.unobserved = false
	}
	if !a.containerMatchesAllocation(container, allocation) {
		// Container IDs are not sufficient proof of ownership after an agent
		// restart. Preserve the record and every resource until a later
		// observation proves that the full execution identity matches.
		allocation.ContainerOwnershipUnverified = true
		allocation.Status = "stopping"
		a.adoptPorts(allocation)
		a.mu.Lock()
		a.allocations[allocation.ID] = allocation
		persistErr := a.persistAllocation(allocation)
		a.mu.Unlock()
		if persistErr != nil {
			a.log.Error("record recovered container ownership mismatch", "allocation", allocation.AllocationID, "error", persistErr)
		}
		return
	}
	// Every recovered container has now proven its ownership. This also
	// resolves a create whose result was previously ambiguous.
	allocation.ContainerOwnershipUnverified = false
	stopping := allocation.Status == "stopping"
	restartSuppressed := stopping || allocation.Draining
	// An exhausted restart budget is terminal for this generation: keep
	// reporting the failed observation instead of asking for a new start.
	notRunning := !stopping && (container.Status == runtime.StatusCreated || container.Status == runtime.StatusStopped)
	exhausted := notRunning && allocation.RestartExhausted
	committed := allocation.Status == "running"
	recoveryPending := notRunning && !exhausted && !committed
	if exhausted {
		allocation.Status = "failed"
		allocation.Health = "unhealthy"
	} else if notRunning && committed {
		// Keep committed tasks on the durable, budgeted restart path rather
		// than recreating them as incomplete starts after every agent crash.
		allocation.Health = "unhealthy"
	} else if recoveryPending {
		// Recovery reports observation; it does not invent desired state.
		// A non-running recovered task stays restart-suppressed until the
		// control plane observes "starting" and reconciliation reissues the
		// appropriate start or stop action.
		allocation.Status = "starting"
		allocation.Health = "unknown"
	} else if !stopping && container.Status == runtime.StatusRunning {
		allocation.Status = "running"
		if allocation.Spec != nil && allocation.Spec.HealthCheck != nil {
			allocation.Health = "unknown"
		} else {
			// Without a check a running task is healthy, including one
			// recorded unhealthy while it was paused.
			allocation.Health = "healthy"
		}
	} else if !stopping && container.Status == runtime.StatusPaused {
		// A paused task still exists, so it is neither restarted nor
		// replaced, but its frozen processes cannot serve. Configured
		// probes report health again once it is resumed.
		allocation.Status = "running"
		allocation.Health = "unhealthy"
	}
	a.adoptPorts(allocation)
	// Persist the initial observation before a probe can publish a result.
	a.mu.Lock()
	a.allocations[allocation.ID] = allocation
	persistErr := a.persistAllocation(allocation)
	a.mu.Unlock()
	if persistErr != nil {
		a.log.Error("refresh recovered allocation record", "allocation", allocation.AllocationID, "error", persistErr)
	}
	healthManaged := allocation.Spec != nil && allocation.Spec.HealthCheck != nil
	if exhausted {
		a.reconciler.TrackFailed(allocation.ID, healthManaged, allocation.Restart, allocation.RestartAttempts, allocation.RestartWindow)
	} else if restartSuppressed {
		a.reconciler.TrackStopping(allocation.ID, healthManaged, allocation.Restart, allocation.RestartAttempts, allocation.RestartWindow, allocation.RestartExhausted)
	} else if !recoveryPending {
		a.reconciler.TrackRecovered(allocation.ID, healthManaged, allocation.Restart, allocation.RestartAttempts, allocation.RestartWindow, allocation.RestartExhausted)
	}
	if healthManaged && !stopping && !notRunning {
		a.health.RegisterTask(allocation.ID, allocation.ContainerID, allocation.Spec.HealthCheck, "namespace", allocation.Namespace, "job", allocation.JobName, "allocation", allocation.AllocationID, "task", allocation.TaskName)
	}
}

// recoverUnobserved keeps an allocation whose container state could not be
// read. Unverified state preserves the record, its resources, and its last
// recorded phase, reported with unknown health; local restarts stay suppressed until a
// later observation classifies the container. The record is left unchanged.
func (a *Agent) recoverUnobserved(allocation *Allocation) {
	allocation.unobserved = true
	a.adoptPorts(allocation)
	a.mu.Lock()
	a.allocations[allocation.ID] = allocation
	a.mu.Unlock()
	a.reconciler.TrackStopping(allocation.ID, false, allocation.Restart, allocation.RestartAttempts, allocation.RestartWindow, allocation.RestartExhausted)
	a.log.Warn("container state unavailable during recovery; preserving allocation", "allocation", allocation.AllocationID, "container", allocation.ContainerID)
}

// recoverMissing cleans up a recorded allocation whose container is absent
// from a successful runtime listing.
func (a *Agent) recoverMissing(ctx context.Context, allocation *Allocation) {
	adopted := allocation.unobserved
	allocation.unobserved = false
	if allocation.ContainerOwnershipUnverified {
		// ListManaged is cluster-filtered, so absence does not prove that
		// an ambiguous Create left no container with this ID. Use the same
		// ownership verification as a live cleanup retry.
		allocation.Status = "stopping"
		a.adoptPorts(allocation)
		a.mu.Lock()
		a.allocations[allocation.ID] = allocation
		a.mu.Unlock()
		if err := a.stopAllocation(context.WithoutCancel(ctx), allocation.ID, false); err != nil {
			a.log.Error("recover unverified allocation container", "allocation", allocation.AllocationID, "error", err)
		}
		return
	}
	var cleanupErr error
	if err := a.detachAllocationNetwork(context.WithoutCancel(ctx), allocation); err != nil {
		cleanupErr = fmt.Errorf("detach network for missing allocation container: %w", err)
	} else if allocation.SecretDir != "" {
		if err := removeSecretDir(allocation.SecretDir); err != nil {
			cleanupErr = fmt.Errorf("remove secret files for missing allocation container: %w", err)
		}
	}
	cleanupErr = errors.Join(cleanupErr, a.volumes.ReleaseStaging(allocation.ID))
	if cleanupErr == nil {
		if err := a.deleteAllocationRecord(allocation.ID); err != nil {
			cleanupErr = fmt.Errorf("delete missing allocation record: %w", err)
		}
	}
	if cleanupErr != nil {
		a.log.Error("recover missing allocation container", "allocation", allocation.AllocationID, "error", cleanupErr)
		allocation.Status = "stopping"
		a.adoptPorts(allocation)
		a.mu.Lock()
		a.allocations[allocation.ID] = allocation
		a.mu.Unlock()
		return
	}
	a.closeExecSessionsForTask(ctx, allocation.ID, allocation.ContainerID)
	a.health.DeregisterTask(allocation.ID)
	_ = a.reconciler.Untrack(allocation.ID)
	a.mu.Lock()
	delete(a.allocations, allocation.ID)
	a.forgetIdleNetworkPlanLocked(allocation.Namespace)
	a.mu.Unlock()
	if adopted {
		for _, port := range allocation.Ports {
			if !a.hostPortInUse(port) {
				_ = a.ports.Release(port)
			}
		}
	}
}

// hostPortInUse reports whether a remaining allocation also holds the port.
func (a *Agent) hostPortInUse(port *runtime.Port) bool {
	if port == nil {
		return false
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	for _, allocation := range a.allocations {
		for _, held := range allocation.Ports {
			if held != nil && held.HostPort == port.HostPort {
				return true
			}
		}
	}
	return false
}

func (a *Agent) adoptPorts(allocation *Allocation) {
	for _, port := range allocation.Ports {
		if err := a.ports.Adopt(port); err != nil {
			a.log.Error("recover port claim", "allocation", allocation.AllocationID, "error", err)
		}
	}
}

// retryRecovery re-observes allocations that recovery could not classify and,
// after a failed initial listing, checks that every listed container has a
// record. It reports whether any recovery work remains.
func (a *Agent) retryRecovery(ctx context.Context) bool {
	managed, ok := a.runtime.(runtime.ManagedRuntime)
	if !ok {
		return false
	}
	a.mu.RLock()
	if a.recoveryErr != nil {
		a.mu.RUnlock()
		return true
	}
	listPending := a.recoveryListPending
	pending := make(map[string]*Allocation)
	for id, allocation := range a.allocations {
		if allocation.unobserved {
			pending[id] = &Allocation{AllocationID: allocation.AllocationID, ContainerID: allocation.ContainerID}
		}
	}
	a.mu.RUnlock()
	if listPending || len(pending) > 0 {
		if err := a.relistRecovery(ctx, managed, listPending, pending); err != nil {
			a.failRecovery(err)
			return true
		}
	}
	a.queueRecoveryStops()
	a.mu.RLock()
	stops := make(map[string]string, len(a.recoveryStops))
	maps.Copy(stops, a.recoveryStops)
	a.mu.RUnlock()
	for id, allocationID := range stops {
		a.stopRecovered(ctx, id, allocationID)
	}
	a.queueRecoveryStops()
	return a.recoveryPending()
}

// relistRecovery lists containers again to classify unobserved allocations
// and, while listing is incomplete, to check that every listed container has
// an allocation record. It returns an error for a container without one.
func (a *Agent) relistRecovery(ctx context.Context, managed runtime.ManagedRuntime, listPending bool, pending map[string]*Allocation) error {
	containers, err := managed.ListManaged(ctx, a.cluster)
	if err != nil {
		a.log.Warn("retry allocation recovery", "error", err)
		return nil
	}
	listed := make(map[string]runtime.ContainerInfo, len(containers))
	for _, container := range containers {
		listed[container.ID] = container
	}
	for id, allocation := range pending {
		container, found := listed[allocation.ContainerID]
		a.reobserve(ctx, id, allocation.AllocationID, container, found)
	}
	if listPending {
		stillPending := false
		for _, container := range containers {
			relist, err := a.checkRecorded(ctx, container)
			if err != nil {
				return err
			}
			stillPending = stillPending || relist
		}
		a.mu.Lock()
		a.recoveryListPending = stillPending
		// Listing completes at most once, and Init skipped the orphaned
		// resource sweep while it was incomplete.
		sweep := !a.recoveryListPending
		a.mu.Unlock()
		if sweep {
			// Every listed container is now recorded, so secret directories
			// and network attachments without an owner are orphans, as at
			// startup.
			a.removeOrphanedResources(ctx)
		}
	}
	return nil
}

// recoveryFailed reports whether recovery stopped with an error.
func (a *Agent) recoveryFailed() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.recoveryErr != nil
}

// recoveryPending reports whether recovery still has containers to observe.
func (a *Agent) recoveryPending() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.recoveryListPending || len(a.recoveryStops) > 0 {
		return true
	}
	for _, allocation := range a.allocations {
		if allocation.unobserved {
			return true
		}
	}
	return false
}

// checkRecorded applies startup's rule to a container found by a late
// listing: a container without an allocation record fails recovery, because
// runtime labels alone do not carry the fencing and drain state needed to
// manage it. The listing predates the operation lock, so the container is
// re-inspected to rule out one stopped and removed in the meantime. It reports
// whether the container must be listed again.
func (a *Agent) checkRecorded(ctx context.Context, listed runtime.ContainerInfo) (bool, error) {
	a.mu.RLock()
	recorded := a.containerRecordedLocked(listed.ID)
	a.mu.RUnlock()
	if recorded {
		return false, nil
	}
	if listed.Labels == nil {
		// Unreadable metadata may belong to another cluster, so it does not
		// prove an unrecorded Trellis container; list it again.
		a.log.Warn("unreadable container blocks allocation recovery; retrying", "container", listed.ID)
		return true, nil
	}
	unlock := a.lockAllocationOperation(listed.Labels["trellis.allocation-id"])
	defer unlock()
	a.mu.RLock()
	recorded = a.containerRecordedLocked(listed.ID)
	a.mu.RUnlock()
	if recorded {
		return false, nil
	}
	if _, err := a.runtime.Inspect(ctx, listed.ID); errdefs.IsNotFound(err) {
		// The container was removed after the listing; the next listing
		// confirms it. Any other result, including an unreadable state,
		// leaves a listed container of this cluster without a record.
		return true, nil
	}
	return false, unrecordedContainerError(listed)
}

// containerRecordedLocked reports whether an allocation owns a container ID.
func (a *Agent) containerRecordedLocked(containerID string) bool {
	for _, allocation := range a.allocations {
		if allocation.ContainerID == containerID {
			return true
		}
	}
	return false
}

// unrecordedContainerError describes a managed container without a durable
// allocation record, naming what its labels claim so an operator can find it.
func unrecordedContainerError(container runtime.ContainerInfo) error {
	return fmt.Errorf("managed runtime container %q (labelled allocation %q, generation %q) has no durable allocation record; %s",
		container.ID, container.Labels["trellis.allocation-id"], container.Labels["trellis.allocation-generation"], recoveryGuidance)
}

// failRecovery stops recovery and reports err through Failed. Recovery does
// not continue after it, as startup does not.
func (a *Agent) failRecovery(err error) {
	a.mu.Lock()
	first := a.recoveryErr == nil
	if first {
		a.recoveryErr = err
	}
	a.mu.Unlock()
	if first {
		a.log.Error("allocation recovery failed", "error", err)
		select {
		case a.failed <- err:
		default:
		}
	}
}

// Failed delivers an error when recovery fails after Init returned. The agent
// must then stop; restarting it refuses the same state at Init.
func (a *Agent) Failed() <-chan error {
	return a.failed
}

// stopRecovered finishes interrupted cleanup if it still qualifies; a
// start or stop may have replaced or removed it since it was queued.
func (a *Agent) stopRecovered(ctx context.Context, id, allocationID string) {
	unlock := a.lockAllocationOperation(allocationID)
	defer unlock()
	a.mu.RLock()
	allocation := a.allocations[id]
	qualifies := allocation != nil && a.pendingRecoveryStopLocked(allocation)
	retainLogs := allocation != nil && a.retainedLogs[allocation.ContainerID] != nil
	a.mu.RUnlock()
	if !qualifies {
		return
	}
	if err := a.stopAllocation(ctx, id, retainLogs); err != nil {
		a.log.Error("finish recovered allocation cleanup", "allocation", allocationID, "error", err)
	}
}

// hasUnreadableUnknownLocked reports whether a listing contained a container
// whose metadata could not be read and that no allocation accounts for. Such a
// container may be an unrecorded Trellis container, so listing is retried.
func (a *Agent) hasUnreadableUnknownLocked(containers []runtime.ContainerInfo) bool {
	for _, container := range containers {
		if _, known := a.allocations[container.ID]; !known && container.Labels == nil && !observedStatus(container.Status) {
			return true
		}
	}
	return false
}

func observedStatus(status runtime.ContainerStatus) bool {
	return status == runtime.StatusRunning || status == runtime.StatusCreated || status == runtime.StatusStopped || status == runtime.StatusPaused
}

func (a *Agent) reobserve(ctx context.Context, id, allocationID string, container runtime.ContainerInfo, found bool) {
	unlock := a.lockAllocationOperation(allocationID)
	defer unlock()
	allocation, ok := a.snapshotAllocation(id)
	// A start or stop may have replaced or removed the allocation meanwhile.
	if !ok || !allocation.unobserved {
		return
	}
	if !found {
		a.recoverMissing(ctx, &allocation)
		return
	}
	if !observedStatus(container.Status) {
		return
	}
	a.recoverContainer(container, &allocation)
}

// observeRecovered inspects an unobserved allocation on demand, so a start
// retry neither acknowledges nor replaces a container of unknown state. It
// returns the observed status, or "" when cleanup confirmed the container
// missing and removed the allocation. The caller holds the allocation
// operation lock.
func (a *Agent) observeRecovered(ctx context.Context, allocID string) (string, error) {
	allocation, ok := a.snapshotAllocation(allocID)
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrAllocationNotFound, allocID)
	}
	if !allocation.unobserved {
		return allocation.Status, nil
	}
	observed, err := a.runtime.Inspect(ctx, allocation.ContainerID)
	if errdefs.IsNotFound(err) {
		// Confirm absence with a successful listing, as recovery does.
		if managed, ok := a.runtime.(runtime.ManagedRuntime); ok {
			containers, listErr := managed.ListManaged(ctx, a.cluster)
			if listErr != nil {
				return "", fmt.Errorf("observe recovered allocation %s: %w", allocID, listErr)
			}
			listed := false
			for _, container := range containers {
				if container.ID != allocation.ContainerID {
					continue
				}
				if !observedStatus(container.Status) {
					return "", fmt.Errorf("observe recovered allocation %s: container state is %q", allocID, container.Status)
				}
				listed = true
				a.recoverContainer(container, &allocation)
			}
			if !listed {
				a.recoverMissing(ctx, &allocation)
			}
		}
	} else if err != nil {
		return "", fmt.Errorf("observe recovered allocation %s: %w", allocID, err)
	} else if !observedStatus(observed.Status) {
		return "", fmt.Errorf("observe recovered allocation %s: container state is %q", allocID, observed.Status)
	} else {
		container := *observed
		container.ID = allocation.ContainerID
		a.recoverContainer(container, &allocation)
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	if recovered := a.allocations[allocID]; recovered != nil {
		if recovered.unobserved {
			return "", fmt.Errorf("observe recovered allocation %s: container state is unavailable", allocID)
		}
		return recovered.Status, nil
	}
	return "", nil
}

// runRecoveryRetry retries recovery until every recorded allocation has been
// observed.
func (a *Agent) runRecoveryRetry(ctx context.Context) {
	delay := recoveryRetryMinDelay
	for pending := a.recoveryPending(); pending && !a.recoveryFailed(); pending = a.retryRecovery(ctx) {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		delay = min(delay*2, recoveryRetryMaxDelay)
	}
}

func (a *Agent) cleanupVolumeStaging(liveContainers []string) {
	if err := a.volumes.CleanupStaging(liveContainers); err != nil {
		a.log.Error("clean up stale volume staging mounts", "error", err)
	}
}

func allocationFromRuntime(container runtime.ContainerInfo) *Allocation {
	generation, err := strconv.ParseUint(container.Labels["trellis.allocation-generation"], 10, 64)
	if err != nil || generation == 0 {
		return nil
	}
	jobRevision, err := strconv.Atoi(container.Labels["trellis.job-revision"])
	if err != nil || jobRevision <= 0 {
		return nil
	}
	allocationID := container.Labels["trellis.allocation-id"]
	executionHash := container.Labels["trellis.execution-hash"]
	if allocationID == "" || executionHash == "" {
		return nil
	}
	return &Allocation{
		ID: container.ID, ContainerID: container.ID, AllocationID: allocationID,
		Generation: generation, JobRevision: jobRevision, ExecutionHash: executionHash,
		Namespace: container.Labels["trellis.namespace"], JobName: container.Labels["trellis.job"],
		GroupName: container.Labels["trellis.task-group"], TaskName: container.Labels["trellis.task"],
		Status: "running", Health: "unknown",
	}
}

func (a *Agent) containerMatchesAllocation(container runtime.ContainerInfo, allocation *Allocation) bool {
	labels := container.Labels
	return container.ID == allocation.ContainerID &&
		labels["trellis.cluster"] == a.cluster &&
		labels["trellis.allocation-id"] == allocation.AllocationID &&
		labels["trellis.allocation-generation"] == strconv.FormatUint(allocation.Generation, 10) &&
		labels["trellis.job-revision"] == strconv.Itoa(allocation.JobRevision) &&
		labels["trellis.execution-hash"] == allocation.ExecutionHash &&
		labels["trellis.namespace"] == allocation.Namespace &&
		labels["trellis.job"] == allocation.JobName &&
		labels["trellis.task-group"] == allocation.GroupName &&
		labels["trellis.task"] == allocation.TaskName
}
