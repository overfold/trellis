package agent

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/overfold/trellis/orchestrator/internal/network"
	"github.com/overfold/trellis/orchestrator/internal/runtime"
	"github.com/overfold/trellis/orchestrator/internal/spec"
)

// Allocation contains agent-local allocation state.
type Allocation struct {
	ID               string                  `json:"ID"`
	AllocationID     string                  `json:"AllocationID"`
	Generation       uint64                  `json:"Generation"`
	JobRevision      int                     `json:"JobRevision"`
	ExecutionHash    string                  `json:"ExecutionHash"`
	Restart          *spec.RestartPolicySpec `json:"Restart"`
	RestartAttempts  int                     `json:"RestartAttempts"`
	RestartWindow    time.Time               `json:"RestartWindow"`
	RestartExhausted bool                    `json:"RestartExhausted"`
	Namespace        string                  `json:"Namespace"`

	JobName   string         `json:"JobName"`
	GroupName string         `json:"GroupName"`
	TaskName  string         `json:"TaskName"`
	Spec      *spec.TaskSpec `json:"Spec"`

	ContainerID                  string                    `json:"ContainerID"`
	ContainerOwnershipUnverified bool                      `json:"ContainerOwnershipUnverified"`
	Ports                        []*runtime.Port           `json:"Ports"`
	Mounts                       []*runtime.Mount          `json:"Mounts"`
	SecretDir                    string                    `json:"SecretDir"`
	Network                      *network.Attachment       `json:"Network"`
	NetworkIntent                *network.AttachmentIntent `json:"NetworkIntent"`
	Status                       string                    `json:"Status"`
	Health                       string                    `json:"Health"`
	Draining                     bool                      `json:"Draining"`

	DrainSequence uint64 `json:"DrainSequence"`

	// unobserved marks a recovered allocation whose container state has not
	// been read since the agent restarted. Its health is reported as unknown
	// while the recorded value is kept for the next observation.
	unobserved bool
}

type retainedTaskLog struct {
	AllocationID string `json:"allocation_id"`
	Generation   uint64 `json:"generation"`
	Namespace    string `json:"namespace"`
	TaskName     string `json:"task_name"`
	ContainerID  string `json:"container_id"`
}

// controlEpochKey is the local storage key of the highest accepted control
// epoch, below the node data directory.
const controlEpochKey = "agent/control-epoch"

// reportedHealth is the health an allocation reports; recovered allocations
// whose container has not been observed report unknown.
func reportedHealth(allocation *Allocation) string {
	if allocation.unobserved {
		return "unknown"
	}
	return allocation.Health
}

// AcceptEpoch validates and persists a leadership epoch.
func (a *Agent) AcceptEpoch(epoch uint64) error {
	if epoch == 0 {
		return ErrInvalidEpoch
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if epoch < a.epoch {
		return fmt.Errorf("%w: received %d, highest accepted %d", ErrStaleEpoch, epoch, a.epoch)
	}
	if epoch == a.epoch {
		return nil
	}
	if a.local != nil {
		if err := a.local.Put(controlEpochKey, epoch); err != nil {
			return fmt.Errorf("persist control-plane epoch: %w", err)
		}
	}
	a.epoch = epoch
	for _, session := range a.execSessions {
		if session.Epoch < epoch {
			session.cancel(errExecLeaderChanged)
		}
	}
	return nil
}

// allocationFileName encodes an allocation ID as one safe path element. Record
// and secret directory names share it so the startup sweep can match them.
func allocationFileName(id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(id))
}

func allocationRecordKey(id string) string {
	return "agent/allocations/" + allocationFileName(id)
}

func retainedLogRecordKey(id string) string {
	return "agent/retained-logs/" + allocationFileName(id)
}

func (a *Agent) persistAllocation(allocation *Allocation) error {
	if a.local == nil {
		return nil
	}
	return a.local.Put(allocationRecordKey(allocation.ID), allocation)
}

func (a *Agent) deleteAllocationRecord(id string) error {
	if a.local == nil {
		return nil
	}
	return a.local.Delete(allocationRecordKey(id))
}

func (a *Agent) persistRetainedLog(record *retainedTaskLog) error {
	if a.local == nil {
		return nil
	}
	return a.local.Put(retainedLogRecordKey(record.ContainerID), record)
}

func (a *Agent) deleteRetainedLogRecord(id string) error {
	if a.local == nil {
		return nil
	}
	return a.local.Delete(retainedLogRecordKey(id))
}

func (a *Agent) markAllocationStopping(id string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	allocation := a.allocations[id]
	if allocation == nil {
		return fmt.Errorf("%w: %s", ErrAllocationNotFound, id)
	}
	allocation.Status = "stopping"
	if err := a.persistAllocation(allocation); err != nil {
		return fmt.Errorf("persist stopping allocation: %w", err)
	}
	return nil
}

// snapshotAllocation copies an allocation so recovery can reclassify it
// without mutating state that readers share.
func (a *Agent) snapshotAllocation(id string) (Allocation, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	current := a.allocations[id]
	if current == nil {
		return Allocation{}, false
	}
	allocation := *current
	allocation.Ports = append([]*runtime.Port(nil), current.Ports...)
	allocation.Mounts = append([]*runtime.Mount(nil), current.Mounts...)
	return allocation, true
}

// GetAllocations returns copies of agent allocation state.
func (a *Agent) GetAllocations() []*Allocation {
	a.mu.RLock()
	defer a.mu.RUnlock()
	result := make([]*Allocation, 0, len(a.allocations))
	for _, alloc := range a.allocations {
		allocationCopy := *alloc
		allocationCopy.Ports = append([]*runtime.Port(nil), alloc.Ports...)
		allocationCopy.Mounts = append([]*runtime.Mount(nil), alloc.Mounts...)
		allocationCopy.Health = reportedHealth(alloc)
		result = append(result, &allocationCopy)
	}

	return result
}

// OnHealthy and OnUnhealthy are observation callbacks from the health manager.
// They intentionally do not mutate allocation status directly; lifecycle state
// transitions are centralized in the allocation reconciler.
func (a *Agent) OnHealthy(ctx context.Context, allocID string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	// A replaced worker can finish a probe after RegisterTask cancels it.
	if ctx.Err() != nil {
		return nil
	}
	if allocation := a.allocations[allocID]; allocation != nil && allocation.Status != "failed" {
		allocation.Health = "healthy"
		return a.persistAllocation(allocation)
	}
	return nil
}

// OnUnhealthy handles an unhealthy allocation.
func (a *Agent) OnUnhealthy(ctx context.Context, allocID string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if ctx.Err() != nil {
		return nil
	}
	if allocation := a.allocations[allocID]; allocation != nil && allocation.Status != "failed" {
		allocation.Health = "unhealthy"
		return a.persistAllocation(allocation)
	}
	return nil
}

// OnReconciledStatus records reconciled allocation status.
func (a *Agent) OnReconciledStatus(allocID, status string) {
	a.mu.Lock()
	if alloc := a.allocations[allocID]; alloc != nil {
		if alloc.Status == "failed" && (status == "healthy" || status == "unhealthy") {
			// A terminally failed task is not probed; ignore late health.
			a.mu.Unlock()
			return
		}
		if status == "healthy" || status == "unhealthy" {
			alloc.Health = status
		} else {
			alloc.Status = status
			if status == "failed" {
				alloc.Health = "unhealthy"
				a.health.DeregisterTask(allocID)
			}
			if status == "running" && alloc.Spec != nil && alloc.Spec.HealthCheck != nil {
				alloc.Health = "unknown"
				a.health.RegisterTask(allocID, alloc.ContainerID, alloc.Spec.HealthCheck, "namespace", alloc.Namespace, "job", alloc.JobName, "allocation", alloc.AllocationID, "task", alloc.TaskName)
			}
		}
		if err := a.persistAllocation(alloc); err != nil {
			a.log.Error("persist reconciled allocation", "allocation", alloc.AllocationID, "error", err)
		}
	}
	a.mu.Unlock()
}

// OnRestartState records allocation restart state. An exhausted budget also
// records the terminal failed observation in the same write, so recovery
// never sees exhaustion without the failure or the reverse.
func (a *Agent) OnRestartState(allocID string, attempts int, window time.Time, exhausted bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	allocation := a.allocations[allocID]
	if allocation == nil {
		return nil
	}
	previousAttempts, previousWindow, previousExhausted := allocation.RestartAttempts, allocation.RestartWindow, allocation.RestartExhausted
	previousStatus, previousHealth := allocation.Status, allocation.Health
	allocation.RestartAttempts, allocation.RestartWindow, allocation.RestartExhausted = attempts, window, exhausted
	if exhausted {
		// The container stopped and will not be restarted; stop probing it.
		allocation.Status, allocation.Health = "failed", "unhealthy"
		a.health.DeregisterTask(allocID)
	}
	if err := a.persistAllocation(allocation); err != nil {
		allocation.RestartAttempts, allocation.RestartWindow, allocation.RestartExhausted = previousAttempts, previousWindow, previousExhausted
		allocation.Status, allocation.Health = previousStatus, previousHealth
		return fmt.Errorf("persist restart tracking for %s: %w", allocID, err)
	}
	return nil
}
