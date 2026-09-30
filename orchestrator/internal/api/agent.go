// Package api defines the wire protocol shared by Trellis components.
package api

import (
	"github.com/overfold/trellis/internal/network"
	"github.com/overfold/trellis/internal/spec"
)

// AllocationRequest describes an allocation for an agent to start. Generation
// and Epoch must both be greater than zero. The agent accepts a fenced start
// and pulls images and creates tasks in the background; heartbeats report the
// outcome.
type AllocationRequest struct {
	AllocationID  string                  `json:"allocation_id"`
	Generation    uint64                  `json:"generation"`
	JobRevision   int                     `json:"job_revision"`
	Epoch         uint64                  `json:"epoch"`
	ExecutionHash string                  `json:"execution_hash"`
	Namespace     string                  `json:"namespace,omitempty"`
	JobName       string                  `json:"job_name"`
	GroupName     string                  `json:"group_name"`
	Tasks         []spec.TaskSpec         `json:"tasks"`
	Runtime       string                  `json:"runtime,omitempty"`
	NetworkPlan   *network.Plan           `json:"network_plan,omitempty"`
	EnvOverrides  map[string]string       `json:"env_overrides,omitempty"`
	Restart       *spec.RestartPolicySpec `json:"restart,omitempty"`
	Secrets       []DeliveredSecret       `json:"secrets,omitempty"`
	// Draining and DrainSequence carry the control plane's drain state for this
	// generation so a started task honors a drain the agent never recorded.
	// They are excluded from the execution hash.
	Draining      bool   `json:"draining,omitempty"`
	DrainSequence uint64 `json:"drain_sequence,omitempty"`
	// Attempt is the control plane's start attempt count for this generation.
	// A background start that fails reports it back in StartFailure, so each
	// failure is counted once. It is excluded from the execution hash.
	Attempt int `json:"attempt,omitempty"`
}

// StopAllocationRequest identifies an allocation generation to stop. Generation
// and Epoch must both be greater than zero.
type StopAllocationRequest struct {
	AllocationID string `json:"allocation_id"`
	Generation   uint64 `json:"generation"`
	Epoch        uint64 `json:"epoch"`
}

// DrainAllocationRequest identifies an allocation generation whose automatic
// restarts must be suppressed while the control plane prepares to stop it.
// Generation and Epoch must both be greater than zero.
type DrainAllocationRequest struct {
	AllocationID string `json:"allocation_id"`
	Generation   uint64 `json:"generation"`
	Epoch        uint64 `json:"epoch"`
	Sequence     uint64 `json:"sequence"`
}

// NetworkPlanRequest updates the peers for an active namespace network. Epoch
// must be greater than zero.
type NetworkPlanRequest struct {
	Epoch     uint64       `json:"epoch"`
	Namespace string       `json:"namespace"`
	Plan      network.Plan `json:"plan"`
}

// OperationCode identifies the result of an agent operation.
type OperationCode string

const (
	// OperationOK and the following values describe agent operation results.
	OperationOK OperationCode = "ok"
	// OperationStaleEpoch indicates that the request used an old leadership epoch.
	OperationStaleEpoch OperationCode = "stale_epoch"
	// OperationStaleGeneration indicates that the request used an old allocation generation.
	OperationStaleGeneration OperationCode = "stale_generation"
	// OperationConflict indicates a conflicting allocation execution.
	OperationConflict OperationCode = "execution_conflict"
	// OperationRestartExhausted indicates that the allocation generation failed
	// terminally after exhausting its restart policy.
	OperationRestartExhausted OperationCode = "restart_budget_exhausted"
	// OperationFailed indicates that an agent operation failed.
	OperationFailed OperationCode = "operation_failed"
)

// OperationResponse reports the outcome of an agent operation.
type OperationResponse struct {
	Code       OperationCode `json:"code"`
	Message    string        `json:"message,omitempty"`
	Generation uint64        `json:"generation,omitempty"`
	Epoch      uint64        `json:"epoch,omitempty"`
}

// AgentTaskMetrics holds resource usage for a single task container.
type AgentTaskMetrics struct {
	Task                string `json:"task"`
	CPUUsageNanoseconds int64  `json:"cpu_usage_nanoseconds"`
	MemoryUsageBytes    int64  `json:"memory_usage_bytes"`
}
