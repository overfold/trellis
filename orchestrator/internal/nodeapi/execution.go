package nodeapi

import (
	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/internal/network"
)

// ExecutionMode separates execution admission from preservation and destruction.
type ExecutionMode string

// Execution modes authorize launch, preservation, or generation-fenced cleanup.
const (
	ExecutionRun     ExecutionMode = "run"
	ExecutionRetain  ExecutionMode = "retain"
	ExecutionCleanup ExecutionMode = "cleanup"
)

// ExecutionIntent is a complete projection of durable node execution authority.
// Version increases when content changes within Epoch and resets in a new epoch.
// Agents order (Epoch, Version) lexicographically. Complete is required before
// absence can authorize cleanup; failure to fetch is never an empty view.
type ExecutionIntent struct {
	Release       uint64                  `json:"release"`
	NodeID        uuid.UUID               `json:"node_id"`
	LeaderID      uuid.UUID               `json:"leader_id"`
	Epoch         uint64                  `json:"epoch"`
	Version       uint64                  `json:"version"`
	Complete      bool                    `json:"complete"`
	CleanupAbsent bool                    `json:"cleanup_absent"`
	Allocations   []AllocationIntent      `json:"allocations"`
	NetworkPlans  map[string]network.Plan `json:"network_plans"`
}

// AllocationIntent carries no delivered secrets or workload bearer tokens.
// Request describes the execution identity even when relaunch is forbidden.
type AllocationIntent struct {
	Mode       ExecutionMode     `json:"mode"`
	Request    AllocationRequest `json:"request"`
	RetainLogs bool              `json:"retain_logs"`
}

// ExecutionCredentials is fetched separately for one authorized execution.
// It must never be included in the cached intent document.
type ExecutionCredentials struct {
	Secrets      []DeliveredSecret            `json:"secrets,omitempty"`
	EnvOverrides map[string]map[string]string `json:"env_overrides,omitempty"`
}
