package api

import "time"

// EventType identifies the kind of a cluster event.
type EventType string

const (
	// EventAllocationPhaseChanged fires when an allocation's phase transitions.
	EventAllocationPhaseChanged EventType = "allocation.phase_changed"
	// EventAllocationHealthChanged fires when an allocation's health changes.
	EventAllocationHealthChanged EventType = "allocation.health_changed"
	// EventJobRegistered fires when an apply changes a job and carries the
	// job's new version and revision.
	EventJobRegistered EventType = "job.registered"
	// EventJobDeleted fires when a job is deleted.
	EventJobDeleted EventType = "job.deleted"
	// EventJobReplacementDelayed fires when failed allocations put a task
	// group's replacements into backoff.
	EventJobReplacementDelayed EventType = "job.replacement_delayed"
	// EventJobReplacementBackoffReset fires when an operator clears a task
	// group's replacement backoff.
	EventJobReplacementBackoffReset EventType = "job.replacement_backoff_reset"
)

// ClusterEvent carries a typed cluster event payload. The events endpoints
// stream one JSON-encoded ClusterEvent per server-sent event.
type ClusterEvent struct {
	Type         EventType `json:"type"`
	Namespace    string    `json:"namespace,omitempty"`
	JobName      string    `json:"job,omitempty"`
	AllocationID string    `json:"allocation_id,omitempty"`
	Phase        string    `json:"phase,omitempty"`
	Health       string    `json:"health,omitempty"`
	Version      int       `json:"version,omitempty"`
	Revision     int       `json:"revision,omitempty"`
	Group        string    `json:"group,omitempty"`
	// Failures and NextReplacementAt describe a replacement backoff.
	Failures          int        `json:"failures,omitempty"`
	NextReplacementAt *time.Time `json:"next_replacement_at,omitempty"`
	At                time.Time  `json:"at"`
}
