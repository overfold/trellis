package api

import (
	"time"

	"github.com/google/uuid"
)

// AllocationPhase describes allocation execution. Health is reported
// separately.
type AllocationPhase string

const (
	// PhasePending describes an allocation waiting for a compatible node.
	PhasePending AllocationPhase = "pending"
	// PhasePlaced describes an allocation accepted for placement.
	PhasePlaced AllocationPhase = "placed"
	// PhaseStarting indicates that the allocation's tasks are starting.
	PhaseStarting AllocationPhase = "starting"
	// PhaseRunning indicates that the allocation is running.
	PhaseRunning AllocationPhase = "running"
	// PhaseStopping indicates that the allocation is stopping.
	PhaseStopping AllocationPhase = "stopping"
	// PhaseStopped indicates that the allocation has stopped.
	PhaseStopped AllocationPhase = "stopped"
	// PhaseFailed indicates that the allocation failed.
	PhaseFailed AllocationPhase = "failed"
	// PhaseLost indicates that the allocation was lost.
	PhaseLost AllocationPhase = "lost"
)

// AllocationHealth describes the health-check state of an allocation.
type AllocationHealth string

const (
	// HealthUnknown indicates that health is not yet known.
	HealthUnknown AllocationHealth = "unknown"
	// HealthHealthy indicates that health checks pass.
	HealthHealthy AllocationHealth = "healthy"
	// HealthUnhealthy indicates that a health check failed.
	HealthUnhealthy AllocationHealth = "unhealthy"
)

// PortMapping maps a host port to a container port.
type PortMapping struct {
	HostPort      int `json:"host_port"`
	ContainerPort int `json:"container_port"`
}

// AllocationEndpoint describes one task's routable endpoint within an allocation.
type AllocationEndpoint struct {
	Task    string        `json:"task"`
	Address string        `json:"address,omitempty"`
	Ports   []PortMapping `json:"ports,omitempty"`
}

// AllocationResponse describes an allocation and its latest state.
type AllocationResponse struct {
	ID               string               `json:"id"`
	Job              string               `json:"job,omitempty"`
	Group            string               `json:"group"`
	Namespace        string               `json:"namespace,omitempty"`
	NodeID           uuid.UUID            `json:"node_id"`
	Labels           map[string]string    `json:"labels,omitempty"`
	Address          string               `json:"address,omitempty"`
	Ports            []PortMapping        `json:"ports,omitempty"`
	Endpoints        []AllocationEndpoint `json:"endpoints,omitempty"`
	Phase            AllocationPhase      `json:"phase"`
	Health           AllocationHealth     `json:"health"`
	Draining         bool                 `json:"draining,omitempty"`
	Generation       uint64               `json:"generation"`
	JobRevision      int                  `json:"job_revision"`
	CreatedAt        time.Time            `json:"created_at"`
	LastTransitionAt time.Time            `json:"last_transition_at"`
	Reason           string               `json:"reason,omitempty"`
	Message          string               `json:"message,omitempty"`
	Attempt          int                  `json:"attempt"`
	NextRetryAt      *time.Time           `json:"next_retry_at,omitempty"`
	// Tasks lists the allocation's task names, independently of network endpoints.
	Tasks []string `json:"tasks,omitempty"`
}

// AllocationListResponse is the response returned when listing allocations.
type AllocationListResponse = []AllocationResponse

// AllocationEventResponse describes an allocation lifecycle event.
type AllocationEventResponse struct {
	Phase   AllocationPhase `json:"phase"`
	Reason  string          `json:"reason,omitempty"`
	Message string          `json:"message,omitempty"`
	At      time.Time       `json:"at"`
}

// AllocationEventListResponse is the response returned when listing allocation events.
type AllocationEventListResponse = []AllocationEventResponse

// AllocationMetricsResponse reports current resource usage for an allocation task.
type AllocationMetricsResponse struct {
	AllocationID        string    `json:"allocation_id"`
	Task                string    `json:"task"`
	CPUUsageNanoseconds int64     `json:"cpu_usage_nanoseconds"`
	MemoryUsageBytes    int64     `json:"memory_usage_bytes"`
	CollectedAt         time.Time `json:"collected_at"`
}

// AllocationMetricsListResponse is the response returned when listing allocation metrics.
type AllocationMetricsListResponse = []AllocationMetricsResponse
