package api

import (
	"time"

	"github.com/google/uuid"
)

// NodeStatusResponse describes the scheduling status of a node.
type NodeStatusResponse string

const (
	// StatusHealthy and the following values describe node states.
	StatusHealthy NodeStatusResponse = "healthy"
	// StatusUnhealthy indicates that a node is not healthy.
	StatusUnhealthy NodeStatusResponse = "unhealthy"
	// StatusDraining indicates that a node is evacuating allocations.
	StatusDraining NodeStatusResponse = "draining"
)

// NodeResponse contains the reported state and capacity of a node.
type NodeResponse struct {
	ID     uuid.UUID          `json:"id"`
	Host   string             `json:"host"`
	Port   int                `json:"port"`
	Status NodeStatusResponse `json:"status"`
	// LastHeartbeat is when the current leader last received a heartbeat from
	// the node. It is absent until the node heartbeats to that leader.
	LastHeartbeat     *time.Time        `json:"last_heartbeat,omitempty"`
	CPU               int               `json:"cpu"`
	Memory            int64             `json:"memory"`
	CPUCapacity       int               `json:"cpu_capacity"`
	MemoryCapacity    int64             `json:"memory_capacity"`
	CPUAllocatable    int               `json:"cpu_allocatable"`
	MemoryAllocatable int64             `json:"memory_allocatable"`
	CPUUsage          *float64          `json:"cpu_usage,omitempty"`
	MemoryUsed        *int64            `json:"memory_used,omitempty"`
	MemoryAvailable   *int64            `json:"memory_available,omitempty"`
	MetricsAt         *time.Time        `json:"metrics_at,omitempty"`
	OS                string            `json:"os,omitempty"`
	Arch              string            `json:"arch,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	Volumes           []string          `json:"volumes,omitempty"`
	// TaskLogBytes is the size of the node's task logs, for running tasks
	// and terminal allocations whose logs are retained. Task logs have no
	// size limit. TaskLogFilesystemAvailable and TaskLogFilesystemCapacity
	// describe the filesystem that holds them. Each is omitted until the
	// node reports it.
	TaskLogBytes               *int64 `json:"task_log_bytes,omitempty"`
	TaskLogFilesystemAvailable *int64 `json:"task_log_filesystem_available,omitempty"`
	TaskLogFilesystemCapacity  *int64 `json:"task_log_filesystem_capacity,omitempty"`
	// Capabilities lists the node's optional features, such as
	// runtime.runsc or network.namespace.
	Capabilities []string `json:"capabilities,omitempty"`
	Version      string   `json:"version,omitempty"`
	// ControlPlane is the node's Raft membership: voter or nonvoter. It is
	// empty for a registered node that is no longer a member.
	ControlPlane ControlPlaneMembership `json:"control_plane,omitempty"`
}

// ControlPlaneMembership describes whether a node votes in the control plane.
type ControlPlaneMembership string

const (
	// ControlPlaneVoter nodes vote in leader elections and commit changes.
	ControlPlaneVoter ControlPlaneMembership = "voter"
	// ControlPlaneNonvoter nodes replicate state and can be promoted to voters.
	ControlPlaneNonvoter ControlPlaneMembership = "nonvoter"
)

// NodeListResponse is the response returned when listing nodes.
type NodeListResponse = []NodeResponse

// NodeIdentityCreateRequest enrolls an externally signed node identity with
// administrator authorization. It contains no private key material.
type NodeIdentityCreateRequest struct {
	Certificate string   `json:"certificate"`
	Role        NodeRole `json:"role"`
}

// NodeIdentityCreateResponse identifies the enrolled external node.
type NodeIdentityCreateResponse struct {
	ID uuid.UUID `json:"id"`
}
