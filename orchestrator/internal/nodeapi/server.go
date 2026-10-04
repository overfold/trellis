package nodeapi

import (
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	"github.com/overfold/trellis/orchestrator/internal/spec"
)

// NodeRegistrationRequest contains the identity and capacity of a joining node.
type NodeRegistrationRequest struct {
	ID                 uuid.UUID             `json:"id"`
	Host               string                `json:"host"`
	Port               int                   `json:"port"`
	CPU                int                   `json:"cpu"`
	Memory             int64                 `json:"memory"`
	CPUCapacity        int                   `json:"cpu_capacity,omitempty"`
	MemoryCapacity     int64                 `json:"memory_capacity,omitempty"`
	CPUAllocatable     int                   `json:"cpu_allocatable,omitempty"`
	MemoryAllocatable  int64                 `json:"memory_allocatable,omitempty"`
	OS                 string                `json:"os"`
	Arch               string                `json:"arch"`
	Labels             map[string]string     `json:"labels,omitempty"`
	Volumes            []string              `json:"volumes,omitempty"`
	Capabilities       []spec.NodeCapability `json:"capabilities,omitempty"`
	WireGuardPublicKey string                `json:"wireguard_public_key,omitempty"`
	WireGuardEndpoint  string                `json:"wireguard_endpoint,omitempty"`
	WireGuardPortBase  int                   `json:"wireguard_port_base,omitempty"`
	WireGuardPortCount int                   `json:"wireguard_port_count,omitempty"`
}

// NodeRegistrationResponse confirms the registered node identity.
type NodeRegistrationResponse struct {
	ID uuid.UUID `json:"id"`
}

// HeartbeatRequest reports a node and its current allocations.
type HeartbeatRequest struct {
	NodeID            uuid.UUID             `json:"id"`
	Timestamp         time.Time             `json:"timestamp"`
	Allocations       []AllocationStatus    `json:"allocations,omitempty"`
	Volumes           []string              `json:"volumes,omitempty"`
	Capabilities      []spec.NodeCapability `json:"capabilities,omitempty"`
	Version           string                `json:"version,omitempty"`
	CPUCapacity       int                   `json:"cpu_capacity,omitempty"`
	MemoryCapacity    int64                 `json:"memory_capacity,omitempty"`
	CPUAllocatable    int                   `json:"cpu_allocatable,omitempty"`
	MemoryAllocatable int64                 `json:"memory_allocatable,omitempty"`
	CPUUsage          *float64              `json:"cpu_usage,omitempty"`
	MemoryUsed        *int64                `json:"memory_used,omitempty"`
	MemoryAvailable   *int64                `json:"memory_available,omitempty"`
	MetricsAt         *time.Time            `json:"metrics_at,omitempty"`
	// RaftAppliedIndex is the node's last applied Raft log index. The leader
	// promotes only non-voters that are caught up.
	RaftAppliedIndex uint64 `json:"raft_applied_index,omitempty"`
}

// AllocationStatus reports the observed state of an allocation.
type AllocationStatus struct {
	ID         string `json:"id"`
	Generation uint64 `json:"generation"`
	Task       string `json:"task,omitempty"`
	// RetainedLogs reports a terminal task whose execution resources are gone
	// but whose logs remain pending control-plane history pruning.
	RetainedLogs bool             `json:"retained_logs,omitempty"`
	Address      string           `json:"address,omitempty"`
	Phase        lifecycle.Phase  `json:"phase"`
	Health       lifecycle.Health `json:"health"`
	// Reason identifies why a task reported phase failed. It is empty for
	// every other phase.
	Reason OperationCode     `json:"reason,omitempty"`
	Ports  []api.PortMapping `json:"ports,omitempty"`
	// StartFailure reports that the agent's background start of this
	// generation failed. It is only set with phase starting.
	StartFailure *StartFailure `json:"start_failure,omitempty"`
}

// MaxStartFailureMessageBytes bounds a reported start failure message.
const MaxStartFailureMessageBytes = 1024

// StartFailure describes a failed background allocation start. Attempt echoes
// the start request's attempt; the agent reports the failure until a start
// with a different attempt, a stop, or a newer generation replaces it. Code is
// set for a failure that retrying the generation cannot fix:
// OperationStaleGeneration, OperationConflict, or OperationRestartExhausted.
type StartFailure struct {
	Attempt int           `json:"attempt"`
	Code    OperationCode `json:"code,omitempty"`
	Message string        `json:"message"`
}

// ServiceEntry describes a discoverable allocation endpoint.
type ServiceEntry struct {
	ID        string            `json:"id"`
	Job       string            `json:"job"`
	Group     string            `json:"group"`
	Namespace string            `json:"namespace"`
	Labels    map[string]string `json:"labels,omitempty"`
	Address   string            `json:"address"`
	Ports     []api.PortMapping `json:"ports,omitempty"`
	Status    string            `json:"status"`
}

// ServiceListResponse is the response returned for service discovery.
type ServiceListResponse = []ServiceEntry

// RaftJoinRequest identifies a server joining the Raft cluster.
type RaftJoinRequest struct {
	RaftAddress   string `json:"raft_address"`
	ServerAddress string `json:"server_address"`
}

// RaftJoinResponse returns managed signing material only after the node's
// certificate-bound identity has been admitted as a Raft member. Members lists
// the Raft member node IDs at admission; a new member accepts inbound Raft
// streams only from these nodes until it has replicated the cluster's own
// membership and certificate bindings.
type RaftJoinResponse struct {
	CAKey   string   `json:"ca_key,omitempty"`
	Members []string `json:"members"`
}

// NodeEnrollmentRequest asks a managed cluster to issue one node identity. The
// request authenticates with a join token as its bearer credential.
type NodeEnrollmentRequest struct {
	ServerAdvertise string `json:"server_advertise"`
	AgentAdvertise  string `json:"agent_advertise"`
	RaftAdvertise   string `json:"raft_advertise"`
}

// NodeEnrollmentResponse returns managed node signing materials.
type NodeEnrollmentResponse struct {
	NodeID uuid.UUID `json:"node_id"`
	CACert string    `json:"ca_cert"`
	Cert   string    `json:"cert"`
	Key    string    `json:"key"`
}

// AgentExecRequest is an ExecRequest forwarded by the leader to an agent.
// Task is always resolved, and Epoch fences the stream to the leadership
// term that opened it.
type AgentExecRequest struct {
	api.ExecRequest
	Epoch uint64
}
