package api

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/internal/lifecycle"
	"github.com/overfold/trellis/internal/spec"
)

// BackupFormatVersion is the current desired-state backup format.
const BackupFormatVersion = 4

// BackupSnapshot contains desired state only. Secret values remain encrypted
// exactly as stored in Raft and still require the separately managed KEK.
type BackupSnapshot struct {
	FormatVersion            int                        `json:"format_version"`
	CreatedAt                time.Time                  `json:"created_at"`
	Jobs                     map[string]json.RawMessage `json:"jobs"`
	JobRevisions             map[string]json.RawMessage `json:"job_revisions"`
	Secrets                  map[string]json.RawMessage `json:"secrets"`
	VolumeRegistrations      map[string]json.RawMessage `json:"volume_registrations"`
	NetworkPortRegistrations map[string]json.RawMessage `json:"network_port_registrations"`
}

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
	LastHeartbeat     *time.Time            `json:"last_heartbeat,omitempty"`
	CPU               int                   `json:"cpu"`
	Memory            int64                 `json:"memory"`
	CPUCapacity       int                   `json:"cpu_capacity"`
	MemoryCapacity    int64                 `json:"memory_capacity"`
	CPUAllocatable    int                   `json:"cpu_allocatable"`
	MemoryAllocatable int64                 `json:"memory_allocatable"`
	CPUUsage          *float64              `json:"cpu_usage,omitempty"`
	MemoryUsed        *int64                `json:"memory_used,omitempty"`
	MemoryAvailable   *int64                `json:"memory_available,omitempty"`
	MetricsAt         *time.Time            `json:"metrics_at,omitempty"`
	OS                string                `json:"os,omitempty"`
	Arch              string                `json:"arch,omitempty"`
	Labels            map[string]string     `json:"labels,omitempty"`
	Volumes           []string              `json:"volumes,omitempty"`
	Capabilities      []spec.NodeCapability `json:"capabilities,omitempty"`
	Version           string                `json:"version,omitempty"`
}

// NodeListResponse is the response returned when listing nodes.
type NodeListResponse = []NodeResponse

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
}

// AllocationStatus reports the observed state of an allocation.
type AllocationStatus struct {
	ID         string           `json:"id"`
	Generation uint64           `json:"generation"`
	Task       string           `json:"task,omitempty"`
	Address    string           `json:"address,omitempty"`
	Phase      lifecycle.Phase  `json:"phase"`
	Health     lifecycle.Health `json:"health"`
	// Reason identifies why a task reported phase failed. It is empty for
	// every other phase.
	Reason OperationCode `json:"reason,omitempty"`
	Ports  []PortMapping `json:"ports,omitempty"`
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

// JobRegistrationRequest contains the job specification to register.
type JobRegistrationRequest struct {
	Spec spec.JobSpec `json:"spec"`
	// ExpectedVersion makes the apply conditional on the job's current
	// version: 0 requires that the job does not exist, and N requires that
	// the job is at version N. When omitted the apply is unconditional.
	ExpectedVersion *int `json:"expected_version,omitempty"`
}

// JobRegistrationResponse reports the job version and revision after an apply.
type JobRegistrationResponse struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Version   int    `json:"version"`
	Revision  int    `json:"revision"`
}

// JobStatusResponse summarizes the desired and observed state of a job.
type JobStatusResponse struct {
	Name        string               `json:"name"`
	Version     int                  `json:"version"`
	Revision    int                  `json:"revision"`
	Desired     int                  `json:"desired"`
	Running     int                  `json:"running"`
	Healthy     int                  `json:"healthy"`
	Allocations []AllocationResponse `json:"allocations"`
	// ReplacementBackoff lists task groups with consecutive failed
	// allocations. New placements for such a group wait until
	// next_replacement_at.
	ReplacementBackoff []ReplacementBackoffResponse `json:"replacement_backoff,omitempty"`
	Spec               *spec.JobSpec                `json:"spec,omitempty"`
}

// ReplacementBackoffResponse describes why replacements for a task group are
// delayed after its allocations failed.
type ReplacementBackoffResponse struct {
	Group             string    `json:"group"`
	JobRevision       int       `json:"job_revision"`
	Failures          int       `json:"failures"`
	LastFailureAt     time.Time `json:"last_failure_at"`
	LastAllocationID  string    `json:"last_allocation_id,omitempty"`
	Reason            string    `json:"reason,omitempty"`
	Message           string    `json:"message,omitempty"`
	NextReplacementAt time.Time `json:"next_replacement_at"`
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
	Phase            lifecycle.Phase      `json:"phase"`
	Health           lifecycle.Health     `json:"health"`
	Draining         bool                 `json:"draining,omitempty"`
	Generation       uint64               `json:"generation"`
	JobRevision      int                  `json:"job_revision"`
	CreatedAt        time.Time            `json:"created_at"`
	LastTransitionAt time.Time            `json:"last_transition_at"`
	Reason           string               `json:"reason,omitempty"`
	Message          string               `json:"message,omitempty"`
	Attempt          int                  `json:"attempt"`
	NextRetryAt      *time.Time           `json:"next_retry_at,omitempty"`
}

// AllocationListResponse is the response returned when listing allocations.
type AllocationListResponse = []AllocationResponse

// AllocationEventResponse describes an allocation lifecycle event.
type AllocationEventResponse struct {
	Phase   lifecycle.Phase `json:"phase"`
	Reason  string          `json:"reason,omitempty"`
	Message string          `json:"message,omitempty"`
	At      time.Time       `json:"at"`
}

// AllocationEventListResponse is the response returned when listing allocation events.
type AllocationEventListResponse = []AllocationEventResponse

// JobListResponse is the response returned when listing jobs.
type JobListResponse = []JobStatusResponse

// ServiceEntry describes a discoverable allocation endpoint.
type ServiceEntry struct {
	ID        string            `json:"id"`
	Job       string            `json:"job"`
	Group     string            `json:"group"`
	Namespace string            `json:"namespace"`
	Labels    map[string]string `json:"labels,omitempty"`
	Address   string            `json:"address"`
	Ports     []PortMapping     `json:"ports,omitempty"`
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
// certificate-bound identity has been admitted as a Raft voter.
type RaftJoinResponse struct {
	CAKey string `json:"ca_key,omitempty"`
}

// NodeEnrollmentRequest asks a managed cluster to issue one node identity.
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

// JobVersionResponse describes one retained version of a job and the execution
// revision it ran.
type JobVersionResponse struct {
	Version   int          `json:"version"`
	Revision  int          `json:"revision"`
	Spec      spec.JobSpec `json:"spec"`
	CreatedAt time.Time    `json:"created_at"`
}

// JobVersionListResponse is the response returned when listing job versions.
type JobVersionListResponse = []JobVersionResponse

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

// ExecRequest is the body for an allocation exec call.
type ExecRequest struct {
	Task    string   `json:"task,omitempty"`
	Command []string `json:"command"`
}

// ExecResponse is the response from an allocation exec call.
type ExecResponse struct {
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
	ExitCode int    `json:"exit_code"`
}

// ExecSessionCreateRequest starts an interactive TTY session in an allocation task.
type ExecSessionCreateRequest struct {
	Task    string   `json:"task,omitempty"`
	Command []string `json:"command"`
	Term    string   `json:"term,omitempty"`
	Cols    uint32   `json:"cols,omitempty"`
	Rows    uint32   `json:"rows,omitempty"`
}

// ExecSessionResponse identifies a live interactive exec session.
type ExecSessionResponse struct {
	ID string `json:"id"`
}

// ExecSessionInputRequest appends terminal input bytes encoded as base64.
type ExecSessionInputRequest struct {
	DataBase64 string `json:"data_base64"`
}

// ExecSessionResizeRequest updates the terminal dimensions.
type ExecSessionResizeRequest struct {
	Cols uint32 `json:"cols"`
	Rows uint32 `json:"rows"`
}

// ExecSessionOutputResponse returns terminal bytes since a byte offset.
type ExecSessionOutputResponse struct {
	DataBase64 string `json:"data_base64,omitempty"`
	NextOffset int64  `json:"next_offset"`
	Exited     bool   `json:"exited"`
	ExitCode   *int   `json:"exit_code,omitempty"`
}

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

// ClusterEvent carries a typed cluster event payload.
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
