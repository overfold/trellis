package api

import (
	"encoding/json"
	"time"

	"github.com/clofour/trellis/internal/lifecycle"
	"github.com/clofour/trellis/internal/spec"
	"github.com/google/uuid"
)

// BackupFormatVersion is the current desired-state backup format.
const BackupFormatVersion = 3

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
	ID            uuid.UUID             `json:"id"`
	Host          string                `json:"host"`
	Port          int                   `json:"port"`
	Status        NodeStatusResponse    `json:"status"`
	LastHeartbeat time.Time             `json:"last_heartbeat"`
	CPU           int                   `json:"cpu"`
	Memory        int64                 `json:"memory"`
	OS            string                `json:"os,omitempty"`
	Arch          string                `json:"arch,omitempty"`
	Labels        map[string]string     `json:"labels,omitempty"`
	Volumes       []string              `json:"volumes,omitempty"`
	Capabilities  []spec.NodeCapability `json:"capabilities,omitempty"`
	Version       string                `json:"version,omitempty"`
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
	NodeID       uuid.UUID             `json:"id"`
	Timestamp    time.Time             `json:"timestamp"`
	Allocations  []AllocationStatus    `json:"allocations,omitempty"`
	Volumes      []string              `json:"volumes,omitempty"`
	Capabilities []spec.NodeCapability `json:"capabilities,omitempty"`
	Version      string                `json:"version,omitempty"`
}

// DesiredAllocation describes the generation an agent should run.
type DesiredAllocation struct {
	ID         string `json:"id"`
	Generation uint64 `json:"generation"`
	Draining   bool   `json:"draining,omitempty"`
}

// HeartbeatResponse returns the desired allocations for a node.
type HeartbeatResponse struct {
	Epoch              uint64              `json:"epoch"`
	LeaderID           uuid.UUID           `json:"leader_id"`
	Desired            []DesiredAllocation `json:"desired"`
	OrphanConfirmation bool                `json:"orphan_confirmation"`
}

// AllocationStatus reports the observed state of an allocation.
type AllocationStatus struct {
	ID         string           `json:"id"`
	Generation uint64           `json:"generation"`
	Task       string           `json:"task,omitempty"`
	Address    string           `json:"address,omitempty"`
	Phase      lifecycle.Phase  `json:"phase"`
	Health     lifecycle.Health `json:"health"`
	Ports      []PortMapping    `json:"ports,omitempty"`
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
}

// JobStatusResponse summarizes the desired and observed state of a job.
type JobStatusResponse struct {
	Name        string               `json:"name"`
	Revision    int                  `json:"revision"`
	Desired     int                  `json:"desired"`
	Running     int                  `json:"running"`
	Healthy     int                  `json:"healthy"`
	Allocations []AllocationResponse `json:"allocations"`
	Spec        *spec.JobSpec        `json:"spec,omitempty"`
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
	ID          string    `json:"id"`
	NodeID      uuid.UUID `json:"node_id"`
	RaftAddress string    `json:"raft_address"`
}

// NodeEnrollmentRequest asks a managed cluster to issue one node identity.
type NodeEnrollmentRequest struct {
	NodeID          uuid.UUID `json:"node_id"`
	ServerAdvertise string    `json:"server_advertise"`
	AgentAdvertise  string    `json:"agent_advertise"`
}

// NodeEnrollmentResponse returns managed node signing materials.
type NodeEnrollmentResponse struct {
	CACert string `json:"ca_cert"`
	CAKey  string `json:"ca_key"`
	Cert   string `json:"cert"`
	Key    string `json:"key"`
}

// JobRevisionResponse describes one persisted revision of a job.
type JobRevisionResponse struct {
	Revision  int          `json:"revision"`
	Spec      spec.JobSpec `json:"spec"`
	CreatedAt time.Time    `json:"created_at"`
}

// JobRevisionListResponse is the response returned when listing job revisions.
type JobRevisionListResponse = []JobRevisionResponse

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
	// EventJobRegistered fires when a job is applied.
	EventJobRegistered EventType = "job.registered"
	// EventJobDeleted fires when a job is deleted.
	EventJobDeleted EventType = "job.deleted"
)

// ClusterEvent carries a typed cluster event payload.
type ClusterEvent struct {
	Type         EventType `json:"type"`
	Namespace    string    `json:"namespace,omitempty"`
	JobName      string    `json:"job,omitempty"`
	AllocationID string    `json:"allocation_id,omitempty"`
	Phase        string    `json:"phase,omitempty"`
	Health       string    `json:"health,omitempty"`
	Revision     int       `json:"revision,omitempty"`
	At           time.Time `json:"at"`
}
