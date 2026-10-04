package server

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	"github.com/overfold/trellis/orchestrator/internal/spec"
)

// Cluster contains persisted cluster identity, leadership fencing, and
// cluster-wide settings.
type Cluster struct {
	AdministratorPublicKey string          `json:"administrator_public_key"`
	ControlEpoch           uint64          `json:"control_epoch,omitempty"`
	Settings               ClusterSettings `json:"settings"`
}

// ClusterBootstrap is the initial replicated state of a new cluster. Only the
// node that creates the cluster supplies it; existing clusters ignore it.
type ClusterBootstrap struct {
	AdministratorPublicKey string
	Settings               ClusterSettings
}

// NodeRegistration contains the identity and capacity of a node.
type NodeRegistration struct {
	ID                 uuid.UUID
	Host               string
	Port               int
	CPUCapacity        int
	MemoryCapacity     int64
	CPUAllocatable     int
	MemoryAllocatable  int64
	OS                 string
	Arch               string
	Labels             map[string]string
	Volumes            []string
	Capabilities       []spec.NodeCapability
	WireGuardPublicKey string
	WireGuardEndpoint  string
	WireGuardPortBase  int
	WireGuardPortCount int
}

// nodeResourceObservation is the latest renewable whole-host resource sample.
type nodeResourceObservation struct {
	CPUCapacity       int
	MemoryCapacity    int64
	CPUAllocatable    int
	MemoryAllocatable int64
	CPUUsage          *float64
	MemoryUsed        *int64
	MemoryAvailable   *int64
	MetricsAt         *time.Time
}

// Node contains the in-memory state of a registered node.
type Node struct {
	ID   uuid.UUID
	Host string
	Port int
	// Status is the node's drain intent or, for a node that is not
	// draining, its liveness as of the latest reconciliation pass, so one
	// pass plans against one view. Current liveness is read from
	// Server.liveness.
	Status              NodeStatus
	CPUCapacity         int
	MemoryCapacity      int64
	CPUAllocatable      int
	MemoryAllocatable   int64
	CPUUsage            *float64
	MemoryUsed          *int64
	MemoryAvailable     *int64
	MetricsAt           *time.Time
	OS                  string
	Arch                string
	Labels              map[string]string
	Volumes             []string
	Capabilities        []spec.NodeCapability
	WireGuardPublicKey  string
	WireGuardEndpoint   string
	WireGuardPortBase   int
	WireGuardPortCount  int
	Version             string
	observedAllocations []observedAllocation
	// observedAt is when the leader recorded observedAllocations.
	observedAt time.Time
}

type observedAllocation struct {
	ID           string
	Generation   uint64
	RetainedLogs bool
	// Phase is the lifecycle phase aggregated from the node's task reports.
	Phase lifecycle.Phase
}

// NodeStatus describes whether a node can receive allocations.
type NodeStatus string

const (
	// NodeStatusHealthy indicates that a node is schedulable.
	NodeStatusHealthy NodeStatus = "healthy"
	// NodeStatusUnhealthy indicates that a node is not schedulable.
	NodeStatusUnhealthy NodeStatus = "unhealthy"
	// NodeStatusDraining indicates that a node is evacuating allocations.
	NodeStatusDraining NodeStatus = "draining"
)

// NodeSummary is the persisted representation of a node. It holds only
// durable node facts: identity, address, capacity, platform, inventory, and
// drain intent. Heartbeat observations (last heartbeat time, liveness, host
// metrics, and observed allocations) live in the leader's memory, so a
// heartbeat that changes none of these facts does not write to Raft.
type NodeSummary struct {
	ID                uuid.UUID             `json:"ID"`
	Host              string                `json:"Host"`
	Port              int                   `json:"Port"`
	CPUCapacity       int                   `json:"CPUCapacity"`
	MemoryCapacity    int64                 `json:"MemoryCapacity"`
	CPUAllocatable    int                   `json:"CPUAllocatable"`
	MemoryAllocatable int64                 `json:"MemoryAllocatable"`
	OS                string                `json:"OS"`
	Arch              string                `json:"Arch"`
	Labels            map[string]string     `json:"Labels"`
	Volumes           []string              `json:"Volumes"`
	Capabilities      []spec.NodeCapability `json:"Capabilities"`
	// Draining is the operator's durable drain intent. Liveness (healthy or
	// unhealthy) is derived from heartbeats and is not persisted.
	Draining           bool   `json:"draining,omitempty"`
	WireGuardPublicKey string `json:"WireGuardPublicKey"`
	WireGuardEndpoint  string `json:"WireGuardEndpoint"`
	WireGuardPortBase  int    `json:"WireGuardPortBase"`
	WireGuardPortCount int    `json:"WireGuardPortCount"`
	Version            string `json:"version,omitempty"`
}

// Job contains a persisted job specification and revision.
type Job struct {
	Spec           *spec.JobSpec     `json:"Spec"`
	ResolvedImages map[string]string `json:"resolved_images"`
	// Incarnation distinguishes jobs recreated with the same namespace and
	// name. Unlike Revision and Version, it never resets within a job's life.
	Incarnation string `json:"incarnation"`
	// Revision identifies the execution content of the job. It advances only
	// when a task group's execution hash changes, which replaces allocations.
	Revision int `json:"Revision"`
	// Version advances on every accepted change to the job specification,
	// including label, count, and update-policy changes that keep the
	// revision. It orders the job's history and fences concurrent applies.
	Version int `json:"Version"`
	// ContentHashes stores the content hash of each task group's non-label
	// fields, keyed by group name. Set at registration time.
	ContentHashes map[string]string `json:"content_hashes,omitempty"`
}

// Allocation contains desired and observed allocation state.
type Allocation struct {
	mu             sync.Mutex
	Namespace      string
	JobName        string
	TaskGroupName  string
	ID             string `json:"allocation_id"`
	Generation     uint64 `json:"generation"`
	JobIncarnation string `json:"job_incarnation"`
	JobRevision    int    `json:"job_revision"`
	Tasks          []spec.TaskSpec
	Phase          lifecycle.Phase  `json:"phase"`
	Health         lifecycle.Health `json:"health"`
	lifecycle.Diagnostic
	// Node is the canonical in-memory node the allocation is placed on. The
	// persisted record stores only its ID (see MarshalJSON); Reload rebinds
	// the pointer, so allocation records never carry node observations.
	Node      *Node                    `json:"-"`
	Endpoints []api.AllocationEndpoint `json:"endpoints,omitempty"`
	Ports     []api.PortMapping        `json:"ports,omitempty"`
	// Draining marks an allocation being replaced during a rolling update or
	// node drain. Draining allocations are not restarted on
	// failure and are not counted toward the desired count.
	Draining      bool   `json:"draining,omitempty"`
	DrainSequence uint64 `json:"drain_sequence,omitempty"`
	// DrainReason distinguishes node evacuation from an explicit replacement.
	DrainReason string `json:"drain_reason,omitempty"`
	// Events is an in-memory ring buffer of recent phase transitions.
	// It is not persisted and resets on leader failover.
	Events *lifecycle.RingBuffer `json:"-"`
}

// Transition records a validated allocation phase change.
func (a *Allocation) Transition(to lifecycle.Phase, now time.Time, reason, message string) error {
	if err := lifecycle.Transition(a.Phase, to); err != nil {
		return err
	}
	if a.Phase != to {
		if a.Events == nil {
			a.Events = &lifecycle.RingBuffer{}
		}
		a.Events.Append(lifecycle.Event{Phase: to, Reason: reason, Message: message, At: now})
		a.Phase = to
		a.TransitionedAt = now
	}
	a.Reason, a.Message = reason, message
	return nil
}

// SetHealth updates the allocation health state.
func (a *Allocation) SetHealth(health lifecycle.Health) error {
	if !health.Valid() {
		return fmt.Errorf("invalid allocation health %q", health)
	}
	a.Health = health
	return nil
}

// allocationRecord has Allocation's fields without its methods, so the
// custom JSON methods below can reuse the default encoding.
type allocationRecord Allocation

type persistedAllocation struct {
	*allocationRecord
	NodeID *uuid.UUID `json:"node_id,omitempty"`
}

// MarshalJSON encodes the durable allocation record. The placement node is
// stored by ID only.
func (a *Allocation) MarshalJSON() ([]byte, error) {
	record := persistedAllocation{allocationRecord: (*allocationRecord)(a)}
	if a.Node != nil {
		id := a.Node.ID
		record.NodeID = &id
	}
	return json.Marshal(record)
}

// UnmarshalJSON decodes a durable allocation record. A placed allocation gets
// a node carrying only its ID; callers bind it to the canonical node.
func (a *Allocation) UnmarshalJSON(raw []byte) error {
	record := persistedAllocation{allocationRecord: (*allocationRecord)(a)}
	if err := json.Unmarshal(raw, &record); err != nil {
		return err
	}
	a.Node = nil
	if record.NodeID != nil {
		a.Node = &Node{ID: *record.NodeID}
	}
	return nil
}

func nodeSummary(node *Node) *NodeSummary {
	return &NodeSummary{ID: node.ID, Host: node.Host, Port: node.Port, CPUCapacity: node.CPUCapacity, MemoryCapacity: node.MemoryCapacity, CPUAllocatable: node.CPUAllocatable, MemoryAllocatable: node.MemoryAllocatable, OS: node.OS, Arch: node.Arch, Labels: node.Labels, Volumes: node.Volumes, Capabilities: node.Capabilities, Draining: node.Status == NodeStatusDraining, WireGuardPublicKey: node.WireGuardPublicKey, WireGuardEndpoint: node.WireGuardEndpoint, WireGuardPortBase: node.WireGuardPortBase, WireGuardPortCount: node.WireGuardPortCount, Version: node.Version}
}

func applyNodeSnapshot(node, snapshot *Node) {
	*node = *snapshot
}

func applyAllocationSnapshot(allocation, snapshot *Allocation) {
	allocation.Namespace = snapshot.Namespace
	allocation.JobName = snapshot.JobName
	allocation.TaskGroupName = snapshot.TaskGroupName
	allocation.ID = snapshot.ID
	allocation.Generation = snapshot.Generation
	allocation.JobIncarnation = snapshot.JobIncarnation
	allocation.JobRevision = snapshot.JobRevision
	allocation.Tasks = snapshot.Tasks
	allocation.Phase = snapshot.Phase
	allocation.Health = snapshot.Health
	allocation.Diagnostic = snapshot.Diagnostic
	allocation.Endpoints = snapshot.Endpoints
	allocation.Ports = snapshot.Ports
	allocation.Draining = snapshot.Draining
	allocation.DrainSequence = snapshot.DrainSequence
	allocation.DrainReason = snapshot.DrainReason
	allocation.Events = snapshot.Events
}

// NodeView is a node as the API reports it: its current liveness status and
// the latest heartbeat this leader received, which is zero until the node
// reports in the current leadership term.
type NodeView struct {
	Node
	LastHeartbeat time.Time
}
