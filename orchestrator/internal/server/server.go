package server

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/netip"
	"net/url"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/overfold/trellis/internal/api"
	"github.com/overfold/trellis/internal/auth"
	"github.com/overfold/trellis/internal/catalog"
	"github.com/overfold/trellis/internal/client"
	"github.com/overfold/trellis/internal/lifecycle"
	"github.com/overfold/trellis/internal/plan"
	secretstore "github.com/overfold/trellis/internal/secrets"
	"github.com/overfold/trellis/internal/spec"
	"github.com/overfold/trellis/internal/state"
	"github.com/overfold/trellis/internal/storage"
	"github.com/overfold/trellis/internal/tlsutil"

	"github.com/google/uuid"
)

const reconcileInterval = 10 * time.Second
const heartbeatInterval = 10 * time.Second

// ErrNodeNotFound indicates that a requested node is absent.
var ErrNodeNotFound = errors.New("node not found")

// ClusterJoiner reads and changes Raft cluster membership.
type ClusterJoiner interface {
	Membership() ([]state.RaftMember, error)
	AddNonvoter(id, address string) error
	PromoteVoter(id, address string) error
	DemoteVoter(id string) error
	RemoveServer(id string) error
	LeadershipTransfer() error
	AppliedIndex() uint64
}

type desiredStore interface {
	BackupDesired(cluster string) (*state.DesiredSnapshot, error)
	RestoreDesired(cluster string, snapshot *state.DesiredSnapshot) error
}

// Server coordinates desired state, scheduling, and node operations.
type Server struct {
	log     *slog.Logger
	storage *storage.LocalStorage
	state   *StateController
	client  *client.AgentClient

	cluster     *Cluster
	nodes       map[uuid.UUID]*Node
	jobs        map[string]*Job
	allocations []*Allocation
	// allocationsByNode indexes the canonical allocation pointers by their
	// assigned node. It is rebuilt whenever reconciliation replaces the
	// allocation slice so heartbeats do not scan cluster-wide state.
	allocationsByNode  map[uuid.UUID][]*Allocation
	networkPool        netip.Prefix
	networkPorts       map[string]int
	wireGuardPortCount int
	tokenManager       *auth.TokenManager
	catalog            *catalog.ServiceCatalog
	serverAddr         string
	nodeID             uuid.UUID
	clusterName        string
	jobLimits          spec.Limits
	joiner             ClusterJoiner
	backupStore        desiredStore
	clientTLS          *tls.Config
	// Locking contract:
	//   - mu protects the in-memory cluster, node, job, allocation, epoch, and
	//     leadership snapshots. It must never be held during network or storage I/O.
	//   - allocation.mu protects lifecycle fields on that allocation. When both
	//     locks are required, mu is always acquired before allocation.mu.
	//   - reconcileMu serializes reconciliation planning and its durable
	//     commit; a pass's agent actions run after it is released.
	//   - refreshMu serializes the network-plan and catalog refresh after a
	//     pass's actions, so overlapping passes cannot apply an older snapshot.
	//   - actionMu guards actionNodes and actionSlots, which serialize agent
	//     actions per node and bound them globally across passes.
	//   - mutationMu serializes durable state mutations and is never acquired
	//     while mu or allocation.mu is held.
	mu                 sync.RWMutex
	reconcileMu        sync.Mutex
	refreshMu          sync.Mutex
	actionMu           sync.Mutex
	actionNodes        map[uuid.UUID]chan struct{}
	actionSlots        chan struct{}
	mutationMu         sync.Mutex
	networkPlanMu      sync.Mutex
	networkPlans       map[networkPlanKey]*networkPlanState
	networkPlanWorkers map[uuid.UUID]uint64
	networkPlanWake    chan struct{}
	controlEpoch       uint64
	leaderSince        time.Time
	now                func() time.Time
	metrics            *Metrics
	secrets            *secretstore.Store
	events             *EventBus

	// resumeMu guards the per-term record of acknowledged allocation
	// resumes. It is a leaf lock: nothing else is acquired while it is held.
	resumeMu    sync.Mutex
	resumes     map[resumeDeliveryKey]uint64
	resumeEpoch uint64

	// replacementPolicy bounds failed-allocation replacement and terminal
	// record retention; the zero value selects DefaultReplacementPolicy.
	replacementPolicy ReplacementPolicy
	// replacementBackoffs holds the committed replacement backoff record of
	// each job task group, keyed by replacementBackoffKey. Records are
	// replaced, never mutated in place. Protected by mu.
	replacementBackoffs map[string]*ReplacementBackoff
	// allocationLossTimeout is how long a node must go without a heartbeat
	// before the leader marks its allocations lost; zero selects
	// DefaultAllocationLossTimeout. Protected by mu.
	allocationLossTimeout time.Duration

	// membershipMu serializes Raft membership reads and changes made by this
	// server so each change is applied to the configuration it was planned
	// from. It is never acquired while mu is held.
	membershipMu sync.Mutex
	// raftProgress is each node's latest reported Raft applied index, a
	// renewable observation used only to decide promotions. Protected by mu.
	raftProgress map[uuid.UUID]raftProgress
	// membershipWake asks the membership loop for an immediate pass.
	membershipWake chan struct{}
}

// SetSecretStore configures encrypted secret storage.
func (s *Server) SetSecretStore(store *secretstore.Store) { s.secrets = store }

// Backup captures desired cluster state.
func (s *Server) Backup(_ context.Context) (*api.BackupSnapshot, error) {
	if s.backupStore == nil {
		return nil, fmt.Errorf("backup is unavailable")
	}
	snapshot, err := s.backupStore.BackupDesired(s.clusterName)
	if err != nil {
		return nil, err
	}
	result := &api.BackupSnapshot{
		FormatVersion:            api.BackupFormatVersion,
		CreatedAt:                s.now().UTC(),
		Jobs:                     make(map[string]json.RawMessage, len(snapshot.Jobs)),
		JobRevisions:             make(map[string]json.RawMessage, len(snapshot.JobRevisions)),
		Secrets:                  make(map[string]json.RawMessage, len(snapshot.Secrets)),
		VolumeRegistrations:      make(map[string]json.RawMessage, len(snapshot.VolumeRegistrations)),
		NetworkPortRegistrations: make(map[string]json.RawMessage, len(snapshot.NetworkPortRegistrations)),
	}
	for key, value := range snapshot.Jobs {
		result.Jobs[key] = json.RawMessage(value)
	}
	for key, value := range snapshot.JobRevisions {
		result.JobRevisions[key] = json.RawMessage(value)
	}
	for key, value := range snapshot.Secrets {
		result.Secrets[key] = json.RawMessage(value)
	}
	for key, value := range snapshot.VolumeRegistrations {
		result.VolumeRegistrations[key] = json.RawMessage(value)
	}
	for key, value := range snapshot.NetworkPortRegistrations {
		result.NetworkPortRegistrations[key] = json.RawMessage(value)
	}
	return result, nil
}

// Restore replaces desired state from a backup.
func (s *Server) Restore(ctx context.Context, backup *api.BackupSnapshot) error {
	if backup.FormatVersion != api.BackupFormatVersion {
		return fmt.Errorf("unsupported backup format version %d", backup.FormatVersion)
	}
	if s.backupStore == nil {
		return fmt.Errorf("restore is unavailable")
	}
	snapshot := &state.DesiredSnapshot{
		Jobs:                     make(map[string][]byte, len(backup.Jobs)),
		JobRevisions:             make(map[string][]byte, len(backup.JobRevisions)),
		Secrets:                  make(map[string][]byte, len(backup.Secrets)),
		VolumeRegistrations:      make(map[string][]byte, len(backup.VolumeRegistrations)),
		NetworkPortRegistrations: make(map[string][]byte, len(backup.NetworkPortRegistrations)),
	}
	canonicalizeJob := func(raw json.RawMessage) (*Job, []byte, error) {
		var record Job
		if err := json.Unmarshal(raw, &record); err != nil {
			return nil, nil, err
		}
		if err := s.CanonicalizeJob(record.Spec); err != nil {
			return nil, nil, err
		}
		canonical, err := json.Marshal(record)
		if err != nil {
			return nil, nil, err
		}
		return &record, canonical, nil
	}
	var restoredJobs []*Job
	for key, value := range backup.Jobs {
		if !json.Valid(value) {
			return fmt.Errorf("job %q contains invalid JSON", key)
		}
		job, canonical, err := canonicalizeJob(value)
		if err != nil {
			return fmt.Errorf("validate job %q: %w", key, err)
		}
		snapshot.Jobs[key] = canonical
		restoredJobs = append(restoredJobs, job)
	}
	for key, value := range backup.JobRevisions {
		if !json.Valid(value) {
			return fmt.Errorf("job revision %q contains invalid JSON", key)
		}
		var record JobRevisionRecord
		if err := json.Unmarshal(value, &record); err != nil {
			return fmt.Errorf("validate job revision %q: %w", key, err)
		}
		if record.Spec == nil || record.Version < 1 || record.Revision < 1 || record.CreatedAt.IsZero() {
			return fmt.Errorf("validate job revision %q: invalid revision record", key)
		}
		snapshot.JobRevisions[key] = value
	}
	if err := s.validateNamespaceAllocationLimit(restoredJobs, "", nil); err != nil {
		return err
	}
	for key, value := range backup.Secrets {
		if !json.Valid(value) {
			return fmt.Errorf("secret %q contains invalid JSON", key)
		}
		snapshot.Secrets[key] = value
	}
	for key, value := range backup.VolumeRegistrations {
		if !json.Valid(value) {
			return fmt.Errorf("volume registration %q contains invalid JSON", key)
		}
		snapshot.VolumeRegistrations[key] = value
	}
	for key, value := range backup.NetworkPortRegistrations {
		if !json.Valid(value) {
			return fmt.Errorf("network port registration %q contains invalid JSON", key)
		}
		snapshot.NetworkPortRegistrations[key] = value
	}
	// Validate the complete backup before dropping legacy excess or orphaned
	// revisions so malformed records cannot hide outside the retained window.
	if err := state.ValidateDesiredSnapshot(snapshot, nil); err != nil {
		return err
	}
	retainedRevisions, err := retainedJobRevisionEntries(snapshot.Jobs, snapshot.JobRevisions)
	if err != nil {
		return err
	}
	snapshot.JobRevisions = retainedRevisions
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	// Job limits can change between validation and this point.
	restored := make(map[string]*Job, len(restoredJobs))
	for _, job := range restoredJobs {
		restored[jobKey(job.Spec.Namespace, job.Spec.Name)] = job
	}
	s.mu.RLock()
	limits := s.jobLimits
	s.mu.RUnlock()
	if limits == (spec.Limits{}) {
		limits = spec.DefaultLimits()
	}
	violations := jobLimitViolations(restored, limits)
	if len(violations) > 0 {
		return fmt.Errorf("restored jobs exceed the current job limits: %s", joinViolations(violations))
	}
	if err := s.backupStore.RestoreDesired(s.clusterName, snapshot); err != nil {
		return err
	}
	return s.Reload(ctx)
}

func retainedJobRevisionEntries(jobs, revisions map[string][]byte) (map[string][]byte, error) {
	type entry struct {
		key     string
		version int
		raw     []byte
	}
	byJob := make(map[string][]entry)
	for key, raw := range revisions {
		var record JobRevisionRecord
		if err := json.Unmarshal(raw, &record); err != nil || record.Spec == nil {
			return nil, fmt.Errorf("invalid job revision record %q", key)
		}
		identity := jobKey(record.Spec.Namespace, record.Spec.Name)
		if jobs[url.QueryEscape(identity)] == nil {
			continue
		}
		entries := append(byJob[identity], entry{key: key, version: record.Version, raw: raw})
		sort.Slice(entries, func(i, j int) bool { return entries[i].version < entries[j].version })
		if len(entries) > jobRevisionRetention {
			entries = entries[1:]
		}
		byJob[identity] = entries
	}
	result := make(map[string][]byte)
	for _, entries := range byJob {
		for _, entry := range entries {
			result[entry.key] = entry.raw
		}
	}
	return result, nil
}

// AllocationLogs opens logs for an allocation.
func (s *Server) AllocationLogs(ctx context.Context, id string, follow bool, tail int) (io.ReadCloser, error) {
	return s.AllocationLogsForNamespace(ctx, "", id, follow, tail)
}

// AllocationLogsForNamespace opens allocation logs after namespace validation.
func (s *Server) AllocationLogsForNamespace(ctx context.Context, namespace, id string, follow bool, tail int) (io.ReadCloser, error) {
	s.mu.RLock()
	var found *Allocation
	for _, alloc := range s.allocations {
		if alloc.ID == id && alloc.Namespace == namespace {
			found = alloc
			break
		}
	}
	if found == nil || found.Node == nil {
		s.mu.RUnlock()
		return nil, ErrAllocationNotFound
	}
	nodeID := found.Node.ID
	address := fmt.Sprintf("%s:%d", found.Node.Host, found.Node.Port)
	s.mu.RUnlock()
	return s.client.Logs(ctx, nodeID, address, id, follow, tail)
}

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

func validateNodeCapacity(cpuCapacity int, memoryCapacity int64, cpuAllocatable int, memoryAllocatable int64) error {
	if cpuCapacity < 0 || memoryCapacity < 0 || cpuAllocatable < 0 || memoryAllocatable < 0 {
		return fmt.Errorf("node resources must be non-negative")
	}
	if cpuAllocatable > cpuCapacity {
		return fmt.Errorf("allocatable CPU %dm exceeds node capacity %dm", cpuAllocatable, cpuCapacity)
	}
	if memoryAllocatable > memoryCapacity {
		return fmt.Errorf("allocatable memory %d bytes exceeds node capacity %d bytes", memoryAllocatable, memoryCapacity)
	}
	return nil
}

// Node contains the in-memory state of a registered node.
type Node struct {
	ID                  uuid.UUID
	Host                string
	Port                int
	Status              NodeStatus
	LastHeartbeat       time.Time
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
	ID         string
	Generation uint64
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
	ID                uuid.UUID
	Host              string
	Port              int
	CPUCapacity       int
	MemoryCapacity    int64
	CPUAllocatable    int
	MemoryAllocatable int64
	OS                string
	Arch              string
	Labels            map[string]string
	Volumes           []string
	Capabilities      []spec.NodeCapability
	// Draining is the operator's durable drain intent. Liveness (healthy or
	// unhealthy) is derived from heartbeats and is not persisted.
	Draining           bool `json:"draining,omitempty"`
	WireGuardPublicKey string
	WireGuardEndpoint  string
	WireGuardPortBase  int
	WireGuardPortCount int
	Version            string `json:"version,omitempty"`
}

// Job contains a persisted job specification and revision.
type Job struct {
	Spec *spec.JobSpec
	// Revision identifies the execution content of the job. It advances only
	// when a task group's execution hash changes, which replaces allocations.
	Revision int
	// Version advances on every accepted change to the job specification,
	// including label, count, and update-policy changes that keep the
	// revision. It orders the job's history and fences concurrent applies.
	Version int
	// ContentHashes stores the content hash of each task group's non-label
	// fields, keyed by group name. Set at registration time.
	ContentHashes map[string]string `json:"content_hashes,omitempty"`
}

// Allocation contains desired and observed allocation state.
type Allocation struct {
	mu            sync.Mutex
	Namespace     string
	JobName       string
	TaskGroupName string
	ID            string `json:"allocation_id"`
	Generation    uint64 `json:"generation"`
	JobRevision   int    `json:"job_revision"`
	Tasks         []spec.TaskSpec
	Phase         lifecycle.Phase  `json:"phase"`
	Health        lifecycle.Health `json:"health"`
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

// sameNodeSummary reports whether two nodes have identical durable facts.
func sameNodeSummary(a, b *Node) (bool, error) {
	aRaw, err := json.Marshal(nodeSummary(a))
	if err != nil {
		return false, err
	}
	bRaw, err := json.Marshal(nodeSummary(b))
	if err != nil {
		return false, err
	}
	return bytes.Equal(aRaw, bRaw), nil
}

// nodeSilentSince returns when the leader last had evidence that a node was
// alive: its latest heartbeat in this leadership term, or the start of the
// term when the node has not heartbeated to this leader yet. Heartbeat times
// are leader observations and are not replicated, so a new leader measures
// allocation loss from the start of its own term.
func nodeSilentSince(node *Node, leaderSince time.Time) time.Time {
	if node.LastHeartbeat.Before(leaderSince) {
		return leaderSince
	}
	return node.LastHeartbeat
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

// rebuildAllocationNodeIndexLocked rebuilds allocationsByNode from the
// canonical allocation slice. The caller must hold s.mu for writing.
func (s *Server) rebuildAllocationNodeIndexLocked() {
	index := make(map[uuid.UUID][]*Allocation)
	for _, allocation := range s.allocations {
		if allocation.Node != nil {
			index[allocation.Node.ID] = append(index[allocation.Node.ID], allocation)
		}
	}
	s.allocationsByNode = index
}

// NewServer constructs an orchestrator server.
func NewServer(log *slog.Logger, storage *storage.LocalStorage, state *StateController, store state.Store, cluster, serverAddr string) *Server {
	settings := DefaultClusterSettings()
	s := &Server{
		log:                log.With("component", "server"),
		storage:            storage,
		state:              state,
		client:             &client.AgentClient{},
		nodes:              make(map[uuid.UUID]*Node),
		jobs:               make(map[string]*Job),
		networkPool:        settings.WireGuardPool,
		networkPorts:       make(map[string]int),
		wireGuardPortCount: settings.WireGuardPortCount,
		networkPlans:       make(map[networkPlanKey]*networkPlanState),
		networkPlanWorkers: make(map[uuid.UUID]uint64),
		networkPlanWake:    make(chan struct{}, 1),
		membershipWake:     make(chan struct{}, 1),
		tokenManager:       auth.NewTokenManager(store, cluster),
		catalog:            catalog.New(),
		serverAddr:         serverAddr,
		clusterName:        cluster,
		jobLimits:          settings.JobLimits,
		replacementPolicy:  DefaultReplacementPolicy(),
		now:                time.Now,
	}
	s.backupStore, _ = store.(desiredStore)
	s.events = newEventBus()
	return s
}

// SetAllocationLossTimeout configures how long a node must go without a
// heartbeat before the leader marks its allocations lost.
func (s *Server) SetAllocationLossTimeout(timeout time.Duration) error {
	if err := ValidateAllocationLossTimeout(timeout); err != nil {
		return err
	}
	s.mu.Lock()
	s.allocationLossTimeout = timeout
	s.mu.Unlock()
	return nil
}

// CanonicalizeJob resolves operator defaults and validates a job before use.
func (s *Server) CanonicalizeJob(job *spec.JobSpec) error {
	s.mu.RLock()
	limits := s.jobLimits
	s.mu.RUnlock()
	if limits == (spec.Limits{}) {
		limits = spec.DefaultLimits()
	}
	return spec.Canonicalize(job, limits)
}

// AcquireLeadership durably advances the fencing epoch. The Raft-backed write
// can only succeed on the current leader, so no separate lock service is
// required.
func (s *Server) AcquireLeadership(ctx context.Context) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

	cluster, err := s.state.GetCluster(ctx)
	if err != nil {
		return fmt.Errorf("load control-plane epoch: %w", err)
	}
	if cluster == nil {
		return fmt.Errorf("load control-plane epoch: cluster is not initialized")
	}
	cluster.ControlEpoch++
	epoch := cluster.ControlEpoch
	s.mu.RLock()
	jobs := make(map[string]*Job, len(s.jobs))
	for key, job := range s.jobs {
		jobs[key] = job
	}
	s.mu.RUnlock()
	if err := s.state.ActivateLeadership(ctx, cluster, jobs); err != nil {
		return fmt.Errorf("persist leadership activation: %w", err)
	}
	s.mu.Lock()
	s.cluster = cluster
	s.controlEpoch = epoch
	// Job limits may have changed under a previous leader. Network settings
	// are fixed when the cluster is created and were loaded by Init.
	s.jobLimits = cluster.Settings.JobLimits
	s.leaderSince = s.now()
	// Raft progress is measured against this server's own applied index;
	// reports from an earlier term must not decide promotions in this one.
	s.raftProgress = nil
	s.mu.Unlock()

	// Desired network plans are derived from the leader's in-memory topology.
	// Never carry them across leadership terms: doing so could wrap stale
	// contents in the new fencing epoch before the first fresh reconcile.
	s.networkPlanMu.Lock()
	s.networkPlans = make(map[networkPlanKey]*networkPlanState)
	if s.networkPlanWorkers == nil {
		s.networkPlanWorkers = make(map[uuid.UUID]uint64)
	}
	s.networkPlanMu.Unlock()
	s.wakeNetworkPlans()
	return nil
}

// Init loads the replicated cluster record, creating it from bootstrap when
// the cluster does not exist yet. Existing members need neither administrator
// key material nor cluster settings in node configuration: every member uses
// the replicated values, whatever its local configuration says.
func (s *Server) Init(ctx context.Context, bootstrap ClusterBootstrap) error {
	cluster, err := s.state.GetCluster(ctx)
	if err != nil {
		return fmt.Errorf("get cluster: %w", err)
	}

	if cluster != nil {
		s.log.Info("cluster already initialized")
		if _, err := parseAdministratorPublicKey(cluster.AdministratorPublicKey); err != nil {
			return fmt.Errorf("replicated administrator public key: %w", err)
		}
		if err := cluster.Settings.Validate(); err != nil {
			return fmt.Errorf("replicated cluster settings: %w", err)
		}
	} else {
		if _, err := parseAdministratorPublicKey(bootstrap.AdministratorPublicKey); err != nil {
			return fmt.Errorf("initial administrator public key: %w", err)
		}
		if err := bootstrap.Settings.Validate(); err != nil {
			return fmt.Errorf("initial cluster settings: %w", err)
		}
		cluster = &Cluster{AdministratorPublicKey: bootstrap.AdministratorPublicKey, Settings: bootstrap.Settings}
		if err := s.state.PutCluster(ctx, cluster); err != nil {
			return fmt.Errorf("save cluster remotely: %w", err)
		}
	}

	s.mu.Lock()
	s.cluster = cluster
	s.controlEpoch = cluster.ControlEpoch
	s.jobLimits = cluster.Settings.JobLimits
	s.networkPool = cluster.Settings.WireGuardPool
	s.wireGuardPortCount = cluster.Settings.WireGuardPortCount
	s.mu.Unlock()
	s.client = client.NewAgentClient("", s.clientTLS)
	return nil
}

// SetClientTLS configures TLS for agent requests.
func (s *Server) SetClientTLS(cfg *tls.Config) {
	s.clientTLS = cfg
}

// SetNodeID configures the immutable identity of this control-plane member.
func (s *Server) SetNodeID(id uuid.UUID) { s.nodeID = id }

// RecordNodeServerAddress persists the control-plane address used to resolve a
// Raft node UUID without conflating Raft identity with network location.
func (s *Server) RecordNodeServerAddress(ctx context.Context, id uuid.UUID, address string) error {
	return s.state.PutNodeServerAddress(ctx, id.String(), address)
}

// NodeServerAddress resolves an immutable Raft node UUID to its control-plane address.
func (s *Server) NodeServerAddress(ctx context.Context, id string) (string, error) {
	return s.state.GetNodeServerAddress(ctx, id)
}

// BindNodeCertificate durably associates a node UUID with exactly one
// certificate. Repeating the same binding is safe; replacing it is forbidden.
func (s *Server) BindNodeCertificate(ctx context.Context, id uuid.UUID, certificate *x509.Certificate) error {
	if id == uuid.Nil || certificate == nil {
		return fmt.Errorf("node ID and certificate are required")
	}
	certificateID, err := tlsutil.NodeID(certificate)
	if err != nil || certificateID != id {
		return fmt.Errorf("node certificate identity does not match %s", id)
	}
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	fingerprint := nodeCertificateFingerprint(certificate)
	existing, found, err := s.state.GetNodeCertificateFingerprint(ctx, id.String())
	if err != nil {
		return err
	}
	if found {
		if subtle.ConstantTimeCompare([]byte(existing), []byte(fingerprint)) != 1 {
			return fmt.Errorf("node identity %s is already bound to another certificate", id)
		}
		return nil
	}
	return s.state.PutNodeCertificateFingerprint(ctx, id.String(), fingerprint)
}

// AuthorizeNodeCertificate checks the durable UUID-to-certificate binding.
func (s *Server) AuthorizeNodeCertificate(ctx context.Context, id uuid.UUID, certificate *x509.Certificate) bool {
	if id == uuid.Nil || certificate == nil {
		return false
	}
	existing, found, err := s.state.GetNodeCertificateFingerprint(ctx, id.String())
	return err == nil && found && subtle.ConstantTimeCompare([]byte(existing), []byte(nodeCertificateFingerprint(certificate))) == 1
}

func nodeCertificateFingerprint(certificate *x509.Certificate) string {
	digest := sha256.Sum256(certificate.Raw)
	return hex.EncodeToString(digest[:])
}

// ClusterCA returns the cluster certificate authority materials.
func (s *Server) ClusterCA() (certPEM, keyPEM string, err error) {
	if err := s.storage.Get("tls/ca-cert", &certPEM); err != nil {
		return "", "", fmt.Errorf("load CA cert: %w", err)
	}
	if err := s.storage.Get("tls/ca-key", &keyPEM); err != nil {
		if os.IsNotExist(unwrapPathError(err)) {
			return certPEM, "", nil
		}
		return "", "", fmt.Errorf("load CA key: %w", err)
	}
	return certPEM, keyPEM, nil
}

// EnrollNode issues a server-assigned node identity and certificate in managed
// signing mode. The CA key is withheld until the identity joins Raft, so an
// enrollment credential alone cannot mint or duplicate an existing identity.
func (s *Server) EnrollNode(ctx context.Context, advertised ...string) (*api.NodeEnrollmentResponse, error) {
	caCert, caKey, err := s.ClusterCA()
	if err != nil || caKey == "" {
		return nil, fmt.Errorf("managed node signer is unavailable")
	}
	nodeID := uuid.New()
	cert, key, err := tlsutil.GenerateNodeCert([]byte(caCert), []byte(caKey), nodeID, advertised...)
	if err != nil {
		return nil, fmt.Errorf("sign node certificate: %w", err)
	}
	block, _ := pem.Decode(cert)
	if block == nil {
		return nil, fmt.Errorf("decode signed node certificate")
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse signed node certificate: %w", err)
	}
	if err := s.BindNodeCertificate(ctx, nodeID, certificate); err != nil {
		return nil, fmt.Errorf("reserve node identity: %w", err)
	}
	return &api.NodeEnrollmentResponse{
		NodeID: nodeID,
		CACert: caCert,
		Cert:   string(cert),
		Key:    string(key),
	}, nil
}

// Run starts background reconciliation until the context ends.
func (s *Server) Run(ctx context.Context) {
	go s.runReconcileLoop(ctx)
	go s.runNetworkPlanLoop(ctx)
	go s.runMembershipLoop(ctx)
}

// ListNodes returns registered nodes.
func (s *Server) ListNodes() []Node {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Node, 0, len(s.nodes))

	for _, node := range s.nodes {
		result = append(result, *node)
	}

	return result
}

// RegisterNode adds or updates a cluster node.
func (s *Server) RegisterNode(ctx context.Context, nodeRegistration *NodeRegistration) error {
	if err := validateNodeCapacity(nodeRegistration.CPUCapacity, nodeRegistration.MemoryCapacity, nodeRegistration.CPUAllocatable, nodeRegistration.MemoryAllocatable); err != nil {
		return err
	}
	if nodeRegistration.WireGuardPublicKey != "" || nodeRegistration.WireGuardEndpoint != "" || nodeRegistration.WireGuardPortBase != 0 || nodeRegistration.WireGuardPortCount != 0 {
		if nodeRegistration.WireGuardPublicKey == "" || nodeRegistration.WireGuardEndpoint == "" {
			return fmt.Errorf("WireGuard registration requires a public key and endpoint")
		}
		if nodeRegistration.WireGuardPortCount != s.wireGuardPortCount {
			return fmt.Errorf("WireGuard port count %d does not match cluster count %d", nodeRegistration.WireGuardPortCount, s.wireGuardPortCount)
		}
		if nodeRegistration.WireGuardPortBase < 1 || nodeRegistration.WireGuardPortBase+nodeRegistration.WireGuardPortCount-1 > 65535 {
			return fmt.Errorf("WireGuard port range is outside 1-65535")
		}
	}
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	s.mu.RLock()
	existing := s.nodes[nodeRegistration.ID]
	status := NodeStatusHealthy
	next := &Node{ID: nodeRegistration.ID}
	if existing != nil && existing.Status == NodeStatusDraining {
		status = existing.Status
	}
	if existing != nil {
		*next = *existing
	}
	s.mu.RUnlock()
	next.Host, next.Port, next.Status = nodeRegistration.Host, nodeRegistration.Port, status
	next.CPUCapacity, next.MemoryCapacity = nodeRegistration.CPUCapacity, nodeRegistration.MemoryCapacity
	next.CPUAllocatable, next.MemoryAllocatable = nodeRegistration.CPUAllocatable, nodeRegistration.MemoryAllocatable
	next.OS, next.Arch, next.Labels = nodeRegistration.OS, nodeRegistration.Arch, maps.Clone(nodeRegistration.Labels)
	next.Volumes, next.Capabilities = append([]string(nil), nodeRegistration.Volumes...), append([]spec.NodeCapability(nil), nodeRegistration.Capabilities...)
	next.WireGuardPublicKey, next.WireGuardEndpoint = nodeRegistration.WireGuardPublicKey, nodeRegistration.WireGuardEndpoint
	next.WireGuardPortBase, next.WireGuardPortCount = nodeRegistration.WireGuardPortBase, nodeRegistration.WireGuardPortCount
	next.LastHeartbeat = s.now().UTC()
	if err := s.state.PutNode(ctx, nodeRegistration.ID.String(), nodeSummary(next)); err != nil {
		return fmt.Errorf("save node remotely: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	node := s.nodes[nodeRegistration.ID]
	if node == nil {
		node = &Node{}
		s.nodes[nodeRegistration.ID] = node
	}
	applyNodeSnapshot(node, next)

	return nil
}

// Heartbeat records a node heartbeat and allocation state.
func (s *Server) Heartbeat(ctx context.Context, nodeID uuid.UUID, actual []api.AllocationStatus, version string, volumes []string, capabilities []spec.NodeCapability, resources nodeResourceObservation) error {
	if len(actual) > maxHeartbeatAllocationStatuses {
		return fmt.Errorf("heartbeat allocation status count %d exceeds limit %d", len(actual), maxHeartbeatAllocationStatuses)
	}
	if err := validateNodeCapacity(resources.CPUCapacity, resources.MemoryCapacity, resources.CPUAllocatable, resources.MemoryAllocatable); err != nil {
		return err
	}
	if resources.CPUUsage != nil && (*resources.CPUUsage < 0 || *resources.CPUUsage > 1) {
		return fmt.Errorf("node CPU usage must be between 0 and 1")
	}
	if (resources.MemoryUsed != nil && *resources.MemoryUsed < 0) || (resources.MemoryAvailable != nil && *resources.MemoryAvailable < 0) {
		return fmt.Errorf("node memory observations must be non-negative")
	}
	type statusInfo struct {
		ID            string
		Generation    uint64
		Phase         lifecycle.Phase
		Health        lifecycle.Health
		Reason        api.OperationCode
		Endpoints     []api.AllocationEndpoint
		Ports         []api.PortMapping
		ObservedTasks map[string]bool
		StartFailure  *api.StartFailure
	}
	statuses := make(map[string]statusInfo, len(actual))
	for _, a := range actual {
		if !a.Phase.Valid() || !a.Health.Valid() {
			return fmt.Errorf("invalid allocation state for %s: phase=%q health=%q", a.ID, a.Phase, a.Health)
		}
		if a.Reason != "" && (a.Phase != lifecycle.PhaseFailed || a.Reason != api.OperationRestartExhausted) {
			return fmt.Errorf("invalid failure reason for %s: phase=%q reason=%q", a.ID, a.Phase, a.Reason)
		}
		if a.StartFailure != nil && (a.Phase != lifecycle.PhaseStarting || a.StartFailure.Attempt < 0 || len(a.StartFailure.Message) > api.MaxStartFailureMessageBytes || !terminalStartFailureCode(a.StartFailure.Code)) {
			return fmt.Errorf("invalid start failure for %s: phase=%q attempt=%d message bytes=%d", a.ID, a.Phase, a.StartFailure.Attempt, len(a.StartFailure.Message))
		}
		phase, health := a.Phase, a.Health
		key := fmt.Sprintf("%s/%d", a.ID, a.Generation)
		info := statuses[key]
		if len(info.ObservedTasks) == 0 {
			info.ID, info.Generation, info.Phase, info.Health = a.ID, a.Generation, phase, health
		} else {
			// A terminally failed task fails the whole group regardless of
			// the order in which the agent reports sibling tasks.
			if phase != lifecycle.PhaseRunning && info.Phase != lifecycle.PhaseFailed {
				info.Phase = phase
			}
			if info.Health == lifecycle.HealthUnhealthy || health == lifecycle.HealthUnhealthy {
				info.Health = lifecycle.HealthUnhealthy
			} else if info.Health == lifecycle.HealthUnknown || health == lifecycle.HealthUnknown {
				info.Health = lifecycle.HealthUnknown
			} else {
				info.Health = lifecycle.HealthHealthy
			}
		}
		if info.ObservedTasks == nil {
			info.ObservedTasks = make(map[string]bool)
		}
		info.ObservedTasks[a.Task] = true
		// Keep the reason of a failed task whatever the report order.
		if a.Reason != "" && (info.Reason == "" || a.Reason < info.Reason) {
			info.Reason = a.Reason
		}
		// Every task of a failed start carries the same failure; choose one
		// deterministically whatever the report order.
		if failure := a.StartFailure; failure != nil && (info.StartFailure == nil || failure.Attempt > info.StartFailure.Attempt ||
			failure.Attempt == info.StartFailure.Attempt && failure.Message < info.StartFailure.Message) {
			info.StartFailure = failure
		}
		info.Ports = append(info.Ports, a.Ports...)
		if a.Task != "" || a.Address != "" || len(a.Ports) > 0 {
			info.Endpoints = append(info.Endpoints, api.AllocationEndpoint{
				Task: a.Task, Address: a.Address, Ports: append([]api.PortMapping(nil), a.Ports...),
			})
		}
		statuses[key] = info
	}
	observed := make([]observedAllocation, 0, len(statuses))
	for _, info := range statuses {
		observed = append(observed, observedAllocation{ID: info.ID, Generation: info.Generation, Phase: info.Phase})
	}
	sort.Slice(observed, func(i, j int) bool {
		if observed[i].ID == observed[j].ID {
			return observed[i].Generation < observed[j].Generation
		}
		return observed[i].ID < observed[j].ID
	})
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	s.mu.RLock()
	node := s.nodes[nodeID]
	if node == nil {
		s.mu.RUnlock()
		return fmt.Errorf("node not found")
	}
	nextNode := *node
	nextNode.Labels = maps.Clone(node.Labels)
	nextNode.Volumes = append([]string(nil), volumes...)
	nextNode.Capabilities = append([]spec.NodeCapability(nil), capabilities...)
	if nextNode.Status != NodeStatusDraining {
		nextNode.Status = NodeStatusHealthy
	}
	heartbeatAt := s.now().UTC()
	nextNode.LastHeartbeat = heartbeatAt
	nextNode.Version = version
	nextNode.CPUCapacity, nextNode.MemoryCapacity = resources.CPUCapacity, resources.MemoryCapacity
	nextNode.CPUAllocatable, nextNode.MemoryAllocatable = resources.CPUAllocatable, resources.MemoryAllocatable
	nextNode.CPUUsage, nextNode.MemoryUsed = resources.CPUUsage, resources.MemoryUsed
	nextNode.MemoryAvailable, nextNode.MetricsAt = resources.MemoryAvailable, resources.MetricsAt
	nextNode.observedAllocations = observed
	nextNode.observedAt = heartbeatAt
	type allocationUpdate struct {
		current *Allocation
		next    *Allocation
	}
	if s.allocationsByNode == nil {
		// Servers assembled directly by focused tests and older embedding code
		// may not have passed through Reload or reconciliation yet.
		s.mu.RUnlock()
		s.mu.Lock()
		if s.allocationsByNode == nil {
			s.rebuildAllocationNodeIndexLocked()
		}
		s.mu.Unlock()
		s.mu.RLock()
	}
	assigned := s.allocationsByNode[nodeID]
	updates := make([]allocationUpdate, 0, len(assigned))
	for _, allocation := range assigned {
		allocation.mu.Lock()
		next, err := cloneAllocationForReconcile(allocation)
		allocation.mu.Unlock()
		if err != nil {
			s.mu.RUnlock()
			return fmt.Errorf("snapshot allocation observation %s: %w", allocation.ID, err)
		}
		updates = append(updates, allocationUpdate{current: allocation, next: next})
	}
	s.mu.RUnlock()

	changed := make([]allocationUpdate, 0, len(updates))
	for _, update := range updates {
		a := update.next
		info, ok := statuses[fmt.Sprintf("%s/%d", a.ID, a.Generation)]
		if !ok {
			if a.Phase != lifecycle.PhaseRunning && a.Phase != lifecycle.PhaseStarting {
				continue
			}
			info = statusInfo{Phase: lifecycle.PhaseStarting, Health: lifecycle.HealthUnknown}
		}
		for _, task := range a.Tasks {
			if !info.ObservedTasks[task.Name] {
				if info.Phase == lifecycle.PhaseRunning {
					// Retry the incomplete allocation on the next reconciliation pass.
					info.Phase = lifecycle.PhaseStarting
				}
				if info.Health != lifecycle.HealthUnhealthy {
					info.Health = lifecycle.HealthUnknown
				}
				break
			}
		}
		// An unchanged phase keeps its reason, such as a counted start failure.
		if info.Phase.Valid() && info.Phase != a.Phase && lifecycle.CanObserve(a.Phase, info.Phase) {
			var reason string
			if info.Phase == lifecycle.PhaseFailed {
				reason = string(info.Reason)
			}
			if info.Phase == lifecycle.PhaseRunning && a.Phase != lifecycle.PhaseRunning {
				// The start completed; later starts get a fresh attempt budget.
				a.Attempt, a.NextRetryAt = 0, nil
			}
			_ = a.Transition(info.Phase, heartbeatAt, reason, "")
		}
		// Count the agent's failed background start once: the failure names
		// the attempt it ran for, and counting advances the attempt.
		if a.Phase == lifecycle.PhaseStarting && info.StartFailure != nil && info.StartFailure.Attempt == a.Attempt {
			if code := info.StartFailure.Code; code != "" {
				// Retrying the generation cannot fix it, as for the same
				// operation code on a start request.
				if a.Transition(lifecycle.PhaseFailed, heartbeatAt, string(code), info.StartFailure.Message) == nil {
					a.NextRetryAt = nil
				}
			} else {
				_ = recordStartFailure(a, heartbeatAt, info.StartFailure.Message)
			}
		}
		_ = a.SetHealth(info.Health)
		sort.Slice(info.Endpoints, func(i, j int) bool { return info.Endpoints[i].Task < info.Endpoints[j].Task })
		a.Endpoints = append([]api.AllocationEndpoint(nil), info.Endpoints...)
		a.Ports = append([]api.PortMapping(nil), info.Ports...)
		update.current.mu.Lock()
		same, err := sameAllocationState(update.current, update.next)
		update.current.mu.Unlock()
		if err != nil {
			return fmt.Errorf("compare allocation observation %s: %w", a.ID, err)
		}
		if !same {
			changed = append(changed, update)
		}
	}
	persisted := make([]*Allocation, 0, len(changed))
	for _, update := range changed {
		persisted = append(persisted, update.next)
	}
	// Liveness, heartbeat time, host metrics, and observed allocations are
	// leader observations. Only durable node facts and allocation changes
	// reach Raft, so a steady-state heartbeat is not a write.
	s.mu.RLock()
	nodeUnchanged, err := sameNodeSummary(node, &nextNode)
	s.mu.RUnlock()
	if err != nil {
		return fmt.Errorf("compare node %s: %w", nodeID, err)
	}
	switch {
	case !nodeUnchanged:
		err = s.state.PutNodeAndAllocations(ctx, nodeSummary(&nextNode), persisted)
	case len(persisted) > 0:
		err = s.state.PutAllocations(ctx, persisted)
	}
	if err != nil {
		return fmt.Errorf("persist heartbeat: %w", err)
	}
	s.mu.Lock()
	applyNodeSnapshot(node, &nextNode)
	for _, update := range changed {
		update.current.mu.Lock()
		applyAllocationSnapshot(update.current, update.next)
		update.current.mu.Unlock()
	}
	s.mu.Unlock()

	if len(changed) > 0 {
		changedAllocations := make([]*Allocation, len(changed))
		for i := range changed {
			changedAllocations[i] = changed[i].current
		}
		s.refreshCatalogAllocations(changedAllocations)
	}
	return nil
}

func jobKey(namespace, name string) string {
	if namespace == "" {
		return name
	}
	return namespace + "\x00" + name
}

// RegisterJob creates or updates desired job state.
func (s *Server) RegisterJob(ctx context.Context, namespace string, jobSpec *spec.JobSpec, expectedVersion *int) (*api.JobRegistrationResponse, error) {
	if err := s.CanonicalizeJob(jobSpec); err != nil {
		return nil, fmt.Errorf("validate job: %w", err)
	}
	if expectedVersion != nil && *expectedVersion < 0 {
		return nil, fmt.Errorf("expected_version must not be negative")
	}
	if jobSpec.Namespace != namespace {
		return nil, fmt.Errorf("job namespace does not match request namespace")
	}
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	key := jobKey(namespace, jobSpec.Name)
	// The precondition is checked under mutationMu, which serializes every
	// job mutation on the leader, so the job cannot change between this
	// check and the commit below. Job records are replaced, never mutated,
	// so existing can be compared without holding s.mu.
	s.mu.RLock()
	existing := s.jobs[key]
	s.mu.RUnlock()
	if err := checkJobVersion(existing, expectedVersion); err != nil {
		return nil, err
	}
	if existing != nil && len(plan.Diff(existing.Spec, jobSpec)) == 0 {
		return &api.JobRegistrationResponse{Namespace: namespace, Name: jobSpec.Name, Version: existing.Version, Revision: existing.Revision}, nil
	}
	s.mu.RLock()
	// Job limits can change between canonicalization and this point.
	limits := s.jobLimits
	if limits == (spec.Limits{}) {
		limits = spec.DefaultLimits()
	}
	err := spec.ValidateWithLimits(jobSpec, limits)
	if err != nil {
		err = fmt.Errorf("validate job: %w", err)
	} else {
		err = s.validateNamespaceAllocationLimitLocked(nil, namespace, jobSpec, key)
	}
	s.mu.RUnlock()
	if err != nil {
		return nil, err
	}

	hashes := make(map[string]string, len(jobSpec.TaskGroups))
	for i := range jobSpec.TaskGroups {
		hashes[jobSpec.TaskGroups[i].Name] = spec.TaskGroupContentHash(&jobSpec.TaskGroups[i])
	}

	revision, version := 1, 1
	labelOnly := false
	if existing != nil {
		version = existing.Version + 1
		revision = existing.Revision + 1
		if isLabelOnlyChange(existing, jobSpec, hashes) {
			revision = existing.Revision
			labelOnly = true
		}
	}
	job := &Job{
		Spec:          jobSpec,
		Revision:      revision,
		Version:       version,
		ContentHashes: hashes,
	}
	revisionRecord := &JobRevisionRecord{Version: version, Revision: revision, Spec: jobSpec, CreatedAt: s.now().UTC()}
	if err := s.state.PutJobWithRevision(ctx, key, job, revisionRecord); err != nil {
		return nil, fmt.Errorf("save job remotely: %w", err)
	}
	s.mu.Lock()
	s.jobs[key] = job
	s.mu.Unlock()

	if labelOnly {
		s.refreshCatalog()
	}
	s.events.publish(api.ClusterEvent{
		Type:      api.EventJobRegistered,
		Namespace: namespace,
		JobName:   jobSpec.Name,
		Version:   version,
		Revision:  revision,
		At:        s.now().UTC(),
	})

	return &api.JobRegistrationResponse{Namespace: namespace, Name: jobSpec.Name, Version: version, Revision: revision}, nil
}

// ErrJobVersionConflict indicates that a job apply's expected version did not
// match the job's current version.
var ErrJobVersionConflict = errors.New("job version conflict")

// JobVersionConflictError describes a failed job apply precondition.
type JobVersionConflictError struct {
	Expected int
	// Current is the job's current version, or 0 when the job does not exist.
	Current int
	Exists  bool
}

func (e *JobVersionConflictError) Error() string {
	switch {
	case e.Expected == 0:
		return fmt.Sprintf("job version conflict: job already exists at version %d; plan the manifest again", e.Current)
	case !e.Exists:
		return fmt.Sprintf("job version conflict: expected version %d but the job does not exist; plan the manifest again", e.Expected)
	default:
		return fmt.Sprintf("job version conflict: expected version %d but the job is at version %d; plan the manifest again", e.Expected, e.Current)
	}
}

// Is reports whether target is ErrJobVersionConflict.
func (e *JobVersionConflictError) Is(target error) bool { return target == ErrJobVersionConflict }

// checkJobVersion enforces an apply precondition. A nil expected version
// applies unconditionally; 0 requires that the job does not exist.
func checkJobVersion(existing *Job, expected *int) error {
	if expected == nil {
		return nil
	}
	if existing == nil {
		if *expected == 0 {
			return nil
		}
		return &JobVersionConflictError{Expected: *expected}
	}
	if *expected == 0 || existing.Version != *expected {
		return &JobVersionConflictError{Expected: *expected, Current: existing.Version, Exists: true}
	}
	return nil
}

// ValidateNamespaceAllocationLimit checks a candidate job against the current
// namespace desired-allocation budget without mutating state.
func (s *Server) ValidateNamespaceAllocationLimit(namespace string, job *spec.JobSpec) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.validateNamespaceAllocationLimitLocked(nil, namespace, job, jobKey(namespace, job.Name))
}

func (s *Server) validateNamespaceAllocationLimit(restored []*Job, namespace string, candidate *spec.JobSpec) error {
	return s.validateNamespaceAllocationLimitLocked(restored, namespace, candidate, "")
}

func (s *Server) validateNamespaceAllocationLimitLocked(source []*Job, namespace string, candidate *spec.JobSpec, replacingKey string) error {
	limits := s.jobLimits
	if limits == (spec.Limits{}) {
		limits = spec.DefaultLimits()
	}
	totals := make(map[string]int64)
	if source == nil {
		for key, job := range s.jobs {
			if key == replacingKey || job == nil || job.Spec == nil {
				continue
			}
			totals[job.Spec.Namespace] += desiredAllocations(job.Spec)
		}
	} else {
		for _, job := range source {
			if job != nil && job.Spec != nil {
				totals[job.Spec.Namespace] += desiredAllocations(job.Spec)
			}
		}
	}
	if candidate != nil {
		totals[namespace] += desiredAllocations(candidate)
	}
	for name, total := range totals {
		if total > int64(limits.MaxDesiredAllocationsPerNamespace) {
			return spec.ValidationErrors{{Path: "task_groups", Code: "limit_exceeded", Message: fmt.Sprintf("namespace %q desired allocations %d exceeds operator limit of %d", name, total, limits.MaxDesiredAllocationsPerNamespace)}}
		}
	}
	return nil
}

func desiredAllocations(job *spec.JobSpec) int64 {
	total := int64(0)
	for _, group := range job.TaskGroups {
		total += int64(group.Count)
	}
	return total
}

// isLabelOnlyChange returns true when the new job spec differs from the
// existing one only in task group labels (and count/update policy), so the
// change keeps the execution revision. The content hashes must have been
// computed from newSpec.
func isLabelOnlyChange(existing *Job, newSpec *spec.JobSpec, newHashes map[string]string) bool {
	if len(existing.Spec.TaskGroups) != len(newSpec.TaskGroups) {
		return false
	}
	if existing.ContentHashes == nil {
		return false
	}
	for name, newHash := range newHashes {
		oldHash, ok := existing.ContentHashes[name]
		if !ok || oldHash != newHash {
			return false
		}
	}
	return true
}

// Reload reconstructs the durable control-plane state before a leadership term starts.
func (s *Server) Reload(ctx context.Context) error {
	jobs, err := s.state.ListJobs(ctx)
	if err != nil {
		return fmt.Errorf("load jobs: %w", err)
	}
	nodeSummaries, err := s.state.ListNodes(ctx)
	if err != nil {
		return fmt.Errorf("load nodes: %w", err)
	}
	allocationMap, err := s.state.ListAllocations(ctx)
	if err != nil {
		return fmt.Errorf("load allocations: %w", err)
	}
	backoffs, err := s.state.ListReplacementBackoffs(ctx)
	if err != nil {
		return fmt.Errorf("load replacement backoffs: %w", err)
	}
	nodes := make(map[uuid.UUID]*Node, len(nodeSummaries))
	for _, summary := range nodeSummaries {
		// A node is unhealthy until it heartbeats to this leader; heartbeat
		// observations are not replicated.
		status := NodeStatusUnhealthy
		if summary.Draining {
			status = NodeStatusDraining
		}
		nodes[summary.ID] = &Node{ID: summary.ID, Host: summary.Host, Port: summary.Port, CPUCapacity: summary.CPUCapacity, MemoryCapacity: summary.MemoryCapacity, CPUAllocatable: summary.CPUAllocatable, MemoryAllocatable: summary.MemoryAllocatable, OS: summary.OS, Arch: summary.Arch, Labels: summary.Labels, Volumes: summary.Volumes, Capabilities: summary.Capabilities, Status: status, WireGuardPublicKey: summary.WireGuardPublicKey, WireGuardEndpoint: summary.WireGuardEndpoint, WireGuardPortBase: summary.WireGuardPortBase, WireGuardPortCount: summary.WireGuardPortCount, Version: summary.Version}
	}
	allocations := make([]*Allocation, 0, len(allocationMap))
	for _, allocation := range allocationMap {
		if allocation.Node != nil {
			allocation.Node = nodes[allocation.Node.ID]
		}
		allocations = append(allocations, allocation)
	}
	sort.Slice(allocations, func(i, j int) bool { return allocations[i].ID < allocations[j].ID })
	s.mu.Lock()
	s.jobs = jobs
	s.nodes = nodes
	s.allocations = allocations
	s.rebuildAllocationNodeIndexLocked()
	s.replacementBackoffs = backoffs
	s.mu.Unlock()
	return nil
}

// DrainNode marks a node for allocation evacuation.
func (s *Server) DrainNode(ctx context.Context, id uuid.UUID) error {
	s.mutationMu.Lock()
	s.mu.RLock()
	node := s.nodes[id]
	if node == nil {
		s.mu.RUnlock()
		s.mutationMu.Unlock()
		return fmt.Errorf("node not found")
	}
	next := *node
	next.Status = NodeStatusDraining
	s.mu.RUnlock()
	if err := s.state.PutNode(ctx, id.String(), nodeSummary(&next)); err != nil {
		s.mutationMu.Unlock()
		return err
	}
	s.mu.Lock()
	applyNodeSnapshot(node, &next)
	s.mu.Unlock()
	s.mutationMu.Unlock()
	s.Reconcile(ctx)
	return nil
}

// UndrainNode makes a drained node schedulable.
func (s *Server) UndrainNode(ctx context.Context, id uuid.UUID) error {
	s.reconcileMu.Lock()
	err := s.resumeNodeAllocations(ctx, id)
	s.reconcileMu.Unlock()
	if err != nil {
		return err
	}
	s.Reconcile(ctx)
	return nil
}

func (s *Server) resumeNodeAllocations(ctx context.Context, id uuid.UUID) error {
	s.mutationMu.Lock()
	mutationLocked := true
	defer func() {
		if mutationLocked {
			s.mutationMu.Unlock()
		}
	}()
	s.mu.RLock()
	node := s.nodes[id]
	if node == nil {
		s.mu.RUnlock()
		return fmt.Errorf("%w: %s", ErrNodeNotFound, id)
	}
	nextNode := *node
	nextNode.Status = NodeStatusHealthy
	var resumes []resumeDelivery
	var updates []*Allocation
	for _, allocation := range s.allocations {
		allocation.mu.Lock()
		if allocation.Node == nil || allocation.Node.ID != id || !allocation.Draining || allocation.DrainReason != "node" ||
			(allocation.Phase != lifecycle.PhaseRunning && allocation.Phase != lifecycle.PhaseStarting && allocation.Phase != lifecycle.PhasePlaced) {
			allocation.mu.Unlock()
			continue
		}
		job := s.jobs[jobKey(allocation.Namespace, allocation.JobName)]
		if job == nil || allocation.JobRevision != job.Revision {
			allocation.mu.Unlock()
			continue
		}
		groupExists := false
		for _, group := range job.Spec.TaskGroups {
			if group.Name == allocation.TaskGroupName {
				groupExists = true
				break
			}
		}
		if !groupExists {
			allocation.mu.Unlock()
			continue
		}
		address := fmt.Sprintf("%s:%d", node.Host, node.Port)
		request := &api.DrainAllocationRequest{AllocationID: allocation.ID, Generation: allocation.Generation, Epoch: s.controlEpoch, Sequence: allocation.DrainSequence + 1}
		next, err := cloneAllocationForReconcile(allocation)
		allocation.mu.Unlock()
		if err != nil {
			s.mu.RUnlock()
			return fmt.Errorf("snapshot resumed allocation %s: %w", allocation.ID, err)
		}
		next.Draining = false
		next.DrainReason = ""
		next.DrainSequence = request.Sequence
		updates = append(updates, next)
		resumes = append(resumes, resumeDelivery{allocation: allocation, address: address, request: request})
	}
	s.mu.RUnlock()
	if err := s.state.PutNodeAndAllocations(ctx, nodeSummary(&nextNode), updates); err != nil {
		return err
	}
	s.mu.Lock()
	applyNodeSnapshot(node, &nextNode)
	for i, resume := range resumes {
		resume.allocation.mu.Lock()
		applyAllocationSnapshot(resume.allocation, updates[i])
		resume.allocation.mu.Unlock()
	}
	s.mu.Unlock()
	s.mutationMu.Unlock()
	mutationLocked = false
	// The undrain is durable. A resume the agent does not acknowledge now is
	// redelivered by reconciliation.
	for _, resume := range resumes {
		allocation := resume.allocation
		allocation.mu.Lock()
		if allocation.Draining || allocation.DrainSequence != resume.request.Sequence {
			allocation.mu.Unlock()
			continue
		}
		if err := s.client.ResumeAllocation(ctx, id, resume.address, resume.request); err != nil {
			s.log.Warn("deliver allocation resume; reconciliation will retry", "allocation", allocation.ID, "node", id, "error", err)
		} else {
			s.recordResumeDelivered(resume.request)
		}
		allocation.mu.Unlock()
	}
	return nil
}

type resumeDelivery struct {
	allocation *Allocation
	address    string
	request    *api.DrainAllocationRequest
}

type resumeDeliveryKey struct {
	allocation string
	generation uint64
}

// deliveredResumes copies the resume sequence each agent acknowledged per
// allocation generation during the given leadership term. The record is
// renewable delivery state, not desired state: a new term starts empty and
// redelivers each resume once.
func (s *Server) deliveredResumes(epoch uint64) map[resumeDeliveryKey]uint64 {
	s.resumeMu.Lock()
	defer s.resumeMu.Unlock()
	if s.resumeEpoch != epoch {
		return nil
	}
	delivered := make(map[resumeDeliveryKey]uint64, len(s.resumes))
	for key, sequence := range s.resumes {
		delivered[key] = sequence
	}
	return delivered
}

func (s *Server) recordResumeDelivered(request *api.DrainAllocationRequest) {
	s.resumeMu.Lock()
	defer s.resumeMu.Unlock()
	if s.resumes == nil || s.resumeEpoch != request.Epoch {
		s.resumes = make(map[resumeDeliveryKey]uint64)
		s.resumeEpoch = request.Epoch
	}
	s.resumes[resumeDeliveryKey{allocation: request.AllocationID, generation: request.Generation}] = request.Sequence
}

// ListJobs returns jobs in a namespace.
func (s *Server) ListJobs(namespace string) api.JobListResponse {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(api.JobListResponse, 0, len(s.jobs))
	jobs := make(map[string]*Job)
	positions := make(map[string]int)
	for _, job := range s.jobs {
		if job.Spec.Namespace != namespace {
			continue
		}
		name := job.Spec.Name
		r := api.JobStatusResponse{Name: name, Version: job.Version, Revision: job.Revision}
		for _, g := range job.Spec.TaskGroups {
			r.Desired += g.Count
		}
		r.ReplacementBackoff = s.replacementBackoffResponsesLocked(namespace, name)
		jobs[name], positions[name] = job, len(result)
		result = append(result, r)
	}
	for _, a := range s.allocations {
		a.mu.Lock()
		job := jobs[a.JobName]
		if a.Namespace != namespace || job == nil {
			a.mu.Unlock()
			continue
		}
		r := &result[positions[a.JobName]]
		r.Allocations = append(r.Allocations, s.allocationResponseLocked(a))
		if a.JobRevision == job.Revision && !a.Draining && a.Phase == lifecycle.PhaseRunning {
			r.Running++
		}
		if a.JobRevision == job.Revision && !a.Draining && a.Phase == lifecycle.PhaseRunning && a.Health == lifecycle.HealthHealthy {
			r.Healthy++
		}
		a.mu.Unlock()
	}
	return result
}

// GetJob returns a job and its allocation state.
func (s *Server) GetJob(namespace, name string) (*api.JobStatusResponse, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, ok := s.jobs[jobKey(namespace, name)]
	if !ok {
		return nil, false
	}
	specCopy := *job.Spec
	r := &api.JobStatusResponse{Name: name, Version: job.Version, Revision: job.Revision, Spec: &specCopy}
	for _, g := range job.Spec.TaskGroups {
		r.Desired += g.Count
	}
	for _, a := range s.allocations {
		a.mu.Lock()
		if a.Namespace != namespace || a.JobName != name {
			a.mu.Unlock()
			continue
		}
		ar := s.allocationResponseLocked(a)
		r.Allocations = append(r.Allocations, ar)
		if a.JobRevision == job.Revision && !a.Draining && a.Phase == lifecycle.PhaseRunning {
			r.Running++
		}
		if a.JobRevision == job.Revision && !a.Draining && a.Phase == lifecycle.PhaseRunning && a.Health == lifecycle.HealthHealthy {
			r.Healthy++
		}
		a.mu.Unlock()
	}
	r.ReplacementBackoff = s.replacementBackoffResponsesLocked(namespace, name)
	return r, true
}

// DeleteJob removes desired job state.
func (s *Server) DeleteJob(ctx context.Context, namespace, name string) error {
	s.mutationMu.Lock()
	s.mu.RLock()
	key := jobKey(namespace, name)
	_, ok := s.jobs[key]
	s.mu.RUnlock()
	if !ok {
		s.mutationMu.Unlock()
		return fmt.Errorf("job %s not found", name)
	}
	if err := s.state.DeleteJob(ctx, key); err != nil {
		s.mutationMu.Unlock()
		return err
	}
	s.mu.Lock()
	delete(s.jobs, key)
	s.mu.Unlock()
	s.mutationMu.Unlock()
	s.events.publish(api.ClusterEvent{
		Type:      api.EventJobDeleted,
		Namespace: namespace,
		JobName:   name,
		At:        s.now().UTC(),
	})
	s.Reconcile(ctx)
	return nil
}

// RestartJob marks all running allocations for a job as draining and
// reconciles so they are replaced with fresh instances.
func (s *Server) RestartJob(ctx context.Context, namespace, name string) error {
	s.reconcileMu.Lock()
	err := s.persistJobRestart(ctx, namespace, name)
	s.reconcileMu.Unlock()
	if err != nil {
		return err
	}
	s.Reconcile(ctx)
	return nil
}

func (s *Server) persistJobRestart(ctx context.Context, namespace, name string) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	s.mu.RLock()
	key := jobKey(namespace, name)
	if s.jobs[key] == nil {
		s.mu.RUnlock()
		return fmt.Errorf("job %s not found", name)
	}
	allocations := append([]*Allocation(nil), s.allocations...)
	s.mu.RUnlock()

	updates := make([]*Allocation, 0)
	for _, alloc := range allocations {
		alloc.mu.Lock()
		if alloc.Namespace == namespace && alloc.JobName == name && alloc.DrainReason != "restart" && activeAllocationPhase(alloc.Phase) {
			update, err := cloneAllocationForReconcile(alloc)
			alloc.mu.Unlock()
			if err != nil {
				return fmt.Errorf("snapshot restart intent: %w", err)
			}
			update.Draining = true
			update.DrainSequence++
			update.DrainReason = "restart"
			updates = append(updates, update)
			continue
		}
		alloc.mu.Unlock()
	}
	if err := s.state.PutAllocations(ctx, updates); err != nil {
		return fmt.Errorf("persist restart intent: %w", err)
	}
	for _, update := range updates {
		for _, alloc := range allocations {
			if alloc.ID != update.ID {
				continue
			}
			alloc.mu.Lock()
			if alloc.Generation == update.Generation {
				alloc.Draining = true
				alloc.DrainSequence = update.DrainSequence
				alloc.DrainReason = "restart"
			}
			alloc.mu.Unlock()
			break
		}
	}
	return nil
}

// StopAllocationByID stops a single allocation identified by namespace and ID.
// The stop runs on the reconciliation action path, so it waits for any pass's
// actions on the same node, and a pass follows it so the task group converges
// on its desired count.
func (s *Server) StopAllocationByID(ctx context.Context, namespace, id string) error {
	s.mu.RLock()
	var found *Allocation
	for _, alloc := range s.allocations {
		if alloc.ID == id && alloc.Namespace == namespace {
			found = alloc
			break
		}
	}
	if found == nil || found.Node == nil {
		s.mu.RUnlock()
		return fmt.Errorf("allocation not found")
	}
	nodeID := found.Node.ID
	s.mu.RUnlock()
	slots, busy := s.claimActionNode(nodeID)
	slots, ok := s.awaitActionNode(ctx, nodeID, slots, busy)
	if !ok {
		return ctx.Err()
	}
	err := s.Execute(ctx, &Action{Type: ActionStop, Allocation: found})
	<-slots
	s.releaseActionNode(nodeID)
	if err != nil {
		return err
	}
	s.Reconcile(ctx)
	return nil
}

// ListJobVersions returns the retained spec history for a job, one entry per
// version in ascending order.
func (s *Server) ListJobVersions(ctx context.Context, namespace, name string) (api.JobVersionListResponse, error) {
	s.mu.RLock()
	key := jobKey(namespace, name)
	_, ok := s.jobs[key]
	s.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("job not found")
	}
	records, err := s.state.ListJobRevisions(ctx, key)
	if err != nil {
		return nil, err
	}
	result := make(api.JobVersionListResponse, 0, len(records))
	for _, r := range records {
		result = append(result, api.JobVersionResponse{
			Version:   r.Version,
			Revision:  r.Revision,
			Spec:      *r.Spec,
			CreatedAt: r.CreatedAt,
		})
	}
	return result, nil
}

// ErrAllocationNotFound indicates that the control plane has no placed
// allocation with the requested ID in the caller's namespace.
var ErrAllocationNotFound = errors.New("allocation not found")

func (s *Server) allocationAgentAddress(namespace, id string) (uuid.UUID, string, []spec.TaskSpec, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, alloc := range s.allocations {
		if alloc.ID == id && alloc.Namespace == namespace {
			if alloc.Node == nil {
				return uuid.Nil, "", nil, ErrAllocationNotFound
			}
			return alloc.Node.ID, fmt.Sprintf("%s:%d", alloc.Node.Host, alloc.Node.Port), append([]spec.TaskSpec(nil), alloc.Tasks...), nil
		}
	}
	return uuid.Nil, "", nil, ErrAllocationNotFound
}

func resolveExecTask(id, task string, tasks []spec.TaskSpec) (string, error) {
	if task == "" {
		switch len(tasks) {
		case 1:
			return tasks[0].Name, nil
		case 0:
			return "", nil
		default:
			return "", fmt.Errorf("%w: allocation %s has multiple tasks; specify task", ErrTaskSelection, id)
		}
	}
	if len(tasks) > 0 {
		for _, candidate := range tasks {
			if candidate.Name == task {
				return task, nil
			}
		}
		return "", fmt.Errorf("%w: allocation %s has no task %q", ErrTaskSelection, id, task)
	}
	return task, nil
}

// ExecAllocation runs a command in an allocation task container.
func (s *Server) ExecAllocation(ctx context.Context, namespace, id, task string, command []string) (*api.ExecResponse, error) {
	nodeID, address, tasks, err := s.allocationAgentAddress(namespace, id)
	if err != nil {
		return nil, err
	}
	task, err = resolveExecTask(id, task, tasks)
	if err != nil {
		return nil, err
	}
	return s.client.ExecAllocation(ctx, nodeID, address, id, task, command)
}

// CreateExecSession starts a persistent interactive terminal in an allocation task.
func (s *Server) CreateExecSession(ctx context.Context, namespace, id string, request *api.ExecSessionCreateRequest) (*api.ExecSessionResponse, error) {
	nodeID, address, tasks, err := s.allocationAgentAddress(namespace, id)
	if err != nil {
		return nil, err
	}
	request.Task, err = resolveExecTask(id, request.Task, tasks)
	if err != nil {
		return nil, err
	}
	return s.client.CreateExecSession(ctx, nodeID, address, id, request)
}

// WriteExecSession sends input to an interactive allocation terminal.
func (s *Server) WriteExecSession(ctx context.Context, namespace, id, sessionID string, request *api.ExecSessionInputRequest) error {
	nodeID, address, _, err := s.allocationAgentAddress(namespace, id)
	if err != nil {
		return err
	}
	return s.client.WriteExecSession(ctx, nodeID, address, id, sessionID, request)
}

// ReadExecSession reads output from an interactive allocation terminal.
func (s *Server) ReadExecSession(ctx context.Context, namespace, id, sessionID string, offset int64) (*api.ExecSessionOutputResponse, error) {
	nodeID, address, _, err := s.allocationAgentAddress(namespace, id)
	if err != nil {
		return nil, err
	}
	return s.client.ReadExecSession(ctx, nodeID, address, id, sessionID, offset)
}

// ResizeExecSession changes an interactive allocation terminal's dimensions.
func (s *Server) ResizeExecSession(ctx context.Context, namespace, id, sessionID string, request *api.ExecSessionResizeRequest) error {
	nodeID, address, _, err := s.allocationAgentAddress(namespace, id)
	if err != nil {
		return err
	}
	return s.client.ResizeExecSession(ctx, nodeID, address, id, sessionID, request)
}

// CloseExecSession terminates an interactive allocation terminal.
func (s *Server) CloseExecSession(ctx context.Context, namespace, id, sessionID string) error {
	nodeID, address, _, err := s.allocationAgentAddress(namespace, id)
	if err != nil {
		return err
	}
	return s.client.CloseExecSession(ctx, nodeID, address, id, sessionID)
}

// AllocationMetrics returns resource usage for all tasks in an allocation.
func (s *Server) AllocationMetrics(ctx context.Context, namespace, id string) (api.AllocationMetricsListResponse, error) {
	s.mu.RLock()
	var found *Allocation
	for _, alloc := range s.allocations {
		if alloc.ID == id && alloc.Namespace == namespace {
			found = alloc
			break
		}
	}
	if found == nil || found.Node == nil {
		s.mu.RUnlock()
		return nil, ErrAllocationNotFound
	}
	nodeID := found.Node.ID
	address := fmt.Sprintf("%s:%d", found.Node.Host, found.Node.Port)
	s.mu.RUnlock()
	return s.client.AllocationMetrics(ctx, nodeID, address, id)
}

// AdministratorVerification returns the replicated administrator public key and current leadership epoch.
func (s *Server) AdministratorVerification() (ed25519.PublicKey, uint64, bool) {
	s.mu.RLock()
	encoded := ""
	epoch := uint64(0)
	if s.cluster != nil {
		encoded = s.cluster.AdministratorPublicKey
		epoch = s.cluster.ControlEpoch
	}
	s.mu.RUnlock()
	publicKey, err := parseAdministratorPublicKey(encoded)
	if err != nil {
		return nil, 0, false
	}
	return publicKey, epoch, true
}

func parseAdministratorPublicKey(encoded string) (ed25519.PublicKey, error) {
	der, err := base64.RawStdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decode base64: %w", err)
	}
	parsed, err := x509.ParsePKIXPublicKey(der)
	if err != nil {
		return nil, fmt.Errorf("parse PKIX key: %w", err)
	}
	publicKey, ok := parsed.(ed25519.PublicKey)
	if !ok {
		return nil, fmt.Errorf("key must be Ed25519")
	}
	return publicKey, nil
}

func unwrapPathError(err error) error {
	for {
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return err
		}
		err = u.Unwrap()
	}
}

// ListServices returns discoverable service instances.
func (s *Server) ListServices(namespace string, filter *catalog.ListFilter) api.ServiceListResponse {
	return s.catalog.List(namespace, filter)
}

// ListServicesForNode returns services only for namespaces with active
// allocations assigned to the authenticated node.
func (s *Server) ListServicesForNode(nodeID uuid.UUID, filter *catalog.ListFilter) api.ServiceListResponse {
	s.mu.RLock()
	namespaces := make(map[string]struct{})
	for _, allocation := range s.allocations {
		allocation.mu.Lock()
		if allocation.Node != nil && allocation.Node.ID == nodeID &&
			allocation.Phase != lifecycle.PhaseStopped && allocation.Phase != lifecycle.PhaseFailed && allocation.Phase != lifecycle.PhaseLost {
			namespaces[allocation.Namespace] = struct{}{}
		}
		allocation.mu.Unlock()
	}
	s.mu.RUnlock()

	names := make([]string, 0, len(namespaces))
	for namespace := range namespaces {
		names = append(names, namespace)
	}
	sort.Strings(names)
	var result api.ServiceListResponse
	for _, namespace := range names {
		result = append(result, s.catalog.List(namespace, filter)...)
	}
	return result
}

// Catalog returns the service catalog.
func (s *Server) Catalog() *catalog.ServiceCatalog {
	return s.catalog
}

func (s *Server) refreshCatalog() {
	s.mu.RLock()
	defer s.mu.RUnlock()

	namespaced := make(map[string][]catalog.ServiceInstance)
	for _, a := range s.allocations {
		a.mu.Lock()
		if a.Phase != lifecycle.PhaseRunning || a.Health != lifecycle.HealthHealthy {
			a.mu.Unlock()
			continue
		}
		var labels map[string]string
		job := s.jobs[jobKey(a.Namespace, a.JobName)]
		if job != nil {
			for _, g := range job.Spec.TaskGroups {
				if g.Name == a.TaskGroupName {
					labels = g.Labels
					break
				}
			}
		}
		address := allocationEndpointAddress(a)
		if address == "" {
			a.mu.Unlock()
			continue
		}
		namespaced[a.Namespace] = append(namespaced[a.Namespace], catalog.ServiceInstance{
			ID:      a.ID,
			Job:     a.JobName,
			Group:   a.TaskGroupName,
			Address: address,
			Ports:   a.Ports,
			Labels:  labels,
		})
		a.mu.Unlock()
	}

	s.catalog.Replace(namespaced)
}

func (s *Server) refreshCatalogAllocations(allocations []*Allocation) {
	ids := make(map[string]bool, len(allocations))
	replacements := make(map[string][]catalog.ServiceInstance)
	s.mu.RLock()
	for _, allocation := range allocations {
		allocation.mu.Lock()
		ids[allocation.ID] = true
		if allocation.Phase == lifecycle.PhaseRunning && allocation.Health == lifecycle.HealthHealthy {
			var labels map[string]string
			job := s.jobs[jobKey(allocation.Namespace, allocation.JobName)]
			if job != nil {
				for _, group := range job.Spec.TaskGroups {
					if group.Name == allocation.TaskGroupName {
						labels = group.Labels
						break
					}
				}
			}
			if address := allocationEndpointAddress(allocation); address != "" {
				replacements[allocation.Namespace] = append(replacements[allocation.Namespace], catalog.ServiceInstance{
					ID: allocation.ID, Job: allocation.JobName, Group: allocation.TaskGroupName,
					Address: address, Ports: allocation.Ports, Labels: labels,
				})
			}
		}
		allocation.mu.Unlock()
	}
	s.mu.RUnlock()
	s.catalog.ReplaceInstances(ids, replacements)
}

// TokenManager returns the namespace token manager.
func (s *Server) TokenManager() *auth.TokenManager {
	return s.tokenManager
}

// ServerAddr returns the advertised server address.
func (s *Server) ServerAddr() string {
	return s.serverAddr
}

// SetClusterJoiner configures Raft membership operations.
func (s *Server) SetClusterJoiner(j ClusterJoiner) {
	s.joiner = j
}

func (s *Server) runReconcileLoop(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	// A newly elected leader must derive current network-plan state before
	// waiting for the ordinary periodic reconciliation interval. Periodic
	// passes do not wait for their agent actions: a node still busy with an
	// earlier pass is skipped while the other nodes proceed.
	s.reconcile(ctx, false)

	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.reconcile(ctx, false)
		}
	}
}
