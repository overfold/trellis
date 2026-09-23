package server

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/netip"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/auth"
	"github.com/clofour/trellis/internal/catalog"
	"github.com/clofour/trellis/internal/client"
	"github.com/clofour/trellis/internal/lifecycle"
	secretstore "github.com/clofour/trellis/internal/secrets"
	"github.com/clofour/trellis/internal/spec"
	"github.com/clofour/trellis/internal/state"
	"github.com/clofour/trellis/internal/storage"

	"github.com/google/uuid"
)

const reconcileInterval = 10 * time.Second
const heartbeatInterval = 10 * time.Second

// ClusterJoiner adds and removes Raft cluster members.
type ClusterJoiner interface {
	AddVoter(id, address string) error
	RemoveServer(id string) error
	LeadershipTransfer() error
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

	cluster            *Cluster
	nodes              map[uuid.UUID]*Node
	jobs               map[string]*Job
	allocations        []*Allocation
	networkPool        netip.Prefix
	networkPorts       map[string]int
	wireGuardPortCount int
	tokenManager       *auth.TokenManager
	catalog            *catalog.ServiceCatalog
	serverAddr         string
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
	//   - reconcileMu serializes complete reconciliation passes.
	//   - mutationMu serializes durable state mutations and is never acquired
	//     while mu or allocation.mu is held.
	//   - networkPortMu serializes durable namespace WireGuard port assignment
	//     and is never acquired while mu or allocation.mu is held.
	mu            sync.RWMutex
	reconcileMu   sync.Mutex
	mutationMu    sync.Mutex
	networkPortMu sync.Mutex
	networkPlanMu sync.Mutex
	networkPlans  map[networkPlanKey]*networkPlanState
	networkPlanWake chan struct{}
	controlEpoch  uint64
	leaderSince   time.Time
	now           func() time.Time
	metrics       *Metrics
	secrets       *secretstore.Store
	events        *EventBus
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
		if record.Spec == nil || record.Revision < 1 || record.CreatedAt.IsZero() {
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
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	if err := s.backupStore.RestoreDesired(s.clusterName, snapshot); err != nil {
		return err
	}
	return s.Reload(ctx)
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
		return nil, fmt.Errorf("allocation not found")
	}
	address := fmt.Sprintf("%s:%d", found.Node.Host, found.Node.Port)
	s.mu.RUnlock()
	return s.client.Logs(ctx, address, id, follow, tail)
}

// Cluster contains persisted cluster identity and TLS state.
type Cluster struct {
	Hash         string
	ControlEpoch uint64 `json:"control_epoch,omitempty"`
}

// NodeRegistration contains the identity and capacity of a node.
type NodeRegistration struct {
	ID                 uuid.UUID
	Host               string
	Port               int
	CPU                int
	Memory             int64
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

// Node contains the in-memory state of a registered node.
type Node struct {
	ID                 uuid.UUID
	Host               string
	Port               int
	Status             NodeStatus
	LastHeartbeat      time.Time
	CPU                int
	Memory             int64
	OS                 string
	Arch               string
	Labels             map[string]string
	Volumes            []string
	Capabilities       []spec.NodeCapability
	WireGuardPublicKey string
	WireGuardEndpoint  string
	WireGuardPortBase  int
	WireGuardPortCount int
	Version            string
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

// NodeSummary is the persisted representation of a node.
type NodeSummary struct {
	ID                 uuid.UUID
	Host               string
	Port               int
	CPU                int
	Memory             int64
	OS                 string
	Arch               string
	Labels             map[string]string
	Volumes            []string
	Capabilities       []spec.NodeCapability
	Status             NodeStatus
	WireGuardPublicKey string
	WireGuardEndpoint  string
	WireGuardPortBase  int
	WireGuardPortCount int
	LastHeartbeat      time.Time
	Version            string `json:"version,omitempty"`
}

// Job contains a persisted job specification and revision.
type Job struct {
	Spec     *spec.JobSpec
	Revision int
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
	Node      *Node
	Endpoints []api.AllocationEndpoint `json:"endpoints,omitempty"`
	Ports     []api.PortMapping        `json:"ports,omitempty"`
	// Draining marks an allocation whose job revision is superseded under
	// a rolling update strategy. Draining allocations are not restarted on
	// failure and are not counted toward the desired count.
	Draining bool `json:"draining,omitempty"`
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

// NewServer constructs an orchestrator server.
func NewServer(log *slog.Logger, storage *storage.LocalStorage, state *StateController, store state.Store, cluster, serverAddr string) *Server {
	pool := netip.MustParsePrefix("10.64.0.0/10")
	s := &Server{
		log:                log.With("component", "server"),
		storage:            storage,
		state:              state,
		client:             &client.AgentClient{},
		nodes:              make(map[uuid.UUID]*Node),
		jobs:               make(map[string]*Job),
		networkPool:        pool,
		networkPorts:       make(map[string]int),
		wireGuardPortCount: 256,
		networkPlans:       make(map[networkPlanKey]*networkPlanState),
		networkPlanWake:    make(chan struct{}, 1),
		tokenManager:       auth.NewTokenManager(store, cluster),
		catalog:            catalog.New(),
		serverAddr:         serverAddr,
		clusterName:        cluster,
		jobLimits:          spec.DefaultLimits(),
		now:                time.Now,
	}
	s.backupStore, _ = store.(desiredStore)
	s.events = newEventBus()
	return s
}

// SetJobLimits configures operator-owned job admission and resource defaults.
func (s *Server) SetJobLimits(limits spec.Limits) error {
	if err := spec.ValidateLimits(limits); err != nil {
		return err
	}
	s.mu.Lock()
	s.jobLimits = limits
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
	if err := s.state.PutCluster(ctx, cluster); err != nil {
		return fmt.Errorf("persist control-plane epoch: %w", err)
	}
	s.mu.Lock()
	s.cluster = cluster
	s.controlEpoch = epoch
	s.leaderSince = s.now()
	s.mu.Unlock()
	return nil
}

// SetNetworkPool configures the allocation address pool.
func (s *Server) SetNetworkPool(pool string) error {
	p, err := netip.ParsePrefix(pool)
	if err != nil || !p.Addr().Is4() || p.Bits() > 16 {
		return fmt.Errorf("WireGuard pool must be an IPv4 prefix of /16 or larger")
	}
	s.networkPool = p.Masked()
	return nil
}

// SetWireGuardPortCount configures how many consecutive UDP ports are
// available for namespace WireGuard pathways on every node.
func (s *Server) SetWireGuardPortCount(count int) error {
	if count < 1 || count > 65535 {
		return fmt.Errorf("WireGuard port count must be between 1 and 65535")
	}
	s.wireGuardPortCount = count
	return nil
}

// Init initializes cluster state and returns its token.
func (s *Server) Init(ctx context.Context) (string, error) {
	return s.InitWithToken(ctx, "")
}

// InitWithToken initializes cluster state with an optional configured token.
func (s *Server) InitWithToken(ctx context.Context, configuredToken string) (string, error) {
	cluster, err := s.state.GetCluster(ctx)
	if err != nil {
		return "", fmt.Errorf("get cluster: %w", err)
	}

	if cluster != nil {
		s.log.Info("cluster already initialized")

		s.cluster = cluster
		token := configuredToken
		if token == "" {
			if err := s.storage.Get("token", &token); err != nil && !os.IsNotExist(unwrapPathError(err)) {
				return "", fmt.Errorf("load local cluster token: %w", err)
			}
		}
		if token == "" || !validateToken(cluster, token) {
			return "", fmt.Errorf("cluster token is missing or does not match cluster")
		}
		s.client = client.NewAgentClient(token, s.clientTLS)
		s.controlEpoch = cluster.ControlEpoch
		return "", nil
	}

	token := configuredToken
	if token == "" {
		b := make([]byte, 32)
		if _, err = rand.Read(b); err != nil {
			return "", fmt.Errorf("generate cluster token: %w", err)
		}
		token = base64.RawURLEncoding.EncodeToString(b)
	}

	hash := sha256.Sum256([]byte(token))
	hashHex := hex.EncodeToString(hash[:])

	err = s.storage.Put("token", token)
	if err != nil {
		return "", fmt.Errorf("save cluster locally: %w", err)
	}

	cluster = &Cluster{
		Hash: hashHex,
	}

	err = s.state.PutCluster(ctx, cluster)
	if err != nil {
		return "", fmt.Errorf("save cluster remotely: %w", err)
	}

	s.cluster = cluster
	s.controlEpoch = cluster.ControlEpoch
	s.client = client.NewAgentClient(token, s.clientTLS)

	return token, nil
}

// SetClientTLS configures TLS for agent requests.
func (s *Server) SetClientTLS(cfg *tls.Config) {
	s.clientTLS = cfg
}

// ClusterCA returns the cluster certificate authority materials.
func (s *Server) ClusterCA() (certPEM, keyPEM string, err error) {
	if err := s.storage.Get("tls/ca-cert", &certPEM); err != nil {
		return "", "", fmt.Errorf("load CA cert: %w", err)
	}
	if err := s.storage.Get("tls/ca-key", &keyPEM); err != nil {
		return "", "", fmt.Errorf("load CA key: %w", err)
	}
	return certPEM, keyPEM, nil
}

func validateToken(cluster *Cluster, token string) bool {
	hash := sha256.Sum256([]byte(token))
	hashHex := hex.EncodeToString(hash[:])
	return subtle.ConstantTimeCompare([]byte(hashHex), []byte(cluster.Hash)) == 1
}

// Run starts background reconciliation until the context ends.
func (s *Server) Run(ctx context.Context) {
	go s.runReconcileLoop(ctx)
	go s.runNetworkPlanLoop(ctx)
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
	s.mu.RLock()
	status := NodeStatusHealthy
	if existing := s.nodes[nodeRegistration.ID]; existing != nil && existing.Status == NodeStatusDraining {
		status = NodeStatusDraining
	}
	s.mu.RUnlock()
	err := s.state.PutNode(ctx, nodeRegistration.ID.String(), &NodeSummary{
		ID:   nodeRegistration.ID,
		Host: nodeRegistration.Host,
		Port: nodeRegistration.Port,
		CPU:  nodeRegistration.CPU, Memory: nodeRegistration.Memory,
		OS: nodeRegistration.OS, Arch: nodeRegistration.Arch, Labels: nodeRegistration.Labels, Status: status,
		Volumes:            nodeRegistration.Volumes,
		Capabilities:       nodeRegistration.Capabilities,
		WireGuardPublicKey: nodeRegistration.WireGuardPublicKey, WireGuardEndpoint: nodeRegistration.WireGuardEndpoint,
		WireGuardPortBase: nodeRegistration.WireGuardPortBase, WireGuardPortCount: nodeRegistration.WireGuardPortCount,
		LastHeartbeat: s.now().UTC(),
	})
	if err != nil {
		return fmt.Errorf("save node remotely: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	node := s.nodes[nodeRegistration.ID]
	if node == nil {
		node = &Node{ID: nodeRegistration.ID}
		s.nodes[nodeRegistration.ID] = node
	}
	node.Host, node.Port = nodeRegistration.Host, nodeRegistration.Port
	if node.Status != NodeStatusDraining {
		node.Status = NodeStatusHealthy
	}
	node.LastHeartbeat = time.Now()
	node.CPU, node.Memory = nodeRegistration.CPU, nodeRegistration.Memory
	node.OS, node.Arch = nodeRegistration.OS, nodeRegistration.Arch
	node.Labels = nodeRegistration.Labels
	node.Volumes = append([]string(nil), nodeRegistration.Volumes...)
	node.Capabilities = append([]spec.NodeCapability(nil), nodeRegistration.Capabilities...)
	node.WireGuardPublicKey, node.WireGuardEndpoint = nodeRegistration.WireGuardPublicKey, nodeRegistration.WireGuardEndpoint
	node.WireGuardPortBase, node.WireGuardPortCount = nodeRegistration.WireGuardPortBase, nodeRegistration.WireGuardPortCount

	return nil
}

// Heartbeat records a node heartbeat and allocation state.
func (s *Server) Heartbeat(ctx context.Context, nodeID uuid.UUID, actual []api.AllocationStatus, version string, volumes []string, capabilities []spec.NodeCapability) error {
	s.mu.Lock()
	node, ok := s.nodes[nodeID]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("node not found")
	}

	if node.Status != NodeStatusDraining {
		node.Status = NodeStatusHealthy
	}
	node.LastHeartbeat = time.Now()
	node.Version = version
	node.Volumes = append([]string(nil), volumes...)
	node.Capabilities = append([]spec.NodeCapability(nil), capabilities...)
	owned := make([]*Allocation, 0)
	for _, allocation := range s.allocations {
		if allocation.Node == node {
			owned = append(owned, allocation)
		}
	}
	summary := &NodeSummary{ID: node.ID, Host: node.Host, Port: node.Port, CPU: node.CPU, Memory: node.Memory, OS: node.OS, Arch: node.Arch, Labels: node.Labels, Volumes: node.Volumes, Capabilities: node.Capabilities, Status: node.Status, WireGuardPublicKey: node.WireGuardPublicKey, WireGuardEndpoint: node.WireGuardEndpoint, WireGuardPortBase: node.WireGuardPortBase, WireGuardPortCount: node.WireGuardPortCount, LastHeartbeat: node.LastHeartbeat, Version: node.Version}
	s.mu.Unlock()
	if err := s.state.PutNode(ctx, node.ID.String(), summary); err != nil {
		return fmt.Errorf("persist node heartbeat: %w", err)
	}
	type statusInfo struct {
		Generation uint64
		Phase      lifecycle.Phase
		Health     lifecycle.Health
		Endpoints  []api.AllocationEndpoint
		Ports      []api.PortMapping
		Tasks      int
	}
	statuses := make(map[string]statusInfo, len(actual))
	for _, a := range actual {
		if !a.Phase.Valid() || !a.Health.Valid() {
			return fmt.Errorf("invalid allocation state for %s: phase=%q health=%q", a.ID, a.Phase, a.Health)
		}
		phase, health := a.Phase, a.Health
		key := fmt.Sprintf("%s/%d", a.ID, a.Generation)
		info := statuses[key]
		if info.Tasks == 0 {
			info.Generation, info.Phase, info.Health = a.Generation, phase, health
		} else {
			if phase != lifecycle.PhaseRunning {
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
		info.Tasks++
		info.Ports = append(info.Ports, a.Ports...)
		if a.Task != "" || a.Address != "" || len(a.Ports) > 0 {
			info.Endpoints = append(info.Endpoints, api.AllocationEndpoint{
				Task: a.Task, Address: a.Address, Ports: append([]api.PortMapping(nil), a.Ports...),
			})
		}
		statuses[key] = info
	}
	var changed []*Allocation
	for _, a := range owned {
		a.mu.Lock()
		info, ok := statuses[fmt.Sprintf("%s/%d", a.ID, a.Generation)]
		if !ok {
			a.mu.Unlock()
			continue // absence is not proof of loss or failure
		}
		if info.Phase.Valid() && lifecycle.CanTransition(a.Phase, info.Phase) {
			_ = a.Transition(info.Phase, time.Now().UTC(), "", "")
		}
		_ = a.SetHealth(info.Health)
		sort.Slice(info.Endpoints, func(i, j int) bool { return info.Endpoints[i].Task < info.Endpoints[j].Task })
		a.Endpoints = append([]api.AllocationEndpoint(nil), info.Endpoints...)
		a.Ports = append([]api.PortMapping(nil), info.Ports...)
		changed = append(changed, a)
		a.mu.Unlock()
	}
	for _, allocation := range changed {
		allocation.mu.Lock()
		if err := s.state.PutAllocation(ctx, allocation); err != nil {
			allocation.mu.Unlock()
			return fmt.Errorf("persist allocation observation: %w", err)
		}
		allocation.mu.Unlock()
	}

	s.refreshCatalog()
	return nil
}

// HeartbeatResponse returns desired allocation state for a node.
func (s *Server) HeartbeatResponse(nodeID uuid.UUID) api.HeartbeatResponse {
	s.mu.RLock()
	epoch, leaderSince := s.controlEpoch, s.leaderSince
	allocations := append([]*Allocation(nil), s.allocations...)
	s.mu.RUnlock()
	response := api.HeartbeatResponse{Epoch: epoch, OrphanConfirmation: !leaderSince.IsZero() && s.now().Sub(leaderSince) >= leaderRecoveryGrace}
	for _, allocation := range allocations {
		allocation.mu.Lock()
		if allocation.Node != nil && allocation.Node.ID == nodeID && allocation.Phase != lifecycle.PhaseStopped && allocation.Phase != lifecycle.PhaseFailed && allocation.Phase != lifecycle.PhaseLost {
			response.Desired = append(response.Desired, api.DesiredAllocation{ID: allocation.ID, Generation: allocation.Generation, Draining: allocation.Draining})
		}
		allocation.mu.Unlock()
	}
	return response
}

func jobKey(namespace, name string) string {
	if namespace == "" {
		return name
	}
	return namespace + "\x00" + name
}

// RegisterJob creates or updates desired job state.
func (s *Server) RegisterJob(ctx context.Context, namespace string, jobSpec *spec.JobSpec) error {
	if err := s.CanonicalizeJob(jobSpec); err != nil {
		return fmt.Errorf("validate job: %w", err)
	}
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	s.mu.Lock()
	if jobSpec.Namespace != namespace {
		s.mu.Unlock()
		return fmt.Errorf("job namespace does not match request namespace")
	}
	key := jobKey(namespace, jobSpec.Name)
	if err := s.validateNamespaceAllocationLimitLocked(nil, namespace, jobSpec, key); err != nil {
		s.mu.Unlock()
		return err
	}

	hashes := make(map[string]string, len(jobSpec.TaskGroups))
	for i := range jobSpec.TaskGroups {
		hashes[jobSpec.TaskGroups[i].Name] = spec.TaskGroupContentHash(&jobSpec.TaskGroups[i])
	}

	revision := 1
	labelOnly := false
	if existing := s.jobs[key]; existing != nil {
		revision = existing.Revision + 1
		if isLabelOnlyChange(existing, jobSpec, hashes) {
			revision = existing.Revision
			labelOnly = true
		}
	}
	job := &Job{
		Spec:          jobSpec,
		Revision:      revision,
		ContentHashes: hashes,
	}
	s.mu.Unlock()
	var revisionRecord *JobRevisionRecord
	if !labelOnly {
		revisionRecord = &JobRevisionRecord{Revision: revision, Spec: jobSpec, CreatedAt: s.now().UTC()}
	}
	if err := s.state.PutJobWithRevision(ctx, key, job, revisionRecord); err != nil {
		return fmt.Errorf("save job remotely: %w", err)
	}
	s.mu.Lock()
	s.jobs[key] = job
	s.mu.Unlock()

	if labelOnly {
		s.refreshCatalog()
	} else {
		s.events.publish(api.ClusterEvent{
			Type:      api.EventJobRegistered,
			Namespace: namespace,
			JobName:   jobSpec.Name,
			Revision:  revision,
			At:        s.now().UTC(),
		})
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
// existing one only in task group labels (and count/update policy). The
// content hashes must have been computed from newSpec.
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
	nodes := make(map[uuid.UUID]*Node, len(nodeSummaries))
	for _, summary := range nodeSummaries {
		status := NodeStatusUnhealthy
		if summary.Status == NodeStatusDraining {
			status = NodeStatusDraining
		}
		nodes[summary.ID] = &Node{ID: summary.ID, Host: summary.Host, Port: summary.Port, CPU: summary.CPU, Memory: summary.Memory, OS: summary.OS, Arch: summary.Arch, Labels: summary.Labels, Volumes: summary.Volumes, Capabilities: summary.Capabilities, Status: status, WireGuardPublicKey: summary.WireGuardPublicKey, WireGuardEndpoint: summary.WireGuardEndpoint, WireGuardPortBase: summary.WireGuardPortBase, WireGuardPortCount: summary.WireGuardPortCount, LastHeartbeat: summary.LastHeartbeat, Version: summary.Version}
	}
	allocations := make([]*Allocation, 0, len(allocationMap))
	for _, allocation := range allocationMap {
		if allocation.Node != nil {
			allocation.Node = nodes[allocation.Node.ID]
		}
		allocations = append(allocations, allocation)
	}
	s.mu.Lock()
	s.jobs = jobs
	s.nodes = nodes
	s.allocations = allocations
	s.mu.Unlock()
	return nil
}

// DrainNode marks a node for allocation evacuation.
func (s *Server) DrainNode(ctx context.Context, id uuid.UUID) error {
	s.mu.Lock()
	node := s.nodes[id]
	if node == nil {
		s.mu.Unlock()
		return fmt.Errorf("node not found")
	}
	previousStatus := node.Status
	node.Status = NodeStatusDraining
	summary := &NodeSummary{ID: node.ID, Host: node.Host, Port: node.Port, CPU: node.CPU, Memory: node.Memory, OS: node.OS, Arch: node.Arch, Labels: node.Labels, Volumes: node.Volumes, Capabilities: node.Capabilities, Status: node.Status, WireGuardPublicKey: node.WireGuardPublicKey, WireGuardEndpoint: node.WireGuardEndpoint, WireGuardPortBase: node.WireGuardPortBase, WireGuardPortCount: node.WireGuardPortCount, LastHeartbeat: node.LastHeartbeat, Version: node.Version}
	s.mu.Unlock()
	if err := s.state.PutNode(ctx, id.String(), summary); err != nil {
		s.mu.Lock()
		if current := s.nodes[id]; current == node && current.Status == NodeStatusDraining {
			current.Status = previousStatus
		}
		s.mu.Unlock()
		return err
	}
	s.Reconcile(ctx)
	return nil
}

// UndrainNode makes a drained node schedulable.
func (s *Server) UndrainNode(ctx context.Context, id uuid.UUID) error {
	s.mu.Lock()
	node := s.nodes[id]
	if node == nil {
		s.mu.Unlock()
		return fmt.Errorf("node not found")
	}
	previousStatus := node.Status
	node.Status = NodeStatusHealthy
	summary := &NodeSummary{ID: node.ID, Host: node.Host, Port: node.Port, CPU: node.CPU, Memory: node.Memory, OS: node.OS, Arch: node.Arch, Labels: node.Labels, Volumes: node.Volumes, Capabilities: node.Capabilities, Status: node.Status, WireGuardPublicKey: node.WireGuardPublicKey, WireGuardEndpoint: node.WireGuardEndpoint, WireGuardPortBase: node.WireGuardPortBase, WireGuardPortCount: node.WireGuardPortCount, LastHeartbeat: node.LastHeartbeat, Version: node.Version}
	s.mu.Unlock()
	if err := s.state.PutNode(ctx, id.String(), summary); err != nil {
		s.mu.Lock()
		if current := s.nodes[id]; current == node && current.Status == NodeStatusHealthy {
			current.Status = previousStatus
		}
		s.mu.Unlock()
		return err
	}
	s.Reconcile(ctx)
	return nil
}

// ListJobs returns jobs in a namespace.
func (s *Server) ListJobs(namespace string) api.JobListResponse {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(api.JobListResponse, 0, len(s.jobs))
	for key, job := range s.jobs {
		if job.Spec.Namespace != namespace {
			continue
		}
		name := job.Spec.Name
		r := api.JobStatusResponse{Name: name, Revision: job.Revision}
		for _, g := range job.Spec.TaskGroups {
			r.Desired += g.Count
		}
		for _, a := range s.allocations {
			a.mu.Lock()
			if jobKey(a.Namespace, a.JobName) != key {
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
		result = append(result, r)
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
	r := &api.JobStatusResponse{Name: name, Revision: job.Revision, Spec: &specCopy}
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
	return r, true
}

// DeleteJob removes desired job state.
func (s *Server) DeleteJob(ctx context.Context, namespace, name string) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	s.mu.RLock()
	key := jobKey(namespace, name)
	_, ok := s.jobs[key]
	s.mu.RUnlock()
	if !ok {
		return fmt.Errorf("job %s not found", name)
	}
	if err := s.state.DeleteJob(ctx, key); err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.jobs, key)
	s.mu.Unlock()
	s.events.publish(api.ClusterEvent{
		Type:      api.EventJobDeleted,
		Namespace: namespace,
		JobName:   name,
		At:        s.now().UTC(),
	})
	s.Reconcile(ctx)
	return nil
}

// RestartJob marks all running allocations for a job as draining so the
// reconciler will replace them with fresh instances.
func (s *Server) RestartJob(ctx context.Context, namespace, name string) error {
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
		if alloc.Namespace == namespace && alloc.JobName == name && !alloc.Draining &&
			alloc.Phase != lifecycle.PhaseStopped && alloc.Phase != lifecycle.PhaseFailed && alloc.Phase != lifecycle.PhaseLost {
			raw, err := json.Marshal(alloc)
			alloc.mu.Unlock()
			if err != nil {
				return fmt.Errorf("marshal restart intent: %w", err)
			}
			var update Allocation
			if err := json.Unmarshal(raw, &update); err != nil {
				return fmt.Errorf("decode restart intent: %w", err)
			}
			update.Draining = true
			updates = append(updates, &update)
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
			}
			alloc.mu.Unlock()
			break
		}
	}
	return nil
}

// StopAllocationByID stops a single allocation identified by namespace and ID.
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
	s.mu.RUnlock()
	return s.Execute(ctx, &Action{Type: ActionStop, Allocation: found})
}

// ListJobRevisions returns the stored spec history for a job.
func (s *Server) ListJobRevisions(ctx context.Context, namespace, name string) (api.JobRevisionListResponse, error) {
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
	result := make(api.JobRevisionListResponse, 0, len(records))
	for _, r := range records {
		result = append(result, api.JobRevisionResponse{
			Revision:  r.Revision,
			Spec:      *r.Spec,
			CreatedAt: r.CreatedAt,
		})
	}
	return result, nil
}

func (s *Server) allocationAgentAddress(namespace, id string) (string, []spec.TaskSpec, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, alloc := range s.allocations {
		if alloc.ID == id && alloc.Namespace == namespace {
			if alloc.Node == nil {
				return "", nil, fmt.Errorf("allocation not found")
			}
			return fmt.Sprintf("%s:%d", alloc.Node.Host, alloc.Node.Port), append([]spec.TaskSpec(nil), alloc.Tasks...), nil
		}
	}
	return "", nil, fmt.Errorf("allocation not found")
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
	address, tasks, err := s.allocationAgentAddress(namespace, id)
	if err != nil {
		return nil, err
	}
	task, err = resolveExecTask(id, task, tasks)
	if err != nil {
		return nil, err
	}
	return s.client.ExecAllocation(ctx, address, id, task, command)
}

// CreateExecSession starts a persistent interactive terminal in an allocation task.
func (s *Server) CreateExecSession(ctx context.Context, namespace, id string, request *api.ExecSessionCreateRequest) (*api.ExecSessionResponse, error) {
	address, tasks, err := s.allocationAgentAddress(namespace, id)
	if err != nil {
		return nil, err
	}
	request.Task, err = resolveExecTask(id, request.Task, tasks)
	if err != nil {
		return nil, err
	}
	return s.client.CreateExecSession(ctx, address, id, request)
}

// WriteExecSession sends input to an interactive allocation terminal.
func (s *Server) WriteExecSession(ctx context.Context, namespace, id, sessionID string, request *api.ExecSessionInputRequest) error {
	address, _, err := s.allocationAgentAddress(namespace, id)
	if err != nil {
		return err
	}
	return s.client.WriteExecSession(ctx, address, id, sessionID, request)
}

// ReadExecSession reads output from an interactive allocation terminal.
func (s *Server) ReadExecSession(ctx context.Context, namespace, id, sessionID string, offset int64) (*api.ExecSessionOutputResponse, error) {
	address, _, err := s.allocationAgentAddress(namespace, id)
	if err != nil {
		return nil, err
	}
	return s.client.ReadExecSession(ctx, address, id, sessionID, offset)
}

// ResizeExecSession changes an interactive allocation terminal's dimensions.
func (s *Server) ResizeExecSession(ctx context.Context, namespace, id, sessionID string, request *api.ExecSessionResizeRequest) error {
	address, _, err := s.allocationAgentAddress(namespace, id)
	if err != nil {
		return err
	}
	return s.client.ResizeExecSession(ctx, address, id, sessionID, request)
}

// CloseExecSession terminates an interactive allocation terminal.
func (s *Server) CloseExecSession(ctx context.Context, namespace, id, sessionID string) error {
	address, _, err := s.allocationAgentAddress(namespace, id)
	if err != nil {
		return err
	}
	return s.client.CloseExecSession(ctx, address, id, sessionID)
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
		return nil, fmt.Errorf("allocation not found")
	}
	address := fmt.Sprintf("%s:%d", found.Node.Host, found.Node.Port)
	s.mu.RUnlock()
	return s.client.AllocationMetrics(ctx, address, id)
}

// ValidateAPIToken validates the cluster API token.
func (s *Server) ValidateAPIToken(token string) bool {
	if s.cluster == nil {
		return false
	}
	hash := sha256.Sum256([]byte(token))
	hashHex := hex.EncodeToString(hash[:])

	return subtle.ConstantTimeCompare([]byte(hashHex), []byte(s.cluster.Hash)) == 1
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
	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.Reconcile(ctx)
		}
	}
}
