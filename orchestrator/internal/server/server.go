package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"maps"
	"net/netip"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/overfold/trellis/orchestrator/internal/auth"
	"github.com/overfold/trellis/orchestrator/internal/catalog"
	"github.com/overfold/trellis/orchestrator/internal/client"
	secretstore "github.com/overfold/trellis/orchestrator/internal/secrets"
	"github.com/overfold/trellis/orchestrator/internal/spec"
	"github.com/overfold/trellis/orchestrator/internal/state"
	"github.com/overfold/trellis/orchestrator/internal/storage"
	"github.com/overfold/trellis/orchestrator/internal/transport"

	"github.com/google/uuid"
)

const reconcileInterval = 10 * time.Second
const heartbeatInterval = 10 * time.Second

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

// Server coordinates desired state, scheduling, and node operations.
type Server struct {
	log          *slog.Logger
	storage      *storage.LocalStorage
	state        *StateController
	client       *client.AgentClient
	resolveImage func(context.Context, string) (string, error)

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
	networkSubnets     map[networkSubnetKey]int
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
	// Locking contract. Locks are acquired in this order, and a lock is never
	// acquired while one later in the order is held:
	//
	//	reconcileMu -> mutationMu -> termMu -> mu -> allocation.mu -> leaf locks
	//
	//   - reconcileMu serializes reconciliation planning and its durable
	//     commit; a pass's agent actions run after it is released.
	//   - mutationMu serializes durable state mutations: reconciliation
	//     commits, API mutations, agent action outcomes, and the observation
	//     applier. Each reads the committed state, commits through Raft, and
	//     applies the result while holding it, so no two overwrite each other.
	//   - termMu orders term admission and renewable receipt against epoch
	//     publication and observation reset, without network or storage I/O.
	//     Epoch writes hold both termMu and mu; either lock admits a read.
	//   - mu protects the in-memory cluster, node, job, allocation, epoch, and
	//     leadership snapshots. It must never be held during network or storage
	//     I/O.
	//   - allocation.mu protects lifecycle fields on that allocation.
	//   - Leaf locks guard self-contained state and acquire nothing while
	//     held: liveness.mu, observations.mu, resumeMu, networkPlanMu, and
	//     actionMu. actionMu guards actionNodes and actionSlots, which
	//     serialize agent actions per node and bound them globally across
	//     passes.
	//   - refreshMu serializes the network-plan and catalog refresh after a
	//     pass's actions, so overlapping passes cannot apply an older
	//     snapshot. It is acquired without reconcileMu or mutationMu, before
	//     mu.
	//   - membershipMu serializes Raft membership changes and is never
	//     acquired while mu is held.
	//
	// Heartbeats take termMu for short admission checks and then the
	// liveness and observations leaf locks, never mutationMu or Raft.
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
	logStreams         *transport.StreamLimiter

	// termWork joins admitted requests and dispatched work before reload.
	termMu   sync.RWMutex
	term     context.Context
	termWork *sync.WaitGroup

	// resumeMu guards the per-term record of acknowledged allocation
	// resumes. It is a leaf lock: nothing else is acquired while it is held.
	resumeMu    sync.Mutex
	resumes     map[resumeDeliveryKey]uint64
	resumeEpoch uint64

	// reconciliation holds the replicated reconciliation settings; the zero
	// value selects DefaultReconciliationSettings. Protected by mu.
	reconciliation ReconciliationSettings
	// replacementBackoffs holds the committed replacement backoff record of
	// each job task group, keyed by replacementBackoffKey. Records are
	// replaced, never mutated in place. Protected by mu.
	replacementBackoffs map[string]*ReplacementBackoff

	// membershipMu serializes Raft membership reads and changes made by this
	// server so each change is applied to the configuration it was planned
	// from. It is never acquired while mu is held.
	membershipMu sync.Mutex
	// membershipWake asks the membership loop for an immediate pass.
	membershipWake chan struct{}

	// exec tracks the exec streams this leader relays.
	exec execRelays

	// liveness is the leader-local record of node heartbeats and reported
	// Raft progress. It is never replicated.
	liveness nodeLiveness
	// observations hands heartbeat reports to the observation applier.
	observations observationQueue
}

// NewServer constructs an orchestrator server.
func NewServer(log *slog.Logger, storage *storage.LocalStorage, state *StateController, store state.Store, cluster, serverAddr string) *Server {
	settings := DefaultClusterSettings()
	s := &Server{
		log:                log.With("component", "server"),
		storage:            storage,
		state:              state,
		client:             &client.AgentClient{},
		resolveImage:       resolveRegistryImage,
		nodes:              make(map[uuid.UUID]*Node),
		jobs:               make(map[string]*Job),
		networkPool:        settings.WireGuardPool,
		networkPorts:       make(map[string]int),
		networkSubnets:     make(map[networkSubnetKey]int),
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
		reconciliation:     settings.Reconciliation,
		now:                time.Now,
	}
	s.backupStore, _ = store.(desiredStore)
	s.events = newEventBus()
	s.logStreams = transport.NewStreamLimiter(logFollowGlobalLimit, logFollowPerAllocationLimit)
	s.tokenManager.SetClock(func() time.Time { return s.now() })
	s.tokenManager.SetWorkloadAuthorizer(s.authorizeWorkloadCredential)
	return s
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
	maps.Copy(jobs, s.jobs)
	s.mu.RUnlock()
	if err := s.state.ActivateLeadership(ctx, cluster, jobs); err != nil {
		return fmt.Errorf("persist leadership activation: %w", err)
	}
	s.termMu.Lock()
	defer s.termMu.Unlock()
	s.mu.Lock()
	s.controlEpoch = epoch
	// Job limits and reconciliation settings may have changed under a
	// previous leader.
	s.loadClusterLocked(cluster)
	s.leaderSince = s.now()
	s.mu.Unlock()
	// Liveness and pending heartbeat reports are leader observations of an
	// earlier term. Raft progress is also measured against this server's own
	// applied index, so reports from an earlier term must not decide
	// promotions in this one.
	s.liveness.startTerm()
	s.observations.reset()

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

	s.termMu.Lock()
	s.mu.Lock()
	s.controlEpoch = cluster.ControlEpoch
	s.loadClusterLocked(cluster)
	s.networkPool = cluster.Settings.WireGuardPool
	s.wireGuardPortCount = cluster.Settings.WireGuardPortCount
	s.mu.Unlock()
	s.termMu.Unlock()
	s.client = client.NewAgentClient("", s.clientTLS)
	return nil
}

// SetClientTLS configures TLS for agent requests.
func (s *Server) SetClientTLS(cfg *tls.Config) {
	s.clientTLS = cfg
}

// SetNodeID configures the immutable identity of this control-plane member.
func (s *Server) SetNodeID(id uuid.UUID) { s.nodeID = id }

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

// Run starts background reconciliation until the context ends. The returned
// channel closes after all leader-owned loops stop; callers must join the old
// term before reloading state or starting another one.
func (s *Server) Run(ctx context.Context) <-chan struct{} {
	work := &sync.WaitGroup{}
	s.termMu.Lock()
	s.term = ctx
	s.termWork = work
	s.termMu.Unlock()
	s.exec.startTerm(ctx)
	ctx, release := s.bindTerm(ctx)
	var group sync.WaitGroup
	for _, run := range []func(context.Context){s.runReconcileLoop, s.runNetworkPlanLoop, s.runMembershipLoop, s.runObservationApplier} {
		group.Go(func() { run(ctx) })
	}
	done := make(chan struct{})
	go func() {
		group.Wait()
		release()
		work.Wait()
		close(done)
	}()
	return done
}

type leadershipContextKey struct{}

type leadershipFence struct {
	term  context.Context
	epoch uint64
	work  *sync.WaitGroup
}

type leadershipContext struct {
	context.Context
	term context.Context
}

func (c leadershipContext) Err() error {
	if err := c.term.Err(); err != nil {
		return err
	}
	return c.Context.Err()
}

// bindTerm preserves the originating term even through WithoutCancel. Only
// request cancellation may be discarded by action outcome accounting.
func (s *Server) bindTerm(ctx context.Context) (context.Context, func()) {
	s.termMu.RLock()
	fence, ok := ctx.Value(leadershipContextKey{}).(leadershipFence)
	if !ok {
		fence = leadershipFence{term: s.term, epoch: s.controlEpoch, work: s.termWork}
	}
	tracked := fence.work != nil && fence.term.Err() == nil
	if tracked {
		fence.work.Add(1)
	}
	s.termMu.RUnlock()
	ctx = context.WithValue(ctx, leadershipContextKey{}, fence)
	if fence.term == nil {
		return ctx, func() {}
	}
	bound, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(fence.term, cancel)
	if fence.term.Err() != nil {
		cancel()
	}
	return leadershipContext{Context: bound, term: fence.term}, func() {
		stop()
		cancel()
		if tracked {
			fence.work.Done()
		}
	}
}

func (s *Server) checkTerm(ctx context.Context) error {
	s.termMu.RLock()
	defer s.termMu.RUnlock()
	return s.checkTermLocked(ctx)
}

// checkTermLocked checks admission while the caller holds termMu, so a
// heartbeat's stamp and queue submission cannot interleave with term reset.
func (s *Server) checkTermLocked(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if fence, ok := ctx.Value(leadershipContextKey{}).(leadershipFence); ok {
		if fence.term != nil && fence.term.Err() != nil {
			return fence.term.Err()
		}
		if fence.epoch != s.controlEpoch {
			return context.Canceled
		}
	}
	return nil
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
	nodeIDs := make([]uuid.UUID, 0, len(nodeSummaries))
	for _, summary := range nodeSummaries {
		nodeIDs = append(nodeIDs, summary.ID)
		// A node is unhealthy until it heartbeats to this leader; heartbeat
		// observations are not replicated. The next reconciliation pass
		// derives its status from liveness.
		status := NodeStatusUnhealthy
		if summary.Draining {
			status = NodeStatusDraining
		}
		runsWorkloads := true
		if summary.RunsWorkloads != nil {
			runsWorkloads = *summary.RunsWorkloads
		}
		nodes[summary.ID] = &Node{ID: summary.ID, Host: summary.Host, Port: summary.Port, CPUCapacity: summary.CPUCapacity, MemoryCapacity: summary.MemoryCapacity, CPUAllocatable: summary.CPUAllocatable, MemoryAllocatable: summary.MemoryAllocatable, OS: summary.OS, Arch: summary.Arch, Labels: summary.Labels, Volumes: summary.Volumes, Capabilities: summary.Capabilities, Status: status, WireGuardPublicKey: summary.WireGuardPublicKey, WireGuardEndpoint: summary.WireGuardEndpoint, WireGuardPortBase: summary.WireGuardPortBase, WireGuardPortCount: summary.WireGuardPortCount, Version: summary.Version, RunsWorkloads: new(runsWorkloads)}
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
	s.liveness.track(nodeIDs)
	return nil
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

// TokenManager returns the API token manager.
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
