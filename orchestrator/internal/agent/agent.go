// Package agent executes and monitors allocations on a Trellis node.
package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/internal/client"
	"github.com/overfold/trellis/orchestrator/internal/health"
	"github.com/overfold/trellis/orchestrator/internal/network"
	"github.com/overfold/trellis/orchestrator/internal/nodecapacity"
	"github.com/overfold/trellis/orchestrator/internal/runtime"
	"github.com/overfold/trellis/orchestrator/internal/spec"
	"github.com/overfold/trellis/orchestrator/internal/storage"
	"github.com/overfold/trellis/orchestrator/internal/transport"
)

// Agent manages allocation lifecycle on a node.
type Agent struct {
	nodeID       uuid.UUID
	allocations  map[string]*Allocation
	retainedLogs map[string]*retainedTaskLog
	execSessions map[string]*execSession
	// execSessionCount includes sessions being started and sessions whose
	// process has not yet exited as well as sessions in execSessions, so a
	// failed kill cannot free admission capacity.
	execSessionCount         int
	execSessionsByAllocation map[string]int
	// logStreams bounds followed log streams; each holds a goroutine, a pipe,
	// and an open log file for as long as its client keeps reading.
	logStreams *transport.StreamLimiter
	// execSessionsClosed refuses new exec sessions once the agent shuts down.
	execSessionsClosed bool
	execTiming         execTiming
	healthProbe        string

	log *slog.Logger

	runtime     runtime.ContainerRuntime
	health      *health.HealthManager
	reconciler  *AllocationReconciler
	ports       *PortManager
	volumes     *VolumeManager
	network     network.Manager
	server      *client.ServerClient
	nodeInfo    client.NodeInfo
	dnsServers  []string
	pidsLimit   int64
	logLimit    int64
	local       *storage.LocalStorage
	cluster     string
	version     string
	epoch       uint64
	mu          sync.RWMutex
	operationMu sync.Mutex
	operations  map[string]*allocationOperation
	// orphanDetaches reserves task-record IDs until cleanup really returns,
	// even if its caller stops waiting. Guarded by mu, like task registration.
	orphanDetaches map[string]bool
	// starts holds accepted allocation starts running in the background, and
	// failed ones until the control plane retries; guarded by mu.
	starts map[string]*groupStart
	// stoppedGenerations fences starts independently of task and log retention.
	stoppedGenerations map[string]uint64
	// lifetime bounds background starts; Init sets it.
	lifetime context.Context

	// planOperation serializes plan application without holding mu across I/O.
	// It is initialized under operationMu.
	planOperation chan struct{}
	// Topology updates are authoritative; guarded by mu. Delayed starts only
	// bootstrap a path, and successful cleanup forgets an idle namespace.
	networkPlans map[string]network.Plan
	readiness    func() bool

	recoveryListPending bool
	// recoveryStops holds interrupted cleanup that recovery retries locally.
	recoveryStops map[string]string
	// recoveryErr stops recovery once a late listing finds a container
	// without an allocation record; failed delivers it to the caller of Init.
	recoveryErr  error
	failed       chan error
	secretMu     sync.Mutex
	secretBase   string
	secretRoot   string
	secretStatfs func(string, *syscall.Statfs_t) error

	// raftAppliedIndex reports the local Raft applied index in heartbeats so
	// the leader promotes only caught-up members to voters.
	raftAppliedIndex func() uint64
}

type allocationOperation struct {
	mu   sync.Mutex
	refs int
}

func (a *Agent) lockAllocationOperation(allocationID string) func() {
	a.operationMu.Lock()
	if a.operations == nil {
		a.operations = make(map[string]*allocationOperation)
	}
	operation := a.operations[allocationID]
	if operation == nil {
		operation = &allocationOperation{}
		a.operations[allocationID] = operation
	}
	operation.refs++
	a.operationMu.Unlock()
	operation.mu.Lock()
	return func() {
		a.operationMu.Lock()
		operation.mu.Unlock()
		operation.refs--
		if operation.refs == 0 {
			delete(a.operations, allocationID)
		}
		a.operationMu.Unlock()
	}
}

var (
	// ErrAllocationNotFound indicates that an allocation does not exist.
	ErrAllocationNotFound = errors.New("allocation not found")
	// ErrAllocationExists indicates that an allocation already exists.
	ErrAllocationExists = errors.New("allocation already exists")
	// ErrStaleEpoch indicates that an operation used an old leadership epoch.
	ErrStaleEpoch = errors.New("stale control-plane epoch")
	// ErrStaleGeneration indicates that an operation used an old allocation generation.
	ErrStaleGeneration = errors.New("stale allocation generation")
	// ErrInvalidEpoch indicates that a mutating operation omitted its leadership epoch.
	ErrInvalidEpoch = errors.New("epoch must be greater than zero")
	// ErrInvalidGeneration indicates that an allocation mutation omitted its generation.
	ErrInvalidGeneration = errors.New("generation must be greater than zero")
	// ErrExecutionConflict indicates conflicting allocation execution metadata.
	ErrExecutionConflict = errors.New("allocation execution metadata conflict")
	// ErrLogStreamLimit indicates that followed log stream admission is full.
	ErrLogStreamLimit = errors.New("log stream limit reached")
	// ErrExecSessionLimit indicates that exec session admission is full.
	ErrExecSessionLimit = errors.New("exec session limit reached")
	// ErrAgentShuttingDown indicates that the agent refuses new work while it shuts down.
	ErrAgentShuttingDown = errors.New("agent is shutting down")
	// ErrExecTaskRequired indicates that an exec request must name one of several running tasks.
	ErrExecTaskRequired = errors.New("exec task selection required")
	// ErrRestartBudgetExhausted indicates that an allocation generation failed
	// terminally after exhausting its restart policy.
	ErrRestartBudgetExhausted = errors.New("restart budget exhausted")
)

// ConfigureDurability enables persistent agent state.
func (a *Agent) ConfigureDurability(local *storage.LocalStorage, cluster string) {
	a.local, a.cluster = local, cluster
}

// NewAgent creates an allocation agent.
func NewAgent(log *slog.Logger, runtime runtime.ContainerRuntime, health *health.HealthManager, reconciler *AllocationReconciler, ports *PortManager, volumes *VolumeManager, server *client.ServerClient, nodeID uuid.UUID) *Agent {
	executable, _ := os.Executable()
	agent := &Agent{
		nodeID:                   nodeID,
		allocations:              make(map[string]*Allocation),
		retainedLogs:             make(map[string]*retainedTaskLog),
		execSessions:             make(map[string]*execSession),
		execSessionsByAllocation: make(map[string]int),
		logStreams:               transport.NewStreamLimiter(logFollowGlobalLimit, logFollowPerAllocationLimit),
		execTiming:               defaultExecTiming,
		healthProbe:              filepath.Join(filepath.Dir(executable), "trellis-health-probe"),
		operations:               make(map[string]*allocationOperation),
		starts:                   make(map[string]*groupStart),

		log: log,

		runtime:    runtime,
		health:     health,
		reconciler: reconciler,
		ports:      ports,
		volumes:    volumes,
		server:     server,
		nodeInfo:   client.NodeInfo{ID: nodeID, Host: "127.0.0.1", Port: 8127, RunsWorkloads: true},
		failed:     make(chan error, 1),
	}

	return agent
}

// SetNetworkManager configures allocation networking. Every node has one; it
// must be set before the agent starts or stops allocations.
func (a *Agent) SetNetworkManager(manager network.Manager) {
	a.network = manager
}

// SetWireGuardIdentity configures the node WireGuard identity and namespace port range.
func (a *Agent) SetWireGuardIdentity(publicKey, endpoint string, portBase, portCount int) {
	a.nodeInfo.WireGuardPublicKey, a.nodeInfo.WireGuardEndpoint = publicKey, endpoint
	a.nodeInfo.WireGuardPortBase, a.nodeInfo.WireGuardPortCount = portBase, portCount
}

// DefaultTaskPidsLimit bounds the processes and threads in each task container
// when the operator does not configure a limit.
const DefaultTaskPidsLimit int64 = 4096

// MaxTaskPidsLimit is the kernel's upper bound on PID values (PID_MAX_LIMIT).
const MaxTaskPidsLimit int64 = 1 << 22

// ValidateTaskPidsLimit checks a node's per-task process limit.
func ValidateTaskPidsLimit(limit int64) error {
	if limit < 1 || limit > MaxTaskPidsLimit {
		return fmt.Errorf("task pids limit must be between 1 and %d", MaxTaskPidsLimit)
	}
	return nil
}

// SetTaskPidsLimit configures the process limit applied to every task
// container this node creates. Existing containers keep their limit.
func (a *Agent) SetTaskPidsLimit(limit int64) error {
	if err := ValidateTaskPidsLimit(limit); err != nil {
		return err
	}
	a.pidsLimit = limit
	return nil
}

func (a *Agent) taskPidsLimit() int64 {
	if a.pidsLimit == 0 {
		return DefaultTaskPidsLimit
	}
	return a.pidsLimit
}

// DefaultTaskLogLimit bounds the disk each task log consumes when the
// operator does not configure a limit.
const DefaultTaskLogLimit int64 = 64 << 20

// MinTaskLogLimit keeps a configured log limit large enough to hold useful
// diagnostics.
const MinTaskLogLimit int64 = 1 << 20

// ValidateTaskLogLimit checks a node's per-task log size limit.
func ValidateTaskLogLimit(limit int64) error {
	if limit < MinTaskLogLimit {
		return fmt.Errorf("task log limit must be at least %d bytes (1MiB)", MinTaskLogLimit)
	}
	return nil
}

// SetTaskLogLimit configures the approximate number of bytes each task log
// on this node keeps. It applies to running tasks and retained logs alike.
func (a *Agent) SetTaskLogLimit(limit int64) error {
	if err := ValidateTaskLogLimit(limit); err != nil {
		return err
	}
	a.logLimit = limit
	return nil
}

func (a *Agent) taskLogLimit() int64 {
	if a.logLimit == 0 {
		return DefaultTaskLogLimit
	}
	return a.logLimit
}

// SetDNSServers configures allocation DNS servers.
func (a *Agent) SetDNSServers(servers []string) {
	a.dnsServers = servers
}

// SetAdvertiseAddress configures the agent endpoint.
func (a *Agent) SetAdvertiseAddress(host string, port int) {
	a.nodeInfo.Host = host
	a.nodeInfo.Port = port
}

// SetResources configures physical and schedulable node resources and platform attributes.
func (a *Agent) SetResources(cpu int, memory int64, osName, arch string) error {
	allocatableCPU, allocatableMemory, err := nodecapacity.Resolve(cpu, memory)
	if err != nil {
		return err
	}
	a.nodeInfo.CPUCapacity, a.nodeInfo.MemoryCapacity = cpu, memory
	a.nodeInfo.CPUAllocatable, a.nodeInfo.MemoryAllocatable = allocatableCPU, allocatableMemory
	a.nodeInfo.OS, a.nodeInfo.Arch = osName, arch
	return nil
}

// SetCapabilities configures the features this node has verified locally.
func (a *Agent) SetCapabilities(capabilities []spec.NodeCapability) {
	a.nodeInfo.Capabilities = append([]spec.NodeCapability(nil), capabilities...)
}

// SetLabels configures node scheduling labels.
func (a *Agent) SetLabels(labels map[string]string) {
	a.nodeInfo.Labels = labels
}

// SetVersion configures the reported agent version.
func (a *Agent) SetVersion(version string) { a.version = version }

// SetRunsWorkloads controls whether the scheduler may place allocations here.
func (a *Agent) SetRunsWorkloads(runs bool) { a.nodeInfo.RunsWorkloads = runs }

// SetRaftAppliedIndex configures how heartbeats read the local Raft applied
// index.
func (a *Agent) SetRaftAppliedIndex(applied func() uint64) { a.raftAppliedIndex = applied }

// Init restores durable allocations and starts reconciliation. A runtime
// listing failure does not fail Init: recorded allocations are kept unobserved
// and the recovery retry lists again. Failed reports a later recovery failure.
func (a *Agent) Init(ctx context.Context) error {
	a.lifetime = ctx
	a.health.Subscriber = a
	a.health.SetContext(ctx)
	a.reconciler.Subscriber = a
	if err := a.recover(ctx); err != nil {
		return fmt.Errorf("recover allocations: %w", err)
	} else if !a.recoveryListPending {
		// Ownership is only known once recovery has adopted every allocation;
		// otherwise the recovery retry runs the sweep when it completes.
		a.removeOrphanedResources(ctx)
	}

	go a.runRecoveryRetry(ctx)
	go a.runHeartbeatLoop(ctx)
	go a.runLogLimitLoop(ctx)
	go a.reconciler.Run(ctx)
	return nil
}

// SetReadiness installs a node prerequisite before Init starts heartbeats.
func (a *Agent) SetReadiness(ready func() bool) { a.readiness = ready }

func (a *Agent) ready() bool { return a.readiness == nil || a.readiness() }
