// Package agent executes and monitors allocations on a Trellis node.
package agent

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/netip"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/client"
	"github.com/clofour/trellis/internal/health"
	"github.com/clofour/trellis/internal/lifecycle"
	"github.com/clofour/trellis/internal/network"
	"github.com/clofour/trellis/internal/nodecapacity"
	"github.com/clofour/trellis/internal/runtime"
	"github.com/clofour/trellis/internal/spec"
	"github.com/clofour/trellis/internal/storage"
	"github.com/containerd/errdefs"
	"github.com/google/uuid"
)

// Agent manages allocation lifecycle on a node.
type Agent struct {
	nodeID       uuid.UUID
	allocations  map[string]*Allocation
	execSessions map[string]*execSession
	// execSessionsClosed refuses new exec sessions once the agent shuts down.
	execSessionsClosed bool
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
	local       *storage.LocalStorage
	cluster     string
	version     string
	epoch       uint64
	mu          sync.RWMutex
	operationMu sync.Mutex
	operations  map[string]*allocationOperation

	recoveryListPending bool
	// supersededStops holds retained older generations that recovery must
	// stop itself because the control plane rejects their stops as stale.
	supersededStops map[string]string
	// newestListed holds the newest generation per scheduler allocation seen
	// by the latest recovery relist, so an older generation counts as
	// superseded even when the newer one could not be adopted yet.
	newestListed map[string]uint64
	secretMu     sync.Mutex
	secretBase   string
	secretRoot   string
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

// Allocation contains agent-local allocation state.
type Allocation struct {
	ID               string
	AllocationID     string
	Generation       uint64
	JobRevision      int
	ExecutionHash    string
	Restart          *spec.RestartPolicySpec
	RestartAttempts  int
	RestartWindow    time.Time
	RestartExhausted bool
	Namespace        string

	JobName   string
	GroupName string
	TaskName  string
	Spec      *spec.TaskSpec

	ContainerID                  string
	ContainerOwnershipUnverified bool
	Ports                        []*runtime.Port
	Mounts                       []*runtime.Mount
	SecretDir                    string
	Network                      *network.Attachment
	Status                       string
	Health                       string
	Draining                     bool

	DrainSequence uint64

	// unobserved marks a recovered allocation whose container state has not
	// been read since the agent restarted. Its health is reported as unknown
	// while the recorded value is kept for the next observation.
	unobserved bool
}

const heartbeatInterval = 10 * time.Second

const (
	recoveryRetryMinDelay = time.Second
	recoveryRetryMaxDelay = 30 * time.Second
)

// reportedHealth is the health an allocation reports; recovered allocations
// whose container has not been observed report unknown.
func reportedHealth(allocation *Allocation) string {
	if allocation.unobserved {
		return "unknown"
	}
	return allocation.Health
}

func allocationNetworkAddress(allocation *Allocation) string {
	if allocation == nil || allocation.Network == nil {
		return ""
	}
	address := allocation.Network.Address
	if host, _, ok := strings.Cut(address, "/"); ok {
		return host
	}
	return address
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
	// ErrExecSessionNotFound indicates that an interactive exec session does not exist.
	ErrExecSessionNotFound = errors.New("exec session not found")
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

// AcceptEpoch validates and persists a leadership epoch.
func (a *Agent) AcceptEpoch(epoch uint64) error {
	if epoch == 0 {
		return ErrInvalidEpoch
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if epoch < a.epoch {
		return fmt.Errorf("%w: received %d, highest accepted %d", ErrStaleEpoch, epoch, a.epoch)
	}
	if epoch == a.epoch {
		return nil
	}
	if a.local != nil {
		if err := a.local.Put("agent/control-epoch", epoch); err != nil {
			return fmt.Errorf("persist control-plane epoch: %w", err)
		}
	}
	a.epoch = epoch
	return nil
}

// allocationFileName encodes an allocation ID as one safe path element. Record
// and secret directory names share it so the startup sweep can match them.
func allocationFileName(id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(id))
}

func allocationRecordKey(id string) string {
	return "agent/allocations/" + allocationFileName(id)
}

func (a *Agent) persistAllocation(allocation *Allocation) error {
	if a.local == nil {
		return nil
	}
	return a.local.Put(allocationRecordKey(allocation.ID), allocation)
}

func (a *Agent) deleteAllocationRecord(id string) error {
	if a.local == nil {
		return nil
	}
	return a.local.Delete(allocationRecordKey(id))
}

func (a *Agent) markAllocationStopping(id string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	allocation := a.allocations[id]
	if allocation == nil {
		return fmt.Errorf("%w: %s", ErrAllocationNotFound, id)
	}
	allocation.Status = "stopping"
	if err := a.persistAllocation(allocation); err != nil {
		return fmt.Errorf("persist stopping allocation: %w", err)
	}
	return nil
}

// NewAgent creates an allocation agent.
func NewAgent(log *slog.Logger, runtime runtime.ContainerRuntime, health *health.HealthManager, reconciler *AllocationReconciler, ports *PortManager, volumes *VolumeManager, server *client.ServerClient, nodeID uuid.UUID) *Agent {
	executable, _ := os.Executable()
	agent := &Agent{
		nodeID:       nodeID,
		allocations:  make(map[string]*Allocation),
		execSessions: make(map[string]*execSession),
		healthProbe:  filepath.Join(filepath.Dir(executable), "trellis-health-probe"),
		operations:   make(map[string]*allocationOperation),

		log: log,

		runtime:    runtime,
		health:     health,
		reconciler: reconciler,
		ports:      ports,
		volumes:    volumes,
		network:    network.DisabledManager{},
		server:     server,
		nodeInfo:   client.NodeInfo{ID: nodeID, Host: "127.0.0.1", Port: 8127},
	}

	return agent
}

// SetNetworkManager configures allocation networking.
func (a *Agent) SetNetworkManager(manager network.Manager) {
	if manager != nil {
		a.network = manager
	}
}

// SetWireGuardIdentity configures the node WireGuard identity and namespace port range.
func (a *Agent) SetWireGuardIdentity(publicKey, endpoint string, portBase, portCount int) {
	a.nodeInfo.WireGuardPublicKey, a.nodeInfo.WireGuardEndpoint = publicKey, endpoint
	a.nodeInfo.WireGuardPortBase, a.nodeInfo.WireGuardPortCount = portBase, portCount
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

// Init restores durable allocations and starts reconciliation.
func (a *Agent) Init(ctx context.Context) error {
	a.health.Subscriber = a
	a.health.SetContext(ctx)
	a.reconciler.Subscriber = a
	if err := a.recover(ctx); err != nil {
		return fmt.Errorf("recover allocations: %w", err)
	} else if !a.recoveryListPending {
		// Ownership is only known once recovery has adopted every allocation;
		// otherwise the recovery retry runs the sweep when it completes.
		a.removeOrphanedSecretDirs()
	}

	go a.runRecoveryRetry(ctx)
	go a.runHeartbeatLoop(ctx)
	go a.reconciler.Run(ctx)
	go a.runExecSessionReaper(ctx)
	return nil
}

func (a *Agent) recover(ctx context.Context) error {
	if a.local == nil {
		a.cleanupVolumeStaging(nil)
		return nil
	}
	var epoch uint64
	epochErr := a.local.Get("agent/control-epoch", &epoch)
	if epochErr != nil && !errors.Is(epochErr, os.ErrNotExist) {
		return fmt.Errorf("read control-plane epoch: %w", epochErr)
	}
	records, recordErrs := a.local.ListRaw("agent/allocations")
	if err := errors.Join(recordErrs...); err != nil {
		return fmt.Errorf("read allocation recovery records: %w", err)
	}
	stored := make(map[string]*Allocation, len(records))
	for name, raw := range records {
		var allocation Allocation
		if err := json.Unmarshal(raw, &allocation); err != nil {
			return fmt.Errorf("decode allocation recovery record %s: %w", name, err)
		}
		if allocation.ID == "" || allocation.ContainerID == "" || allocation.AllocationID == "" {
			return fmt.Errorf("decode allocation recovery record %s: allocation identity is required", name)
		}
		expectedName := base64.RawURLEncoding.EncodeToString([]byte(allocation.ID))
		if name != expectedName {
			return fmt.Errorf("decode allocation recovery record %s: record name does not match allocation ID %q", name, allocation.ID)
		}
		if _, exists := stored[allocation.ContainerID]; exists {
			return fmt.Errorf("decode allocation recovery record %s: duplicate container ID %q", name, allocation.ContainerID)
		}
		stored[allocation.ContainerID] = &allocation
	}
	managed, ok := a.runtime.(runtime.ManagedRuntime)
	if !ok {
		if len(records) != 0 {
			return fmt.Errorf("runtime cannot recover existing allocation records")
		}
		if epochErr != nil {
			if err := a.local.Put("agent/control-epoch", epoch); err != nil {
				return fmt.Errorf("initialize control-plane epoch: %w", err)
			}
		}
		a.epoch = epoch
		a.cleanupVolumeStaging(nil)
		return nil
	}
	containers, err := managed.ListManaged(ctx, a.cluster)
	if err != nil {
		// A failed listing proves nothing about any container. Keep every
		// record and its resources until the runtime can be observed again.
		a.mu.Lock()
		a.recoveryListPending = true
		a.mu.Unlock()
		for containerID, allocation := range stored {
			if containerID != "" {
				a.recoverUnobserved(allocation, false)
			}
		}
		return fmt.Errorf("list managed containers: %w", err)
	}
	if epochErr != nil {
		if len(records) != 0 || len(containers) != 0 {
			return fmt.Errorf("control-plane epoch is missing while recoverable allocation state exists")
		}
		if err := a.local.Put("agent/control-epoch", epoch); err != nil {
			return fmt.Errorf("initialize control-plane epoch: %w", err)
		}
	}
	for _, container := range containers {
		if stored[container.ID] == nil {
			return fmt.Errorf("managed runtime container %q has no durable allocation record", container.ID)
		}
	}
	a.epoch = epoch
	// Existing containers still reference their staging mounts as OCI mount
	// sources; keep those so a later restart can create a new task.
	liveContainers := make([]string, 0, len(containers))
	for _, container := range containers {
		liveContainers = append(liveContainers, container.ID)
	}
	a.cleanupVolumeStaging(liveContainers)
	seen := make(map[string]bool, len(containers))
	for _, container := range containers {
		seen[container.ID] = true
		a.recoverContainer(container, stored[container.ID])
	}
	a.mu.Lock()
	a.recoveryListPending = a.hasUnreadableUnknownLocked(containers)
	a.mu.Unlock()
	for containerID, allocation := range stored {
		if containerID == "" || seen[containerID] {
			continue
		}
		a.recoverMissing(ctx, allocation)
	}
	a.queueSupersededStops()
	return nil
}

// queueSupersededStops rebuilds the set of retained stopping records whose
// newer generation is known. The control plane rejects their stops as stale,
// so recovery finishes them; a failed stop survives a restart only as its
// stopping record.
func (a *Agent) queueSupersededStops() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.supersededStops = nil
	for id, allocation := range a.allocations {
		if a.pendingSupersededStopLocked(allocation) {
			if a.supersededStops == nil {
				a.supersededStops = make(map[string]string)
			}
			a.supersededStops[id] = allocation.AllocationID
		}
	}
}

func (a *Agent) pendingSupersededStopLocked(allocation *Allocation) bool {
	// Unverified ownership keeps its existing retained-record handling.
	return allocation.Status == "stopping" && !allocation.unobserved && !allocation.ContainerOwnershipUnverified && a.supersededLocked(allocation)
}

// supersededLocked reports whether a newer generation of the allocation is
// known or was listed by recovery.
func (a *Agent) supersededLocked(allocation *Allocation) bool {
	if a.newestListed[allocation.AllocationID] > allocation.Generation {
		return true
	}
	for _, known := range a.allocations {
		if known.AllocationID == allocation.AllocationID && known.Generation > allocation.Generation {
			return true
		}
	}
	return false
}

// recoverContainer restores one observed container. allocation is its durable
// record, or nil when the container is known only from runtime labels.
func (a *Agent) recoverContainer(container runtime.ContainerInfo, allocation *Allocation) {
	hadRecord := allocation != nil
	if allocation == nil {
		allocation = allocationFromRuntime(container)
		if allocation != nil {
			allocation.SecretDir = a.recoveredSecretDir(allocation.ID)
		}
	}
	if allocation == nil {
		a.log.Warn("leave unidentifiable Trellis container untouched", "container", container.ID)
		return
	}
	if !observedStatus(container.Status) {
		a.recoverUnobserved(allocation, !hadRecord)
		return
	}
	if allocation.unobserved {
		// Replace the restart suppression applied while state was unknown.
		_ = a.reconciler.Untrack(allocation.ID)
		allocation.unobserved = false
	}
	if hadRecord && allocation.ContainerOwnershipUnverified && !a.containerMatchesAllocation(container, allocation) {
		allocation.Status = "stopping"
		a.adoptPorts(allocation)
		a.mu.Lock()
		a.allocations[allocation.ID] = allocation
		a.mu.Unlock()
		return
	}
	stopping := hadRecord && allocation.Status == "stopping"
	restartSuppressed := stopping || allocation.Draining
	// An exhausted restart budget is terminal for this generation: keep
	// reporting the failed observation instead of asking for a new start.
	notRunning := !stopping && (container.Status == runtime.StatusCreated || container.Status == runtime.StatusStopped)
	exhausted := notRunning && allocation.RestartExhausted
	recoveryPending := notRunning && !exhausted
	if exhausted {
		allocation.Status = "failed"
		allocation.Health = "unhealthy"
	} else if recoveryPending {
		// Recovery reports observation; it does not invent desired state.
		// A non-running recovered task stays restart-suppressed until the
		// control plane observes "starting" and reconciliation reissues the
		// appropriate start or stop action.
		allocation.Status = "starting"
		allocation.Health = "unknown"
	} else if !stopping && container.Status == runtime.StatusRunning {
		allocation.Status = "running"
		if allocation.Spec != nil && allocation.Spec.HealthCheck != nil {
			allocation.Health = "unknown"
		}
	}
	a.adoptPorts(allocation)
	// Persist the initial observation before a probe can publish a result.
	a.mu.Lock()
	a.allocations[allocation.ID] = allocation
	persistErr := a.persistAllocation(allocation)
	a.mu.Unlock()
	if persistErr != nil {
		a.log.Error("refresh recovered allocation record", "allocation", allocation.AllocationID, "error", persistErr)
	}
	healthManaged := allocation.Spec != nil && allocation.Spec.HealthCheck != nil
	if exhausted {
		a.reconciler.TrackFailed(allocation.ID, healthManaged, allocation.Restart, allocation.RestartAttempts, allocation.RestartWindow)
	} else if restartSuppressed {
		a.reconciler.TrackStopping(allocation.ID, healthManaged, allocation.Restart, allocation.RestartAttempts, allocation.RestartWindow, allocation.RestartExhausted)
	} else if !recoveryPending {
		a.reconciler.TrackRecovered(allocation.ID, healthManaged, allocation.Restart, allocation.RestartAttempts, allocation.RestartWindow, allocation.RestartExhausted)
	}
	if healthManaged && !stopping && !recoveryPending && !exhausted {
		a.health.RegisterTask(allocation.ID, allocation.ContainerID, allocation.Spec.HealthCheck)
	}
}

// recoverUnobserved keeps an allocation whose container state could not be
// read. Unverified state preserves the record, its resources, and its last
// recorded phase, reported with unknown health; local restarts stay suppressed until a
// later observation classifies the container. An existing record is left
// unchanged; persist records an allocation known only from runtime labels.
func (a *Agent) recoverUnobserved(allocation *Allocation, persist bool) {
	allocation.unobserved = true
	a.adoptPorts(allocation)
	a.mu.Lock()
	a.allocations[allocation.ID] = allocation
	var persistErr error
	if persist {
		persistErr = a.persistAllocation(allocation)
	}
	a.mu.Unlock()
	if persistErr != nil {
		a.log.Error("record unobserved allocation", "allocation", allocation.AllocationID, "error", persistErr)
	}
	a.reconciler.TrackStopping(allocation.ID, false, allocation.Restart, allocation.RestartAttempts, allocation.RestartWindow, allocation.RestartExhausted)
	a.log.Warn("container state unavailable during recovery; preserving allocation", "allocation", allocation.AllocationID, "container", allocation.ContainerID)
}

// recoverMissing cleans up a recorded allocation whose container is absent
// from a successful runtime listing.
func (a *Agent) recoverMissing(ctx context.Context, allocation *Allocation) {
	adopted := allocation.unobserved
	allocation.unobserved = false
	if allocation.ContainerOwnershipUnverified {
		// ListManaged is cluster-filtered, so absence does not prove that
		// an ambiguous Create left no container with this ID. Use the same
		// ownership verification as a live cleanup retry.
		allocation.Status = "stopping"
		a.adoptPorts(allocation)
		a.mu.Lock()
		a.allocations[allocation.ID] = allocation
		a.mu.Unlock()
		if err := a.stopAllocation(context.WithoutCancel(ctx), allocation.ID); err != nil {
			a.log.Error("recover unverified allocation container", "allocation", allocation.AllocationID, "error", err)
		}
		return
	}
	var cleanupErr error
	if err := a.network.Detach(context.WithoutCancel(ctx), allocation.Network); err != nil {
		cleanupErr = fmt.Errorf("detach network for missing allocation container: %w", err)
	} else if allocation.SecretDir != "" {
		if err := removeSecretDir(allocation.SecretDir); err != nil {
			cleanupErr = fmt.Errorf("remove secret files for missing allocation container: %w", err)
		}
	}
	cleanupErr = errors.Join(cleanupErr, a.volumes.ReleaseStaging(allocation.ID))
	if cleanupErr == nil {
		if err := a.deleteAllocationRecord(allocation.ID); err != nil {
			cleanupErr = fmt.Errorf("delete missing allocation record: %w", err)
		}
	}
	if cleanupErr != nil {
		a.log.Error("recover missing allocation container", "allocation", allocation.AllocationID, "error", cleanupErr)
		allocation.Status = "stopping"
		a.adoptPorts(allocation)
		a.mu.Lock()
		a.allocations[allocation.ID] = allocation
		a.mu.Unlock()
		return
	}
	a.closeExecSessionsForTask(ctx, allocation.ID, allocation.ContainerID)
	a.health.DeregisterTask(allocation.ID)
	_ = a.reconciler.Untrack(allocation.ID)
	a.mu.Lock()
	delete(a.allocations, allocation.ID)
	a.mu.Unlock()
	if adopted {
		for _, port := range allocation.Ports {
			if !a.hostPortInUse(port) {
				_ = a.ports.Release(port)
			}
		}
	}
}

// hostPortInUse reports whether a remaining allocation also holds the port.
func (a *Agent) hostPortInUse(port *runtime.Port) bool {
	if port == nil {
		return false
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	for _, allocation := range a.allocations {
		for _, held := range allocation.Ports {
			if held != nil && held.HostPort == port.HostPort {
				return true
			}
		}
	}
	return false
}

func (a *Agent) adoptPorts(allocation *Allocation) {
	for _, port := range allocation.Ports {
		if err := a.ports.Adopt(port); err != nil {
			a.log.Error("recover port claim", "allocation", allocation.AllocationID, "error", err)
		}
	}
}

// retryRecovery re-observes allocations that recovery could not classify and,
// after a failed initial listing, adopts containers known only from runtime
// labels. It reports whether any recovery work remains.
func (a *Agent) retryRecovery(ctx context.Context) bool {
	managed, ok := a.runtime.(runtime.ManagedRuntime)
	if !ok {
		return false
	}
	a.mu.RLock()
	listPending := a.recoveryListPending
	pending := make(map[string]*Allocation)
	for id, allocation := range a.allocations {
		if allocation.unobserved {
			pending[id] = &Allocation{AllocationID: allocation.AllocationID, ContainerID: allocation.ContainerID}
		}
	}
	a.mu.RUnlock()
	if listPending || len(pending) > 0 {
		a.relistRecovery(ctx, managed, listPending, pending)
	}
	a.queueSupersededStops()
	a.mu.RLock()
	superseded := make(map[string]string, len(a.supersededStops))
	for id, allocationID := range a.supersededStops {
		superseded[id] = allocationID
	}
	a.mu.RUnlock()
	for id, allocationID := range superseded {
		a.stopSuperseded(ctx, id, allocationID)
	}
	a.queueSupersededStops()
	return a.recoveryPending()
}

// relistRecovery lists containers again to classify unobserved allocations
// and, while listing is incomplete, adopt unrecorded containers.
func (a *Agent) relistRecovery(ctx context.Context, managed runtime.ManagedRuntime, listPending bool, pending map[string]*Allocation) {
	containers, err := managed.ListManaged(ctx, a.cluster)
	if err != nil {
		a.log.Warn("retry allocation recovery", "error", err)
		return
	}
	listed := make(map[string]runtime.ContainerInfo, len(containers))
	for _, container := range containers {
		listed[container.ID] = container
	}
	for id, allocation := range pending {
		container, found := listed[allocation.ContainerID]
		a.reobserve(ctx, id, allocation.AllocationID, container, found)
	}
	if listPending {
		// Adopt newer generations first so an older one is recognised as
		// superseded whatever order the runtime lists them in.
		adoptable := make([]runtime.ContainerInfo, 0, len(containers))
		generations := make(map[string]uint64, len(containers))
		newestListed := make(map[string]uint64)
		for _, container := range containers {
			if allocation := allocationFromRuntime(container); allocation != nil {
				adoptable = append(adoptable, container)
				generations[container.ID] = allocation.Generation
				newestListed[allocation.AllocationID] = max(newestListed[allocation.AllocationID], allocation.Generation)
			}
		}
		a.mu.Lock()
		a.newestListed = newestListed
		a.mu.Unlock()
		sort.SliceStable(adoptable, func(i, j int) bool {
			return generations[adoptable[i].ID] > generations[adoptable[j].ID]
		})
		stillPending := false
		for _, container := range adoptable {
			if !a.adoptUnrecorded(ctx, container) {
				stillPending = true
			}
		}
		a.mu.Lock()
		a.recoveryListPending = stillPending || a.hasUnreadableUnknownLocked(containers)
		// Listing completes at most once, and Init skipped the orphaned
		// secret sweep while it was incomplete.
		sweep := !a.recoveryListPending
		a.mu.Unlock()
		if sweep {
			// Every listed container is now recorded or retained, so secret
			// directories without an owner are orphans, as at startup.
			a.removeOrphanedSecretDirs()
		}
	}
}

// recoveryPending reports whether recovery still has containers to observe.
func (a *Agent) recoveryPending() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.recoveryListPending || len(a.supersededStops) > 0 {
		return true
	}
	for _, allocation := range a.allocations {
		if allocation.unobserved {
			return true
		}
	}
	return false
}

// adoptUnrecorded recovers a labelled container that has no allocation record.
// The listing predates the operation lock, so the container is re-inspected to
// avoid resurrecting an allocation stopped in the meantime. It reports false
// when the container must be listed again.
func (a *Agent) adoptUnrecorded(ctx context.Context, listed runtime.ContainerInfo) bool {
	a.mu.RLock()
	_, known := a.allocations[listed.ID]
	a.mu.RUnlock()
	if known {
		return true
	}
	unlock := a.lockAllocationOperation(listed.Labels["trellis.allocation-id"])
	defer unlock()
	a.mu.RLock()
	_, known = a.allocations[listed.ID]
	a.mu.RUnlock()
	if known {
		return true
	}
	observed, err := a.runtime.Inspect(ctx, listed.ID)
	if errdefs.IsNotFound(err) {
		// The container may have been removed after the listing, or only
		// its task may be missing; the next listing decides.
		return false
	}
	if err != nil {
		// Keep it as unobserved, as initial recovery does, until a later
		// observation classifies it.
		observed = &runtime.ContainerInfo{Status: runtime.StatusUnknown}
	}
	container := *observed
	container.ID = listed.ID
	if container.Labels == nil {
		container.Labels = listed.Labels
	}
	allocation := allocationFromRuntime(container)
	if allocation == nil {
		return true
	}
	a.mu.RLock()
	superseded := a.supersededLocked(allocation)
	a.mu.RUnlock()
	if superseded {
		// A newer generation started while the container was unlisted, so
		// that start could not stop it. Retain it as stopping;
		// queueSupersededStops schedules the stop.
		allocation.Status = "stopping"
		allocation.SecretDir = a.recoveredSecretDir(allocation.ID)
		a.adoptPorts(allocation)
		a.mu.Lock()
		a.allocations[allocation.ID] = allocation
		persistErr := a.persistAllocation(allocation)
		a.mu.Unlock()
		if persistErr != nil {
			a.log.Error("record superseded allocation", "allocation", allocation.AllocationID, "error", persistErr)
		}
		a.reconciler.TrackStopping(allocation.ID, false, nil, 0, time.Time{}, false)
		return true
	}
	a.recoverContainer(container, nil)
	return true
}

// stopSuperseded stops a retained older generation if it still qualifies; a
// start or stop may have replaced or removed it since it was queued.
func (a *Agent) stopSuperseded(ctx context.Context, id, allocationID string) {
	unlock := a.lockAllocationOperation(allocationID)
	defer unlock()
	a.mu.RLock()
	allocation := a.allocations[id]
	qualifies := allocation != nil && a.pendingSupersededStopLocked(allocation)
	a.mu.RUnlock()
	if !qualifies {
		return
	}
	if err := a.stopAllocation(context.WithoutCancel(ctx), id); err != nil {
		a.log.Error("stop superseded allocation", "allocation", allocationID, "error", err)
	}
}

// hasUnreadableUnknownLocked reports whether a listing contained a container
// whose metadata could not be read and that no allocation accounts for. Such a
// container may be an unrecorded Trellis container, so listing is retried.
func (a *Agent) hasUnreadableUnknownLocked(containers []runtime.ContainerInfo) bool {
	for _, container := range containers {
		if _, known := a.allocations[container.ID]; !known && container.Labels == nil && !observedStatus(container.Status) {
			return true
		}
	}
	return false
}

func observedStatus(status runtime.ContainerStatus) bool {
	return status == runtime.StatusRunning || status == runtime.StatusCreated || status == runtime.StatusStopped
}

func (a *Agent) reobserve(ctx context.Context, id, allocationID string, container runtime.ContainerInfo, found bool) {
	unlock := a.lockAllocationOperation(allocationID)
	defer unlock()
	allocation, ok := a.snapshotAllocation(id)
	// A start or stop may have replaced or removed the allocation meanwhile.
	if !ok || !allocation.unobserved {
		return
	}
	if !found {
		a.recoverMissing(ctx, &allocation)
		return
	}
	if !observedStatus(container.Status) {
		return
	}
	a.recoverContainer(container, &allocation)
}

// observeRecovered inspects an unobserved allocation on demand, so a start
// retry neither acknowledges nor replaces a container of unknown state. It
// returns the observed status, or "" when cleanup confirmed the container
// missing and removed the allocation. The caller holds the allocation
// operation lock.
func (a *Agent) observeRecovered(ctx context.Context, allocID string) (string, error) {
	allocation, ok := a.snapshotAllocation(allocID)
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrAllocationNotFound, allocID)
	}
	if !allocation.unobserved {
		return allocation.Status, nil
	}
	observed, err := a.runtime.Inspect(ctx, allocation.ContainerID)
	if errdefs.IsNotFound(err) {
		// Confirm absence with a successful listing, as recovery does.
		if managed, ok := a.runtime.(runtime.ManagedRuntime); ok {
			containers, listErr := managed.ListManaged(ctx, a.cluster)
			if listErr != nil {
				return "", fmt.Errorf("observe recovered allocation %s: %w", allocID, listErr)
			}
			listed := false
			for _, container := range containers {
				if container.ID != allocation.ContainerID {
					continue
				}
				if !observedStatus(container.Status) {
					return "", fmt.Errorf("observe recovered allocation %s: container state is %q", allocID, container.Status)
				}
				listed = true
				a.recoverContainer(container, &allocation)
			}
			if !listed {
				a.recoverMissing(ctx, &allocation)
			}
		}
	} else if err != nil {
		return "", fmt.Errorf("observe recovered allocation %s: %w", allocID, err)
	} else if !observedStatus(observed.Status) {
		return "", fmt.Errorf("observe recovered allocation %s: container state is %q", allocID, observed.Status)
	} else {
		container := *observed
		container.ID = allocation.ContainerID
		a.recoverContainer(container, &allocation)
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	if recovered := a.allocations[allocID]; recovered != nil {
		if recovered.unobserved {
			return "", fmt.Errorf("observe recovered allocation %s: container state is unavailable", allocID)
		}
		return recovered.Status, nil
	}
	return "", nil
}

// snapshotAllocation copies an allocation so recovery can reclassify it
// without mutating state that readers share.
func (a *Agent) snapshotAllocation(id string) (Allocation, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	current := a.allocations[id]
	if current == nil {
		return Allocation{}, false
	}
	allocation := *current
	allocation.Ports = append([]*runtime.Port(nil), current.Ports...)
	allocation.Mounts = append([]*runtime.Mount(nil), current.Mounts...)
	return allocation, true
}

// runRecoveryRetry retries recovery until every recorded allocation has been
// observed.
func (a *Agent) runRecoveryRetry(ctx context.Context) {
	delay := recoveryRetryMinDelay
	for pending := a.recoveryPending(); pending; pending = a.retryRecovery(ctx) {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		delay = min(delay*2, recoveryRetryMaxDelay)
	}
}

func (a *Agent) cleanupVolumeStaging(liveContainers []string) {
	if err := a.volumes.CleanupStaging(liveContainers); err != nil {
		a.log.Error("clean up stale volume staging mounts", "error", err)
	}
}

func allocationFromRuntime(container runtime.ContainerInfo) *Allocation {
	generation, err := strconv.ParseUint(container.Labels["trellis.allocation-generation"], 10, 64)
	if err != nil || generation == 0 {
		return nil
	}
	jobRevision, err := strconv.Atoi(container.Labels["trellis.job-revision"])
	if err != nil || jobRevision <= 0 {
		return nil
	}
	allocationID := container.Labels["trellis.allocation-id"]
	executionHash := container.Labels["trellis.execution-hash"]
	if allocationID == "" || executionHash == "" {
		return nil
	}
	return &Allocation{
		ID: container.ID, ContainerID: container.ID, AllocationID: allocationID,
		Generation: generation, JobRevision: jobRevision, ExecutionHash: executionHash,
		Namespace: container.Labels["trellis.namespace"], JobName: container.Labels["trellis.job"],
		GroupName: container.Labels["trellis.task-group"], TaskName: container.Labels["trellis.task"],
		Status: "running", Health: "unknown",
	}
}

func (a *Agent) containerMatchesAllocation(container runtime.ContainerInfo, allocation *Allocation) bool {
	labels := container.Labels
	return container.ID == allocation.ContainerID &&
		labels["trellis.cluster"] == a.cluster &&
		labels["trellis.allocation-id"] == allocation.AllocationID &&
		labels["trellis.allocation-generation"] == strconv.FormatUint(allocation.Generation, 10) &&
		labels["trellis.job-revision"] == strconv.Itoa(allocation.JobRevision) &&
		labels["trellis.execution-hash"] == allocation.ExecutionHash &&
		labels["trellis.namespace"] == allocation.Namespace &&
		labels["trellis.job"] == allocation.JobName &&
		labels["trellis.task-group"] == allocation.GroupName &&
		labels["trellis.task"] == allocation.TaskName
}

// GetAllocations returns copies of agent allocation state.
func (a *Agent) GetAllocations() []*Allocation {
	a.mu.RLock()
	defer a.mu.RUnlock()
	result := make([]*Allocation, 0, len(a.allocations))
	for _, alloc := range a.allocations {
		allocationCopy := *alloc
		allocationCopy.Ports = append([]*runtime.Port(nil), alloc.Ports...)
		allocationCopy.Mounts = append([]*runtime.Mount(nil), alloc.Mounts...)
		allocationCopy.Health = reportedHealth(alloc)
		result = append(result, &allocationCopy)
	}

	return result
}

// PrepareStart validates and begins an allocation start operation.
func (a *Agent) PrepareStart(ctx context.Context, request *api.AllocationRequest) error {
	unlock := a.lockAllocationOperation(request.AllocationID)
	defer unlock()
	return a.prepareStart(ctx, request)
}

func (a *Agent) prepareStart(ctx context.Context, request *api.AllocationRequest) error {
	if err := a.AcceptEpoch(request.Epoch); err != nil {
		return err
	}
	a.mu.RLock()
	var oldIDs []string
	var exhaustedTask string
	for id, allocation := range a.allocations {
		if allocation.AllocationID != request.AllocationID {
			continue
		}
		if allocation.Generation > request.Generation {
			a.mu.RUnlock()
			return fmt.Errorf("%w: current %d, requested %d", ErrStaleGeneration, allocation.Generation, request.Generation)
		}
		if allocation.Generation == request.Generation && (allocation.JobRevision != request.JobRevision || allocation.ExecutionHash != request.ExecutionHash) {
			a.mu.RUnlock()
			return fmt.Errorf("%w: allocation %s generation %d", ErrExecutionConflict, request.AllocationID, request.Generation)
		}
		if allocation.Generation == request.Generation && allocation.RestartExhausted && (exhaustedTask == "" || allocation.TaskName < exhaustedTask) {
			exhaustedTask = allocation.TaskName
		}
		if allocation.Generation < request.Generation {
			oldIDs = append(oldIDs, id)
		}
	}
	// Reject after the fencing checks, and before touching any task, so a
	// start retry cannot churn the siblings of a task whose restart budget is
	// terminally exhausted. Pick the task deterministically.
	if exhaustedTask != "" {
		a.mu.RUnlock()
		return fmt.Errorf("%w: allocation %s generation %d task %s", ErrRestartBudgetExhausted, request.AllocationID, request.Generation, exhaustedTask)
	}
	a.mu.RUnlock()
	for _, id := range oldIDs {
		if err := a.stopAllocation(ctx, id); err != nil {
			return fmt.Errorf("replace older generation: %w", err)
		}
	}
	return nil
}

// RunGroup serializes and starts every task in one scheduler allocation.
func (a *Agent) RunGroup(ctx context.Context, request *api.AllocationRequest) error {
	unlock := a.lockAllocationOperation(request.AllocationID)
	defer unlock()
	if err := a.prepareStart(ctx, request); err != nil {
		return err
	}
	draining, drainSequence := a.startDrainState(request)
	// Tasks that are already running keep their records, so apply the drain
	// state to them before any task starts. Each start reapplies it so a
	// partially applied state converges. Records that are neither running nor
	// starting are rebuilt by RunAllocation with the same state.
	var err error
	if draining {
		err = a.applyDrain(request.AllocationID, request.Generation, drainSequence)
	} else if drainSequence > 0 {
		err = a.applyResume(request.AllocationID, request.Generation, drainSequence, true)
	}
	if err != nil {
		return err
	}
	for i := range request.Tasks {
		task := &request.Tasks[i]
		id := fmt.Sprintf("%s-g%d-%s", request.AllocationID, request.Generation, task.Name)
		if err := a.RunAllocation(ctx, id, request.AllocationID, request.Generation, request.JobRevision, request.ExecutionHash, request.Namespace, request.JobName, request.GroupName, task.Name, task, request.Runtime, request.NetworkPlan, request.EnvOverrides, request.Secrets, request.Restart, draining, drainSequence); err != nil {
			return err
		}
	}
	return nil
}

// startDrainState combines the drain state carried by a start request with the
// newest drain or resume the agent already applied to that generation. The
// higher sequence wins, so a delayed start cannot roll back a later drain.
func (a *Agent) startDrainState(request *api.AllocationRequest) (bool, uint64) {
	draining, sequence := request.Draining, request.DrainSequence
	a.mu.RLock()
	defer a.mu.RUnlock()
	for _, allocation := range a.allocations {
		if allocation.AllocationID != request.AllocationID || allocation.Generation != request.Generation || allocation.DrainSequence <= request.DrainSequence {
			continue
		}
		// Local records that disagree at one sequence resolve to draining, so
		// the result never depends on map iteration order.
		if allocation.DrainSequence > sequence || (allocation.DrainSequence == sequence && allocation.Draining) {
			draining, sequence = allocation.Draining, allocation.DrainSequence
		}
	}
	return draining, sequence
}

// UpdateNetworkPlan refreshes the network shared by running allocations.
func (a *Agent) UpdateNetworkPlan(ctx context.Context, request *api.NetworkPlanRequest) error {
	if err := a.AcceptEpoch(request.Epoch); err != nil {
		return err
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	if request.Epoch < a.epoch {
		return fmt.Errorf("%w: received %d, highest accepted %d", ErrStaleEpoch, request.Epoch, a.epoch)
	}
	active := false
	var desiredCIDR netip.Prefix
	if request.Plan.CIDR != "" {
		var err error
		desiredCIDR, err = netip.ParsePrefix(request.Plan.CIDR)
		if err != nil {
			return fmt.Errorf("invalid network plan CIDR %q: %w", request.Plan.CIDR, err)
		}
		desiredCIDR = desiredCIDR.Masked()
	}
	for _, allocation := range a.allocations {
		if allocation.Namespace != request.Namespace || allocation.Network == nil {
			continue
		}
		active = true
		if request.Plan.Gateway != "" && allocation.Network.Gateway != "" && request.Plan.Gateway != allocation.Network.Gateway {
			return fmt.Errorf("network plan would change active namespace gateway from %s to %s", allocation.Network.Gateway, request.Plan.Gateway)
		}
		if desiredCIDR.IsValid() && allocation.Network.Address != "" {
			current, err := netip.ParsePrefix(allocation.Network.Address)
			if err != nil {
				return fmt.Errorf("invalid active network address %q: %w", allocation.Network.Address, err)
			}
			if current.Masked() != desiredCIDR {
				return fmt.Errorf("network plan would change active namespace CIDR from %s to %s", current.Masked(), desiredCIDR)
			}
		}
	}
	if !active {
		return nil
	}
	updater, ok := a.network.(network.PlanUpdater)
	if !ok {
		return network.ErrDisabled
	}
	return updater.UpdatePlan(ctx, request.Namespace, request.Plan)
}

// StopGroup stops all tasks in an allocation group.
func (a *Agent) StopGroup(ctx context.Context, request *api.StopAllocationRequest) error {
	if request.Generation == 0 {
		return ErrInvalidGeneration
	}
	unlock := a.lockAllocationOperation(request.AllocationID)
	defer unlock()
	if err := a.AcceptEpoch(request.Epoch); err != nil {
		return err
	}
	a.mu.RLock()
	var ids []string
	for id, allocation := range a.allocations {
		if allocation.AllocationID != request.AllocationID {
			continue
		}
		if allocation.Generation > request.Generation {
			a.mu.RUnlock()
			return fmt.Errorf("%w: current %d, requested %d", ErrStaleGeneration, allocation.Generation, request.Generation)
		}
		if allocation.Generation == request.Generation {
			ids = append(ids, id)
		}
	}
	listPending := a.recoveryListPending
	a.mu.RUnlock()
	var containers []runtime.ContainerInfo
	var listErr error
	if listPending {
		// Recovery has not listed every container, so tasks of this
		// generation may run without a record. Fence against a newer listed
		// generation before stopping anything.
		containers, listErr = a.listUnrecorded(ctx, request)
		if errors.Is(listErr, ErrStaleGeneration) {
			return listErr
		}
	}
	var errs []error
	for _, id := range ids {
		if err := a.stopAllocation(ctx, id); err != nil {
			errs = append(errs, err)
		}
	}
	if listPending {
		if listErr != nil {
			errs = append(errs, listErr)
		} else {
			errs = append(errs, a.stopUnrecorded(ctx, request, containers, ids))
		}
	}
	return errors.Join(errs...)
}

// listUnrecorded lists containers for stopUnrecorded and rejects a stop for a
// generation older than a listed unrecorded one.
func (a *Agent) listUnrecorded(ctx context.Context, request *api.StopAllocationRequest) ([]runtime.ContainerInfo, error) {
	managed, ok := a.runtime.(runtime.ManagedRuntime)
	if !ok {
		return nil, nil
	}
	containers, err := managed.ListManaged(ctx, a.cluster)
	if err != nil {
		return nil, fmt.Errorf("list containers for unrecorded allocation %s: %w", request.AllocationID, err)
	}
	for _, container := range containers {
		if allocation := allocationFromRuntime(container); allocation != nil && allocation.AllocationID == request.AllocationID && allocation.Generation > request.Generation {
			return nil, fmt.Errorf("%w: listed %d, requested %d", ErrStaleGeneration, allocation.Generation, request.Generation)
		}
	}
	return containers, nil
}

// stopUnrecorded stops listed unrecorded containers of an allocation
// generation, and of its older generations, while recovery has not completed a
// listing. handled names the recorded tasks the caller already stopped; the
// listing predates those stops. The caller holds the allocation operation lock.
func (a *Agent) stopUnrecorded(ctx context.Context, request *api.StopAllocationRequest, containers []runtime.ContainerInfo, handled []string) error {
	skip := make(map[string]bool, len(handled))
	for _, id := range handled {
		skip[id] = true
	}
	var errs []error
	a.mu.RLock()
	unidentified := a.hasUnreadableUnknownLocked(containers)
	a.mu.RUnlock()
	if unidentified {
		errs = append(errs, fmt.Errorf("stop allocation %s: an unreadable container may belong to it", request.AllocationID))
	}
	for _, container := range containers {
		allocation := allocationFromRuntime(container)
		// Older unrecorded generations are stopped too, as a start stops
		// known older generations; otherwise they would be adopted later.
		if allocation == nil || allocation.AllocationID != request.AllocationID || allocation.Generation > request.Generation || skip[allocation.ID] {
			continue
		}
		a.mu.RLock()
		_, known := a.allocations[allocation.ID]
		a.mu.RUnlock()
		if known {
			continue
		}
		allocation.Status = "stopping"
		allocation.SecretDir = a.recoveredSecretDir(allocation.ID)
		a.adoptPorts(allocation)
		a.mu.Lock()
		a.allocations[allocation.ID] = allocation
		persistErr := a.persistAllocation(allocation)
		a.mu.Unlock()
		a.reconciler.TrackStopping(allocation.ID, false, nil, 0, time.Time{}, false)
		if err := a.stopAllocation(ctx, allocation.ID); err != nil {
			errs = append(errs, errors.Join(persistErr, err))
		}
	}
	return errors.Join(errs...)
}

// DrainGroup suppresses automatic restarts for one allocation generation until
// the control plane delivers the normal stop operation.
func (a *Agent) DrainGroup(request *api.DrainAllocationRequest) error {
	unlock := a.lockAllocationOperation(request.AllocationID)
	defer unlock()
	if err := a.AcceptEpoch(request.Epoch); err != nil {
		return err
	}
	return a.applyDrain(request.AllocationID, request.Generation, request.Sequence)
}

// applyDrain marks one allocation generation draining at sequence. The caller
// must hold the allocation operation lock.
func (a *Agent) applyDrain(allocationID string, generation, sequence uint64) error {
	a.mu.Lock()
	var ids []string
	var persistErr error
	for _, allocation := range a.allocations {
		if allocation.AllocationID != allocationID {
			continue
		}
		if allocation.Generation > generation {
			a.mu.Unlock()
			return fmt.Errorf("%w: current %d, requested %d", ErrStaleGeneration, allocation.Generation, generation)
		}
		if allocation.Generation != generation {
			continue
		}
		if sequence < allocation.DrainSequence {
			continue
		}
		previousDraining, previousSequence := allocation.Draining, allocation.DrainSequence
		allocation.Draining = true
		allocation.DrainSequence = sequence
		if err := a.persistAllocation(allocation); err != nil {
			allocation.Draining, allocation.DrainSequence = previousDraining, previousSequence
			persistErr = fmt.Errorf("persist draining allocation: %w", err)
			break
		}
		ids = append(ids, allocation.ID)
	}
	a.mu.Unlock()
	for _, id := range ids {
		a.reconciler.SuppressRestarts(id)
	}
	return persistErr
}

// ResumeGroup cancels a drain for a retained allocation generation.
func (a *Agent) ResumeGroup(request *api.DrainAllocationRequest) error {
	unlock := a.lockAllocationOperation(request.AllocationID)
	defer unlock()
	if err := a.AcceptEpoch(request.Epoch); err != nil {
		return err
	}
	return a.applyResume(request.AllocationID, request.Generation, request.Sequence, false)
}

// applyResume cancels a drain for one allocation generation at sequence. A
// record that is neither running nor starting fails the resume unless
// skipInactive is set. The caller must hold the allocation operation lock.
func (a *Agent) applyResume(allocationID string, generation, sequence uint64, skipInactive bool) error {
	a.mu.Lock()
	var resumed []*Allocation
	for _, allocation := range a.allocations {
		if allocation.AllocationID != allocationID {
			continue
		}
		if allocation.Generation > generation {
			a.mu.Unlock()
			return fmt.Errorf("%w: current %d, requested %d", ErrStaleGeneration, allocation.Generation, generation)
		}
		if allocation.Generation != generation {
			continue
		}
		if sequence < allocation.DrainSequence {
			continue
		}
		if allocation.Status != "running" && allocation.Status != "starting" && allocation.Status != "failed" {
			if skipInactive {
				continue
			}
			a.mu.Unlock()
			return fmt.Errorf("cannot resume allocation %s task %s with status %q", allocationID, allocation.ID, allocation.Status)
		}
		resumed = append(resumed, allocation)
	}
	// Snapshot reconciler inputs under a.mu; restart callbacks update them.
	var running []Allocation
	for _, allocation := range resumed {
		previousDraining, previousSequence := allocation.Draining, allocation.DrainSequence
		allocation.Draining = false
		allocation.DrainSequence = sequence
		if err := a.persistAllocation(allocation); err != nil {
			allocation.Draining, allocation.DrainSequence = previousDraining, previousSequence
			a.mu.Unlock()
			return fmt.Errorf("persist resumed allocation: %w", err)
		}
		// The control plane will retry the start for a recovered starting task.
		// Leave it untracked until that retry resolves its runtime state. A
		// terminally failed task stays restart-suppressed. Recovery retries
		// classify a task whose runtime state is unknown.
		if allocation.Status == "running" && !allocation.unobserved {
			running = append(running, *allocation)
		}
	}
	a.mu.Unlock()
	for _, allocation := range running {
		a.reconciler.ResumeRestarts(allocation.ID, allocation.Spec != nil && allocation.Spec.HealthCheck != nil, allocation.Restart, allocation.RestartAttempts, allocation.RestartWindow, allocation.RestartExhausted)
	}
	return nil
}

// RunAllocation creates and starts one allocation task. draining and
// drainSequence are the generation's drain state; a draining task starts
// restart-suppressed. RunGroup applies that state to existing records first.
func (a *Agent) RunAllocation(ctx context.Context, allocID, schedulerID string, generation uint64, jobRevision int, executionHash, namespace, jobName, groupName, taskName string, taskSpec *spec.TaskSpec, groupRuntime string, networkPlan *network.Plan, envOverrides map[string]string, delivered []api.DeliveredSecret, restartPolicy *spec.RestartPolicySpec, draining bool, drainSequence uint64) (runErr error) {
	ts := taskSpec
	if ts == nil {
		return fmt.Errorf("task spec is required")
	}
	if allocID == "" {
		return fmt.Errorf("allocation ID is required")
	}
	a.mu.RLock()
	known := a.allocations[allocID] != nil
	a.mu.RUnlock()
	if !known {
		// Checked before any start state exists, so a refused start never
		// reaches cleanup that would release staging it does not own. A tracked
		// allocation's staging is released by its own stop below.
		if err := a.releaseOrphanedStaging(ctx, allocID); err != nil {
			return err
		}
	}
	a.mu.Lock()
	existing := a.allocations[allocID]
	if existing != nil {
		matching := existing.AllocationID == schedulerID && existing.Generation == generation && existing.JobRevision == jobRevision && existing.ExecutionHash == executionHash
		status, exhausted, unobserved := existing.Status, existing.RestartExhausted, existing.unobserved
		a.mu.Unlock()
		if !matching {
			return fmt.Errorf("%w: %s", ErrAllocationExists, allocID)
		}
		// An exhausted restart budget is terminal for this generation; a
		// start retry must not recreate the task with a fresh budget.
		if exhausted {
			return fmt.Errorf("%w: allocation %s", ErrRestartBudgetExhausted, allocID)
		}
		if unobserved {
			observed, err := a.observeRecovered(ctx, allocID)
			if err != nil {
				return err
			}
			status = observed
		}
		if status == "running" {
			return nil
		}
		// A previous start may have reached the runtime but failed before it
		// could be committed. Preserve its resources while Stop is uncertain,
		// then finish that cleanup on a later retry instead of converting the
		// retry into a terminal execution conflict. An empty status means
		// recovery already confirmed the container missing and cleaned it up.
		if status != "" {
			if err := a.stopAllocation(context.WithoutCancel(ctx), allocID); err != nil {
				return fmt.Errorf("clean up incomplete allocation %s before retry: %w", allocID, err)
			}
		}
		a.mu.Lock()
	}
	alloc := &Allocation{ID: allocID, ContainerID: allocID, AllocationID: schedulerID, Generation: generation, JobRevision: jobRevision, ExecutionHash: executionHash, Restart: restartPolicy, Namespace: namespace, JobName: jobName, GroupName: groupName, TaskName: taskName, Spec: ts, Status: "starting", Health: "unknown", Draining: draining, DrainSequence: drainSequence}
	starting := *alloc
	a.allocations[allocID] = &starting
	a.mu.Unlock()
	if err := a.persistAllocation(alloc); err != nil {
		a.mu.Lock()
		delete(a.allocations, allocID)
		a.mu.Unlock()
		return fmt.Errorf("persist starting allocation: %w", err)
	}
	committed := false
	containerCreated := false
	startAttempted := false
	tracked := false
	healthRegistered := false
	var netAttachment *network.Attachment
	var ports []*runtime.Port
	var secretDir string
	defer func() {
		if committed {
			return
		}
		if startAttempted {
			if tracked {
				a.reconciler.BeginStop(allocID)
			} else {
				// Start may have succeeded even if its response was lost. Track
				// the retained allocation as stopping from the outset so an
				// observed stopped task can never be restarted.
				a.reconciler.TrackStopping(allocID, false, restartPolicy, 0, time.Time{}, false)
				tracked = true
			}
		}
		// Publish the latest durable startup state before marking it stopping.
		// Startup builds alloc privately so readers never observe it changing.
		failed := *alloc
		a.mu.Lock()
		a.allocations[allocID] = &failed
		a.mu.Unlock()
		persistStopErr := a.markAllocationStopping(allocID)
		runErr = errors.Join(runErr, persistStopErr)
		if alloc.ContainerOwnershipUnverified {
			runErr = errors.Join(runErr, fmt.Errorf("container ownership for %s remains unverified", allocID))
			return
		}
		if startAttempted {
			if err := a.runtime.Stop(context.WithoutCancel(ctx), allocID); err != nil {
				runErr = errors.Join(runErr, fmt.Errorf("stop container %s during failed start: %w", allocID, err))
				return
			}
		}
		if healthRegistered {
			a.health.DeregisterTask(allocID)
		}
		var cleanupErrs []error
		if tracked {
			if err := a.reconciler.Untrack(allocID); err != nil {
				cleanupErrs = append(cleanupErrs, fmt.Errorf("untrack allocation %s: %w", allocID, err))
			}
		}
		containerRemoved := true
		if containerCreated {
			if err := a.runtime.Remove(context.WithoutCancel(ctx), allocID); err != nil {
				containerRemoved = false
				cleanupErrs = append(cleanupErrs, fmt.Errorf("remove container %s: %w", allocID, err))
			}
		}
		if err := a.network.Detach(context.WithoutCancel(ctx), netAttachment); err != nil {
			cleanupErrs = append(cleanupErrs, fmt.Errorf("detach allocation network: %w", err))
		}
		if secretDir != "" {
			if err := removeSecretDir(secretDir); err != nil {
				cleanupErrs = append(cleanupErrs, fmt.Errorf("remove secret files: %w", err))
			}
		}
		// A container that still exists keeps its staging mounts as OCI mount
		// sources; a cleanup retry releases them after removal succeeds.
		if containerRemoved {
			if err := a.volumes.ReleaseStaging(allocID); err != nil {
				cleanupErrs = append(cleanupErrs, fmt.Errorf("release volume staging: %w", err))
			}
		}
		if err := errors.Join(cleanupErrs...); err != nil {
			runErr = errors.Join(runErr, err)
			return
		}
		if err := a.deleteAllocationRecord(allocID); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("delete allocation record: %w", err))
			return
		}
		// Port release is infallible. Keep claims until no cleanup retry can
		// release a port that has since been assigned to another allocation.
		for _, p := range ports {
			_ = a.ports.Release(p)
		}
		a.mu.Lock()
		delete(a.allocations, allocID)
		a.mu.Unlock()
	}()

	var taskPorts []spec.PortSpec
	if ts.Networking != nil {
		taskPorts = ts.Networking.Ports
	}
	for _, p := range taskPorts {
		port, err := a.ports.Claim(p)
		if err != nil {
			return fmt.Errorf("claim port %d: %w", p.HostPort, err)
		}

		ports = append(ports, port)
		alloc.Ports = append([]*runtime.Port(nil), ports...)
		if err := a.persistAllocation(alloc); err != nil {
			return fmt.Errorf("persist port claim: %w", err)
		}
	}

	var mounts []*runtime.Mount
	for _, v := range ts.Volumes {
		mount, err := a.volumes.Create(namespace, jobName, allocID, v)
		if err != nil {
			return fmt.Errorf("create volume %s: %w", v.Name, err)
		}

		mounts = append(mounts, mount)
		alloc.Mounts = append([]*runtime.Mount(nil), mounts...)
	}
	if err := a.persistAllocation(alloc); err != nil {
		return fmt.Errorf("persist volume metadata: %w", err)
	}

	err := a.runtime.Pull(ctx, ts.Image)
	if err != nil {
		return fmt.Errorf("pull image %s: %w", ts.Image, err)
	}

	containerID := allocID
	alloc.ContainerID = containerID
	var taskNetMode spec.TaskNetworkMode
	if ts.Networking != nil {
		taskNetMode = ts.Networking.Mode
	}
	hostMode := taskNetMode == spec.TaskNetworkHost
	wireGuard := taskNetMode == spec.TaskNetworkWireGuard
	if wireGuard {
		if networkPlan == nil {
			return fmt.Errorf("automatic WireGuard network plan is required")
		}
		attachRequest := network.AttachRequest{AllocationID: allocID, Namespace: namespace, Network: namespace, Plan: *networkPlan}
		if durable, ok := a.network.(network.DurableAttacher); ok {
			netAttachment, err = durable.AttachRecorded(ctx, attachRequest, func(attachment *network.Attachment) error {
				netAttachment = attachment
				alloc.Network = attachment
				return a.persistAllocation(alloc)
			})
		} else {
			netAttachment, err = a.network.Attach(ctx, attachRequest)
		}
		if err != nil {
			return fmt.Errorf("attach WireGuard network: %w", err)
		}
		if alloc.Network == nil {
			alloc.Network = netAttachment
			if err := a.persistAllocation(alloc); err != nil {
				return fmt.Errorf("persist network attachment: %w", err)
			}
		}
	}
	env := make(map[string]string, len(ts.Env)+len(envOverrides))
	for k, v := range ts.Env {
		env[k] = v
	}
	for k, v := range envOverrides {
		env[k] = v
	}
	if taskHasFileSecrets(taskName, delivered) {
		secretDir, err = a.secretDirFor(allocID)
		if err != nil {
			return err
		}
		// Record the location before any plaintext is written so a restarted
		// agent can always find and remove it.
		alloc.SecretDir = secretDir
		if err := a.persistAllocation(alloc); err != nil {
			return fmt.Errorf("persist secret metadata: %w", err)
		}
		if err := createSecretDir(secretDir); err != nil {
			// Never clean up a directory this start did not create.
			secretDir, alloc.SecretDir = "", ""
			return err
		}
	}
	secretEnv, secretMounts, err := materializeSecrets(secretDir, taskName, delivered)
	if err != nil {
		return err
	}
	for k, v := range secretEnv {
		env[k] = v
	}
	mounts = append(mounts, secretMounts...)
	alloc.Mounts = append([]*runtime.Mount(nil), mounts...)
	labels := map[string]string{
		"trellis.cluster":               a.cluster,
		"trellis.allocation-id":         schedulerID,
		"trellis.allocation-generation": strconv.FormatUint(generation, 10),
		"trellis.job-revision":          strconv.Itoa(jobRevision),
		"trellis.execution-hash":        executionHash,
		"trellis.namespace":             namespace,
		"trellis.job":                   jobName,
		"trellis.task-group":            groupName,
		"trellis.task":                  taskName,
	}
	extraHosts := map[string]string{}
	if _, apiAccess := envOverrides["TRELLIS_ADDR"]; apiAccess {
		switch {
		case hostMode:
			extraHosts["trellis"] = "127.0.0.1"
		case wireGuard && networkPlan != nil:
			extraHosts["trellis"] = networkPlan.Gateway
		}
	}
	runtimeMounts := append([]*runtime.Mount(nil), mounts...)
	runtimeMounts = append(runtimeMounts, &runtime.Mount{
		HostPath:      a.healthProbe,
		ContainerPath: health.ProbeContainerPath,
		ReadOnly:      true,
	})
	alloc.ContainerOwnershipUnverified = true
	if err := a.persistAllocation(alloc); err != nil {
		alloc.ContainerOwnershipUnverified = false
		return fmt.Errorf("persist pending container creation: %w", err)
	}
	_, err = a.runtime.Create(ctx, runtime.CreateOptions{
		ID:     containerID,
		Image:  ts.Image,
		Env:    env,
		Mounts: runtimeMounts,
		CPU: func() int {
			if ts.Resources != nil {
				return ts.Resources.CPU
			}
			return 0
		}(),
		Memory: func() int64 {
			if ts.Resources != nil {
				return int64(ts.Resources.Memory)
			}
			return 0
		}(),
		Runtime: groupRuntime,
		NetworkNamespace: func() string {
			if netAttachment != nil {
				return netAttachment.NetworkNamespace
			}
			if hostMode {
				return "/proc/1/ns/net"
			}
			return ""
		}(),
		DNSServers: a.dnsServers,
		ExtraHosts: extraHosts,
		Labels:     labels,
	})
	if err != nil {
		observed, inspectErr := a.runtime.Inspect(context.WithoutCancel(ctx), containerID)
		if inspectErr != nil {
			return errors.Join(fmt.Errorf("create container %s: %w", containerID, err), fmt.Errorf("inspect container ownership: %w", inspectErr))
		}
		if !a.containerMatchesAllocation(*observed, alloc) {
			return fmt.Errorf("create container %s: %w", containerID, err)
		}
	}
	containerCreated = true
	alloc.ContainerOwnershipUnverified = false
	if err := a.persistAllocation(alloc); err != nil {
		return fmt.Errorf("persist verified container creation: %w", err)
	}

	startAttempted = true
	err = a.runtime.Start(ctx, containerID)
	if err != nil {
		observed, inspectErr := a.runtime.Inspect(context.WithoutCancel(ctx), containerID)
		if inspectErr != nil || observed.Status != runtime.StatusRunning {
			return fmt.Errorf("start container %s: %w", containerID, err)
		}
	}
	ready := &Allocation{
		ID:            allocID,
		AllocationID:  schedulerID,
		Generation:    generation,
		JobRevision:   jobRevision,
		ExecutionHash: executionHash,
		Restart:       restartPolicy,
		Namespace:     namespace,

		JobName:   jobName,
		GroupName: groupName,
		TaskName:  ts.Name,
		Spec:      ts,

		ContainerID: containerID,
		Ports:       ports,
		Mounts:      mounts,
		SecretDir:   secretDir,
		Network:     netAttachment,
		Status:      "running",
		Health:      "unknown",

		Draining:      draining,
		DrainSequence: drainSequence,
	}
	if ts.HealthCheck == nil {
		ready.Health = "healthy"
	}
	if err := a.persistAllocation(ready); err != nil {
		return fmt.Errorf("persist allocation: %w", err)
	}
	a.mu.Lock()
	a.allocations[allocID] = ready
	a.mu.Unlock()
	// Track only after the running record is stored, so a restart decision
	// (including terminal exhaustion) is never overwritten by startup.
	if draining {
		a.reconciler.TrackStopping(allocID, ts.HealthCheck != nil, restartPolicy, 0, time.Time{}, false)
	} else {
		a.reconciler.Track(allocID, ts.HealthCheck != nil, restartPolicy)
	}
	tracked = true
	if ts.HealthCheck != nil {
		check := *ts.HealthCheck
		a.health.RegisterTask(allocID, containerID, &check)
		healthRegistered = true
	}
	if ts.HealthCheck == nil {
		if err := a.reconciler.ObserveHealth(allocID, true); err != nil {
			return fmt.Errorf("mark allocation healthy: %w", err)
		}
	}
	// Keep managed-volume staging mounts until the container is removed: they
	// are its OCI mount sources, which every restarted task resolves again.
	committed = true

	return nil
}

// releaseOrphanedStaging ensures a start never stages volumes over staging
// kept for an existing container. Staging whose container is gone was left
// behind, for example when recovery could not list containers, and is released.
func (a *Agent) releaseOrphanedStaging(ctx context.Context, allocID string) error {
	inUse, err := a.volumes.StagingInUse(allocID)
	if err != nil {
		return fmt.Errorf("check volume staging: %w", err)
	}
	if !inUse {
		return nil
	}
	if _, err := a.runtime.Inspect(ctx, allocID); !errdefs.IsNotFound(err) {
		if err != nil {
			return fmt.Errorf("verify container %s before releasing volume staging: %w", allocID, err)
		}
		return fmt.Errorf("%w: container %s still exists", errStagingInUse, allocID)
	}
	if err := a.volumes.ReleaseStaging(allocID); err != nil {
		return fmt.Errorf("release orphaned volume staging: %w", err)
	}
	return nil
}

// Logs opens the log stream for an allocation.
func (a *Agent) Logs(ctx context.Context, allocID string, follow bool, tail int) (io.ReadCloser, error) {
	a.mu.RLock()
	alloc := a.allocations[allocID]
	a.mu.RUnlock()
	if alloc == nil {
		return nil, fmt.Errorf("%w: %s", ErrAllocationNotFound, allocID)
	}
	return a.runtime.Logs(ctx, alloc.ContainerID, follow, tail)
}

// StopAllocation stops and removes an allocation.
func (a *Agent) StopAllocation(ctx context.Context, allocID string) error {
	a.mu.RLock()
	allocation := a.allocations[allocID]
	a.mu.RUnlock()
	if allocation == nil {
		return fmt.Errorf("%w: %s", ErrAllocationNotFound, allocID)
	}
	unlock := a.lockAllocationOperation(allocation.AllocationID)
	defer unlock()
	return a.stopAllocation(ctx, allocID)
}

func (a *Agent) stopAllocation(ctx context.Context, allocID string) error {
	a.mu.RLock()
	stored, ok := a.allocations[allocID]
	var alloc Allocation
	if ok {
		alloc = *stored
		alloc.Ports = append([]*runtime.Port(nil), stored.Ports...)
	}
	a.mu.RUnlock()
	if !ok {
		return fmt.Errorf("%w: %s", ErrAllocationNotFound, allocID)
	}

	containerID := alloc.ContainerID
	containerMissing := false
	if alloc.ContainerOwnershipUnverified {
		observed, err := a.runtime.Inspect(ctx, containerID)
		if errdefs.IsNotFound(err) {
			containerMissing = true
		} else if err != nil {
			return fmt.Errorf("verify container %s before cleanup: %w", containerID, err)
		} else if !a.containerMatchesAllocation(*observed, &alloc) {
			return fmt.Errorf("%w: container %s has different execution metadata", ErrExecutionConflict, containerID)
		}
	}
	a.reconciler.BeginStop(allocID)
	persistStopErr := a.markAllocationStopping(allocID)
	a.closeExecSessionsForTask(ctx, allocID, containerID)
	if !containerMissing {
		if err := a.runtime.Stop(ctx, containerID); err != nil {
			return errors.Join(persistStopErr, fmt.Errorf("stop container %s: %w", containerID, err))
		}
	}

	var errs []error
	a.health.DeregisterTask(allocID)
	if err := a.reconciler.Untrack(allocID); err != nil {
		errs = append(errs, fmt.Errorf("untrack allocation %s: %w", allocID, err))
	}

	if err := a.network.Detach(ctx, alloc.Network); err != nil {
		errs = append(errs, fmt.Errorf("detach allocation network: %w", err))
	}
	containerRemoved := true
	if !containerMissing {
		if err := a.runtime.Remove(ctx, containerID); err != nil {
			containerRemoved = false
			errs = append(errs, fmt.Errorf("remove container %s: %w", containerID, err))
		}
	}
	if alloc.SecretDir != "" {
		if err := removeSecretDir(alloc.SecretDir); err != nil {
			errs = append(errs, fmt.Errorf("remove secret files: %w", err))
		}
	}

	// Staging mounts remain the OCI mount sources of a container that still
	// exists; a stop retry releases them after removal succeeds.
	if containerRemoved {
		if err := a.volumes.ReleaseStaging(allocID); err != nil {
			errs = append(errs, fmt.Errorf("release volume staging: %w", err))
		}
	}
	if err := errors.Join(errs...); err != nil {
		return errors.Join(persistStopErr, err)
	}
	if err := a.deleteAllocationRecord(allocID); err != nil {
		return errors.Join(persistStopErr, fmt.Errorf("delete allocation record: %w", err))
	}
	a.mu.Lock()
	delete(a.allocations, allocID)
	a.mu.Unlock()
	for _, p := range alloc.Ports {
		// Recovery may retain a stale record sharing a live allocation's port.
		if !a.hostPortInUse(p) {
			_ = a.ports.Release(p)
		}
	}

	return persistStopErr
}

// OnHealthy and OnUnhealthy are observation callbacks from the health manager.
// They intentionally do not mutate allocation status directly; lifecycle state
// transitions are centralized in the allocation reconciler.
func (a *Agent) OnHealthy(ctx context.Context, allocID string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	// A replaced worker can finish a probe after RegisterTask cancels it.
	if ctx.Err() != nil {
		return nil
	}
	if allocation := a.allocations[allocID]; allocation != nil && allocation.Status != "failed" {
		allocation.Health = "healthy"
		return a.persistAllocation(allocation)
	}
	return nil
}

// OnUnhealthy handles an unhealthy allocation.
func (a *Agent) OnUnhealthy(ctx context.Context, allocID string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if ctx.Err() != nil {
		return nil
	}
	if allocation := a.allocations[allocID]; allocation != nil && allocation.Status != "failed" {
		allocation.Health = "unhealthy"
		return a.persistAllocation(allocation)
	}
	return nil
}

// OnReconciledStatus records reconciled allocation status.
func (a *Agent) OnReconciledStatus(allocID, status string) {
	a.mu.Lock()
	if alloc := a.allocations[allocID]; alloc != nil {
		if alloc.Status == "failed" && (status == "healthy" || status == "unhealthy") {
			// A terminally failed task is not probed; ignore late health.
			a.mu.Unlock()
			return
		}
		if status == "healthy" || status == "unhealthy" {
			alloc.Health = status
		} else {
			alloc.Status = status
			if status == "running" && alloc.Spec != nil && alloc.Spec.HealthCheck != nil {
				alloc.Health = "unknown"
				a.health.RegisterTask(allocID, alloc.ContainerID, alloc.Spec.HealthCheck)
			}
		}
		if err := a.persistAllocation(alloc); err != nil {
			a.log.Error("persist reconciled allocation", "allocation", alloc.AllocationID, "error", err)
		}
	}
	a.mu.Unlock()
}

// OnRestartState records allocation restart state. An exhausted budget also
// records the terminal failed observation in the same write, so recovery
// never sees exhaustion without the failure or the reverse.
func (a *Agent) OnRestartState(allocID string, attempts int, window time.Time, exhausted bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	allocation := a.allocations[allocID]
	if allocation == nil {
		return nil
	}
	allocation.RestartAttempts, allocation.RestartWindow, allocation.RestartExhausted = attempts, window, exhausted
	if exhausted {
		// The container stopped and will not be restarted; stop probing it.
		allocation.Status, allocation.Health = "failed", "unhealthy"
		a.health.DeregisterTask(allocID)
	}
	if err := a.persistAllocation(allocation); err != nil {
		// Keep reporting the accurate in-memory observation; the reconciler
		// logs the error and retries persisting an exhaustion on its next pass.
		return fmt.Errorf("persist restart tracking for %s: %w", allocID, err)
	}
	return nil
}

func (a *Agent) runHeartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	registered := false

	for {
		if !registered {
			if a.server.Ready() {
				a.nodeInfo.Volumes = a.volumes.AvailableHostVolumes()
				if _, err := a.server.RegisterNode(ctx, &a.nodeInfo); err != nil {
					a.log.Error("register node failed", "error", err)
				} else {
					registered = true
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !a.server.Ready() {
				continue
			}
			if !registered {
				continue
			}
			heartbeat := &client.Heartbeat{
				NodeID:            a.nodeID,
				Timestamp:         time.Now(),
				Allocations:       a.allocationStatuses(),
				Volumes:           a.volumes.AvailableHostVolumes(),
				Capabilities:      a.nodeInfo.Capabilities,
				Version:           a.version,
				CPUCapacity:       a.nodeInfo.CPUCapacity,
				MemoryCapacity:    a.nodeInfo.MemoryCapacity,
				CPUAllocatable:    a.nodeInfo.CPUAllocatable,
				MemoryAllocatable: a.nodeInfo.MemoryAllocatable,
			}
			if metrics, ok := nodecapacity.SampleHostMetrics(); ok {
				if metrics.CPUValid {
					heartbeat.CPUUsage = &metrics.CPUUsage
				}
				if metrics.MemoryValid {
					heartbeat.MemoryUsed = &metrics.MemoryUsed
					heartbeat.MemoryAvailable = &metrics.MemoryAvailable
				}
				heartbeat.MetricsAt = &metrics.CollectedAt
			}
			err := a.server.SendHeartbeat(ctx, a.nodeID, heartbeat)
			if err != nil {
				a.log.Error("send heartbeat failed", "error", err)
				registered = false
			}
		}
	}
}

func (a *Agent) allocationStatuses() []api.AllocationStatus {
	a.mu.RLock()
	defer a.mu.RUnlock()
	actual := make([]api.AllocationStatus, 0, len(a.allocations))
	for _, alloc := range a.allocations {
		ports := make([]api.PortMapping, 0, len(alloc.Ports))
		for _, p := range alloc.Ports {
			ports = append(ports, api.PortMapping{HostPort: p.HostPort, ContainerPort: p.ContainerPort})
		}
		actual = append(actual, api.AllocationStatus{ID: alloc.AllocationID, Generation: alloc.Generation, Task: alloc.TaskName, Address: allocationNetworkAddress(alloc), Phase: lifecycle.Phase(alloc.Status), Health: lifecycle.Health(reportedHealth(alloc)), Ports: ports})
	}
	return actual
}
