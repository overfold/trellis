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
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/client"
	"github.com/clofour/trellis/internal/health"
	"github.com/clofour/trellis/internal/lifecycle"
	"github.com/clofour/trellis/internal/network"
	"github.com/clofour/trellis/internal/runtime"
	"github.com/clofour/trellis/internal/spec"
	"github.com/clofour/trellis/internal/storage"
	"github.com/google/uuid"
)

// Agent manages allocation lifecycle on a node.
type Agent struct {
	nodeID       uuid.UUID
	allocations  map[string]*Allocation
	execSessions map[string]*execSession
	healthProbe  string

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
	leaderID    uuid.UUID
	orphans     map[string]int
	mu          sync.RWMutex
	operationMu sync.Mutex
	operations  map[string]*allocationOperation
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

type execSession struct {
	AllocationID string
	Task         string
	Terminal     runtime.TerminalSession
}

// Allocation contains agent-local allocation state.
type Allocation struct {
	ID              string
	AllocationID    string
	Generation      uint64
	JobRevision     int
	ExecutionHash   string
	Restart         *spec.RestartPolicySpec
	RestartAttempts int
	RestartWindow   time.Time
	Namespace       string

	JobName   string
	GroupName string
	TaskName  string
	Spec      *spec.TaskSpec

	ContainerID string
	Ports       []*runtime.Port
	Mounts      []*runtime.Mount
	SecretDir   string
	Network     *network.Attachment
	Status      string
	Health      string
}

const heartbeatInterval = 10 * time.Second

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
	// ErrExecutionConflict indicates conflicting allocation execution metadata.
	ErrExecutionConflict = errors.New("allocation execution metadata conflict")
	// ErrExecSessionNotFound indicates that an interactive exec session does not exist.
	ErrExecSessionNotFound = errors.New("exec session not found")
)

// ConfigureDurability enables persistent agent state.
func (a *Agent) ConfigureDurability(local *storage.LocalStorage, cluster string) {
	a.local, a.cluster = local, cluster
}

// AcceptEpoch validates and persists a leadership epoch.
func (a *Agent) AcceptEpoch(epoch uint64) error {
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

// AuthorizeLeader reports whether id is the current Raft leader learned from
// the latest authenticated heartbeat response.
func (a *Agent) AuthorizeLeader(id uuid.UUID) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return id != uuid.Nil && id == a.leaderID
}

func allocationRecordKey(id string) string {
	return "agent/allocations/" + base64.RawURLEncoding.EncodeToString([]byte(id))
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
		orphans:      make(map[string]int),
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

// SetResources configures node capacity and platform attributes.
func (a *Agent) SetResources(cpu int, memory int64, osName, arch string) {
	a.nodeInfo.CPU, a.nodeInfo.Memory, a.nodeInfo.OS, a.nodeInfo.Arch = cpu, memory, osName, arch
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
func (a *Agent) Init(ctx context.Context) {
	a.health.Subscriber = a
	a.health.SetContext(ctx)
	a.reconciler.Subscriber = a
	if err := a.recover(ctx); err != nil {
		a.log.Error("recover allocations", "error", err)
	}

	go a.runHeartbeatLoop(ctx)
	go a.reconciler.Run(ctx)
}

func (a *Agent) recover(ctx context.Context) error {
	if a.local == nil {
		return nil
	}
	var epoch uint64
	if err := a.local.Get("agent/control-epoch", &epoch); err == nil {
		a.epoch = epoch
	}
	records, recordErrs := a.local.ListRaw("agent/allocations")
	stored := make(map[string]*Allocation, len(records))
	for name, raw := range records {
		var allocation Allocation
		if err := json.Unmarshal(raw, &allocation); err != nil {
			a.log.Error("skip malformed allocation record", "record", name, "error", err)
			continue
		}
		stored[allocation.ContainerID] = &allocation
	}
	for _, err := range recordErrs {
		a.log.Error("read allocation recovery record", "error", err)
	}
	managed, ok := a.runtime.(runtime.ManagedRuntime)
	if !ok {
		return nil
	}
	containers, err := managed.ListManaged(ctx, a.cluster)
	if err != nil {
		return err
	}
	seen := make(map[string]bool, len(containers))
	for _, container := range containers {
		seen[container.ID] = true
		allocation := stored[container.ID]
		hadRecord := allocation != nil
		if allocation == nil {
			allocation = allocationFromRuntime(container)
		}
		if allocation == nil {
			a.log.Warn("leave unidentifiable Trellis container untouched", "container", container.ID)
			continue
		}
		stopping := hadRecord && allocation.Status == "stopping"
		recoveryPending := !stopping && (container.Status == runtime.StatusCreated || container.Status == runtime.StatusStopped)
		if recoveryPending {
			// Recovery reports observation; it does not invent desired state.
			// A non-running recovered task stays restart-suppressed until the
			// control plane observes "starting" and reconciliation reissues the
			// appropriate start or stop action.
			allocation.Status = "starting"
			allocation.Health = "unknown"
		} else if !stopping && container.Status == runtime.StatusRunning {
			allocation.Status = "running"
		}
		if container.Status == runtime.StatusRunning || container.Status == runtime.StatusCreated || container.Status == runtime.StatusStopped {
			for _, port := range allocation.Ports {
				if err := a.ports.Adopt(port); err != nil {
					a.log.Error("recover port claim", "allocation", allocation.AllocationID, "error", err)
				}
			}
			a.allocations[allocation.ID] = allocation
			if allocation.Spec != nil {
				if !stopping && !recoveryPending && allocation.Spec.HealthCheck != nil {
					check := *allocation.Spec.HealthCheck
					for _, port := range allocation.Ports {
						if port.ContainerPort == check.Port {
							check.Port = port.HostPort
							break
						}
					}
					a.health.RegisterTask(allocation.ID, allocation.ContainerID, &check)
				}
				if stopping {
					a.reconciler.TrackStopping(allocation.ID, allocation.Spec.HealthCheck != nil, allocation.Restart)
				} else if !recoveryPending {
					a.reconciler.TrackRecovered(allocation.ID, allocation.Spec.HealthCheck != nil, allocation.Restart, allocation.RestartAttempts, allocation.RestartWindow)
				}
			} else if stopping {
				a.reconciler.TrackStopping(allocation.ID, false, nil)
			} else if !recoveryPending {
				a.reconciler.Track(allocation.ID, false, nil)
			}
			if err := a.persistAllocation(allocation); err != nil {
				a.log.Error("refresh recovered allocation record", "allocation", allocation.AllocationID, "error", err)
			}
		}
	}
	for containerID, allocation := range stored {
		if containerID == "" || seen[containerID] {
			continue
		}
		for _, port := range allocation.Ports {
			_ = a.ports.Adopt(port)
			_ = a.ports.Release(port)
		}
		_ = a.network.Detach(context.WithoutCancel(ctx), allocation.Network)
		_ = a.deleteAllocationRecord(allocation.ID)
	}
	return nil
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

// GetAllocations returns copies of agent allocation state.
func (a *Agent) GetAllocations() []*Allocation {
	a.mu.RLock()
	defer a.mu.RUnlock()
	result := make([]*Allocation, 0, len(a.allocations))
	for _, alloc := range a.allocations {
		allocationCopy := *alloc
		allocationCopy.Ports = append([]*runtime.Port(nil), alloc.Ports...)
		allocationCopy.Mounts = append([]*runtime.Mount(nil), alloc.Mounts...)
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
	for id, allocation := range a.allocations {
		if allocation.AllocationID != request.AllocationID {
			continue
		}
		if allocation.Generation > request.Generation {
			a.mu.RUnlock()
			return fmt.Errorf("%w: current %d, requested %d", ErrStaleGeneration, allocation.Generation, request.Generation)
		}
		if allocation.Generation == request.Generation && allocation.ExecutionHash != request.ExecutionHash {
			a.mu.RUnlock()
			return fmt.Errorf("%w: allocation %s generation %d", ErrExecutionConflict, request.AllocationID, request.Generation)
		}
		if allocation.Generation < request.Generation {
			oldIDs = append(oldIDs, id)
		}
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
	for i := range request.Tasks {
		task := &request.Tasks[i]
		id := fmt.Sprintf("%s-g%d-%s", request.AllocationID, request.Generation, task.Name)
		if err := a.RunAllocation(ctx, id, request.AllocationID, request.Generation, request.JobRevision, request.ExecutionHash, request.Namespace, request.JobName, request.GroupName, task.Name, task, request.Runtime, request.NetworkPlan, request.EnvOverrides, request.Secrets, request.Restart); err != nil {
			return err
		}
	}
	return nil
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
	a.mu.RUnlock()
	var errs []error
	for _, id := range ids {
		if err := a.stopAllocation(ctx, id); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (a *Agent) reconcileDesired(ctx context.Context, response *api.HeartbeatResponse) {
	if response == nil || response.LeaderID == uuid.Nil {
		return
	}
	if err := a.AcceptEpoch(response.Epoch); err != nil {
		return
	}
	a.mu.Lock()
	a.leaderID = response.LeaderID
	a.mu.Unlock()
	if !response.OrphanConfirmation {
		return
	}
	type desiredState struct {
		wanted   bool
		draining bool
	}
	desired := make(map[string]desiredState, len(response.Desired))
	for _, allocation := range response.Desired {
		desired[fmt.Sprintf("%s/%d", allocation.ID, allocation.Generation)] = desiredState{wanted: true, draining: allocation.Draining}
	}
	a.mu.Lock()
	var collect []string
	for id, allocation := range a.allocations {
		key := fmt.Sprintf("%s/%d", allocation.AllocationID, allocation.Generation)
		state := desired[key]
		if state.wanted {
			delete(a.orphans, key)
			if state.draining {
				_ = a.reconciler.Untrack(allocation.ID)
			}
			continue
		}
		a.orphans[key]++
		if a.orphans[key] >= 2 {
			collect = append(collect, id)
		}
	}
	a.mu.Unlock()
	for _, id := range collect {
		if err := a.StopAllocation(context.WithoutCancel(ctx), id); err != nil {
			a.log.Error("collect confirmed orphan", "task", id, "error", err)
		}
	}
}

// RunAllocation creates and starts one allocation task.
func (a *Agent) RunAllocation(ctx context.Context, allocID, schedulerID string, generation uint64, jobRevision int, executionHash, namespace, jobName, groupName, taskName string, taskSpec *spec.TaskSpec, groupRuntime string, networkPlan *network.Plan, envOverrides map[string]string, delivered []api.DeliveredSecret, restartPolicy *spec.RestartPolicySpec) (runErr error) {
	ts := taskSpec
	if ts == nil {
		return fmt.Errorf("task spec is required")
	}
	if allocID == "" {
		return fmt.Errorf("allocation ID is required")
	}
	a.mu.Lock()
	existing := a.allocations[allocID]
	if existing != nil {
		matching := existing.AllocationID == schedulerID && existing.Generation == generation && existing.ExecutionHash == executionHash
		status := existing.Status
		a.mu.Unlock()
		if !matching {
			return fmt.Errorf("%w: %s", ErrAllocationExists, allocID)
		}
		if status == "running" {
			return nil
		}
		// A previous start may have reached the runtime but failed before it
		// could be committed. Preserve its resources while Stop is uncertain,
		// then finish that cleanup on a later retry instead of converting the
		// retry into a terminal execution conflict.
		if err := a.stopAllocation(context.WithoutCancel(ctx), allocID); err != nil {
			return fmt.Errorf("clean up incomplete allocation %s before retry: %w", allocID, err)
		}
		a.mu.Lock()
	}
	alloc := &Allocation{ID: allocID, ContainerID: allocID, AllocationID: schedulerID, Generation: generation, JobRevision: jobRevision, ExecutionHash: executionHash, Restart: restartPolicy, Namespace: namespace, JobName: jobName, GroupName: groupName, TaskName: taskName, Spec: ts, Status: "starting", Health: "unknown"}
	a.allocations[allocID] = alloc
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
		if err := a.volumes.ReleaseStaging(allocID, ts.Volumes); err != nil {
			a.log.Error("release volume staging", "allocation", allocID, "error", err)
		}
	}()
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
				a.reconciler.TrackStopping(allocID, false, restartPolicy)
				tracked = true
			}
			persistStopErr := a.markAllocationStopping(allocID)
			runErr = errors.Join(runErr, persistStopErr)
			if err := a.runtime.Stop(context.WithoutCancel(ctx), allocID); err != nil {
				runErr = errors.Join(runErr, fmt.Errorf("stop container %s during failed start: %w", allocID, err))
				return
			}
		}
		if healthRegistered {
			a.health.DeregisterTask(allocID)
		}
		if tracked {
			_ = a.reconciler.Untrack(allocID)
		}
		if containerCreated {
			_ = a.runtime.Remove(context.WithoutCancel(ctx), allocID)
		}
		_ = a.network.Detach(context.WithoutCancel(ctx), netAttachment)
		for _, p := range ports {
			_ = a.ports.Release(p)
		}
		if secretDir != "" {
			_ = os.RemoveAll(secretDir)
		}
		a.mu.Lock()
		delete(a.allocations, allocID)
		a.mu.Unlock()
		_ = a.deleteAllocationRecord(allocID)
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
		netAttachment, err = a.network.Attach(ctx, network.AttachRequest{AllocationID: allocID, Namespace: namespace, Network: namespace, Plan: *networkPlan})
		if err != nil {
			return fmt.Errorf("attach WireGuard network: %w", err)
		}
		a.mu.Lock()
		alloc.Network = netAttachment
		a.mu.Unlock()
		if err := a.persistAllocation(alloc); err != nil {
			return fmt.Errorf("persist network attachment: %w", err)
		}
	}
	env := make(map[string]string, len(ts.Env)+len(envOverrides))
	for k, v := range ts.Env {
		env[k] = v
	}
	for k, v := range envOverrides {
		env[k] = v
	}
	secretDir, secretEnv, secretMounts, err := prepareSecrets(allocID, taskName, delivered)
	if err != nil {
		return err
	}
	alloc.SecretDir = secretDir
	if err := a.persistAllocation(alloc); err != nil {
		return fmt.Errorf("persist secret metadata: %w", err)
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
		if inspectErr != nil || observed.Labels["trellis.allocation-id"] != schedulerID || observed.Labels["trellis.allocation-generation"] != labels["trellis.allocation-generation"] {
			return fmt.Errorf("create container %s: %w", containerID, err)
		}
	}
	containerCreated = true

	startAttempted = true
	err = a.runtime.Start(ctx, containerID)
	if err != nil {
		observed, inspectErr := a.runtime.Inspect(context.WithoutCancel(ctx), containerID)
		if inspectErr != nil || observed.Status != runtime.StatusRunning {
			return fmt.Errorf("start container %s: %w", containerID, err)
		}
	}
	if ts.HealthCheck != nil {
		check := *ts.HealthCheck
		a.health.RegisterTask(allocID, containerID, &check)
		healthRegistered = true
	}

	a.reconciler.Track(allocID, ts.HealthCheck != nil, restartPolicy)
	tracked = true

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
	if ts.HealthCheck == nil {
		if err := a.reconciler.ObserveHealth(allocID, true); err != nil {
			return fmt.Errorf("mark allocation healthy: %w", err)
		}
	}
	committed = true

	return nil
}

// ExecAllocation runs a command in an allocation task container and returns its output.
func (a *Agent) ExecAllocation(ctx context.Context, allocID, task string, command []string) (*api.AgentExecResponse, error) {
	a.mu.RLock()
	var containerID string
	for k, alloc := range a.allocations {
		if alloc.AllocationID == allocID && (task == "" || alloc.TaskName == task) {
			containerID = alloc.ContainerID
			_ = k
			break
		}
	}
	a.mu.RUnlock()
	if containerID == "" {
		return nil, fmt.Errorf("%w: %s", ErrAllocationNotFound, allocID)
	}
	stdout, stderr, exitCode, err := a.runtime.ExecOutput(ctx, containerID, command)
	if err != nil {
		return nil, fmt.Errorf("exec in container %s: %w", containerID, err)
	}
	return &api.AgentExecResponse{
		Stdout:   string(stdout),
		Stderr:   string(stderr),
		ExitCode: exitCode,
	}, nil
}

// CreateExecSession starts a persistent interactive terminal in an allocation task.
func (a *Agent) CreateExecSession(ctx context.Context, allocID, task string, command []string, term string, cols, rows uint32) (*api.ExecSessionResponse, error) {
	a.mu.RLock()
	var containerID, taskName string
	for _, alloc := range a.allocations {
		if alloc.AllocationID == allocID && (task == "" || alloc.TaskName == task) {
			containerID = alloc.ContainerID
			taskName = alloc.TaskName
			break
		}
	}
	a.mu.RUnlock()
	if containerID == "" {
		return nil, fmt.Errorf("%w: %s", ErrAllocationNotFound, allocID)
	}
	terminal, err := a.runtime.StartTerminal(ctx, containerID, command, term, cols, rows)
	if err != nil {
		return nil, fmt.Errorf("start terminal in container %s: %w", containerID, err)
	}
	sessionID := uuid.NewString()
	a.mu.Lock()
	a.execSessions[sessionID] = &execSession{AllocationID: allocID, Task: taskName, Terminal: terminal}
	a.mu.Unlock()
	return &api.ExecSessionResponse{ID: sessionID}, nil
}

// WriteExecSession writes raw bytes to an interactive terminal.
func (a *Agent) WriteExecSession(allocID, sessionID string, data []byte) error {
	a.mu.RLock()
	session := a.execSessions[sessionID]
	a.mu.RUnlock()
	if session == nil || session.AllocationID != allocID {
		return fmt.Errorf("%w: %s", ErrExecSessionNotFound, sessionID)
	}
	if _, err := session.Terminal.Write(data); err != nil {
		return fmt.Errorf("write exec session %s: %w", sessionID, err)
	}
	return nil
}

// ReadExecSession reads terminal bytes produced since offset.
func (a *Agent) ReadExecSession(allocID, sessionID string, offset int64) (*api.ExecSessionOutputResponse, error) {
	a.mu.RLock()
	session := a.execSessions[sessionID]
	a.mu.RUnlock()
	if session == nil || session.AllocationID != allocID {
		return nil, fmt.Errorf("%w: %s", ErrExecSessionNotFound, sessionID)
	}
	data, next, exited, exitCode, err := session.Terminal.Read(offset)
	if err != nil {
		return nil, fmt.Errorf("read exec session %s: %w", sessionID, err)
	}
	return &api.ExecSessionOutputResponse{
		DataBase64: base64.StdEncoding.EncodeToString(data),
		NextOffset: next,
		Exited:     exited,
		ExitCode:   exitCode,
	}, nil
}

// ResizeExecSession updates the terminal dimensions.
func (a *Agent) ResizeExecSession(ctx context.Context, allocID, sessionID string, cols, rows uint32) error {
	a.mu.RLock()
	session := a.execSessions[sessionID]
	a.mu.RUnlock()
	if session == nil || session.AllocationID != allocID {
		return fmt.Errorf("%w: %s", ErrExecSessionNotFound, sessionID)
	}
	if err := session.Terminal.Resize(ctx, cols, rows); err != nil {
		return fmt.Errorf("resize exec session %s: %w", sessionID, err)
	}
	return nil
}

// CloseExecSession terminates and forgets an interactive terminal.
func (a *Agent) CloseExecSession(ctx context.Context, allocID, sessionID string) error {
	a.mu.Lock()
	session := a.execSessions[sessionID]
	if session == nil || session.AllocationID != allocID {
		a.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrExecSessionNotFound, sessionID)
	}
	delete(a.execSessions, sessionID)
	a.mu.Unlock()
	if err := session.Terminal.Close(ctx); err != nil {
		return fmt.Errorf("close exec session %s: %w", sessionID, err)
	}
	return nil
}

func (a *Agent) closeExecSessionsForAllocation(ctx context.Context, allocID string) {
	a.mu.Lock()
	var sessions []runtime.TerminalSession
	for id, session := range a.execSessions {
		if session.AllocationID == allocID {
			sessions = append(sessions, session.Terminal)
			delete(a.execSessions, id)
		}
	}
	a.mu.Unlock()
	for _, session := range sessions {
		if err := session.Close(ctx); err != nil {
			a.log.Warn("close exec session", "allocation", allocID, "error", err)
		}
	}
}

// AllocationMetrics returns resource usage for all tasks in an allocation.
func (a *Agent) AllocationMetrics(ctx context.Context, allocID string) ([]api.AgentTaskMetrics, error) {
	a.mu.RLock()
	var tasks []Allocation
	for _, alloc := range a.allocations {
		if alloc.AllocationID == allocID {
			tasks = append(tasks, *alloc)
		}
	}
	a.mu.RUnlock()
	if len(tasks) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrAllocationNotFound, allocID)
	}
	result := make([]api.AgentTaskMetrics, 0, len(tasks))
	for _, task := range tasks {
		m, err := a.runtime.Metrics(ctx, task.ContainerID)
		if err != nil {
			a.log.Warn("metrics unavailable", "container", task.ContainerID, "error", err)
			continue
		}
		result = append(result, api.AgentTaskMetrics{
			Task:                task.TaskName,
			CPUUsageNanoseconds: m.CPUUsageNanoseconds,
			MemoryUsageBytes:    m.MemoryUsageBytes,
		})
	}
	return result, nil
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
	a.reconciler.BeginStop(allocID)
	persistStopErr := a.markAllocationStopping(allocID)
	if err := a.runtime.Stop(ctx, containerID); err != nil {
		return errors.Join(persistStopErr, fmt.Errorf("stop container %s: %w", containerID, err))
	}

	var errs []error
	a.closeExecSessionsForAllocation(ctx, alloc.AllocationID)
	a.health.DeregisterTask(allocID)
	if err := a.reconciler.Untrack(allocID); err != nil {
		errs = append(errs, fmt.Errorf("untrack allocation %s: %w", allocID, err))
	}

	if err := a.network.Detach(ctx, alloc.Network); err != nil {
		errs = append(errs, fmt.Errorf("detach allocation network: %w", err))
	}
	if err := a.runtime.Remove(ctx, containerID); err != nil {
		errs = append(errs, fmt.Errorf("remove container %s: %w", containerID, err))
	}
	if alloc.SecretDir != "" {
		if err := os.RemoveAll(alloc.SecretDir); err != nil {
			errs = append(errs, fmt.Errorf("remove secret files: %w", err))
		}
	}

	for _, p := range alloc.Ports {
		err := a.ports.Release(p)
		if err != nil {
			errs = append(errs, fmt.Errorf("release port %d: %w", p.HostPort, err))
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

	return persistStopErr
}

func prepareSecrets(allocID, taskName string, delivered []api.DeliveredSecret) (string, map[string]string, []*runtime.Mount, error) {
	env := map[string]string{}
	var taskSecrets []api.DeliveredSecret
	for _, secret := range delivered {
		if secret.Task == taskName {
			taskSecrets = append(taskSecrets, secret)
		}
	}
	if len(taskSecrets) == 0 {
		return "", env, nil, nil
	}
	dir, err := os.MkdirTemp("/dev/shm", "trellis-secret-"+filepath.Base(allocID)+"-")
	if err != nil {
		return "", nil, nil, fmt.Errorf("create memory-backed secret directory: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		_ = os.RemoveAll(dir)
		return "", nil, nil, err
	}
	var mounts []*runtime.Mount
	for i, secret := range taskSecrets {
		switch secret.Target {
		case spec.SecretTargetEnv:
			env[secret.Env] = string(secret.Value)
		case spec.SecretTargetFile:
			hostPath := filepath.Join(dir, fmt.Sprintf("secret-%d", i))
			mode := os.FileMode(secret.Mode)
			if mode == 0 {
				mode = 0o400
			}
			file, err := os.OpenFile(hostPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, mode)
			if err != nil {
				_ = os.RemoveAll(dir)
				return "", nil, nil, fmt.Errorf("create secret file: %w", err)
			}
			if _, err = file.Write(secret.Value); err == nil {
				err = file.Sync()
			}
			closeErr := file.Close()
			if err == nil {
				err = closeErr
			}
			if err != nil {
				_ = os.RemoveAll(dir)
				return "", nil, nil, fmt.Errorf("write secret file: %w", err)
			}
			mounts = append(mounts, &runtime.Mount{HostPath: hostPath, ContainerPath: secret.Path, ReadOnly: true})
		default:
			_ = os.RemoveAll(dir)
			return "", nil, nil, fmt.Errorf("unsupported secret target")
		}
	}
	return dir, env, mounts, nil
}

// OnHealthy and OnUnhealthy are observation callbacks from the health manager.
// They intentionally do not mutate allocation status directly; lifecycle state
// transitions are centralized in the allocation reconciler.
func (a *Agent) OnHealthy(_ context.Context, allocID string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if allocation := a.allocations[allocID]; allocation != nil {
		allocation.Health = "healthy"
		return a.persistAllocation(allocation)
	}
	return nil
}

// OnUnhealthy handles an unhealthy allocation.
func (a *Agent) OnUnhealthy(_ context.Context, allocID string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if allocation := a.allocations[allocID]; allocation != nil {
		allocation.Health = "unhealthy"
		return a.persistAllocation(allocation)
	}
	return nil
}

// OnReconciledStatus records reconciled allocation status.
func (a *Agent) OnReconciledStatus(allocID, status string) {
	a.mu.Lock()
	if alloc := a.allocations[allocID]; alloc != nil {
		if status == "healthy" || status == "unhealthy" {
			alloc.Health = status
		} else {
			alloc.Status = status
		}
		if err := a.persistAllocation(alloc); err != nil {
			a.log.Error("persist reconciled allocation", "allocation", alloc.AllocationID, "error", err)
		}
	}
	a.mu.Unlock()
}

// OnRestartState records allocation restart state.
func (a *Agent) OnRestartState(allocID string, attempts int, window time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if allocation := a.allocations[allocID]; allocation != nil {
		allocation.RestartAttempts, allocation.RestartWindow = attempts, window
		if err := a.persistAllocation(allocation); err != nil {
			a.log.Error("persist restart tracking", "allocation", allocation.AllocationID, "error", err)
		}
	}
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
			a.mu.RLock()
			actual := make([]api.AllocationStatus, 0, len(a.allocations))
			for _, alloc := range a.allocations {
				ports := make([]api.PortMapping, 0, len(alloc.Ports))
				for _, p := range alloc.Ports {
					ports = append(ports, api.PortMapping{HostPort: p.HostPort, ContainerPort: p.ContainerPort})
				}
				actual = append(actual, api.AllocationStatus{ID: alloc.AllocationID, Generation: alloc.Generation, Task: alloc.TaskName, Address: allocationNetworkAddress(alloc), Phase: lifecycle.Phase(alloc.Status), Health: lifecycle.Health(alloc.Health), Ports: ports})
			}
			a.mu.RUnlock()
			response, err := a.server.SendHeartbeat(ctx, a.nodeID, &client.Heartbeat{
				NodeID:       a.nodeID,
				Timestamp:    time.Now(),
				Allocations:  actual,
				Volumes:      a.volumes.AvailableHostVolumes(),
				Capabilities: a.nodeInfo.Capabilities,
				Version:      a.version,
			})
			if err != nil {
				a.log.Error("send heartbeat failed", "error", err)
				registered = false
			} else {
				a.reconcileDesired(ctx, response)
			}
		}
	}
}
