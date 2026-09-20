package server

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/client"
	"github.com/clofour/trellis/internal/lifecycle"
	"github.com/clofour/trellis/internal/network"
	"github.com/clofour/trellis/internal/spec"
	"github.com/google/uuid"
)

// ActionType identifies a reconciliation operation.
type ActionType string

const (
	// ActionStart starts or updates an allocation.
	ActionStart ActionType = "start"
	// ActionStop stops an allocation.
	ActionStop ActionType = "stop"
)

// Action describes one allocation reconciliation operation.
type Action struct {
	Type       ActionType
	Allocation *Allocation
}

const (
	allocationLossTimeout = 45 * time.Second
	leaderRecoveryGrace   = 30 * time.Second
	maxExecutionAttempts  = 8
)

func retryDelay(id string, attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	shift := min(attempt-1, 6)
	base := time.Second * time.Duration(1<<shift)
	h := sha256.Sum256([]byte(fmt.Sprintf("%s:%d", id, attempt)))
	return base + time.Duration(binary.BigEndian.Uint16(h[:2])%500)*time.Millisecond
}

func agentOperationCode(err error) api.OperationCode {
	var operation *client.AgentOperationError
	if errors.As(err, &operation) {
		return operation.Response.Code
	}
	return ""
}

func workloadAPIAddress(serverAddr string) (string, error) {
	_, port, err := net.SplitHostPort(strings.TrimSpace(serverAddr))
	if err != nil {
		return "", fmt.Errorf("control-plane advertise address %q: %w", serverAddr, err)
	}
	return net.JoinHostPort("trellis", port), nil
}

func updateStrategy(job *Job, groupName string) spec.UpdateStrategy {
	for i := range job.Spec.TaskGroups {
		if job.Spec.TaskGroups[i].Name == groupName {
			if job.Spec.TaskGroups[i].Update != nil && job.Spec.TaskGroups[i].Update.Strategy != "" {
				return job.Spec.TaskGroups[i].Update.Strategy
			}
			return spec.UpdateRecreate
		}
	}
	return spec.UpdateRecreate
}

func maxParallel(job *Job, groupName string) int {
	for i := range job.Spec.TaskGroups {
		if job.Spec.TaskGroups[i].Name == groupName {
			if job.Spec.TaskGroups[i].Update != nil && job.Spec.TaskGroups[i].Update.MaxParallel > 0 {
				return job.Spec.TaskGroups[i].Update.MaxParallel
			}
			return 1
		}
	}
	return 1
}

// Reconcile converges the in-memory allocation set on the latest job specs.
func (s *Server) Reconcile(ctx context.Context) {
	start := time.Now()
	s.reconcileMu.Lock()
	defer func() {
		s.reconcileMu.Unlock()
		if s.metrics != nil {
			s.metrics.ReconcileDuration.Observe(time.Since(start).Seconds())
		}
	}()
	now := s.now().UTC()
	volumeOwners, err := s.state.ListVolumeRegistrations(ctx)
	if err != nil {
		s.log.Error("load volume registrations", "error", err)
		return
	}
	s.mu.RLock()
	networkNamespaces := make([]string, 0)
	seenNetworkNamespaces := make(map[string]struct{})
	addNetworkNamespace := func(namespace string) {
		if _, exists := seenNetworkNamespaces[namespace]; exists {
			return
		}
		networkNamespaces = append(networkNamespaces, namespace)
		seenNetworkNamespaces[namespace] = struct{}{}
	}
	for _, job := range s.jobs {
		for i := range job.Spec.TaskGroups {
			group := &job.Spec.TaskGroups[i]
			if group.Count > 0 && spec.GroupUsesWireGuard(group) {
				addNetworkNamespace(job.Spec.Namespace)
				break
			}
		}
	}
	for _, allocation := range s.allocations {
		allocation.mu.Lock()
		active := allocation.Phase != lifecycle.PhaseStopped && allocation.Phase != lifecycle.PhaseFailed && allocation.Phase != lifecycle.PhaseLost
		if active && tasksUseWireGuard(allocation.Tasks) {
			addNetworkNamespace(allocation.Namespace)
		}
		allocation.mu.Unlock()
	}
	s.mu.RUnlock()
	networkPorts, err := s.ensureNetworkPortRegistrations(ctx, networkNamespaces)
	if err != nil {
		s.log.Error("prepare namespace WireGuard ports", "error", err)
		return
	}
	persistedVolumeOwners := make(map[string]uuid.UUID, len(volumeOwners))
	for key, owner := range volumeOwners {
		persistedVolumeOwners[key] = owner
	}
	var newAllocations []*Allocation
	s.mu.Lock()
	s.networkPorts = networkPorts
	for _, node := range s.nodes {
		if node.Status == NodeStatusHealthy && now.Sub(node.LastHeartbeat) > 3*heartbeatInterval {
			node.Status = NodeStatusUnhealthy
		}
	}
	var actions []Action
	valid := make([]*Allocation, 0, len(s.allocations))
	for _, allocation := range s.allocations {
		allocation.mu.Lock()
		job := s.jobs[jobKey(allocation.Namespace, allocation.JobName)]
		if allocation.Phase == lifecycle.PhasePending {
			valid = append(valid, allocation)
			allocation.mu.Unlock()
			continue
		}
		if allocation.Phase == lifecycle.PhaseStopped || allocation.Phase == lifecycle.PhaseFailed || allocation.Phase == lifecycle.PhaseLost {
			allocation.mu.Unlock()
			continue
		}
		if allocation.NextRetryAt != nil && now.Before(*allocation.NextRetryAt) {
			valid = append(valid, allocation)
			allocation.mu.Unlock()
			continue
		}
		if job == nil {
			actions = append(actions, Action{Type: ActionStop, Allocation: allocation})
			allocation.mu.Unlock()
			continue
		}
		if allocation.JobRevision < job.Revision {
			strategy := updateStrategy(job, allocation.TaskGroupName)
			switch strategy {
			case spec.UpdateRolling:
				if !allocation.Draining {
					allocation.Draining = true
					_ = s.state.PutAllocation(context.WithoutCancel(ctx), allocation)
				}
				if allocation.Node == nil || allocation.Node.Status != NodeStatusHealthy {
					if now.Sub(s.leaderSince) >= leaderRecoveryGrace && allocation.Node != nil && !allocation.Node.LastHeartbeat.IsZero() && now.Sub(allocation.Node.LastHeartbeat) >= allocationLossTimeout {
						_ = allocation.Transition(lifecycle.PhaseLost, now, "node_unavailable", "node did not re-register before the allocation loss timeout")
						_ = s.state.PutAllocation(context.WithoutCancel(ctx), allocation)
					}
					allocation.mu.Unlock()
					continue
				}
				valid = append(valid, allocation)
				allocation.mu.Unlock()
				continue
			default:
				actions = append(actions, Action{Type: ActionStop, Allocation: allocation})
				allocation.mu.Unlock()
				continue
			}
		}
		if allocation.Node == nil || allocation.Node.Status != NodeStatusHealthy {
			if now.Sub(s.leaderSince) >= leaderRecoveryGrace && allocation.Node != nil && !allocation.Node.LastHeartbeat.IsZero() && now.Sub(allocation.Node.LastHeartbeat) >= allocationLossTimeout {
				_ = allocation.Transition(lifecycle.PhaseLost, now, "node_unavailable", "node did not re-register before the allocation loss timeout")
				_ = s.state.PutAllocation(context.WithoutCancel(ctx), allocation)
			}
			allocation.mu.Unlock()
			continue
		}
		if allocation.Phase == lifecycle.PhasePlaced || allocation.Phase == lifecycle.PhaseStarting || allocation.Phase == lifecycle.PhaseStopping {
			actionType := ActionStart
			if allocation.Phase == lifecycle.PhaseStopping {
				actionType = ActionStop
			}
			actions = append(actions, Action{Type: actionType, Allocation: allocation})
		}
		valid = append(valid, allocation)
		allocation.mu.Unlock()
	}

	jobKeys := make([]string, 0, len(s.jobs))
	for key := range s.jobs {
		jobKeys = append(jobKeys, key)
	}
	sort.Strings(jobKeys)
	limits := s.jobLimits
	if limits == (spec.Limits{}) {
		limits = spec.DefaultLimits()
	}
	namespaceDesired := make(map[string]int64)
	for _, key := range jobKeys {
		job := s.jobs[key]
		if err := spec.Canonicalize(job.Spec, limits); err != nil {
			s.log.Error("skip invalid job during reconciliation", "job", key, "error", err)
			continue
		}
		jobName := job.Spec.Name
		namespace := job.Spec.Namespace
		desired := desiredAllocations(job.Spec)
		if namespaceDesired[namespace]+desired > int64(limits.MaxDesiredAllocationsPerNamespace) {
			s.log.Error("skip job exceeding namespace allocation limit during reconciliation", "job", key, "namespace", namespace, "limit", limits.MaxDesiredAllocationsPerNamespace)
			continue
		}
		namespaceDesired[namespace] += desired
		for _, group := range job.Spec.TaskGroups {
			var current []*Allocation
			var pending []*Allocation
			var draining []*Allocation
			for _, alloc := range valid {
				if alloc.Namespace == namespace && alloc.JobName == jobName && alloc.TaskGroupName == group.Name {
					if alloc.Draining {
						draining = append(draining, alloc)
					} else if alloc.Phase == lifecycle.PhasePending {
						pending = append(pending, alloc)
					} else {
						current = append(current, alloc)
					}
				}
			}
			for len(current) > group.Count {
				actions = append(actions, Action{Type: ActionStop, Allocation: current[len(current)-1]})
				current = current[:len(current)-1]
			}
			if len(draining) > 0 {
				healthyNew := 0
				for _, alloc := range current {
					if alloc.Phase == lifecycle.PhaseRunning && alloc.Health == lifecycle.HealthHealthy {
						healthyNew++
					}
				}
				drainsToStop := min(max(healthyNew+len(draining)-group.Count, 0), len(draining))
				for i := 0; i < drainsToStop; i++ {
					actions = append(actions, Action{Type: ActionStop, Allocation: draining[i]})
				}
			}
			deficit := group.Count - len(current)
			if deficit > 0 {
				strategy := updateStrategy(job, group.Name)
				if strategy == spec.UpdateRolling && len(draining) > 0 {
					parallel := maxParallel(job, group.Name)
					inFlight := 0
					for _, alloc := range current {
						if alloc.Phase != lifecycle.PhaseRunning || alloc.Health != lifecycle.HealthHealthy {
							inFlight++
						}
					}
					allowed := parallel - inFlight
					if allowed < deficit {
						deficit = allowed
					}
					if deficit < 0 {
						deficit = 0
					}
				}
			}
			requiredCapabilities := spec.GroupRequiredCapabilities(&group)
			placements := Schedule(&PlacementIntent{Namespace: namespace, JobName: jobName, TaskGroupName: group.Name, Count: deficit, Nodes: s.nodePointers(), Allocations: valid, Tasks: group.Tasks, Constraints: group.Constraints, RequiredCapabilities: requiredCapabilities, VolumeOwners: volumeOwners})
			for i, placement := range placements {
				node := s.nodes[placement.NodeID]
				if i < len(pending) {
					allocation := pending[i]
					allocation.Node = node
					_ = allocation.Transition(lifecycle.PhasePlaced, now, "", "")
					actions = append(actions, Action{Type: ActionStart, Allocation: allocation})
					continue
				}
				name := fmt.Sprintf("%s-%s-%s-%s", namespace, jobName, group.Name, uuid.NewString()[:8])
				allocation := &Allocation{ID: name, Namespace: namespace, JobName: jobName, TaskGroupName: group.Name, Tasks: group.Tasks, Node: node, Generation: 1, JobRevision: job.Revision, Phase: lifecycle.PhasePlaced, Health: lifecycle.HealthUnknown, Diagnostic: lifecycle.Diagnostic{CreatedAt: now, TransitionedAt: now}}
				actions = append(actions, Action{Type: ActionStart, Allocation: allocation})
				newAllocations = append(newAllocations, allocation)
				valid = append(valid, allocation)
			}
			if len(placements) == 0 && len(pending) == 0 && len(requiredCapabilities) > 0 && noCompatibleCapabilityNode(s.nodePointers(), group.Constraints, group.Tasks, volumeOwners, namespace, requiredCapabilities) {
				for i := 0; i < deficit; i++ {
					name := fmt.Sprintf("%s-%s-%s-%s", namespace, jobName, group.Name, uuid.NewString()[:8])
					allocation := &Allocation{ID: name, Namespace: namespace, JobName: jobName, TaskGroupName: group.Name, Tasks: group.Tasks, Generation: 1, JobRevision: job.Revision, Phase: lifecycle.PhasePending, Health: lifecycle.HealthUnknown, Diagnostic: lifecycle.Diagnostic{CreatedAt: now, TransitionedAt: now, Reason: "missing_capability", Message: fmt.Sprintf("no eligible node supports required capabilities: %s", strings.Join(capabilityNames(requiredCapabilities), ", "))}}
					newAllocations = append(newAllocations, allocation)
					valid = append(valid, allocation)
				}
			}
		}
	}
	s.mu.Unlock()

	for key, owner := range volumeOwners {
		if _, exists := persistedVolumeOwners[key]; exists {
			continue
		}
		namespace, name, ok := strings.Cut(key, "/")
		if !ok || namespace == "" || name == "" {
			s.log.Error("invalid volume registration key", "key", key)
			return
		}
		if err := s.state.PutVolumeRegistration(ctx, &VolumeRegistration{Namespace: namespace, Name: name, NodeID: owner}); err != nil {
			s.log.Error("persist volume registration", "volume", key, "node_id", owner, "error", err)
			return
		}
	}
	for _, allocation := range newAllocations {
		if allocation.Phase != lifecycle.PhasePending {
			continue
		}
		if err := s.state.PutAllocation(ctx, allocation); err != nil {
			s.log.Error("persist pending allocation", "allocation", allocation.ID, "error", err)
			return
		}
	}
	if len(newAllocations) > 0 {
		s.mu.Lock()
		s.allocations = append(s.allocations, newAllocations...)
		s.mu.Unlock()
	}

	for i := range actions {
		if err := s.Execute(ctx, &actions[i]); err != nil {
			s.log.Error("reconcile action failed", "action", actions[i].Type, "allocation", actions[i].Allocation.ID, "error", err)
		}
	}
	s.refreshCatalog()
}

func tasksUseWireGuard(tasks []spec.TaskSpec) bool {
	for i := range tasks {
		if tasks[i].Networking != nil && tasks[i].Networking.Mode == spec.TaskNetworkWireGuard {
			return true
		}
	}
	return false
}

func noCompatibleCapabilityNode(nodes []*Node, constraints []spec.ConstraintSpec, tasks []spec.TaskSpec, volumeOwners map[string]uuid.UUID, namespace string, required []spec.NodeCapability) bool {
	hasCandidate := false
	for _, node := range nodes {
		if node.Status != NodeStatusHealthy || !nodeMatchesConstraints(node, constraints) || !nodeHasTaskVolumes(node.ID, namespace, tasks, volumeOwners) {
			continue
		}
		hasCandidate = true
		if nodeHasCapabilities(node, required) {
			return false
		}
	}
	return hasCandidate
}

func capabilityNames(capabilities []spec.NodeCapability) []string {
	names := make([]string, len(capabilities))
	for i, capability := range capabilities {
		names[i] = string(capability)
	}
	return names
}

func (s *Server) nodePointers() []*Node {
	nodes := make([]*Node, 0, len(s.nodes))
	for _, node := range s.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

// Execute performs a reconciliation action.
func (s *Server) Execute(ctx context.Context, action *Action) error {
	alloc := action.Allocation

	// Follow the server locking contract: when both locks are needed, acquire the
	// global server lock before the allocation lock. In particular, do not hold
	// allocation.mu while waiting for mu; readers such as metrics and allocation
	// listing acquire them in the opposite direction and would otherwise be able
	// to deadlock the whole control plane once a writer (for example a heartbeat)
	// is queued on mu.
	s.mu.RLock()
	alloc.mu.Lock()
	defer alloc.mu.Unlock()

	serverLocked := true
	unlockServer := func() {
		if serverLocked {
			s.mu.RUnlock()
			serverLocked = false
		}
	}
	defer unlockServer()

	now := s.now().UTC()
	address := fmt.Sprintf("%s:%d", alloc.Node.Host, alloc.Node.Port)
	epoch := s.controlEpoch
	serverAddr := s.serverAddr
	nodeStatus := alloc.Node.Status

	switch action.Type {
	case ActionStart:
		job := s.jobs[jobKey(alloc.Namespace, alloc.JobName)]
		if job == nil {
			return fmt.Errorf("job %s was deleted before allocation start", alloc.JobName)
		}
		var groupRuntime string
		var groupAPIAccess *spec.APIAccessSpec
		var groupRestart *spec.RestartPolicySpec
		var groupUsesWireGuard bool
		for _, group := range job.Spec.TaskGroups {
			if group.Name == alloc.TaskGroupName {
				groupRuntime = string(group.Runtime)
				groupAPIAccess = group.APIAccess
				groupRestart = group.Restart
				groupUsesWireGuard = spec.GroupUsesWireGuard(&group)
				break
			}
		}
		request := &api.AllocationRequest{AllocationID: alloc.ID, Generation: alloc.Generation, JobRevision: alloc.JobRevision, Epoch: epoch, Namespace: alloc.Namespace, JobName: alloc.JobName, GroupName: alloc.TaskGroupName, Tasks: alloc.Tasks, Runtime: groupRuntime, Restart: groupRestart}
		if groupUsesWireGuard {
			plan, err := s.networkPlan(alloc.Namespace, alloc.Node)
			if err != nil {
				return err
			}
			request.NetworkPlan = plan
		}

		// Everything below may perform storage or network I/O. Release mu while
		// retaining allocation.mu so the allocation lifecycle remains serialized.
		unlockServer()

		for _, task := range request.Tasks {
			for _, ref := range task.Secrets {
				if s.secrets == nil {
					return fmt.Errorf("secret %s is unavailable: secrets are not configured", ref.Name)
				}
				value, version, err := s.secrets.Resolve(ctx, alloc.Namespace, ref.Name)
				if err != nil {
					return fmt.Errorf("secret %s is unavailable", ref.Name)
				}
				request.Secrets = append(request.Secrets, api.DeliveredSecret{Task: task.Name, Name: ref.Name, Version: version, Target: ref.Target, Env: ref.Env, Path: ref.Path, Mode: ref.Mode, Value: value})
			}
		}
		defer func() {
			for i := range request.Secrets {
				clear(request.Secrets[i].Value)
			}
		}()
		if groupAPIAccess != nil {
			token, err := s.apiAccessToken(ctx, groupAPIAccess, alloc.Namespace)
			if err != nil {
				return err
			}
			apiAddr, err := workloadAPIAddress(serverAddr)
			if err != nil {
				return err
			}
			request.EnvOverrides = map[string]string{
				"TRELLIS_TOKEN":     token,
				"TRELLIS_ADDR":      apiAddr,
				"TRELLIS_NAMESPACE": alloc.Namespace,
			}
			_, port, err := net.SplitHostPort(serverAddr)
			if err != nil {
				return fmt.Errorf("control-plane advertise address %q: %w", serverAddr, err)
			}
			apiPort, err := strconv.Atoi(port)
			if err != nil || apiPort < 1 || apiPort > 65535 {
				return fmt.Errorf("control-plane advertise port %q is invalid", port)
			}
			if request.NetworkPlan != nil {
				request.NetworkPlan.APIPort = apiPort
			}
			if caCert, _, caErr := s.ClusterCA(); caErr == nil && caCert != "" {
				request.EnvOverrides["TRELLIS_CA_CERT"] = caCert
			}
		}
		hashInput := *request
		hashInput.Epoch, hashInput.ExecutionHash = 0, ""
		hashInput.Secrets = append([]api.DeliveredSecret(nil), request.Secrets...)
		for i := range hashInput.Secrets {
			hashInput.Secrets[i].Value = nil
		}
		if hashInput.EnvOverrides != nil {
			hashInput.EnvOverrides = make(map[string]string, len(request.EnvOverrides))
			for key, value := range request.EnvOverrides {
				if key == "TRELLIS_TOKEN" {
					digest := sha256.Sum256([]byte(value))
					hashInput.EnvOverrides[key] = hex.EncodeToString(digest[:])
					continue
				}
				hashInput.EnvOverrides[key] = value
			}
		}
		raw, _ := json.Marshal(hashInput)
		hash := sha256.Sum256(raw)
		request.ExecutionHash = hex.EncodeToString(hash[:])
		if alloc.Phase == lifecycle.PhasePlaced || alloc.Phase == lifecycle.PhaseStopped || alloc.Phase == lifecycle.PhaseFailed || alloc.Phase == lifecycle.PhaseLost {
			if err := alloc.Transition(lifecycle.PhaseStarting, now, "", ""); err != nil {
				return err
			}
		}
		if err := s.state.PutAllocation(ctx, alloc); err != nil {
			return fmt.Errorf("persist allocation: %w", err)
		}
		if err := s.client.RunAllocation(ctx, address, request); err != nil {
			if code := agentOperationCode(err); code == api.OperationStaleEpoch {
				return err
			} else if code == api.OperationStaleGeneration || code == api.OperationConflict {
				_ = alloc.Transition(lifecycle.PhaseFailed, now, string(code), err.Error())
				alloc.NextRetryAt = nil
				_ = s.state.PutAllocation(context.WithoutCancel(ctx), alloc)
				return err
			}
			alloc.Attempt++
			alloc.Reason, alloc.Message = "agent_start_failed", err.Error()
			if alloc.Attempt >= maxExecutionAttempts {
				_ = alloc.Transition(lifecycle.PhaseFailed, now, "retry_limit", err.Error())
				alloc.NextRetryAt = nil
			} else {
				next := now.Add(retryDelay(alloc.ID, alloc.Attempt))
				alloc.NextRetryAt = &next
			}
			_ = s.state.PutAllocation(context.WithoutCancel(ctx), alloc)
			return err
		}
		alloc.Attempt, alloc.NextRetryAt = 0, nil
		_ = alloc.Transition(lifecycle.PhaseRunning, now, "", "")
		if err := s.state.PutAllocation(ctx, alloc); err != nil {
			return fmt.Errorf("persist running allocation: %w", err)
		}
	case ActionStop:
		unlockServer()

		if alloc.Phase != lifecycle.PhaseStopping {
			if err := alloc.Transition(lifecycle.PhaseStopping, now, "", ""); err != nil {
				return err
			}
			if err := s.state.PutAllocation(ctx, alloc); err != nil {
				return err
			}
		}
		if nodeStatus != NodeStatusHealthy && nodeStatus != NodeStatusDraining {
			return fmt.Errorf("node %s is unavailable for allocation stop", alloc.Node.ID)
		}
		if err := s.client.StopAllocation(ctx, address, &api.StopAllocationRequest{AllocationID: alloc.ID, Generation: alloc.Generation, Epoch: epoch}); err != nil {
			if code := agentOperationCode(err); code == api.OperationStaleEpoch || code == api.OperationStaleGeneration {
				return err
			}
			alloc.Attempt++
			alloc.Reason, alloc.Message = "agent_stop_failed", err.Error()
			next := now.Add(retryDelay(alloc.ID, alloc.Attempt))
			alloc.NextRetryAt = &next
			_ = s.state.PutAllocation(context.WithoutCancel(ctx), alloc)
			return err
		}
		alloc.Attempt, alloc.NextRetryAt = 0, nil
		_ = alloc.Transition(lifecycle.PhaseStopped, now, "", "")
		if err := s.state.PutAllocation(ctx, alloc); err != nil {
			return fmt.Errorf("persist stopped allocation: %w", err)
		}
	default:
		unlockServer()
	}
	return nil
}

func (s *Server) networkPlan(namespace string, target *Node) (*network.Plan, error) {
	slot, ok := s.networkPorts[namespace]
	if !ok {
		return nil, fmt.Errorf("namespace %q has no WireGuard port registration", namespace)
	}
	listenPort, err := namespaceWireGuardPort(target.WireGuardPortBase, target.WireGuardPortCount, slot)
	if err != nil {
		return nil, fmt.Errorf("node %s WireGuard port range: %w", target.ID, err)
	}
	local := namespaceNodeSubnet(s.networkPool, namespace, target.ID)
	plan := &network.Plan{CIDR: local.String(), Gateway: local.Addr().Next().String(), WireGuardAddress: wireGuardAddress(namespace, target.ID), ListenPort: listenPort}
	seen := map[string]uuid.UUID{local.String(): target.ID}
	for _, node := range s.nodes {
		if node.ID == target.ID || node.WireGuardPublicKey == "" || node.WireGuardEndpoint == "" {
			continue
		}
		if _, err := namespaceWireGuardPort(node.WireGuardPortBase, node.WireGuardPortCount, slot); err != nil {
			return nil, fmt.Errorf("peer node %s WireGuard port range: %w", node.ID, err)
		}
		endpoint, err := namespaceWireGuardEndpoint(node.WireGuardEndpoint, slot)
		if err != nil {
			return nil, fmt.Errorf("peer node %s WireGuard endpoint: %w", node.ID, err)
		}
		subnet := namespaceNodeSubnet(s.networkPool, namespace, node.ID)
		if previous, ok := seen[subnet.String()]; ok && previous != node.ID {
			return nil, fmt.Errorf("automatic network subnet collision between nodes %s and %s", previous, node.ID)
		}
		seen[subnet.String()] = node.ID
		plan.Peers = append(plan.Peers, network.PeerPlan{PublicKey: node.WireGuardPublicKey, Endpoint: endpoint, AllowedIPs: []string{subnet.String()}})
	}
	sort.Slice(plan.Peers, func(i, j int) bool {
		return plan.Peers[i].PublicKey < plan.Peers[j].PublicKey
	})
	for _, job := range s.jobs {
		if job.Spec.Namespace == namespace {
			continue
		}
		usesWireGuard := false
		for i := range job.Spec.TaskGroups {
			if spec.GroupUsesWireGuard(&job.Spec.TaskGroups[i]) {
				usesWireGuard = true
				break
			}
		}
		if !usesWireGuard {
			continue
		}
		for _, node := range s.nodes {
			other := namespaceNodeSubnet(s.networkPool, job.Spec.Namespace, node.ID)
			if owner, exists := seen[other.String()]; exists {
				return nil, fmt.Errorf("automatic network subnet %s for namespace %q conflicts with namespace %q on node %s", other, job.Spec.Namespace, namespace, owner)
			}
		}
	}
	return plan, nil
}

func namespaceWireGuardPort(base, count, slot int) (int, error) {
	if base < 1 || base > 65535 {
		return 0, fmt.Errorf("base port %d is invalid", base)
	}
	if count < 1 || slot < 0 || slot >= count {
		return 0, fmt.Errorf("slot %d is outside advertised range of %d ports", slot, count)
	}
	port := base + slot
	if port > 65535 {
		return 0, fmt.Errorf("base port %d plus slot %d exceeds 65535", base, slot)
	}
	return port, nil
}

func namespaceWireGuardEndpoint(endpoint string, slot int) (string, error) {
	host, rawPort, err := net.SplitHostPort(endpoint)
	if err != nil {
		return "", err
	}
	base, err := strconv.Atoi(rawPort)
	if err != nil {
		return "", fmt.Errorf("invalid base port %q", rawPort)
	}
	port := base + slot
	if port < 1 || port > 65535 {
		return "", fmt.Errorf("base port %d plus slot %d is outside valid UDP port range", base, slot)
	}
	return net.JoinHostPort(host, strconv.Itoa(port)), nil
}

func namespaceNodeSubnet(pool netip.Prefix, namespace string, node uuid.UUID) netip.Prefix {
	h := sha256.Sum256(append([]byte(namespace+"\x00"), node[:]...))
	base := binary.BigEndian.Uint32(pool.Addr().AsSlice())
	available := uint32(1) << uint32(24-pool.Bits())
	index := binary.BigEndian.Uint32(h[:4]) % available
	b := base + index*256
	return netip.PrefixFrom(netip.AddrFrom4([4]byte{byte(b >> 24), byte(b >> 16), byte(b >> 8), byte(b)}), 24)
}

func wireGuardAddress(namespace string, node uuid.UUID) string {
	h := sha256.Sum256(append([]byte(namespace+"wg"), node[:]...))
	return fmt.Sprintf("169.254.%d.%d/32", h[0], max(byte(1), h[1]))
}
