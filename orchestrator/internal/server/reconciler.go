package server

import (
	"bytes"
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
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/internal/api"
	"github.com/overfold/trellis/internal/client"
	"github.com/overfold/trellis/internal/lifecycle"
	"github.com/overfold/trellis/internal/network"
	"github.com/overfold/trellis/internal/spec"
)

// ActionType identifies a reconciliation operation.
type ActionType string

const (
	// ActionStart starts or updates an allocation.
	ActionStart ActionType = "start"
	// ActionDrain suppresses local restarts before a later stop.
	ActionDrain ActionType = "drain"
	// ActionResume redelivers a persisted resume the agent has not acknowledged.
	ActionResume ActionType = "resume"
	// ActionStop stops an allocation.
	ActionStop ActionType = "stop"
	// ActionStopObserved removes a node-observed allocation generation that is
	// absent from control-plane desired state.
	ActionStopObserved ActionType = "stop_observed"
)

// Action describes one allocation reconciliation operation.
type Action struct {
	Type       ActionType
	Allocation *Allocation
	Node       *Node
	ID         string
	Generation uint64
}

const (
	// DefaultAllocationLossTimeout is how long a node must go without a
	// heartbeat before its allocations become lost, unless configured.
	DefaultAllocationLossTimeout = 45 * time.Second
	// MinAllocationLossTimeout keeps the loss timeout at or above the point
	// where a silent node is marked unhealthy (three heartbeat intervals).
	MinAllocationLossTimeout = 3 * heartbeatInterval
	// MaxAllocationLossTimeout bounds the configurable loss timeout.
	MaxAllocationLossTimeout = 24 * time.Hour
)

// ValidateAllocationLossTimeout checks an operator-configured loss timeout.
func ValidateAllocationLossTimeout(timeout time.Duration) error {
	if timeout < MinAllocationLossTimeout || timeout > MaxAllocationLossTimeout {
		return fmt.Errorf("allocation loss timeout %s must be between %s and %s", timeout, MinAllocationLossTimeout, MaxAllocationLossTimeout)
	}
	return nil
}

const (
	leaderRecoveryGrace           = 30 * time.Second
	maxExecutionAttempts          = 8
	networkPlanBaseTimeout        = 15 * time.Second
	networkPlanPeerTimeoutBudget  = 25 * time.Millisecond
	networkPlanRouteTimeoutBudget = 100 * time.Millisecond
	networkPlanRepairInterval     = 5 * time.Minute
	maxConcurrentReconcileActions = 32
)

func networkPlanOperationTimeout(plan *network.Plan, attempt int) time.Duration {
	timeout := networkPlanBaseTimeout
	if plan != nil {
		timeout += time.Duration(len(plan.Peers)) * networkPlanPeerTimeoutBudget
		for _, peer := range plan.Peers {
			timeout += time.Duration(len(peer.AllowedIPs)) * networkPlanRouteTimeoutBudget
		}
	}
	const maxDuration = time.Duration(1<<63 - 1)
	for i := 0; i < attempt; i++ {
		if timeout > maxDuration/2 {
			return maxDuration
		}
		timeout *= 2
	}
	return timeout
}

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

func cloneAllocationForReconcile(allocation *Allocation) (*Allocation, error) {
	raw, err := json.Marshal(allocation)
	if err != nil {
		return nil, err
	}
	var clone Allocation
	if err := json.Unmarshal(raw, &clone); err != nil {
		return nil, err
	}
	if allocation.Events != nil {
		clone.Events = &lifecycle.RingBuffer{}
		for _, event := range allocation.Events.Entries() {
			clone.Events.Append(event)
		}
	}
	return &clone, nil
}

func applyReconciledAllocation(allocation, update *Allocation, node *Node) {
	allocation.Phase = update.Phase
	allocation.Diagnostic = update.Diagnostic
	allocation.Node = node
	allocation.Draining = update.Draining
	allocation.DrainSequence = update.DrainSequence
	allocation.DrainReason = update.DrainReason
	allocation.Events = update.Events
}

func (s *Server) persistAllocationUpdate(ctx context.Context, allocation *Allocation, update func(*Allocation) error) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	s.mu.RLock()
	allocation.mu.Lock()
	next, err := cloneAllocationForReconcile(allocation)
	allocation.mu.Unlock()
	s.mu.RUnlock()
	if err != nil {
		return err
	}
	if err := update(next); err != nil {
		return err
	}
	if err := s.state.PutAllocation(ctx, next); err != nil {
		return err
	}
	s.mu.RLock()
	allocation.mu.Lock()
	applyAllocationSnapshot(allocation, next)
	allocation.mu.Unlock()
	s.mu.RUnlock()
	return nil
}

func sameAllocationState(a, b *Allocation) (bool, error) {
	aRaw, err := json.Marshal(a)
	if err != nil {
		return false, err
	}
	bRaw, err := json.Marshal(b)
	if err != nil {
		return false, err
	}
	return bytes.Equal(aRaw, bRaw), nil
}

// Reconcile converges the in-memory allocation set on the latest job specs.
func (s *Server) Reconcile(ctx context.Context) {
	start := time.Now()
	s.reconcileMu.Lock()
	s.mutationMu.Lock()
	mutationLocked := true
	defer func() {
		if mutationLocked {
			s.mutationMu.Unlock()
		}
		s.reconcileMu.Unlock()
		if s.metrics != nil {
			s.metrics.ReconcileDuration.Observe(time.Since(start).Seconds())
		}
	}()
	s.mu.RLock()
	registeredNodes := make(map[uuid.UUID]struct{}, len(s.nodes))
	for nodeID := range s.nodes {
		registeredNodes[nodeID] = struct{}{}
	}
	s.mu.RUnlock()
	s.client.RetainNodes(registeredNodes)
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
		if !errors.Is(err, errNetworkPortExhausted) {
			return
		}
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
	jobKeys := make([]string, 0, len(s.jobs))
	for key := range s.jobs {
		jobKeys = append(jobKeys, key)
	}
	sort.Strings(jobKeys)
	limits := s.jobLimits
	if limits == (spec.Limits{}) {
		limits = spec.DefaultLimits()
	}
	policy := s.replacementPolicy
	if policy == (ReplacementPolicy{}) {
		policy = DefaultReplacementPolicy()
	}
	allocationLossTimeout := s.allocationLossTimeout
	if allocationLossTimeout == 0 {
		allocationLossTimeout = DefaultAllocationLossTimeout
	}
	admittedJobs := make(map[string]bool, len(jobKeys))
	namespaceDesired := make(map[string]int64)
	for _, key := range jobKeys {
		job := s.jobs[key]
		if err := spec.Canonicalize(job.Spec, limits); err != nil {
			s.log.Error("skip invalid job during reconciliation", "job", key, "error", err)
			continue
		}
		namespace := job.Spec.Namespace
		desired := desiredAllocations(job.Spec)
		if namespaceDesired[namespace]+desired > int64(limits.MaxDesiredAllocationsPerNamespace) {
			s.log.Error("skip job exceeding namespace allocation limit during reconciliation", "job", key, "namespace", namespace, "limit", limits.MaxDesiredAllocationsPerNamespace)
			continue
		}
		namespaceDesired[namespace] += desired
		admittedJobs[key] = true
	}
	allocations := make([]*Allocation, len(s.allocations))
	originalByPlan := make(map[*Allocation]*Allocation, len(s.allocations))
	baseByPlan := make(map[*Allocation]*Allocation, len(s.allocations))
	for i, allocation := range s.allocations {
		allocation.mu.Lock()
		base, err := cloneAllocationForReconcile(allocation)
		allocation.mu.Unlock()
		if err != nil {
			s.mu.Unlock()
			s.log.Error("snapshot allocation for reconciliation", "allocation", allocation.ID, "error", err)
			return
		}
		planned, err := cloneAllocationForReconcile(base)
		if err != nil {
			s.mu.Unlock()
			s.log.Error("copy allocation reconciliation snapshot", "allocation", allocation.ID, "error", err)
			return
		}
		allocations[i] = planned
		originalByPlan[planned] = allocation
		baseByPlan[planned] = base
	}
	sort.Slice(allocations, func(i, j int) bool { return allocations[i].ID < allocations[j].ID })
	allocationsByGroup := make(map[string][]*Allocation)
	for _, allocation := range allocations {
		key := replacementBackoffKey(allocation.Namespace, allocation.JobName, allocation.TaskGroupName)
		allocationsByGroup[key] = append(allocationsByGroup[key], allocation)
	}
	plannedUpdates := make(map[*Allocation]bool)
	markUpdated := func(allocation *Allocation) {
		plannedUpdates[allocation] = true
	}
	var actions []Action
	// Lost allocations whose returning node still runs their container. The
	// group loop decides whether each is kept until replacements run or
	// released; any it does not keep is stopped after the loop.
	var retained []*retainedOriginal
	// retainedStops release retained originals. They run before every other
	// action so a replacement never starts while an original it conflicts
	// with still holds its node's ports.
	var retainedStops []Action
	if !s.leaderSince.IsZero() && now.Sub(s.leaderSince) >= leaderRecoveryGrace {
		type observationKey struct {
			nodeID     uuid.UUID
			allocation string
			generation uint64
		}
		desired := make(map[observationKey]bool)
		lost := make(map[observationKey]*Allocation)
		for _, allocation := range allocations {
			allocation.mu.Lock()
			if allocation.Node != nil && allocation.Phase != lifecycle.PhaseStopped && allocation.Phase != lifecycle.PhaseFailed && allocation.Phase != lifecycle.PhaseLost {
				desired[observationKey{nodeID: allocation.Node.ID, allocation: allocation.ID, generation: allocation.Generation}] = true
			}
			if allocation.Node != nil && allocation.Phase == lifecycle.PhaseLost {
				lost[observationKey{nodeID: allocation.Node.ID, allocation: allocation.ID, generation: allocation.Generation}] = allocation
			}
			allocation.mu.Unlock()
		}
		for _, node := range s.nodes {
			if node.Status != NodeStatusHealthy && node.Status != NodeStatusDraining {
				continue
			}
			for _, observed := range node.observedAllocations {
				key := observationKey{nodeID: node.ID, allocation: observed.ID, generation: observed.Generation}
				if desired[key] {
					continue
				}
				if original := lost[key]; original != nil && observed.Phase == lifecycle.PhaseRunning {
					retained = append(retained, &retainedOriginal{allocation: original, node: node})
					continue
				}
				actions = append(actions, Action{Type: ActionStopObserved, Node: node, ID: observed.ID, Generation: observed.Generation})
			}
		}
		sortRetainedOriginals(retained)
	}
	valid := make([]*Allocation, 0, len(allocations))
	validByGroup := make(map[string][]*Allocation)
	for _, allocation := range allocations {
		allocation.mu.Lock()
		key := jobKey(allocation.Namespace, allocation.JobName)
		job := s.jobs[key]
		if !admittedJobs[key] {
			if allocation.Phase != lifecycle.PhaseStopped && allocation.Phase != lifecycle.PhaseFailed && allocation.Phase != lifecycle.PhaseLost && allocation.Node != nil {
				actions = append(actions, Action{Type: ActionStop, Allocation: allocation})
			} else if allocation.Phase == lifecycle.PhasePending {
				_ = allocation.Transition(lifecycle.PhaseStopping, now, "namespace_limit", "job exceeds the namespace desired-allocation limit")
				_ = allocation.Transition(lifecycle.PhaseStopped, now, "namespace_limit", "job exceeds the namespace desired-allocation limit")
				markUpdated(allocation)
			}
			allocation.mu.Unlock()
			continue
		}
		if allocation.Phase == lifecycle.PhasePending {
			groupExists := false
			for _, group := range job.Spec.TaskGroups {
				if group.Name == allocation.TaskGroupName {
					groupExists = true
					break
				}
			}
			if !groupExists || allocation.JobRevision != job.Revision {
				_ = allocation.Transition(lifecycle.PhaseStopping, now, "job_changed", "pending allocation is obsolete")
				_ = allocation.Transition(lifecycle.PhaseStopped, now, "job_changed", "pending allocation is obsolete")
				markUpdated(allocation)
				allocation.mu.Unlock()
				continue
			}
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
		if allocation.Draining && allocation.Node != nil && (allocation.Node.Status == NodeStatusHealthy || allocation.Node.Status == NodeStatusDraining) {
			actions = append(actions, Action{Type: ActionDrain, Allocation: allocation})
		}
		if allocation.Node != nil && allocation.Node.Status == NodeStatusDraining {
			if now.Sub(s.leaderSince) >= leaderRecoveryGrace && !allocation.Node.LastHeartbeat.IsZero() && now.Sub(allocation.Node.LastHeartbeat) >= allocationLossTimeout {
				_ = allocation.Transition(lifecycle.PhaseLost, now, "node_unavailable", "node did not re-register before the allocation loss timeout")
				markUpdated(allocation)
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
				actions = append(actions, Action{Type: ActionStop, Allocation: allocation})
				allocation.mu.Unlock()
				continue
			}
			if !allocation.Draining {
				allocation.Draining = true
				allocation.DrainSequence++
				allocation.DrainReason = "node"
				markUpdated(allocation)
				actions = append(actions, Action{Type: ActionDrain, Allocation: allocation})
			}
			valid = append(valid, allocation)
			allocation.mu.Unlock()
			continue
		}
		if allocation.JobRevision < job.Revision {
			strategy := updateStrategy(job, allocation.TaskGroupName)
			switch strategy {
			case spec.UpdateRolling:
				if !allocation.Draining {
					allocation.Draining = true
					allocation.DrainSequence++
					allocation.DrainReason = "update"
					markUpdated(allocation)
					if allocation.Node != nil && (allocation.Node.Status == NodeStatusHealthy || allocation.Node.Status == NodeStatusDraining) {
						actions = append(actions, Action{Type: ActionDrain, Allocation: allocation})
					}
				}
				if allocation.Node == nil || allocation.Node.Status != NodeStatusHealthy {
					if now.Sub(s.leaderSince) >= leaderRecoveryGrace && allocation.Node != nil && !allocation.Node.LastHeartbeat.IsZero() && now.Sub(allocation.Node.LastHeartbeat) >= allocationLossTimeout {
						_ = allocation.Transition(lifecycle.PhaseLost, now, "node_unavailable", "node did not re-register before the allocation loss timeout")
						markUpdated(allocation)
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
				markUpdated(allocation)
			}
			allocation.mu.Unlock()
			continue
		}
		// A start request carries the drain state itself, so only a running
		// allocation needs a separate resume redelivery.
		if allocation.Phase == lifecycle.PhaseRunning && !allocation.Draining && allocation.DrainSequence > 0 &&
			!s.resumeDelivered(s.controlEpoch, allocation.ID, allocation.Generation, allocation.DrainSequence) {
			actions = append(actions, Action{Type: ActionResume, Allocation: allocation})
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
	for _, allocation := range valid {
		key := replacementBackoffKey(allocation.Namespace, allocation.JobName, allocation.TaskGroupName)
		validByGroup[key] = append(validByGroup[key], allocation)
	}
	occupied := make([]*Allocation, 0, len(allocations))
	occupiedSet := make(map[*Allocation]bool, len(allocations))
	for _, allocation := range allocations {
		if allocation.Node != nil && allocation.Phase != lifecycle.PhaseStopped && allocation.Phase != lifecycle.PhaseFailed && allocation.Phase != lifecycle.PhaseLost {
			occupied = append(occupied, allocation)
			occupiedSet[allocation] = true
		}
	}

	plannedBackoffs := make(map[string]*ReplacementBackoff)
	for _, key := range jobKeys {
		job := s.jobs[key]
		if !admittedJobs[key] {
			continue
		}
		jobName := job.Spec.Name
		namespace := job.Spec.Namespace
		for _, group := range job.Spec.TaskGroups {
			backoffKey := replacementBackoffKey(namespace, jobName, group.Name)
			backoff := planReplacementBackoff(policy, s.replacementBackoffs[backoffKey], namespace, jobName, group.Name, job.Revision, allocationsByGroup[backoffKey], now)
			plannedBackoffs[backoffKey] = backoff
			var current []*Allocation
			var pending []*Allocation
			var draining []*Allocation
			for _, alloc := range validByGroup[backoffKey] {
				if alloc.Draining {
					draining = append(draining, alloc)
				} else if alloc.Phase == lifecycle.PhasePending {
					pending = append(pending, alloc)
				} else {
					current = append(current, alloc)
				}
			}
			for len(current) > group.Count {
				actions = append(actions, Action{Type: ActionStop, Allocation: current[len(current)-1]})
				current = current[:len(current)-1]
			}
			// Keep a lost original's container running while the group has
			// fewer running replacements than it needs, unless it holds a host
			// port an already placed allocation on its node needs.
			missing := group.Count
			for _, alloc := range current {
				if alloc.Phase == lifecycle.PhaseRunning {
					missing--
				}
			}
			for _, original := range retained {
				if original.released || original.kept || !original.inGroup(namespace, jobName, group.Name) {
					continue
				}
				if missing > 0 && (original.allocation.JobRevision == job.Revision || updateStrategy(job, group.Name) == spec.UpdateRolling) && !original.blocksPlacedAllocation(occupied) {
					original.kept = true
					missing--
					continue
				}
				retainedStops = append(retainedStops, original.release())
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
			if backoff != nil && backoff.DelayedReplacements > max(deficit, 0) {
				// Failed allocations whose capacity is no longer missing have
				// nothing left to replace.
				backoff.DelayedReplacements = max(deficit, 0)
			}
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
			for len(pending) > deficit {
				allocation := pending[len(pending)-1]
				pending = pending[:len(pending)-1]
				_ = allocation.Transition(lifecycle.PhaseStopping, now, "scaled_down", "pending allocation exceeds the desired count")
				_ = allocation.Transition(lifecycle.PhaseStopped, now, "scaled_down", "pending allocation exceeds the desired count")
				markUpdated(allocation)
			}
			if spec.GroupUsesWireGuard(&group) {
				if _, assigned := networkPorts[namespace]; !assigned {
					continue
				}
			}
			// Replacements of failed allocations wait until the backoff
			// elapses; the rest of the deficit is placed now.
			placeable := deficit - backoff.withheld(deficit, now)
			if placeable <= 0 {
				continue
			}
			requiredCapabilities := spec.GroupRequiredCapabilities(&group)
			placements, released := scheduleAroundRetained(PlacementIntent{Namespace: namespace, JobName: jobName, TaskGroupName: group.Name, Count: placeable, Nodes: s.nodePointers(), Allocations: occupied, DesiredAllocations: valid, Tasks: group.Tasks, Constraints: group.Constraints, RequiredCapabilities: requiredCapabilities, VolumeOwners: volumeOwners}, retained)
			for _, original := range released {
				retainedStops = append(retainedStops, original.stopAction())
			}
			for i, placement := range placements {
				for _, claim := range placement.VolumeClaims {
					volumeOwners[volumeRegistrationKey(claim.Namespace, claim.Name)] = claim.NodeID
				}
				node := s.nodes[placement.NodeID]
				if i < len(pending) {
					allocation := pending[i]
					allocation.Node = node
					_ = allocation.Transition(lifecycle.PhasePlaced, now, "", "")
					markUpdated(allocation)
					actions = append(actions, Action{Type: ActionStart, Allocation: allocation})
					if !occupiedSet[allocation] {
						occupied = append(occupied, allocation)
						occupiedSet[allocation] = true
					}
					continue
				}
				name := fmt.Sprintf("%s-%s-%s-%s", namespace, jobName, group.Name, uuid.NewString()[:8])
				allocation := &Allocation{ID: name, Namespace: namespace, JobName: jobName, TaskGroupName: group.Name, Tasks: group.Tasks, Node: node, Generation: 1, JobRevision: job.Revision, Phase: lifecycle.PhasePlaced, Health: lifecycle.HealthUnknown, Diagnostic: lifecycle.Diagnostic{CreatedAt: now, TransitionedAt: now}}
				actions = append(actions, Action{Type: ActionStart, Allocation: allocation})
				newAllocations = append(newAllocations, allocation)
				valid = append(valid, allocation)
				occupied = append(occupied, allocation)
				occupiedSet[allocation] = true
			}
			if len(placements) == 0 && len(pending) == 0 && len(requiredCapabilities) > 0 && noCompatibleCapabilityNode(s.nodePointers(), group.Constraints, group.Tasks, volumeOwners, namespace, requiredCapabilities) {
				for i := 0; i < placeable; i++ {
					name := fmt.Sprintf("%s-%s-%s-%s", namespace, jobName, group.Name, uuid.NewString()[:8])
					allocation := &Allocation{ID: name, Namespace: namespace, JobName: jobName, TaskGroupName: group.Name, Tasks: group.Tasks, Generation: 1, JobRevision: job.Revision, Phase: lifecycle.PhasePending, Health: lifecycle.HealthUnknown, Diagnostic: lifecycle.Diagnostic{CreatedAt: now, TransitionedAt: now, Reason: "missing_capability", Message: fmt.Sprintf("no eligible node supports required capabilities: %s", strings.Join(capabilityNames(requiredCapabilities), ", "))}}
					newAllocations = append(newAllocations, allocation)
					valid = append(valid, allocation)
				}
			}
		}
	}
	for _, original := range retained {
		if !original.kept && !original.released {
			retainedStops = append(retainedStops, original.release())
		}
	}
	actions = append(retainedStops, actions...)
	pruned := planTerminalPruning(policy.RetainTerminal, allocations, plannedUpdates)
	prunedSet := make(map[*Allocation]bool, len(pruned))
	for _, allocation := range pruned {
		prunedSet[allocation] = true
	}
	retainedGroups := make(map[string]bool)
	for _, allocation := range allocations {
		if !prunedSet[allocation] {
			retainedGroups[replacementBackoffKey(allocation.Namespace, allocation.JobName, allocation.TaskGroupName)] = true
		}
	}
	for _, allocation := range newAllocations {
		retainedGroups[replacementBackoffKey(allocation.Namespace, allocation.JobName, allocation.TaskGroupName)] = true
	}
	var backoffPuts, backoffDeletes []*ReplacementBackoff
	var delayed []*ReplacementBackoff
	for key, previous := range s.replacementBackoffs {
		if _, planned := plannedBackoffs[key]; planned {
			continue
		}
		job := s.jobs[jobKey(previous.Namespace, previous.JobName)]
		if job != nil && !admittedJobs[jobKey(previous.Namespace, previous.JobName)] {
			continue
		}
		if !retainedGroups[key] {
			backoffDeletes = append(backoffDeletes, previous)
			continue
		}
		// The group is no longer desired: forget its failures but keep the
		// record, and with it the failed allocations already seen, while the
		// group's allocation records remain.
		plannedBackoffs[key] = planReplacementBackoff(policy, previous, previous.Namespace, previous.JobName, previous.TaskGroupName, 0, allocations, now)
	}
	backoffKeys := make([]string, 0, len(plannedBackoffs))
	for key := range plannedBackoffs {
		backoffKeys = append(backoffKeys, key)
	}
	sort.Strings(backoffKeys)
	for _, key := range backoffKeys {
		next, previous := plannedBackoffs[key], s.replacementBackoffs[key]
		if next == nil || next.equal(previous) {
			continue
		}
		backoffPuts = append(backoffPuts, next)
		if previous == nil || next.Failures > previous.Failures {
			delayed = append(delayed, next)
		}
	}
	sort.Slice(backoffDeletes, func(i, j int) bool { return backoffDeletes[i].key() < backoffDeletes[j].key() })

	lockedUpdates := make([]*Allocation, 0, len(plannedUpdates)+len(pruned))
	canonicalNodes := make(map[*Allocation]*Node, len(plannedUpdates))
	for _, allocation := range allocations {
		if !plannedUpdates[allocation] && !prunedSet[allocation] {
			continue
		}
		original := originalByPlan[allocation]
		original.mu.Lock()
		same, err := sameAllocationState(original, baseByPlan[allocation])
		if err != nil || !same {
			original.mu.Unlock()
			for _, locked := range lockedUpdates {
				locked.mu.Unlock()
			}
			s.mu.Unlock()
			if err != nil {
				s.log.Error("compare allocation reconciliation snapshot", "allocation", allocation.ID, "error", err)
			}
			return
		}
		if plannedUpdates[allocation] && allocation.Node != nil {
			canonicalNodes[allocation] = s.nodes[allocation.Node.ID]
			if canonicalNodes[allocation] == nil {
				original.mu.Unlock()
				for _, locked := range lockedUpdates {
					locked.mu.Unlock()
				}
				s.mu.Unlock()
				return
			}
		}
		lockedUpdates = append(lockedUpdates, original)
	}
	persistedNewAllocations := make([]*Allocation, len(newAllocations))
	for i, allocation := range newAllocations {
		persisted, err := cloneAllocationForReconcile(allocation)
		if err != nil {
			for _, locked := range lockedUpdates {
				locked.mu.Unlock()
			}
			s.mu.Unlock()
			s.log.Error("snapshot new allocation for persistence", "allocation", allocation.ID, "error", err)
			return
		}
		persistedNewAllocations[i] = persisted
	}
	s.mu.Unlock()
	unlockUpdates := func() {
		for _, allocation := range lockedUpdates {
			allocation.mu.Unlock()
		}
	}

	volumeKeys := make([]string, 0, len(volumeOwners))
	for key := range volumeOwners {
		if _, exists := persistedVolumeOwners[key]; exists {
			continue
		}
		volumeKeys = append(volumeKeys, key)
	}
	sort.Strings(volumeKeys)
	volumeRegistrations := make([]*VolumeRegistration, 0, len(volumeKeys))
	for _, key := range volumeKeys {
		owner := volumeOwners[key]
		namespace, name, ok := strings.Cut(key, "/")
		if !ok || namespace == "" || name == "" {
			s.log.Error("invalid volume registration key", "key", key)
			unlockUpdates()
			return
		}
		volumeRegistrations = append(volumeRegistrations, &VolumeRegistration{Namespace: namespace, Name: name, NodeID: owner})
	}
	updates := make([]*Allocation, 0, len(plannedUpdates)+len(newAllocations))
	for _, allocation := range allocations {
		if plannedUpdates[allocation] {
			updates = append(updates, allocation)
		}
	}
	updates = append(updates, persistedNewAllocations...)
	prunedIDs := make([]string, len(pruned))
	for i, allocation := range pruned {
		prunedIDs[i] = allocation.ID
	}
	if err := s.state.CommitReconciliation(ctx, &ReconciliationCommit{Allocations: updates, DeleteAllocations: prunedIDs, VolumeRegistrations: volumeRegistrations, Backoffs: backoffPuts, DeleteBackoffs: backoffDeletes}); err != nil {
		s.log.Error("persist reconciliation allocation updates", "error", err)
		unlockUpdates()
		return
	}
	for _, allocation := range allocations {
		if plannedUpdates[allocation] {
			applyReconciledAllocation(originalByPlan[allocation], allocation, canonicalNodes[allocation])
		}
	}
	unlockUpdates()
	for i := range actions {
		if original := originalByPlan[actions[i].Allocation]; original != nil {
			actions[i].Allocation = original
		}
	}
	if len(plannedUpdates) > 0 || len(newAllocations) > 0 || len(pruned) > 0 || len(backoffPuts) > 0 || len(backoffDeletes) > 0 {
		s.mu.Lock()
		if len(pruned) > 0 {
			removed := make(map[*Allocation]bool, len(pruned))
			for _, allocation := range pruned {
				removed[originalByPlan[allocation]] = true
			}
			kept := s.allocations[:0:0]
			for _, allocation := range s.allocations {
				if !removed[allocation] {
					kept = append(kept, allocation)
				}
			}
			s.allocations = kept
		}
		s.allocations = append(s.allocations, newAllocations...)
		s.rebuildAllocationNodeIndexLocked()
		if len(backoffPuts) > 0 || len(backoffDeletes) > 0 {
			backoffs := make(map[string]*ReplacementBackoff, len(s.replacementBackoffs)+len(backoffPuts))
			for key, backoff := range s.replacementBackoffs {
				backoffs[key] = backoff
			}
			for _, backoff := range backoffPuts {
				backoffs[backoff.key()] = backoff
			}
			for _, backoff := range backoffDeletes {
				delete(backoffs, backoff.key())
			}
			s.replacementBackoffs = backoffs
		}
		s.mu.Unlock()
	}
	s.mutationMu.Unlock()
	mutationLocked = false
	s.revokeStaleWorkloadCredentials(ctx)
	for _, backoff := range delayed {
		next := backoff.NextReplacementAt
		s.log.Info("delaying task group replacement after failed allocations", "namespace", backoff.Namespace, "job", backoff.JobName, "group", backoff.TaskGroupName, "failures", backoff.Failures, "next_replacement_at", next, "last_allocation", backoff.LastAllocationID)
		s.events.publish(api.ClusterEvent{
			Type:              api.EventJobReplacementDelayed,
			Namespace:         backoff.Namespace,
			JobName:           backoff.JobName,
			Group:             backoff.TaskGroupName,
			AllocationID:      backoff.LastAllocationID,
			Revision:          backoff.JobRevision,
			Failures:          backoff.Failures,
			NextReplacementAt: &next,
			At:                now,
		})
	}

	executableActions := make([]Action, 0, len(actions))
	for i := range actions {
		if actions[i].Type == ActionStart && tasksUseWireGuard(actions[i].Allocation.Tasks) {
			if _, assigned := networkPorts[actions[i].Allocation.Namespace]; !assigned {
				continue
			}
		}
		executableActions = append(executableActions, actions[i])
	}
	// Retained originals must release their conflicting resources before their
	// replacements start on the same node. Actions remain ordered per node, while
	// independent nodes run concurrently so an unreachable peer delays only its
	// own queue.
	s.executeReconcileActions(ctx, executableActions)
	s.refreshNetworkPlans()
	s.refreshCatalog()
}

func (s *Server) executeReconcileActions(ctx context.Context, actions []Action) {
	type nodeActions struct {
		actions []Action
	}
	groupByNode := make(map[uuid.UUID]int)
	groups := make([]nodeActions, 0, len(actions))
	for _, action := range actions {
		var nodeID uuid.UUID
		if action.Allocation != nil {
			nodeID = action.Allocation.Node.ID
		} else {
			nodeID = action.Node.ID
		}
		index, exists := groupByNode[nodeID]
		if !exists {
			index = len(groups)
			groupByNode[nodeID] = index
			groups = append(groups, nodeActions{})
		}
		groups[index].actions = append(groups[index].actions, action)
	}
	workers := min(len(groups), maxConcurrentReconcileActions)
	if workers == 0 {
		return
	}
	work := make(chan []Action)
	var group sync.WaitGroup
	group.Add(workers)
	for range workers {
		go func() {
			defer group.Done()
			for nodeBatch := range work {
				for i := range nodeBatch {
					action := &nodeBatch[i]
					if err := s.Execute(ctx, action); err != nil {
						allocationID := action.ID
						if action.Allocation != nil {
							allocationID = action.Allocation.ID
						}
						s.log.Error("reconcile action failed", "action", action.Type, "allocation", allocationID, "error", err)
					}
				}
			}
		}()
	}
	for _, nodeBatch := range groups {
		work <- nodeBatch.actions
	}
	close(work)
	group.Wait()
}

type networkPlanKey struct {
	nodeID    uuid.UUID
	namespace string
}

type networkPlanTarget struct {
	key                networkPlanKey
	namespace          string
	address            string
	nodeID             uuid.UUID
	wireGuardPublicKey string
	plan               *network.Plan
	hash               string
	epoch              uint64
	attempt            int
}

type networkPlanState struct {
	target        networkPlanTarget
	appliedHash   string
	appliedAt     time.Time
	attempt       int
	retryAt       time.Time
	lastAttemptAt time.Time
}

// refreshNetworkPlans computes desired network plans without performing agent
// I/O. Delivery is handled independently by runNetworkPlanLoop.
func (s *Server) refreshNetworkPlans() {
	var targets []networkPlanTarget
	seen := make(map[networkPlanKey]bool)
	s.mu.RLock()
	epoch := s.controlEpoch
	for _, allocation := range s.allocations {
		allocation.mu.Lock()
		if allocation.Node != nil && allocation.Phase == lifecycle.PhaseRunning && tasksUseWireGuard(allocation.Tasks) &&
			(allocation.Node.Status == NodeStatusHealthy || allocation.Node.Status == NodeStatusDraining) {
			key := networkPlanKey{nodeID: allocation.Node.ID, namespace: allocation.Namespace}
			if !seen[key] {
				seen[key] = true
				plan, err := s.networkPlan(allocation.Namespace, allocation.Node)
				if err != nil {
					s.log.Error("build namespace network plan", "namespace", allocation.Namespace, "node", allocation.Node.ID, "error", err)
				} else {
					targets = append(targets, networkPlanTarget{
						key:                key,
						namespace:          allocation.Namespace,
						address:            fmt.Sprintf("%s:%d", allocation.Node.Host, allocation.Node.Port),
						nodeID:             allocation.Node.ID,
						wireGuardPublicKey: allocation.Node.WireGuardPublicKey,
						plan:               plan,
						epoch:              epoch,
					})
				}
			}
		}
		allocation.mu.Unlock()
	}
	s.mu.RUnlock()
	s.setDesiredNetworkPlans(targets)
}

func networkPlanHash(address, publicKey string, plan *network.Plan) string {
	raw, _ := json.Marshal(struct {
		Address   string        `json:"address"`
		PublicKey string        `json:"public_key"`
		Plan      *network.Plan `json:"plan"`
	}{Address: address, PublicKey: publicKey, Plan: plan})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func (s *Server) setDesiredNetworkPlans(targets []networkPlanTarget) {
	desired := make(map[networkPlanKey]bool, len(targets))
	dirty := false
	s.networkPlanMu.Lock()
	if s.networkPlans == nil {
		s.networkPlans = make(map[networkPlanKey]*networkPlanState)
	}
	if s.networkPlanWorkers == nil {
		s.networkPlanWorkers = make(map[uuid.UUID]uint64)
	}
	for _, target := range targets {
		target.hash = networkPlanHash(target.address, target.wireGuardPublicKey, target.plan)
		desired[target.key] = true
		state := s.networkPlans[target.key]
		if state == nil || state.target.epoch != target.epoch {
			state = &networkPlanState{}
			s.networkPlans[target.key] = state
		}
		if state.target.hash != target.hash {
			state.attempt = 0
			state.retryAt = time.Time{}
		}
		state.target = target
		if state.appliedHash != target.hash {
			dirty = true
		}
	}
	for key := range s.networkPlans {
		if !desired[key] {
			delete(s.networkPlans, key)
		}
	}
	s.networkPlanMu.Unlock()
	if dirty {
		s.wakeNetworkPlans()
	}
}

func (s *Server) wakeNetworkPlans() {
	select {
	case s.networkPlanWake <- struct{}{}:
	default:
	}
}

func networkPlanStateBefore(a, b *networkPlanState) bool {
	if b == nil {
		return true
	}
	if !a.lastAttemptAt.Equal(b.lastAttemptAt) {
		if a.lastAttemptAt.IsZero() {
			return true
		}
		if b.lastAttemptAt.IsZero() {
			return false
		}
		return a.lastAttemptAt.Before(b.lastAttemptAt)
	}
	return a.target.namespace < b.target.namespace
}

// claimPendingNetworkPlans chooses at most one namespace per node. A node
// worker therefore cannot monopolize the dispatcher, while lastAttemptAt makes
// repeated failures rotate behind other dirty namespaces on the same node.
func (s *Server) claimPendingNetworkPlans(now time.Time, epoch uint64) []networkPlanTarget {
	s.networkPlanMu.Lock()
	defer s.networkPlanMu.Unlock()
	if s.networkPlanWorkers == nil {
		s.networkPlanWorkers = make(map[uuid.UUID]uint64)
	}

	chosen := make(map[uuid.UUID]*networkPlanState)
	for _, state := range s.networkPlans {
		if state.target.epoch != epoch {
			continue
		}
		if workerEpoch, busy := s.networkPlanWorkers[state.target.nodeID]; busy && workerEpoch == epoch {
			continue
		}
		dirty := state.appliedHash != state.target.hash || state.appliedAt.IsZero() || now.Sub(state.appliedAt) >= networkPlanRepairInterval
		if !dirty || (!state.retryAt.IsZero() && now.Before(state.retryAt)) {
			continue
		}
		if networkPlanStateBefore(state, chosen[state.target.nodeID]) {
			chosen[state.target.nodeID] = state
		}
	}

	nodeIDs := make([]uuid.UUID, 0, len(chosen))
	for nodeID := range chosen {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Slice(nodeIDs, func(i, j int) bool { return nodeIDs[i].String() < nodeIDs[j].String() })

	targets := make([]networkPlanTarget, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		state := chosen[nodeID]
		s.networkPlanWorkers[nodeID] = epoch
		target := state.target
		target.attempt = state.attempt
		targets = append(targets, target)
	}
	return targets
}

func (s *Server) currentControlEpoch() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.controlEpoch
}

func (s *Server) finishNetworkPlanAttempt(target networkPlanTarget, err error) {
	if target.epoch != s.currentControlEpoch() {
		return
	}
	now := s.now().UTC()
	s.networkPlanMu.Lock()
	defer s.networkPlanMu.Unlock()
	state := s.networkPlans[target.key]
	if state == nil || state.target.epoch != target.epoch || state.target.hash != target.hash {
		return
	}
	state.lastAttemptAt = now
	if err == nil {
		state.appliedHash = target.hash
		state.appliedAt = now
		state.attempt = 0
		state.retryAt = time.Time{}
		return
	}
	state.attempt++
	state.retryAt = now.Add(retryDelay("network/"+target.nodeID.String()+"/"+target.namespace, state.attempt))
}

func (s *Server) releaseNetworkPlanWorker(nodeID uuid.UUID, epoch uint64) {
	s.networkPlanMu.Lock()
	if s.networkPlanWorkers[nodeID] == epoch {
		delete(s.networkPlanWorkers, nodeID)
	}
	s.networkPlanMu.Unlock()
	s.wakeNetworkPlans()
}

func (s *Server) runNetworkPlanLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	s.wakeNetworkPlans()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-s.networkPlanWake:
		}
		s.dispatchPendingNetworkPlans(ctx, s.client.UpdateNetworkPlan)
	}
}

func (s *Server) dispatchPendingNetworkPlans(ctx context.Context, update func(context.Context, uuid.UUID, string, *api.NetworkPlanRequest) error) {
	if ctx.Err() != nil {
		return
	}
	epoch := s.currentControlEpoch()
	targets := s.claimPendingNetworkPlans(s.now().UTC(), epoch)
	for _, target := range targets {
		go s.sendNetworkPlanTarget(ctx, target, update)
	}
}

func (s *Server) sendNetworkPlanTarget(ctx context.Context, target networkPlanTarget, update func(context.Context, uuid.UUID, string, *api.NetworkPlanRequest) error) {
	defer s.releaseNetworkPlanWorker(target.nodeID, target.epoch)
	if ctx.Err() != nil || target.epoch != s.currentControlEpoch() {
		return
	}
	planCtx, cancel := context.WithTimeout(ctx, networkPlanOperationTimeout(target.plan, target.attempt))
	request := &api.NetworkPlanRequest{Epoch: target.epoch, Namespace: target.namespace, Plan: *target.plan}
	err := update(planCtx, target.nodeID, target.address, request)
	cancel()
	s.finishNetworkPlanAttempt(target, err)
	if err != nil {
		s.log.Error("reconcile namespace network plan", "namespace", target.namespace, "node", target.nodeID, "error", err)
	}
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
	if action.Type == ActionStopObserved {
		s.mu.RLock()
		epoch := s.controlEpoch
		node := action.Node
		nodeStatus := node.Status
		address := fmt.Sprintf("%s:%d", node.Host, node.Port)
		s.mu.RUnlock()
		if nodeStatus != NodeStatusHealthy && nodeStatus != NodeStatusDraining {
			return fmt.Errorf("node %s is unavailable for observed allocation stop", node.ID)
		}
		return s.client.StopAllocation(ctx, node.ID, address, &api.StopAllocationRequest{AllocationID: action.ID, Generation: action.Generation, Epoch: epoch})
	}
	alloc := action.Allocation

	// Follow the server locking contract: when both locks are needed, acquire the
	// global server lock before the allocation lock. In particular, do not hold
	// allocation.mu while waiting for mu; readers such as metrics and allocation
	// listing acquire them in the opposite direction and would otherwise be able
	// to deadlock the whole control plane once a writer (for example a heartbeat)
	// is queued on mu.
	s.mu.RLock()
	alloc.mu.Lock()

	serverLocked := true
	allocationLocked := true
	unlockAllocation := func() {
		if allocationLocked {
			alloc.mu.Unlock()
			allocationLocked = false
		}
	}
	unlockServer := func() {
		if serverLocked {
			s.mu.RUnlock()
			serverLocked = false
		}
	}
	unlockState := func() {
		unlockAllocation()
		unlockServer()
	}
	defer unlockAllocation()
	defer unlockServer()

	now := s.now().UTC()
	address := fmt.Sprintf("%s:%d", alloc.Node.Host, alloc.Node.Port)
	requestNodeID := alloc.Node.ID
	allocationID := alloc.ID
	generation := alloc.Generation
	drainSequence := alloc.DrainSequence
	draining := alloc.Draining
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
		request := &api.AllocationRequest{AllocationID: alloc.ID, Generation: alloc.Generation, JobRevision: alloc.JobRevision, Epoch: epoch, Namespace: alloc.Namespace, JobName: alloc.JobName, GroupName: alloc.TaskGroupName, Tasks: alloc.Tasks, Runtime: groupRuntime, Restart: groupRestart, Draining: alloc.Draining, DrainSequence: alloc.DrainSequence}
		if groupUsesWireGuard {
			plan, err := s.networkPlan(alloc.Namespace, alloc.Node)
			if err != nil {
				return err
			}
			request.NetworkPlan = plan
		}

		// Everything below may perform storage or network I/O. Release both state
		// locks; each durable lifecycle update takes a fresh serialized snapshot.
		unlockState()

		for _, task := range request.Tasks {
			for _, ref := range task.Secrets {
				if s.secrets == nil {
					return fmt.Errorf("secret %s is unavailable: secrets are not configured", ref.Name)
				}
				value, version, err := s.secrets.Resolve(ctx, request.Namespace, ref.Name)
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
			token, err := s.apiAccessToken(ctx, groupAPIAccess, request)
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
				"TRELLIS_NAMESPACE": request.Namespace,
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
		// Drain state changes while an execution is unchanged.
		hashInput.Draining, hashInput.DrainSequence = false, 0
		if request.NetworkPlan != nil {
			// An active allocation cannot move to a different subnet or gateway.
			// Peer changes can be refreshed independently.
			hashInput.NetworkPlan = &network.Plan{CIDR: request.NetworkPlan.CIDR, Gateway: request.NetworkPlan.Gateway}
		}
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
		if err := s.persistAllocationUpdate(ctx, alloc, func(next *Allocation) error {
			if next.Phase == lifecycle.PhasePlaced || next.Phase == lifecycle.PhaseStopped || next.Phase == lifecycle.PhaseFailed || next.Phase == lifecycle.PhaseLost {
				return next.Transition(lifecycle.PhaseStarting, now, "", "")
			}
			return nil
		}); err != nil {
			return fmt.Errorf("persist allocation: %w", err)
		}
		if err := s.client.RunAllocation(ctx, requestNodeID, address, request); err != nil {
			if code := agentOperationCode(err); code == api.OperationStaleEpoch {
				return err
			} else if code == api.OperationStaleGeneration || code == api.OperationConflict || code == api.OperationRestartExhausted {
				if persistErr := s.persistAllocationUpdate(context.WithoutCancel(ctx), alloc, func(next *Allocation) error {
					if next.Phase != lifecycle.PhaseStarting && next.Phase != lifecycle.PhaseRunning {
						return nil
					}
					if transitionErr := next.Transition(lifecycle.PhaseFailed, now, string(code), err.Error()); transitionErr != nil {
						return transitionErr
					}
					next.NextRetryAt = nil
					return nil
				}); persistErr != nil {
					return fmt.Errorf("%w (persist allocation failure: %v)", err, persistErr)
				}
				return err
			}
			if persistErr := s.persistAllocationUpdate(context.WithoutCancel(ctx), alloc, func(next *Allocation) error {
				if next.Phase != lifecycle.PhaseStarting && next.Phase != lifecycle.PhaseRunning {
					return nil
				}
				next.Attempt++
				next.Reason, next.Message = "agent_start_failed", err.Error()
				if next.Attempt >= maxExecutionAttempts {
					if transitionErr := next.Transition(lifecycle.PhaseFailed, now, "retry_limit", err.Error()); transitionErr != nil {
						return transitionErr
					}
					next.NextRetryAt = nil
				} else {
					retryAt := now.Add(retryDelay(next.ID, next.Attempt))
					next.NextRetryAt = &retryAt
				}
				return nil
			}); persistErr != nil {
				return fmt.Errorf("%w (persist allocation failure: %v)", err, persistErr)
			}
			return err
		}
		if err := s.persistAllocationUpdate(ctx, alloc, func(next *Allocation) error {
			if next.Phase != lifecycle.PhaseStarting && next.Phase != lifecycle.PhaseRunning {
				return nil
			}
			next.Attempt, next.NextRetryAt = 0, nil
			return next.Transition(lifecycle.PhaseRunning, now, "", "")
		}); err != nil {
			return fmt.Errorf("persist running allocation: %w", err)
		}
	case ActionDrain:
		unlockState()
		if nodeStatus != NodeStatusHealthy && nodeStatus != NodeStatusDraining {
			return fmt.Errorf("node %s is unavailable for allocation drain", requestNodeID)
		}
		return s.client.DrainAllocation(ctx, requestNodeID, address, &api.DrainAllocationRequest{AllocationID: allocationID, Generation: generation, Epoch: epoch, Sequence: drainSequence})
	case ActionResume:
		unlockState()
		if nodeStatus != NodeStatusHealthy {
			return fmt.Errorf("node %s is unavailable for allocation resume", requestNodeID)
		}
		if draining {
			return nil
		}
		request := &api.DrainAllocationRequest{AllocationID: allocationID, Generation: generation, Epoch: epoch, Sequence: drainSequence}
		if err := s.client.ResumeAllocation(ctx, requestNodeID, address, request); err != nil {
			return err
		}
		s.recordResumeDelivered(request)
	case ActionStop:
		unlockState()

		if err := s.persistAllocationUpdate(ctx, alloc, func(next *Allocation) error {
			if next.Phase != lifecycle.PhaseStopping {
				return next.Transition(lifecycle.PhaseStopping, now, "", "")
			}
			return nil
		}); err != nil {
			return err
		}
		if nodeStatus != NodeStatusHealthy && nodeStatus != NodeStatusDraining {
			return fmt.Errorf("node %s is unavailable for allocation stop", requestNodeID)
		}
		if err := s.client.StopAllocation(ctx, requestNodeID, address, &api.StopAllocationRequest{AllocationID: allocationID, Generation: generation, Epoch: epoch}); err != nil {
			if code := agentOperationCode(err); code == api.OperationStaleEpoch || code == api.OperationStaleGeneration {
				return err
			}
			if persistErr := s.persistAllocationUpdate(context.WithoutCancel(ctx), alloc, func(next *Allocation) error {
				if next.Phase != lifecycle.PhaseStopping {
					return nil
				}
				next.Attempt++
				next.Reason, next.Message = "agent_stop_failed", err.Error()
				retryAt := now.Add(retryDelay(next.ID, next.Attempt))
				next.NextRetryAt = &retryAt
				return nil
			}); persistErr != nil {
				return fmt.Errorf("%w (persist allocation failure: %v)", err, persistErr)
			}
			return err
		}
		if err := s.persistAllocationUpdate(ctx, alloc, func(next *Allocation) error {
			if next.Phase != lifecycle.PhaseStopping && next.Phase != lifecycle.PhaseStopped {
				return nil
			}
			next.Attempt, next.NextRetryAt = 0, nil
			return next.Transition(lifecycle.PhaseStopped, now, "", "")
		}); err != nil {
			return fmt.Errorf("persist stopped allocation: %w", err)
		}
	default:
		unlockState()
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
