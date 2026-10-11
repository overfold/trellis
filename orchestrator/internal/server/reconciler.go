package server

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	"github.com/overfold/trellis/orchestrator/internal/network"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	"github.com/overfold/trellis/orchestrator/internal/spec"
	"github.com/overfold/trellis/orchestrator/internal/state"
)

// ActionType identifies a reconciliation operation.
type ActionType string

const (
	// ActionStart starts or updates an allocation.
	ActionStart ActionType = "start"
	// ActionDrain suppresses local restarts before a later stop.
	ActionDrain ActionType = "drain"
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
	RetainLogs bool
	// Planned control state must not be replaced with a later resume's sequence.
	drainSequence  uint64
	controlPlanned bool
}

const (
	leaderRecoveryGrace  = 30 * time.Second
	maxExecutionAttempts = 8
)

func retryDelay(id string, attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	shift := min(attempt-1, 6)
	base := time.Second * time.Duration(1<<shift)
	h := sha256.Sum256(fmt.Appendf(nil, "%s:%d", id, attempt))
	return base + time.Duration(binary.BigEndian.Uint16(h[:2])%500)*time.Millisecond
}

func workloadAPIAddress(serverAddr string) (string, error) {
	_, port, err := net.SplitHostPort(strings.TrimSpace(serverAddr))
	if err != nil {
		return "", fmt.Errorf("control-plane advertise address %q: %w", serverAddr, err)
	}
	return net.JoinHostPort("trellis", port), nil
}

// updateStrategy returns the canonical update strategy of a job task group.
// Admitted jobs are canonical, so every group carries an explicit strategy. A
// group removed from the job has nothing to roll to, so its allocations are
// replaced as with recreate.
func updateStrategy(job *Job, groupName string) spec.UpdateStrategy {
	for i := range job.Spec.TaskGroups {
		if job.Spec.TaskGroups[i].Name == groupName {
			return job.Spec.TaskGroups[i].Update.Strategy
		}
	}
	return spec.UpdateRecreate
}

func applyReconciledAllocation(allocation, update *Allocation, node *Node) {
	allocation.Phase = update.Phase
	allocation.StableSince = update.StableSince
	allocation.Diagnostic = update.Diagnostic
	allocation.Node = node
	allocation.Draining = update.Draining
	allocation.DrainSequence = update.DrainSequence
	allocation.DrainReason = update.DrainReason
	allocation.Events = update.Events
}

func (s *Server) persistAllocationUpdate(ctx context.Context, allocation *Allocation, update func(*Allocation) error) error {
	ctx, release := s.bindTerm(ctx)
	defer release()
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	if err := s.checkTerm(ctx); err != nil {
		return err
	}
	s.mu.RLock()
	if !slices.Contains(s.allocations, allocation) {
		s.mu.RUnlock()
		return context.Canceled
	}
	allocation.mu.Lock()
	next := allocation.cloneRecord()
	allocation.mu.Unlock()
	s.mu.RUnlock()
	if err := update(next); err != nil {
		return err
	}
	var decisions []state.Mutation
	if next.Node != nil && next.Phase == lifecycle.PhaseStopping {
		decision := executionCleanupDecision{Node: next.Node.ID, ID: next.ID, Generation: next.Generation, RetainLogs: true}
		raw, err := json.Marshal(decision)
		if err != nil {
			return err
		}
		decisions = append(decisions, state.Mutation{Key: s.state.executionDecisionKey(decision.Node, decision.ID, decision.Generation), Value: raw})
	}
	if err := s.state.PutNodesAndAllocations(ctx, nil, []*Allocation{next}, decisions...); err != nil {
		return err
	}
	s.mu.RLock()
	allocation.mu.Lock()
	applyAllocationSnapshot(allocation, next)
	allocation.mu.Unlock()
	s.mu.RUnlock()
	return nil
}

// Reconcile converges the in-memory allocation set on the latest job specs and
// publishes the pass's committed execution authority.
func (s *Server) Reconcile(ctx context.Context) {
	s.reconcile(ctx)
}

// reconcile runs one reconciliation pass. Planning and its durable commit are
// serialized with intent publication under reconcileMu. No transport work is
// dispatched.
func (s *Server) reconcile(ctx context.Context) {
	ctx, release := s.bindTerm(ctx)
	defer release()
	start := time.Now()
	var executable []Action
	planned := false
	s.reconcileMu.Lock()
	s.mutationMu.Lock()
	mutationLocked := true
	defer func() {
		if mutationLocked {
			s.mutationMu.Unlock()
		}
		if planned {
			if err := s.publishReconcileIntent(ctx, executable); err != nil {
				s.log.Error("publish node execution intent", "error", err)
			}
			s.refreshCatalog()
		}
		s.reconcileMu.Unlock()
		if s.metrics != nil {
			s.metrics.ReconcileDuration.Observe(time.Since(start).Seconds())
		}
	}()
	if s.checkTerm(ctx) != nil {
		return
	}
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
		// Lost is terminal for scheduling, not proof of node-side cleanup.
		if allocation.Node != nil && !allocation.CleanupComplete && tasksUseWireGuard(allocation.Tasks) {
			addNetworkNamespace(allocation.Namespace)
		}
		allocation.mu.Unlock()
	}
	s.mu.RUnlock()
	networkPorts, networkPortRegistrations, deleteNetworkPortNamespaces, err := s.planNetworkPortRegistrations(ctx, networkNamespaces)
	if err != nil {
		s.log.Error("prepare namespace WireGuard ports", "error", err)
		if !errors.Is(err, errNetworkPortExhausted) {
			return
		}
	}
	currentSubnets, err := s.state.listNetworkSubnetRegistrations(ctx)
	if err != nil {
		s.log.Error("load network subnet registrations", "error", err)
		return
	}
	// The pass plans against one liveness view: each node's status is
	// derived from the heartbeats received up to now, and later heartbeats
	// are seen by the next pass.
	heartbeats := s.liveness.heartbeats()
	s.mu.Lock()
	for _, node := range s.nodes {
		node.Status = livenessStatus(node.Status, heartbeats[node.ID], now)
	}
	// Subnets are planned under s.mu so every node the scheduler can place on
	// is addressed. Only namespaces with a port slot and a subnet on every
	// node accept new placements.
	nodeIDs := make([]uuid.UUID, 0, len(s.nodes))
	for nodeID := range s.nodes {
		nodeIDs = append(nodeIDs, nodeID)
	}
	protectedSubnets := make(map[networkSubnetKey]bool)
	for _, allocation := range s.allocations {
		allocation.mu.Lock()
		if allocation.Node != nil && !allocation.CleanupComplete && tasksUseWireGuard(allocation.Tasks) {
			protectedSubnets[networkSubnetKey{namespace: allocation.Namespace, node: allocation.Node.ID}] = true
		}
		allocation.mu.Unlock()
	}
	subnets, err := planNetworkSubnetRegistrations(s.networkPool, currentSubnets, networkNamespaces, nodeIDs, protectedSubnets)
	if err != nil {
		s.log.Error("prepare namespace network subnets", "error", err)
		if !errors.Is(err, errNetworkSubnetExhausted) {
			s.mu.Unlock()
			return
		}
	}
	networkReady := make(map[string]bool, len(subnets.Ready))
	for namespace := range subnets.Ready {
		if _, assigned := networkPorts[namespace]; assigned {
			networkReady[namespace] = true
		}
	}
	input, originals := s.reconcilePlanInputLocked(now, heartbeats, volumeOwners, networkReady)
	plan, err := planReconciliation(input)
	if err != nil {
		s.mu.Unlock()
		s.log.Error("plan reconciliation", "error", err)
		return
	}
	plan.Commit.NetworkPortRegistrations = networkPortRegistrations
	plan.Commit.DeleteNetworkPortNamespaces = deleteNetworkPortNamespaces
	plan.Commit.NetworkSubnetRegistrations = subnets.Registrations
	plan.Commit.DeleteNetworkSubnetRegistrations = subnets.Deletions
	for _, diagnostic := range plan.Diagnostics {
		s.log.Error(diagnostic.Message, diagnostic.Args...)
	}
	for _, action := range plan.Actions {
		if action.Type == ActionStop && action.Allocation != nil && action.Allocation.Node != nil {
			a := action.Allocation
			plan.Commit.ExecutionCleanup = append(plan.Commit.ExecutionCleanup, executionCleanupDecision{Node: a.Node.ID, ID: a.ID, Generation: a.Generation, RetainLogs: true})
		} else if action.Type == ActionStopObserved && action.Node != nil {
			plan.Commit.ExecutionCleanup = append(plan.Commit.ExecutionCleanup, executionCleanupDecision{Node: action.Node.ID, ID: action.ID, Generation: action.Generation, RetainLogs: action.RetainLogs})
		}
	}
	for _, allocation := range plan.Pruned {
		if allocation.Node != nil {
			plan.Commit.ExecutionCleanup = append(plan.Commit.ExecutionCleanup, executionCleanupDecision{Node: allocation.Node.ID, ID: allocation.ID, Generation: allocation.Generation, RetainLogs: false})
		}
	}
	originalOf := func(planned *Allocation) *Allocation {
		return originals[plan.Source[planned]]
	}
	lockedUpdates, canonicalNodes, ok := s.lockReconcileTargetsLocked(plan, originals)
	if !ok {
		s.mu.Unlock()
		return
	}
	unlockUpdates := func() {
		for _, allocation := range lockedUpdates {
			allocation.mu.Unlock()
		}
	}
	s.mu.Unlock()

	if err := s.state.CommitReconciliation(ctx, &plan.Commit); err != nil {
		s.log.Error("persist reconciliation allocation updates", "error", err)
		unlockUpdates()
		return
	}
	for _, allocation := range plan.Updated {
		applyReconciledAllocation(originalOf(allocation), allocation, canonicalNodes[allocation])
	}
	unlockUpdates()
	actions := plan.Actions
	for i := range actions {
		if allocation := actions[i].Allocation; allocation != nil {
			actions[i].drainSequence = allocation.DrainSequence
			actions[i].controlPlanned = true
		}
		if original := originalOf(actions[i].Allocation); original != nil {
			actions[i].Allocation = original
		}
	}
	s.mu.Lock()
	s.networkPorts = networkPorts
	s.networkSubnets = subnets.Subnets
	if !plan.empty() {
		if len(plan.Pruned) > 0 {
			removed := make(map[*Allocation]bool, len(plan.Pruned))
			for _, allocation := range plan.Pruned {
				removed[originalOf(allocation)] = true
			}
			kept := s.allocations[:0:0]
			for _, allocation := range s.allocations {
				if !removed[allocation] {
					kept = append(kept, allocation)
				}
			}
			s.allocations = kept
		}
		s.allocations = append(s.allocations, plan.NewAllocations...)
		s.rebuildAllocationNodeIndexLocked()
		if len(plan.Commit.Backoffs) > 0 || len(plan.Commit.DeleteBackoffs) > 0 {
			backoffs := make(map[string]*ReplacementBackoff, len(s.replacementBackoffs)+len(plan.Commit.Backoffs))
			maps.Copy(backoffs, s.replacementBackoffs)
			for _, backoff := range plan.Commit.Backoffs {
				backoffs[backoff.key()] = backoff
			}
			for _, backoff := range plan.Commit.DeleteBackoffs {
				delete(backoffs, backoff.key())
			}
			s.replacementBackoffs = backoffs
		}
	}
	s.mu.Unlock()
	s.mutationMu.Unlock()
	mutationLocked = false
	s.revokeStaleWorkloadCredentials(ctx)
	for _, event := range plan.Events {
		s.log.Info("delaying task group replacement after failed allocations", "namespace", event.Namespace, "job", event.JobName, "group", event.Group, "failures", event.Failures, "next_replacement_at", *event.NextReplacementAt, "last_allocation", event.AllocationID)
		s.events.publish(event)
	}

	executableActions := make([]Action, 0, len(actions))
	for i := range actions {
		if actions[i].Type == ActionStart && tasksUseWireGuard(actions[i].Allocation.Tasks) {
			allocation := actions[i].Allocation
			if _, assigned := networkPorts[allocation.Namespace]; !assigned {
				continue
			}
			if allocation.Node == nil {
				continue
			}
			if _, addressed := subnets.Subnets[networkSubnetKey{namespace: allocation.Namespace, node: allocation.Node.ID}]; !addressed {
				continue
			}
		}
		executableActions = append(executableActions, actions[i])
	}
	executable, planned = executableActions, true
}

// lockReconcileTargetsLocked locks every stored allocation the plan rewrites
// or prunes, in allocation ID order, after confirming each still matches the
// snapshot the plan was made from. It also resolves the canonical node of each
// updated allocation. When anything changed it unlocks, records the aborted
// pass, and returns false; a later pass replans from the newer state. The
// caller must hold s.mu and, on success, unlock the returned allocations.
func (s *Server) lockReconcileTargetsLocked(plan *reconcilePlan, originals map[*Allocation]*Allocation) ([]*Allocation, map[*Allocation]*Node, bool) {
	updatedSet := make(map[*Allocation]bool, len(plan.Updated))
	for _, allocation := range plan.Updated {
		updatedSet[allocation] = true
	}
	targets := make([]*Allocation, 0, len(plan.Updated)+len(plan.Pruned))
	targets = append(targets, plan.Updated...)
	for _, allocation := range plan.Pruned {
		if !updatedSet[allocation] {
			targets = append(targets, allocation)
		}
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].ID < targets[j].ID })
	locked := make([]*Allocation, 0, len(targets))
	release := func() {
		for _, allocation := range locked {
			allocation.mu.Unlock()
		}
	}
	canonicalNodes := make(map[*Allocation]*Node, len(plan.Updated))
	for _, allocation := range targets {
		snapshot := plan.Source[allocation]
		original := originals[snapshot]
		original.mu.Lock()
		if !original.sameRecord(snapshot) {
			original.mu.Unlock()
			release()
			s.abortReconcile("allocation_changed", "abort reconciliation: allocation changed during planning", "allocation", allocation.ID)
			return nil, nil, false
		}
		if updatedSet[allocation] && allocation.Node != nil {
			canonicalNodes[allocation] = s.nodes[allocation.Node.ID]
			if canonicalNodes[allocation] == nil && snapshot.Node != nil && snapshot.Node.ID == allocation.Node.ID {
				// An existing execution on a removed node may still need a
				// durable loss/cleanup transition. Preserve its placement ID.
				canonicalNodes[allocation] = original.Node
			}
			if canonicalNodes[allocation] == nil {
				original.mu.Unlock()
				release()
				s.abortReconcile("node_removed", "abort reconciliation: allocation node was removed during planning", "allocation", allocation.ID, "node", allocation.Node.ID)
				return nil, nil, false
			}
		}
		locked = append(locked, original)
	}
	return locked, canonicalNodes, true
}

// abortReconcile records a pass abandoned before its commit because the state
// it planned from changed. The next pass replans from the newer state.
func (s *Server) abortReconcile(reason, message string, args ...any) {
	s.log.Warn(message, append([]any{"reason", reason}, args...)...)
	if s.metrics != nil {
		s.metrics.ReconcileAborted.WithLabelValues(reason).Inc()
	}
}

// reconcilePlanInputLocked snapshots the state a reconciliation pass plans
// from. It returns the input and a map from each allocation snapshot to the
// stored allocation it copies. The caller must hold s.mu and keep holding it
// while planning, because jobs, nodes, and backoffs are borrowed.
func (s *Server) reconcilePlanInputLocked(now time.Time, heartbeats map[uuid.UUID]time.Time, volumeOwners map[string]uuid.UUID, networkReady map[string]bool) (*reconcilePlanInput, map[*Allocation]*Allocation) {
	limits := s.jobLimits
	if limits == (spec.Limits{}) {
		limits = spec.DefaultLimits()
	}
	reconciliation := s.reconciliationSettingsLocked()
	snapshots := make([]*Allocation, len(s.allocations))
	originals := make(map[*Allocation]*Allocation, len(s.allocations))
	// Snapshots are detached from the canonical nodes, and the snapshots of
	// one node's allocations share one copy of it.
	nodeCopies := make(map[*Node]*Node, len(s.nodes))
	for i, allocation := range s.allocations {
		allocation.mu.Lock()
		node := allocation.Node
		if node != nil && nodeCopies[node] == nil {
			nodeCopies[node] = node.Clone()
		}
		snapshot := allocation.cloneOnto(nodeCopies[node])
		allocation.mu.Unlock()
		snapshots[i] = snapshot
		originals[snapshot] = allocation
	}
	return &reconcilePlanInput{
		Now:                   now,
		LeaderSince:           s.leaderSince,
		Heartbeats:            heartbeats,
		Limits:                limits,
		Policy:                reconciliation.replacementPolicy(),
		AllocationLossTimeout: reconciliation.AllocationLossTimeout,
		Jobs:                  s.jobs,
		Nodes:                 s.nodes,
		Allocations:           snapshots,
		Backoffs:              s.replacementBackoffs,
		VolumeOwners:          volumeOwners,
		NetworkReady:          networkReady,
	}, originals
}

func (s *Server) currentControlEpoch() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.controlEpoch
}

func tasksUseWireGuard(tasks []spec.TaskSpec) bool {
	for i := range tasks {
		if tasks[i].Networking != nil && tasks[i].Networking.Mode == spec.TaskNetworkWireGuard {
			return true
		}
	}
	return false
}

func capabilityNames(capabilities []spec.NodeCapability) []string {
	names := make([]string, len(capabilities))
	for i, capability := range capabilities {
		names[i] = string(capability)
	}
	return names
}

// recordStartFailure counts one failed start attempt of an allocation: it
// schedules the retry, or fails the allocation once attempts are exhausted.
func recordStartFailure(next *Allocation, now time.Time, message string) error {
	next.Attempt++
	next.Reason, next.Message = "agent_start_failed", message
	if next.Attempt >= maxExecutionAttempts {
		if err := next.Transition(lifecycle.PhaseFailed, now, "retry_limit", message); err != nil {
			return err
		}
		next.NextRetryAt = nil
		return nil
	}
	retryAt := now.Add(retryDelay(next.ID, next.Attempt))
	next.NextRetryAt = &retryAt
	return nil
}

// terminalStartFailureCode reports whether code may accompany a reported start
// failure.
func terminalStartFailureCode(code nodeapi.OperationCode) bool {
	switch code {
	case "", nodeapi.OperationStaleGeneration, nodeapi.OperationConflict, nodeapi.OperationRestartExhausted:
		return true
	case nodeapi.OperationOK, nodeapi.OperationStaleEpoch, nodeapi.OperationFailed:
		return false
	}
	return false
}

// Execute performs a reconciliation action.
func (s *Server) Execute(ctx context.Context, action *Action) error {
	ctx, release := s.bindTerm(ctx)
	defer release()
	if err := s.checkTerm(ctx); err != nil {
		return err
	}
	s.reconcileMu.Lock()
	defer s.reconcileMu.Unlock()
	return s.publishReconcileIntent(ctx, []Action{*action})
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
	index, ok := s.networkSubnets[networkSubnetKey{namespace: namespace, node: target.ID}]
	if !ok {
		return nil, fmt.Errorf("namespace %q has no network subnet registration on node %s", namespace, target.ID)
	}
	local := networkSubnet(s.networkPool, index)
	plan := &network.Plan{CIDR: local.String(), Gateway: local.Addr().Next().String(), WireGuardAddress: networkLinkAddress(index), ListenPort: listenPort}
	for _, node := range s.nodes {
		if node.ID == target.ID || node.WireGuardPublicKey == "" || node.WireGuardEndpoint == "" {
			continue
		}
		// A node without a registration for this namespace joined after the
		// pool was exhausted; it cannot host the namespace, so it is no peer.
		peerIndex, ok := s.networkSubnets[networkSubnetKey{namespace: namespace, node: node.ID}]
		if !ok {
			continue
		}
		if _, err := namespaceWireGuardPort(node.WireGuardPortBase, node.WireGuardPortCount, slot); err != nil {
			return nil, fmt.Errorf("peer node %s WireGuard port range: %w", node.ID, err)
		}
		endpoint, err := namespaceWireGuardEndpoint(node.WireGuardEndpoint, slot)
		if err != nil {
			return nil, fmt.Errorf("peer node %s WireGuard endpoint: %w", node.ID, err)
		}
		plan.Peers = append(plan.Peers, network.PeerPlan{PublicKey: node.WireGuardPublicKey, Endpoint: endpoint, AllowedIPs: []string{networkSubnet(s.networkPool, peerIndex).String()}})
	}
	sort.Slice(plan.Peers, func(i, j int) bool {
		return plan.Peers[i].PublicKey < plan.Peers[j].PublicKey
	})
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
