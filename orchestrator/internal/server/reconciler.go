package server

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/internal/client"
	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	"github.com/overfold/trellis/orchestrator/internal/network"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	"github.com/overfold/trellis/orchestrator/internal/spec"
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
	RetainLogs bool
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
	for range attempt {
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
	h := sha256.Sum256(fmt.Appendf(nil, "%s:%d", id, attempt))
	return base + time.Duration(binary.BigEndian.Uint16(h[:2])%500)*time.Millisecond
}

func agentOperationCode(err error) nodeapi.OperationCode {
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
	next := allocation.cloneRecord()
	allocation.mu.Unlock()
	s.mu.RUnlock()
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

// Reconcile converges the in-memory allocation set on the latest job specs and
// waits for the pass's agent actions.
func (s *Server) Reconcile(ctx context.Context) {
	<-s.reconcile(ctx, true)
}

// reconcile runs one reconciliation pass. Planning and its durable commit are
// serialized with other passes; the agent actions run after reconcileMu is
// released, so a slow node delays neither later passes nor other nodes. A
// queueing pass waits for a node busy with an earlier pass instead of skipping
// its actions. The returned channel closes once the pass's actions finish.
func (s *Server) reconcile(ctx context.Context, queue bool) (finished <-chan struct{}) {
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
		s.reconcileMu.Unlock()
		if s.metrics != nil {
			s.metrics.ReconcileDuration.Observe(time.Since(start).Seconds())
		}
		if !planned {
			closed := make(chan struct{})
			close(closed)
			finished = closed
			return
		}
		finished = s.dispatchReconcileActions(ctx, executable, queue)
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
		if activeAllocationPhase(allocation.Phase) && tasksUseWireGuard(allocation.Tasks) {
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
	subnets, err := planNetworkSubnetRegistrations(s.networkPool, currentSubnets, networkNamespaces, nodeIDs)
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
	return
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
		DeliveredResumes:      s.deliveredResumes(s.controlEpoch),
	}, originals
}

// dispatchReconcileActions runs a pass's actions. Retained originals must
// release their conflicting resources before their replacements start on the
// same node, so each node's actions run in order, and a node runs one pass's
// actions at a time. A node still busy with an earlier pass delays only its
// own actions: a queueing pass (one an API mutation waits for) runs them once
// the node is free, and a periodic pass skips them for the next pass to plan
// again. Independent nodes run
// concurrently, bounded by maxConcurrentReconcileActions across all passes, so
// an unreachable agent delays only its own queue. The returned channel closes
// once the dispatched actions finish and the catalog is refreshed.
func (s *Server) dispatchReconcileActions(ctx context.Context, actions []Action, queue bool) <-chan struct{} {
	var nodes []uuid.UUID
	byNode := make(map[uuid.UUID][]Action)
	for _, action := range actions {
		var nodeID uuid.UUID
		if action.Allocation != nil {
			nodeID = action.Allocation.Node.ID
		} else {
			nodeID = action.Node.ID
		}
		if _, exists := byNode[nodeID]; !exists {
			nodes = append(nodes, nodeID)
		}
		byNode[nodeID] = append(byNode[nodeID], action)
	}
	var group sync.WaitGroup
	for _, nodeID := range nodes {
		slots, busy := s.claimActionNode(nodeID)
		if busy != nil && !queue {
			s.log.Debug("node is still running earlier reconcile actions; deferring", "node", nodeID, "actions", len(byNode[nodeID]))
			continue
		}
		group.Add(1)
		go func(nodeID uuid.UUID, batch []Action) {
			defer group.Done()
			slots, ok := s.awaitActionNode(ctx, nodeID, slots, busy)
			if !ok {
				return
			}
			defer s.releaseActionNode(nodeID)
			defer func() { <-slots }()
			for i := range batch {
				action := &batch[i]
				if err := s.Execute(ctx, action); err != nil {
					allocationID := action.ID
					if action.Allocation != nil {
						allocationID = action.Allocation.ID
					}
					s.log.Error("reconcile action failed", "action", action.Type, "allocation", allocationID, "error", err)
				}
			}
		}(nodeID, byNode[nodeID])
	}
	done := make(chan struct{})
	go func() {
		group.Wait()
		s.refreshMu.Lock()
		s.refreshNetworkPlans()
		s.refreshCatalog()
		s.refreshMu.Unlock()
		close(done)
	}()
	return done
}

// claimActionNode reserves a node for one pass's actions and returns the
// global action slots. When the node is busy it returns the channel that
// closes when the node is released instead.
func (s *Server) claimActionNode(nodeID uuid.UUID) (chan struct{}, <-chan struct{}) {
	s.actionMu.Lock()
	defer s.actionMu.Unlock()
	if s.actionSlots == nil {
		s.actionSlots = make(chan struct{}, maxConcurrentReconcileActions)
		s.actionNodes = make(map[uuid.UUID]chan struct{})
	}
	if released := s.actionNodes[nodeID]; released != nil {
		return nil, released
	}
	s.actionNodes[nodeID] = make(chan struct{})
	return s.actionSlots, nil
}

// awaitActionNode completes a claim started by claimActionNode: it waits for
// an earlier pass to release the node, claims it, and then takes a global
// action slot. On success the caller must return the slot and then call
// releaseActionNode. It reports false, holding nothing, when ctx ends first.
func (s *Server) awaitActionNode(ctx context.Context, nodeID uuid.UUID, slots chan struct{}, busy <-chan struct{}) (chan struct{}, bool) {
	for busy != nil {
		select {
		case <-busy:
		case <-ctx.Done():
			return nil, false
		}
		slots, busy = s.claimActionNode(nodeID)
	}
	select {
	case slots <- struct{}{}:
		return slots, true
	case <-ctx.Done():
		s.releaseActionNode(nodeID)
		return nil, false
	}
}

func (s *Server) releaseActionNode(nodeID uuid.UUID) {
	s.actionMu.Lock()
	close(s.actionNodes[nodeID])
	delete(s.actionNodes, nodeID)
	s.actionMu.Unlock()
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

func (s *Server) dispatchPendingNetworkPlans(ctx context.Context, update func(context.Context, uuid.UUID, string, *nodeapi.NetworkPlanRequest) error) {
	if ctx.Err() != nil {
		return
	}
	epoch := s.currentControlEpoch()
	targets := s.claimPendingNetworkPlans(s.now().UTC(), epoch)
	for _, target := range targets {
		go s.sendNetworkPlanTarget(ctx, target, update)
	}
}

func (s *Server) sendNetworkPlanTarget(ctx context.Context, target networkPlanTarget, update func(context.Context, uuid.UUID, string, *nodeapi.NetworkPlanRequest) error) {
	defer s.releaseNetworkPlanWorker(target.nodeID, target.epoch)
	if ctx.Err() != nil || target.epoch != s.currentControlEpoch() {
		return
	}
	planCtx, cancel := context.WithTimeout(ctx, networkPlanOperationTimeout(target.plan, target.attempt))
	request := &nodeapi.NetworkPlanRequest{Epoch: target.epoch, Namespace: target.namespace, Plan: *target.plan}
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
		return s.client.StopAllocation(ctx, node.ID, address, &nodeapi.StopAllocationRequest{AllocationID: action.ID, Generation: action.Generation, Epoch: epoch, RetainLogs: action.RetainLogs})
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
	attempt := alloc.Attempt

	switch action.Type {
	case ActionStart:
		job := s.jobs[jobKey(alloc.Namespace, alloc.JobName)]
		if job == nil || job.Incarnation != alloc.JobIncarnation {
			return fmt.Errorf("job %s was deleted or recreated before allocation start", alloc.JobName)
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
		request := &nodeapi.AllocationRequest{AllocationID: alloc.ID, Generation: alloc.Generation, JobRevision: alloc.JobRevision, Epoch: epoch, Namespace: alloc.Namespace, JobName: alloc.JobName, GroupName: alloc.TaskGroupName, Tasks: alloc.Tasks, Runtime: groupRuntime, Restart: groupRestart, Draining: alloc.Draining, DrainSequence: alloc.DrainSequence, Attempt: attempt}
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
				request.Secrets = append(request.Secrets, nodeapi.DeliveredSecret{Task: task.Name, Name: ref.Name, Version: version, Target: ref.Target, Env: ref.Env, Path: ref.Path, Mode: ref.Mode, Value: value})
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
		hashInput.Epoch, hashInput.ExecutionHash, hashInput.Attempt = 0, "", 0
		// Drain state changes while an execution is unchanged.
		hashInput.Draining, hashInput.DrainSequence = false, 0
		if request.NetworkPlan != nil {
			// An active allocation cannot move to a different subnet or gateway.
			// Peer changes can be refreshed independently.
			hashInput.NetworkPlan = &network.Plan{CIDR: request.NetworkPlan.CIDR, Gateway: request.NetworkPlan.Gateway}
		}
		hashInput.Secrets = append([]nodeapi.DeliveredSecret(nil), request.Secrets...)
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
			if code := agentOperationCode(err); code == nodeapi.OperationStaleEpoch {
				return err
			} else if code == nodeapi.OperationStaleGeneration || code == nodeapi.OperationConflict || code == nodeapi.OperationRestartExhausted {
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
					return fmt.Errorf("%w (persist allocation failure: %w)", err, persistErr)
				}
				return err
			}
			if persistErr := s.persistAllocationUpdate(context.WithoutCancel(ctx), alloc, func(next *Allocation) error {
				// A heartbeat may have observed the start complete meanwhile.
				if next.Phase != lifecycle.PhaseStarting {
					return nil
				}
				return recordStartFailure(next, now, err.Error())
			}); persistErr != nil {
				return fmt.Errorf("%w (persist allocation failure: %w)", err, persistErr)
			}
			return err
		}
		// The agent accepted the start and pulls and creates the tasks in the
		// background. Heartbeats report the allocation running, or report a
		// failure of this attempt, which recordStartFailure counts.
	case ActionDrain:
		unlockState()
		if nodeStatus != NodeStatusHealthy && nodeStatus != NodeStatusDraining {
			return fmt.Errorf("node %s is unavailable for allocation drain", requestNodeID)
		}
		return s.client.DrainAllocation(ctx, requestNodeID, address, &nodeapi.DrainAllocationRequest{AllocationID: allocationID, Generation: generation, Epoch: epoch, Sequence: drainSequence})
	case ActionResume:
		unlockState()
		if nodeStatus != NodeStatusHealthy {
			return fmt.Errorf("node %s is unavailable for allocation resume", requestNodeID)
		}
		if draining {
			return nil
		}
		request := &nodeapi.DrainAllocationRequest{AllocationID: allocationID, Generation: generation, Epoch: epoch, Sequence: drainSequence}
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
		if err := s.client.StopAllocation(ctx, requestNodeID, address, &nodeapi.StopAllocationRequest{AllocationID: allocationID, Generation: generation, Epoch: epoch, RetainLogs: true}); err != nil {
			if code := agentOperationCode(err); code == nodeapi.OperationStaleEpoch || code == nodeapi.OperationStaleGeneration {
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
				return fmt.Errorf("%w (persist allocation failure: %w)", err, persistErr)
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
