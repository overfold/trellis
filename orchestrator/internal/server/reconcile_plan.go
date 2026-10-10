package server

import (
	"bytes"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	"github.com/overfold/trellis/orchestrator/internal/spec"
)

// reconcilePlanInput is the cluster view a reconciliation pass plans against.
// planReconciliation reads it and never mutates it: jobs, nodes, backoffs,
// volume owners, network ports, and resume deliveries are borrowed
// read-only, and Allocations are detached snapshots the planner copies before
// changing. The caller must keep the borrowed values stable for the duration
// of the call.
type reconcilePlanInput struct {
	Now         time.Time
	LeaderSince time.Time
	// Heartbeats holds the latest heartbeat this leader received from each
	// node in the current term. Node statuses were derived from the same
	// view.
	Heartbeats            map[uuid.UUID]time.Time
	Limits                spec.Limits
	Policy                ReplacementPolicy
	AllocationLossTimeout time.Duration
	Jobs                  map[string]*Job
	Nodes                 map[uuid.UUID]*Node
	// Allocations are detached snapshots of the stored allocation records.
	// Their Node fields are copies; placements use the canonical Nodes.
	Allocations  []*Allocation
	Backoffs     map[string]*ReplacementBackoff
	VolumeOwners map[string]uuid.UUID
	// NetworkReady holds namespaces with a WireGuard port slot and a subnet
	// on every node; only they accept new namespace-networked placements.
	NetworkReady map[string]bool
	// DeliveredResumes holds the drain sequence of the latest resume each
	// allocation generation acknowledged in the current control epoch.
	DeliveredResumes map[resumeDeliveryKey]uint64
	// NewAllocationSuffix returns the unique suffix of a new allocation ID.
	NewAllocationSuffix func() string
}

// reconcilePlan is the outcome of one reconciliation pass: the durable commit,
// the allocation records it changes, and the agent actions to dispatch once
// the commit succeeds.
type reconcilePlan struct {
	Commit ReconciliationCommit
	// Updated are working copies of existing allocations carrying planned
	// changes, in allocation ID order. They lead Commit.Allocations.
	Updated []*Allocation
	// Pruned are working copies of terminal allocations to delete.
	Pruned []*Allocation
	// Source maps every working copy of an existing allocation to the input
	// snapshot it was copied from.
	Source map[*Allocation]*Allocation
	// NewAllocations are created by this pass. Their Node fields reference the
	// canonical input Nodes; Commit.Allocations carries detached copies.
	NewAllocations []*Allocation
	// Actions are ordered for dispatch. Retained-original stops come first.
	// Allocation fields reference working copies or new allocations.
	Actions []Action
	// Events are cluster events to publish after the commit.
	Events []api.ClusterEvent
	// Diagnostics explain jobs the pass skipped.
	Diagnostics []reconcileDiagnostic
}

// reconcileDiagnostic is a structured log record produced while planning.
type reconcileDiagnostic struct {
	Message string
	Args    []any
}

func (p *reconcilePlan) empty() bool {
	return len(p.Updated) == 0 && len(p.NewAllocations) == 0 && len(p.Pruned) == 0 && len(p.Commit.Backoffs) == 0 && len(p.Commit.DeleteBackoffs) == 0
}

func activeAllocationPhase(phase lifecycle.Phase) bool {
	return phase != lifecycle.PhaseStopped && phase != lifecycle.PhaseFailed && phase != lifecycle.PhaseLost
}

// potentiallyLiveAllocation reports whether an allocation may still have a
// live container or may create one from an already accepted start. Pending
// allocations have no node-side execution and therefore consume no rollout
// surge capacity.
func potentiallyLiveAllocation(phase lifecycle.Phase) bool {
	switch phase {
	case lifecycle.PhasePlaced, lifecycle.PhaseStarting, lifecycle.PhaseRunning, lifecycle.PhaseStopping:
		return true
	default:
		return false
	}
}

func jobHasGroup(job *Job, name string) bool {
	for _, group := range job.Spec.TaskGroups {
		if group.Name == name {
			return true
		}
	}
	return false
}

func sortedNodes(nodes map[uuid.UUID]*Node) []*Node {
	sorted := make([]*Node, 0, len(nodes))
	for _, node := range nodes {
		sorted = append(sorted, node)
	}
	sort.Slice(sorted, func(i, j int) bool { return bytes.Compare(sorted[i].ID[:], sorted[j].ID[:]) < 0 })
	return sorted
}

// planReconciliation decides how to converge allocations on the desired job
// specs. It performs no I/O, takes no locks, and is deterministic apart from
// NewAllocationSuffix.
func planReconciliation(in *reconcilePlanInput) (*reconcilePlan, error) {
	now := in.Now
	limits := in.Limits
	policy := in.Policy
	plan := &reconcilePlan{Source: make(map[*Allocation]*Allocation, len(in.Allocations))}
	newSuffix := in.NewAllocationSuffix
	if newSuffix == nil {
		newSuffix = func() string { return uuid.NewString()[:8] }
	}
	recoveryElapsed := now.Sub(in.LeaderSince) >= leaderRecoveryGrace
	lossTimedOut := func(node *Node) bool {
		return recoveryElapsed && node != nil && now.Sub(silentSince(in.Heartbeats[node.ID], in.LeaderSince)) >= in.AllocationLossTimeout
	}

	jobKeys := make([]string, 0, len(in.Jobs))
	for key := range in.Jobs {
		jobKeys = append(jobKeys, key)
	}
	sort.Strings(jobKeys)
	// admittedJobs holds the stored canonical jobs this pass converges on.
	// Stored jobs are never modified while planning.
	admittedJobs := make(map[string]*Job, len(jobKeys))
	namespaceDesired := make(map[string]int64)
	for _, key := range jobKeys {
		stored := in.Jobs[key]
		if err := spec.ValidateWithLimits(stored.Spec, limits); err != nil {
			plan.Diagnostics = append(plan.Diagnostics, reconcileDiagnostic{Message: "skip invalid job during reconciliation", Args: []any{"job", key, "error", err}})
			continue
		}
		job := stored
		namespace := job.Spec.Namespace
		desired := desiredAllocations(job.Spec)
		if namespaceDesired[namespace]+desired > int64(limits.MaxDesiredAllocationsPerNamespace) {
			plan.Diagnostics = append(plan.Diagnostics, reconcileDiagnostic{Message: "skip job exceeding namespace allocation limit during reconciliation", Args: []any{"job", key, "namespace", namespace, "limit", limits.MaxDesiredAllocationsPerNamespace}})
			continue
		}
		namespaceDesired[namespace] += desired
		admittedJobs[key] = job
	}

	allocations := make([]*Allocation, len(in.Allocations))
	for i, snapshot := range in.Allocations {
		// Working copies share the snapshot's detached node, which planning
		// only reads.
		planned := snapshot.cloneOnto(snapshot.Node)
		allocations[i] = planned
		plan.Source[planned] = snapshot
	}
	sort.Slice(allocations, func(i, j int) bool { return allocations[i].ID < allocations[j].ID })
	allocationsByGroup := make(map[string][]*Allocation)
	for _, allocation := range allocations {
		key := replacementBackoffKey(allocation.Namespace, allocation.JobName, allocation.TaskGroupName)
		allocationsByGroup[key] = append(allocationsByGroup[key], allocation)
	}
	volumeOwners := make(map[string]uuid.UUID, len(in.VolumeOwners))
	maps.Copy(volumeOwners, in.VolumeOwners)
	nodes := sortedNodes(in.Nodes)
	// An orphan has no record from which to recover its resource and port
	// reservations. Withhold only its node until acknowledged cleanup, even
	// while recovery grace prevents sending that cleanup.
	placementNodes := make([]*Node, 0, len(nodes))
	blockedNodes := make(map[uuid.UUID]bool)
	for _, node := range nodes {
		for _, observed := range node.observedAllocations {
			if observed.RetainedLogs {
				continue
			}
			owned := slices.ContainsFunc(allocations, func(allocation *Allocation) bool {
				return allocation.Node != nil && allocation.Node.ID == node.ID && allocation.ID == observed.ID && allocation.Generation == observed.Generation
			})
			if !owned {
				blockedNodes[node.ID] = true
				break
			}
		}
		if !blockedNodes[node.ID] {
			placementNodes = append(placementNodes, node)
		}
	}
	rolloutEvidence := make(map[*Allocation]bool)
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
	// Terminal task failure does not prove that live siblings were cleaned up.
	observedOccupancy := make(map[*Allocation]bool)
	retainedSet := make(map[*Allocation]bool)
	for _, allocation := range allocations {
		if allocation.Node == nil || activeAllocationPhase(allocation.Phase) {
			continue
		}
		if node := in.Nodes[allocation.Node.ID]; node != nil && !lossTimedOut(node) {
			// A completed stop is stronger proof than an older heartbeat.
			if allocation.Phase == lifecycle.PhaseStopped && !node.observedAt.After(allocation.TransitionedAt) {
				continue
			}
			if allocation.Phase == lifecycle.PhaseFailed && node.observedAt.Before(allocation.TransitionedAt) {
				observedOccupancy[allocation] = true
			}
			for _, observed := range node.observedAllocations {
				if observed.ID == allocation.ID && observed.Generation == allocation.Generation && !observed.RetainedLogs {
					observedOccupancy[allocation] = true
				}
			}
		}
	}
	if !in.LeaderSince.IsZero() && recoveryElapsed {
		type observationKey struct {
			nodeID     uuid.UUID
			allocation string
			generation uint64
		}
		desired := make(map[observationKey]bool)
		active := make(map[observationKey]bool)
		lost := make(map[observationKey]*Allocation)
		for _, allocation := range allocations {
			if allocation.Node != nil {
				key := observationKey{nodeID: allocation.Node.ID, allocation: allocation.ID, generation: allocation.Generation}
				desired[key] = true
				active[key] = activeAllocationPhase(allocation.Phase)
			}
			if allocation.Node != nil && allocation.Phase == lifecycle.PhaseLost {
				lost[observationKey{nodeID: allocation.Node.ID, allocation: allocation.ID, generation: allocation.Generation}] = allocation
			}
		}
		for _, node := range nodes {
			if node.Status != NodeStatusHealthy && node.Status != NodeStatusDraining {
				continue
			}
			for _, observed := range node.observedAllocations {
				key := observationKey{nodeID: node.ID, allocation: observed.ID, generation: observed.Generation}
				// A lost record may have missed persisting a successful stop.
				// Redeliver it even for retained logs to durably confirm cleanup.
				if desired[key] && observed.RetainedLogs && lost[key] == nil {
					continue
				}
				if active[key] && !observed.RetainedLogs {
					continue
				}
				if original := lost[key]; original != nil && observed.Phase == lifecycle.PhaseRunning && observed.Health == lifecycle.HealthHealthy {
					complete := true
					for _, task := range original.Tasks {
						if !observed.Tasks[task.Name] {
							complete = false
						}
					}
					if complete {
						retained = append(retained, &retainedOriginal{allocation: original, node: node})
						retainedSet[original] = true
						continue
					}
				}
				actions = append(actions, Action{Type: ActionStopObserved, Node: node, ID: observed.ID, Generation: observed.Generation, RetainLogs: desired[key]})
			}
		}
		sortRetainedOriginals(retained)
	}
	valid := make([]*Allocation, 0, len(allocations))
	validByGroup := make(map[string][]*Allocation)
	for _, allocation := range allocations {
		key := jobKey(allocation.Namespace, allocation.JobName)
		job := admittedJobs[key]
		// Loss applies to every execution, including deleted jobs and obsolete
		// incarnations. Cleanup and retry policy must not bypass this timeout.
		if activeAllocationPhase(allocation.Phase) && allocation.Node != nil && lossTimedOut(allocation.Node) {
			_ = allocation.Transition(lifecycle.PhaseLost, now, "node_unavailable", "node did not re-register before the allocation loss timeout")
			markUpdated(allocation)
			continue
		}
		// Once stopping is durable, health changes cannot revoke that intent.
		if allocation.Phase == lifecycle.PhaseStopping {
			if allocation.NextRetryAt == nil || !now.Before(*allocation.NextRetryAt) {
				actions = append(actions, Action{Type: ActionStop, Allocation: allocation})
			}
			continue
		}
		if job == nil {
			if activeAllocationPhase(allocation.Phase) && allocation.Node != nil {
				actions = append(actions, Action{Type: ActionStop, Allocation: allocation})
			} else if allocation.Phase == lifecycle.PhasePending {
				_ = allocation.Transition(lifecycle.PhaseStopping, now, "namespace_limit", "job exceeds the namespace desired-allocation limit")
				_ = allocation.Transition(lifecycle.PhaseStopped, now, "namespace_limit", "job exceeds the namespace desired-allocation limit")
				markUpdated(allocation)
			}
			continue
		}
		if allocation.JobIncarnation != job.Incarnation {
			if activeAllocationPhase(allocation.Phase) && allocation.Node != nil {
				actions = append(actions, Action{Type: ActionStop, Allocation: allocation})
			} else if allocation.Phase == lifecycle.PhasePending {
				_ = allocation.Transition(lifecycle.PhaseStopping, now, "job_changed", "pending allocation belongs to an earlier job incarnation")
				_ = allocation.Transition(lifecycle.PhaseStopped, now, "job_changed", "pending allocation belongs to an earlier job incarnation")
				markUpdated(allocation)
			}
			continue
		}
		if allocation.Phase == lifecycle.PhasePending {
			if !jobHasGroup(job, allocation.TaskGroupName) || allocation.JobRevision != job.Revision {
				_ = allocation.Transition(lifecycle.PhaseStopping, now, "job_changed", "pending allocation is obsolete")
				_ = allocation.Transition(lifecycle.PhaseStopped, now, "job_changed", "pending allocation is obsolete")
				markUpdated(allocation)
				continue
			}
			valid = append(valid, allocation)
			continue
		}
		if !activeAllocationPhase(allocation.Phase) {
			continue
		}
		if allocation.NextRetryAt != nil && now.Before(*allocation.NextRetryAt) {
			valid = append(valid, allocation)
			continue
		}
		if updateStrategy(job, allocation.TaskGroupName) == spec.UpdateRecreate && (allocation.JobRevision < job.Revision || allocation.DrainReason == "restart") {
			actions = append(actions, Action{Type: ActionStop, Allocation: allocation})
			continue
		}
		if allocation.Draining && allocation.Node != nil && (allocation.Node.Status == NodeStatusHealthy || allocation.Node.Status == NodeStatusDraining) {
			actions = append(actions, Action{Type: ActionDrain, Allocation: allocation})
		}
		if allocation.Node != nil && allocation.Node.Status == NodeStatusDraining {
			if !jobHasGroup(job, allocation.TaskGroupName) {
				actions = append(actions, Action{Type: ActionStop, Allocation: allocation})
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
					continue
				}
				valid = append(valid, allocation)
				continue
			default:
				actions = append(actions, Action{Type: ActionStop, Allocation: allocation})
				continue
			}
		}
		if allocation.Node == nil || allocation.Node.Status != NodeStatusHealthy {
			continue
		}
		// A start request carries the drain state itself, so only a running
		// allocation needs a separate resume redelivery.
		if allocation.Phase == lifecycle.PhaseRunning && !allocation.Draining && allocation.DrainSequence > 0 &&
			in.DeliveredResumes[resumeDeliveryKey{allocation: allocation.ID, generation: allocation.Generation}] != allocation.DrainSequence {
			actions = append(actions, Action{Type: ActionResume, Allocation: allocation})
		}
		if allocation.Phase == lifecycle.PhasePlaced || allocation.Phase == lifecycle.PhaseStarting {
			if !blockedNodes[allocation.Node.ID] {
				actions = append(actions, Action{Type: ActionStart, Allocation: allocation})
			}
		}
		valid = append(valid, allocation)
	}
	for _, allocation := range valid {
		key := replacementBackoffKey(allocation.Namespace, allocation.JobName, allocation.TaskGroupName)
		validByGroup[key] = append(validByGroup[key], allocation)
	}
	occupied := make([]*Allocation, 0, len(allocations))
	occupiedSet := make(map[*Allocation]bool, len(allocations))
	for _, allocation := range allocations {
		if allocation.Node != nil && (activeAllocationPhase(allocation.Phase) || observedOccupancy[allocation] && !retainedSet[allocation]) {
			occupied = append(occupied, allocation)
			occupiedSet[allocation] = true
		}
	}

	plannedBackoffs := make(map[string]*ReplacementBackoff)
	for _, key := range jobKeys {
		job := admittedJobs[key]
		if job == nil {
			continue
		}
		jobName := job.Spec.Name
		namespace := job.Spec.Namespace
		for _, group := range executionSpec(job.Spec, job.ResolvedImages).TaskGroups {
			backoffKey := replacementBackoffKey(namespace, jobName, group.Name)
			backoff := planReplacementBackoff(policy, in.Backoffs[backoffKey], namespace, jobName, group.Name, job.Revision, allocationsByGroup[backoffKey], now, job.Incarnation)
			plannedBackoffs[backoffKey] = backoff
			live := 0
			for _, alloc := range allocationsByGroup[backoffKey] {
				if potentiallyLiveAllocation(alloc.Phase) || observedOccupancy[alloc] && !retainedSet[alloc] {
					live++
				}
			}
			for _, original := range retained {
				if original.inGroup(namespace, jobName, group.Name) {
					// A lost record is terminal, but a returned node may report
					// its container still running. Keep charging that observed
					// execution until a stop has actually completed.
					live++
				}
			}
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
			// Stopping and terminal originals still identify an unfinished
			// rollout. Keep one durable witness until desired new capacity is
			// healthy, so history pruning cannot erase the parallel limit.
			var rolloutOriginal *Allocation
			healthyCurrent := 0
			for _, alloc := range current {
				if alloc.Phase == lifecycle.PhaseRunning && alloc.Health == lifecycle.HealthHealthy {
					healthyCurrent++
				}
			}
			if group.Update.Strategy == spec.UpdateRolling && healthyCurrent < group.Count {
				for _, alloc := range allocationsByGroup[backoffKey] {
					if alloc.JobRevision != job.Revision || alloc.JobIncarnation != job.Incarnation || alloc.Draining {
						rolloutOriginal = alloc
						rolloutEvidence[alloc] = true
						break
					}
				}
			}
			unavailable := 0
			for _, alloc := range allocationsByGroup[backoffKey] {
				if alloc.JobIncarnation == job.Incarnation && activeAllocationPhase(alloc.Phase) && !alloc.Draining && alloc.Node != nil && alloc.Node.Status != NodeStatusHealthy && !lossTimedOut(alloc.Node) {
					unavailable++
				}
			}
			// Keep healthy running replicas first, with ID order breaking ties.
			sort.SliceStable(current, func(i, j int) bool {
				iHealthy := current[i].Phase == lifecycle.PhaseRunning && current[i].Health == lifecycle.HealthHealthy
				jHealthy := current[j].Phase == lifecycle.PhaseRunning && current[j].Health == lifecycle.HealthHealthy
				return iHealthy && !jHealthy
			})
			// Draining stops consume the front, rather than the back.
			sort.SliceStable(draining, func(i, j int) bool {
				iHealthy := draining[i].Phase == lifecycle.PhaseRunning && draining[i].Health == lifecycle.HealthHealthy
				jHealthy := draining[j].Phase == lifecycle.PhaseRunning && draining[j].Health == lifecycle.HealthHealthy
				return !iHealthy && jHealthy
			})
			for len(current) > group.Count {
				actions = append(actions, Action{Type: ActionStop, Allocation: current[len(current)-1]})
				current = current[:len(current)-1]
			}
			// Retain only capacity missing from healthy current and draining
			// replicas. Redundant returned originals otherwise fill surge slots
			// forever while waiting for replacements that cannot be placed.
			missing := group.Count
			for _, alloc := range current {
				if alloc.Phase == lifecycle.PhaseRunning && alloc.Health == lifecycle.HealthHealthy {
					missing--
				}
			}
			for _, alloc := range draining {
				if alloc.Phase == lifecycle.PhaseRunning && alloc.Health == lifecycle.HealthHealthy {
					missing--
				}
			}
			retainedAvailable := 0
			for _, original := range retained {
				if original.released || original.kept || !original.inGroup(namespace, jobName, group.Name) {
					continue
				}
				if missing > 0 && original.allocation.JobIncarnation == job.Incarnation && (original.allocation.JobRevision == job.Revision && original.allocation.DrainReason != "restart" || updateStrategy(job, group.Name) == spec.UpdateRolling) && !original.blocksPlacedAllocation(occupied) {
					original.kept = true
					missing--
					retainedAvailable++
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
				// Retained capacity can only justify stopping unhealthy drains:
				// healthy drains were already deducted from the retention deficit.
				drainsToStop := min(max(healthyNew+retainedAvailable+len(draining)-group.Count, 0), len(draining))
				for i := range drainsToStop {
					if draining[i].NextRetryAt == nil || !now.Before(*draining[i].NextRetryAt) {
						actions = append(actions, Action{Type: ActionStop, Allocation: draining[i]})
					}
				}
			}
			deficit := max(group.Count-len(current)-unavailable, 0)
			strategy := group.Update.Strategy
			if strategy == spec.UpdateRecreate {
				// Planned stops may fail or run concurrently on other nodes.
				// Admit replacements only in a later pass after cleanup is proven.
				blocked := false
				for _, alloc := range allocationsByGroup[backoffKey] {
					obsolete := alloc.JobIncarnation != job.Incarnation || alloc.JobRevision != job.Revision || alloc.DrainReason == "restart"
					if obsolete && (observedOccupancy[alloc] || potentiallyLiveAllocation(alloc.Phase)) {
						blocked = true
					}
				}
				if blocked {
					actions = slices.DeleteFunc(actions, func(action Action) bool {
						return action.Type == ActionStart && replacementBackoffKey(action.Allocation.Namespace, action.Allocation.JobName, action.Allocation.TaskGroupName) == backoffKey
					})
					continue
				}
			}
			parallel := 0
			if strategy == spec.UpdateRolling {
				parallel = group.Update.MaxParallel
			}
			if backoff != nil && backoff.DelayedReplacements > max(deficit, 0) {
				// Failed allocations whose capacity is no longer missing have
				// nothing left to replace.
				backoff.DelayedReplacements = max(deficit, 0)
			}
			if deficit > 0 {
				if strategy == spec.UpdateRolling && rolloutOriginal != nil {
					inFlight := unavailable
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
				if !in.NetworkReady[namespace] {
					continue
				}
			}
			// Replacements of failed allocations wait until the backoff
			// elapses; the rest of the deficit is placed now.
			placeable := deficit - backoff.withheld(deficit, now)
			if strategy == spec.UpdateRolling {
				// A stop planned by this pass has not released capacity yet: it
				// can run concurrently with starts on other nodes and can fail.
				// Only a durably completed stop (or a terminal loss) frees a
				// surge slot for a later pass. This keeps actual and accepted
				// executions at or below count + max_parallel without imposing
				// cross-node serialization on unrelated actions.
				// Subtract first: max_parallel can be MaxInt.
				if slots, overflow := checkedAddInt(group.Count-live, parallel); !overflow {
					placeable = min(placeable, max(slots, 0))
				}
			}
			if placeable <= 0 {
				continue
			}
			requiredCapabilities := spec.GroupRequiredCapabilities(&group)
			intent := PlacementIntent{Namespace: namespace, JobName: jobName, TaskGroupName: group.Name, Count: placeable, Nodes: placementNodes, Allocations: occupied, DesiredAllocations: valid, Tasks: group.Tasks, Constraints: group.Constraints, RequiredCapabilities: requiredCapabilities, VolumeOwners: volumeOwners}
			placements, released, diagnostic := scheduleAroundRetained(intent, retained)
			for _, original := range released {
				retainedStops = append(retainedStops, original.stopAction())
			}
			for i, placement := range placements {
				for _, claim := range placement.VolumeClaims {
					volumeOwners[volumeRegistrationKey(claim.Namespace, claim.Name)] = claim.NodeID
				}
				node := in.Nodes[placement.NodeID]
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
				name := fmt.Sprintf("%s-%s-%s-%s", namespace, jobName, group.Name, newSuffix())
				allocation := &Allocation{ID: name, Namespace: namespace, JobName: jobName, TaskGroupName: group.Name, Tasks: group.Tasks, Node: node, Generation: 1, JobIncarnation: job.Incarnation, JobRevision: job.Revision, Phase: lifecycle.PhasePlaced, Health: lifecycle.HealthUnknown, Diagnostic: lifecycle.Diagnostic{CreatedAt: now, TransitionedAt: now}}
				actions = append(actions, Action{Type: ActionStart, Allocation: allocation})
				plan.NewAllocations = append(plan.NewAllocations, allocation)
				valid = append(valid, allocation)
				occupied = append(occupied, allocation)
				occupiedSet[allocation] = true
			}
			unplaced := placeable - len(placements)
			if unplaced > 0 {
				placedPending := min(len(placements), len(pending))
				reusable := pending[placedPending:]
				for i := 0; i < min(unplaced, len(reusable)); i++ {
					allocation := reusable[i]
					if allocation.Reason != diagnostic.Reason || allocation.Message != diagnostic.Message {
						_ = allocation.Transition(lifecycle.PhasePending, now, diagnostic.Reason, diagnostic.Message)
						markUpdated(allocation)
					}
				}
				for i := len(reusable); i < unplaced; i++ {
					name := fmt.Sprintf("%s-%s-%s-%s", namespace, jobName, group.Name, newSuffix())
					allocation := &Allocation{ID: name, Namespace: namespace, JobName: jobName, TaskGroupName: group.Name, Tasks: group.Tasks, Generation: 1, JobIncarnation: job.Incarnation, JobRevision: job.Revision, Phase: lifecycle.PhasePending, Health: lifecycle.HealthUnknown, Diagnostic: lifecycle.Diagnostic{CreatedAt: now, TransitionedAt: now, Reason: diagnostic.Reason, Message: diagnostic.Message}}
					plan.NewAllocations = append(plan.NewAllocations, allocation)
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
	plan.Actions = append(retainedStops, actions...)

	pruneSkip := maps.Clone(plannedUpdates)
	maps.Copy(pruneSkip, rolloutEvidence)
	for allocation := range observedOccupancy {
		pruneSkip[allocation] = true
	}
	plan.Pruned = planTerminalPruning(policy.RetainTerminal, allocations, pruneSkip)
	prunedSet := make(map[*Allocation]bool, len(plan.Pruned))
	for _, allocation := range plan.Pruned {
		prunedSet[allocation] = true
		for i := range plan.Actions {
			action := &plan.Actions[i]
			if action.Type == ActionStopObserved && action.ID == allocation.ID && action.Generation == allocation.Generation {
				action.RetainLogs = false
			}
		}
	}
	retainedGroups := make(map[string]bool)
	for _, allocation := range allocations {
		if !prunedSet[allocation] {
			retainedGroups[replacementBackoffKey(allocation.Namespace, allocation.JobName, allocation.TaskGroupName)] = true
		}
	}
	for _, allocation := range plan.NewAllocations {
		retainedGroups[replacementBackoffKey(allocation.Namespace, allocation.JobName, allocation.TaskGroupName)] = true
	}
	var backoffPuts, backoffDeletes []*ReplacementBackoff
	for key, previous := range in.Backoffs {
		if _, planned := plannedBackoffs[key]; planned {
			continue
		}
		owner := jobKey(previous.Namespace, previous.JobName)
		if in.Jobs[owner] != nil && admittedJobs[owner] == nil {
			continue
		}
		if !retainedGroups[key] {
			backoffDeletes = append(backoffDeletes, previous)
			continue
		}
		// The group is no longer desired: forget its failures but keep the
		// record, and with it the failed allocations already seen, while the
		// group's allocation records remain.
		plannedBackoffs[key] = planReplacementBackoff(policy, previous, previous.Namespace, previous.JobName, previous.TaskGroupName, 0, allocations, now, previous.JobIncarnation)
	}
	backoffKeys := make([]string, 0, len(plannedBackoffs))
	for key := range plannedBackoffs {
		backoffKeys = append(backoffKeys, key)
	}
	sort.Strings(backoffKeys)
	for _, key := range backoffKeys {
		next, previous := plannedBackoffs[key], in.Backoffs[key]
		if next == nil || next.equal(previous) {
			continue
		}
		backoffPuts = append(backoffPuts, next)
		if previous == nil || next.Failures > previous.Failures {
			nextReplacement := next.NextReplacementAt
			plan.Events = append(plan.Events, api.ClusterEvent{
				Type:              api.EventJobReplacementDelayed,
				Namespace:         next.Namespace,
				JobName:           next.JobName,
				Group:             next.TaskGroupName,
				AllocationID:      next.LastAllocationID,
				Revision:          next.JobRevision,
				Failures:          next.Failures,
				NextReplacementAt: &nextReplacement,
				At:                now,
			})
		}
	}
	sort.Slice(backoffDeletes, func(i, j int) bool { return backoffDeletes[i].key() < backoffDeletes[j].key() })

	volumeKeys := make([]string, 0, len(volumeOwners))
	for key := range volumeOwners {
		if _, exists := in.VolumeOwners[key]; exists {
			continue
		}
		volumeKeys = append(volumeKeys, key)
	}
	sort.Strings(volumeKeys)
	volumeRegistrations := make([]*VolumeRegistration, 0, len(volumeKeys))
	for _, key := range volumeKeys {
		namespace, name, ok := strings.Cut(key, "/")
		if !ok || namespace == "" || name == "" {
			return nil, fmt.Errorf("invalid volume registration key %q", key)
		}
		volumeRegistrations = append(volumeRegistrations, &VolumeRegistration{Namespace: namespace, Name: name, NodeID: volumeOwners[key]})
	}

	for _, allocation := range allocations {
		if plannedUpdates[allocation] {
			plan.Updated = append(plan.Updated, allocation)
		}
	}
	updates := make([]*Allocation, 0, len(plan.Updated)+len(plan.NewAllocations))
	updates = append(updates, plan.Updated...)
	for _, allocation := range plan.NewAllocations {
		updates = append(updates, allocation.Clone())
	}
	prunedIDs := make([]string, len(plan.Pruned))
	for i, allocation := range plan.Pruned {
		prunedIDs[i] = allocation.ID
	}
	plan.Commit = ReconciliationCommit{Allocations: updates, DeleteAllocations: prunedIDs, VolumeRegistrations: volumeRegistrations, Backoffs: backoffPuts, DeleteBackoffs: backoffDeletes}
	return plan, nil
}
