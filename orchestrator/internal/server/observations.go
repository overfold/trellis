package server

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/internal/api"
	"github.com/overfold/trellis/internal/lifecycle"
	"github.com/overfold/trellis/internal/spec"
)

// maxObservationCommitRecords bounds the node and allocation records one
// observation commit writes. A node's records are never split across
// commits, so one node's report stays atomic even when it alone exceeds the
// bound.
const maxObservationCommitRecords = 1024

// allocationObservation aggregates one allocation generation's task reports
// from a heartbeat.
type allocationObservation struct {
	ID            string
	Generation    uint64
	Phase         lifecycle.Phase
	Health        lifecycle.Health
	Reason        api.OperationCode
	Endpoints     []api.AllocationEndpoint
	Ports         []api.PortMapping
	ObservedTasks map[string]bool
	StartFailure  *api.StartFailure
}

type allocationGeneration struct {
	id         string
	generation uint64
}

// nodeObservation is one validated heartbeat report awaiting application.
// Each report is a complete snapshot of the node, so a newer report of the
// same node supersedes an older one that has not been applied yet.
type nodeObservation struct {
	node         uuid.UUID
	at           time.Time
	version      string
	volumes      []string
	capabilities []spec.NodeCapability
	resources    nodeResourceObservation
	allocations  map[allocationGeneration]allocationObservation
	// observed lists the reported allocation generations in ID order.
	observed []observedAllocation
}

// observationQueue hands heartbeat reports to the observation applier. It
// holds at most one pending report per node: a heartbeat for a node whose
// previous report is still pending replaces it. Heartbeats are accepted only
// from registered nodes, so the queue is bounded by the registered node count
// and a heartbeat never blocks on it. Under overload (the applier slower than
// heartbeats, for example behind a slow Raft commit or reconciliation pass)
// intermediate reports are superseded rather than queued, and the applier
// catches up with each node's latest report. mu is a leaf lock.
type observationQueue struct {
	mu      sync.Mutex
	pending map[uuid.UUID]*nodeObservation
	// term advances with every leadership term. A batch taken in an earlier
	// term is discarded instead of applied.
	term uint64
	// wake holds a signal while reports are pending.
	wake chan struct{}
}

// wakeChannel returns the channel signalled when reports are pending.
func (q *observationQueue) wakeChannel() chan struct{} {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.wake == nil {
		q.wake = make(chan struct{}, 1)
	}
	return q.wake
}

// submit queues a report and reports whether it superseded a pending one.
func (q *observationQueue) submit(observation *nodeObservation) bool {
	q.mu.Lock()
	if q.pending == nil {
		q.pending = make(map[uuid.UUID]*nodeObservation)
	}
	_, superseded := q.pending[observation.node]
	q.pending[observation.node] = observation
	q.mu.Unlock()
	select {
	case q.wakeChannel() <- struct{}{}:
	default:
	}
	return superseded
}

// take removes every pending report, in node ID order, together with the
// term they were taken in.
func (q *observationQueue) take() ([]*nodeObservation, uint64) {
	q.mu.Lock()
	pending, term := q.pending, q.term
	q.pending = nil
	q.mu.Unlock()
	batch := make([]*nodeObservation, 0, len(pending))
	for _, observation := range pending {
		batch = append(batch, observation)
	}
	sort.Slice(batch, func(i, j int) bool { return bytes.Compare(batch[i].node[:], batch[j].node[:]) < 0 })
	return batch, term
}

// reset discards every pending report and starts a new term.
func (q *observationQueue) reset() {
	q.mu.Lock()
	q.pending = nil
	q.term++
	q.mu.Unlock()
}

func (q *observationQueue) currentTerm() uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.term
}

func (q *observationQueue) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.pending)
}

// runObservationApplier applies heartbeat reports until the term ends.
func (s *Server) runObservationApplier(ctx context.Context) {
	wake := s.observations.wakeChannel()
	for {
		// Reports may have arrived before the term's applier started.
		s.applyObservations(ctx)
		select {
		case <-ctx.Done():
			return
		case <-wake:
		}
	}
}

// observationUpdate is the planned outcome of one node's report.
type observationUpdate struct {
	observation *nodeObservation
	node        *Node
	// next holds the node's durable facts and observations after the report.
	next *Node
	// summary is set when the node's durable facts changed.
	summary     *NodeSummary
	allocations []allocationObservationUpdate
}

type allocationObservationUpdate struct {
	current *Allocation
	next    *Allocation
}

func (u *observationUpdate) records() int {
	records := len(u.allocations)
	if u.summary != nil {
		records++
	}
	return records
}

// applyObservations applies every pending heartbeat report. It holds
// mutationMu, like every other durable mutation, so each allocation's
// observed state is computed from and committed against the latest committed
// record: a reconciliation pass and the applier never overwrite each other's
// changes. Reports are applied in node ID order and each commit lists its
// records in that order, so the written Raft entries do not depend on arrival
// order. A report whose commit fails is dropped; the node's next heartbeat
// carries its complete state again.
func (s *Server) applyObservations(ctx context.Context) {
	for {
		batch, term := s.observations.take()
		if len(batch) == 0 {
			return
		}
		s.applyObservationBatch(ctx, batch, term)
	}
}

func (s *Server) applyObservationBatch(ctx context.Context, batch []*nodeObservation, term uint64) {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	if s.observations.currentTerm() != term {
		s.discardObservations("term_changed", len(batch))
		return
	}
	updates := make([]*observationUpdate, 0, len(batch))
	for _, observation := range batch {
		update := s.planObservation(observation)
		if update == nil {
			s.discardObservations("node_removed", 1)
			continue
		}
		updates = append(updates, update)
	}
	for start := 0; start < len(updates); {
		end, records := start, 0
		for end < len(updates) && (end == start || records+updates[end].records() <= maxObservationCommitRecords) {
			records += updates[end].records()
			end++
		}
		s.commitObservations(ctx, updates[start:end])
		start = end
	}
}

// planObservation computes a node's state after its report. It returns nil
// when the node is no longer registered.
func (s *Server) planObservation(observation *nodeObservation) *observationUpdate {
	s.mu.RLock()
	node := s.nodes[observation.node]
	if node == nil {
		s.mu.RUnlock()
		return nil
	}
	next := node.Clone()
	previous := nodeSummary(next)
	assigned := s.allocationsByNode[observation.node]
	if s.allocationsByNode == nil {
		for _, allocation := range s.allocations {
			if allocation.Node != nil && allocation.Node.ID == observation.node {
				assigned = append(assigned, allocation)
			}
		}
	}
	currents := make([]*Allocation, len(assigned))
	snapshots := make([]*Allocation, len(assigned))
	for i, allocation := range assigned {
		allocation.mu.Lock()
		currents[i], snapshots[i] = allocation, allocation.cloneOnto(next)
		allocation.mu.Unlock()
	}
	s.mu.RUnlock()

	next.Version = observation.version
	next.Volumes = observation.volumes
	next.Capabilities = observation.capabilities
	resources := observation.resources
	next.CPUCapacity, next.MemoryCapacity = resources.CPUCapacity, resources.MemoryCapacity
	next.CPUAllocatable, next.MemoryAllocatable = resources.CPUAllocatable, resources.MemoryAllocatable
	next.CPUUsage, next.MemoryUsed = resources.CPUUsage, resources.MemoryUsed
	next.MemoryAvailable, next.MetricsAt = resources.MemoryAvailable, resources.MetricsAt
	next.observedAllocations = observation.observed
	next.observedAt = observation.at

	update := &observationUpdate{observation: observation, node: node, next: next}
	if summary := nodeSummary(next); !summary.equal(previous) {
		update.summary = summary
	}
	for i, snapshot := range snapshots {
		if observeAllocation(snapshot, observation) && !snapshot.sameRecord(currents[i]) {
			update.allocations = append(update.allocations, allocationObservationUpdate{current: currents[i], next: snapshot})
		}
	}
	sort.Slice(update.allocations, func(i, j int) bool { return update.allocations[i].next.ID < update.allocations[j].next.ID })
	return update
}

// observeAllocation applies a node's report to a copy of one of its assigned
// allocations and reports whether the report concerns it. The fencing rules
// match every other observation path: a report is keyed by allocation ID and
// generation, a phase advances only along lifecycle.CanObserve, and a start
// failure counts once, for the attempt it ran.
func observeAllocation(a *Allocation, observation *nodeObservation) bool {
	heartbeatAt := observation.at
	info, ok := observation.allocations[allocationGeneration{id: a.ID, generation: a.Generation}]
	if !ok {
		if a.Phase != lifecycle.PhaseRunning && a.Phase != lifecycle.PhaseStarting {
			return false
		}
		info = allocationObservation{Phase: lifecycle.PhaseStarting, Health: lifecycle.HealthUnknown}
	}
	for _, task := range a.Tasks {
		if !info.ObservedTasks[task.Name] {
			if info.Phase == lifecycle.PhaseRunning {
				// Retry the incomplete allocation on the next reconciliation pass.
				info.Phase = lifecycle.PhaseStarting
			}
			if info.Health != lifecycle.HealthUnhealthy {
				info.Health = lifecycle.HealthUnknown
			}
			break
		}
	}
	// An unchanged phase keeps its reason, such as a counted start failure.
	if info.Phase.Valid() && info.Phase != a.Phase && lifecycle.CanObserve(a.Phase, info.Phase) {
		var reason string
		if info.Phase == lifecycle.PhaseFailed {
			reason = string(info.Reason)
		}
		if info.Phase == lifecycle.PhaseRunning && a.Phase != lifecycle.PhaseRunning {
			// The start completed; later starts get a fresh attempt budget.
			a.Attempt, a.NextRetryAt = 0, nil
		}
		_ = a.Transition(info.Phase, heartbeatAt, reason, "")
	}
	// Count the agent's failed background start once: the failure names
	// the attempt it ran for, and counting advances the attempt.
	if a.Phase == lifecycle.PhaseStarting && info.StartFailure != nil && info.StartFailure.Attempt == a.Attempt {
		if code := info.StartFailure.Code; code != "" {
			// Retrying the generation cannot fix it, as for the same
			// operation code on a start request.
			if a.Transition(lifecycle.PhaseFailed, heartbeatAt, string(code), info.StartFailure.Message) == nil {
				a.NextRetryAt = nil
			}
		} else {
			_ = recordStartFailure(a, heartbeatAt, info.StartFailure.Message)
		}
	}
	_ = a.SetHealth(info.Health)
	endpoints := cloneEndpoints(info.Endpoints)
	sort.SliceStable(endpoints, func(i, j int) bool { return endpoints[i].Task < endpoints[j].Task })
	a.Endpoints = endpoints
	a.Ports = append([]api.PortMapping(nil), info.Ports...)
	return true
}

// commitObservations persists the durable part of a group of node reports as
// one Raft entry, then applies every report to the in-memory state. Host
// metrics and observed allocations are leader observations: only durable node
// facts and allocation changes reach Raft, so a steady-state heartbeat is not
// a write.
func (s *Server) commitObservations(ctx context.Context, updates []*observationUpdate) {
	var summaries []*NodeSummary
	var allocations []*Allocation
	for _, update := range updates {
		if update.summary != nil {
			summaries = append(summaries, update.summary)
		}
		for _, allocation := range update.allocations {
			allocations = append(allocations, allocation.next)
		}
	}
	if len(summaries) > 0 || len(allocations) > 0 {
		if err := s.state.PutNodesAndAllocations(ctx, summaries, allocations); err != nil {
			if s.log != nil {
				s.log.Error("persist heartbeat observations; the next heartbeats retry them", "nodes", len(updates), "error", err)
			}
			s.discardObservations("commit_failed", len(updates))
			return
		}
	}
	var changed []*Allocation
	s.mu.Lock()
	for _, update := range updates {
		applyNodeObservation(update.node, update.next)
		for _, allocation := range update.allocations {
			allocation.current.mu.Lock()
			applyAllocationSnapshot(allocation.current, allocation.next)
			allocation.current.mu.Unlock()
			changed = append(changed, allocation.current)
		}
	}
	s.mu.Unlock()
	if len(changed) > 0 {
		s.refreshCatalogAllocations(changed)
	}
}

// applyNodeObservation copies the fields a heartbeat reports onto the
// canonical node. Status and registration facts are left to their own
// writers. The caller holds s.mu for writing.
func applyNodeObservation(node, next *Node) {
	node.Version = next.Version
	node.Volumes, node.Capabilities = next.Volumes, next.Capabilities
	node.CPUCapacity, node.MemoryCapacity = next.CPUCapacity, next.MemoryCapacity
	node.CPUAllocatable, node.MemoryAllocatable = next.CPUAllocatable, next.MemoryAllocatable
	node.CPUUsage, node.MemoryUsed = next.CPUUsage, next.MemoryUsed
	node.MemoryAvailable, node.MetricsAt = next.MemoryAvailable, next.MetricsAt
	node.observedAllocations, node.observedAt = next.observedAllocations, next.observedAt
}

func (s *Server) discardObservations(reason string, count int) {
	if s.metrics != nil {
		s.metrics.ObservationsDiscarded.WithLabelValues(reason).Add(float64(count))
	}
}

// newNodeObservation validates a heartbeat report and aggregates its task
// reports per allocation generation. It reads no server state.
func newNodeObservation(nodeID uuid.UUID, at time.Time, actual []api.AllocationStatus, version string, volumes []string, capabilities []spec.NodeCapability, resources nodeResourceObservation) (*nodeObservation, error) {
	if len(actual) > maxHeartbeatAllocationStatuses {
		return nil, fmt.Errorf("heartbeat allocation status count %d exceeds limit %d", len(actual), maxHeartbeatAllocationStatuses)
	}
	if err := validateNodeCapacity(resources.CPUCapacity, resources.MemoryCapacity, resources.CPUAllocatable, resources.MemoryAllocatable); err != nil {
		return nil, err
	}
	if resources.CPUUsage != nil && (*resources.CPUUsage < 0 || *resources.CPUUsage > 1) {
		return nil, fmt.Errorf("node CPU usage must be between 0 and 1")
	}
	if (resources.MemoryUsed != nil && *resources.MemoryUsed < 0) || (resources.MemoryAvailable != nil && *resources.MemoryAvailable < 0) {
		return nil, fmt.Errorf("node memory observations must be non-negative")
	}
	statuses := make(map[allocationGeneration]allocationObservation, len(actual))
	for _, a := range actual {
		if !a.Phase.Valid() || !a.Health.Valid() {
			return nil, fmt.Errorf("invalid allocation state for %s: phase=%q health=%q", a.ID, a.Phase, a.Health)
		}
		if a.Reason != "" && (a.Phase != lifecycle.PhaseFailed || a.Reason != api.OperationRestartExhausted) {
			return nil, fmt.Errorf("invalid failure reason for %s: phase=%q reason=%q", a.ID, a.Phase, a.Reason)
		}
		if a.StartFailure != nil && (a.Phase != lifecycle.PhaseStarting || a.StartFailure.Attempt < 0 || len(a.StartFailure.Message) > api.MaxStartFailureMessageBytes || !terminalStartFailureCode(a.StartFailure.Code)) {
			return nil, fmt.Errorf("invalid start failure for %s: phase=%q attempt=%d message bytes=%d", a.ID, a.Phase, a.StartFailure.Attempt, len(a.StartFailure.Message))
		}
		phase, health := a.Phase, a.Health
		key := allocationGeneration{id: a.ID, generation: a.Generation}
		info := statuses[key]
		if len(info.ObservedTasks) == 0 {
			info.ID, info.Generation, info.Phase, info.Health = a.ID, a.Generation, phase, health
		} else {
			// A terminally failed task fails the whole group regardless of
			// the order in which the agent reports sibling tasks.
			if phase != lifecycle.PhaseRunning && info.Phase != lifecycle.PhaseFailed {
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
		if info.ObservedTasks == nil {
			info.ObservedTasks = make(map[string]bool)
		}
		info.ObservedTasks[a.Task] = true
		// Keep the reason of a failed task whatever the report order.
		if a.Reason != "" && (info.Reason == "" || a.Reason < info.Reason) {
			info.Reason = a.Reason
		}
		// Every task of a failed start carries the same failure; choose one
		// deterministically whatever the report order.
		if failure := a.StartFailure; failure != nil && (info.StartFailure == nil || failure.Attempt > info.StartFailure.Attempt ||
			failure.Attempt == info.StartFailure.Attempt && failure.Message < info.StartFailure.Message) {
			info.StartFailure = failure
		}
		info.Ports = append(info.Ports, a.Ports...)
		if a.Task != "" || a.Address != "" || len(a.Ports) > 0 {
			info.Endpoints = append(info.Endpoints, api.AllocationEndpoint{
				Task: a.Task, Address: a.Address, Ports: append([]api.PortMapping(nil), a.Ports...),
			})
		}
		statuses[key] = info
	}
	observed := make([]observedAllocation, 0, len(statuses))
	for _, info := range statuses {
		observed = append(observed, observedAllocation{ID: info.ID, Generation: info.Generation, Phase: info.Phase})
	}
	sort.Slice(observed, func(i, j int) bool {
		if observed[i].ID == observed[j].ID {
			return observed[i].Generation < observed[j].Generation
		}
		return observed[i].ID < observed[j].ID
	})
	return &nodeObservation{
		node:         nodeID,
		at:           at,
		version:      version,
		volumes:      append([]string(nil), volumes...),
		capabilities: append([]spec.NodeCapability(nil), capabilities...),
		resources:    resources,
		allocations:  statuses,
		observed:     observed,
	}, nil
}
