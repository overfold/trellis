package server

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/netip"
	"regexp"
	"strings"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	"github.com/overfold/trellis/orchestrator/internal/spec"
)

// ErrNodeNotFound indicates that a requested node is absent.
var ErrNodeNotFound = errors.New("node not found")

var nodeHostLabel = regexp.MustCompile(`^[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?$`)

func validateNodeEndpoint(host string, port int) error {
	if port < 1 || port > 65535 {
		return fmt.Errorf("node port must be in 1-65535")
	}
	if addr, err := netip.ParseAddr(host); err == nil {
		if strings.Trim(addr.Zone(), "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_.-") != "" {
			return fmt.Errorf("node IPv6 zone must be an interface name or index")
		}
		return nil
	}
	name := strings.TrimSuffix(host, ".")
	if name == "" || len(name) > 253 || strings.Contains(name, ".") && strings.Trim(name, "0123456789.") == "" {
		return fmt.Errorf("node host must be an IP address or DNS hostname")
	}
	for label := range strings.SplitSeq(name, ".") {
		if !nodeHostLabel.MatchString(label) {
			return fmt.Errorf("node host must be an IP address or DNS hostname")
		}
	}
	return nil
}

func validateNodeCapacity(cpuCapacity int, memoryCapacity int64, cpuAllocatable int, memoryAllocatable int64) error {
	if cpuCapacity < 0 || memoryCapacity < 0 || cpuAllocatable < 0 || memoryAllocatable < 0 {
		return fmt.Errorf("node resources must be non-negative")
	}
	if cpuAllocatable > cpuCapacity {
		return fmt.Errorf("allocatable CPU %dm exceeds node capacity %dm", cpuAllocatable, cpuCapacity)
	}
	if memoryAllocatable > memoryCapacity {
		return fmt.Errorf("allocatable memory %d bytes exceeds node capacity %d bytes", memoryAllocatable, memoryCapacity)
	}
	return nil
}

// ListNodes returns registered nodes.
func (s *Server) ListNodes() []NodeView {
	heartbeats := s.liveness.heartbeats()
	now := s.now()
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]NodeView, 0, len(s.nodes))
	for _, node := range s.nodes {
		view := NodeView{Node: *node.Clone(), LastHeartbeat: heartbeats[node.ID]}
		view.Status = livenessStatus(node.Status, view.LastHeartbeat, now)
		result = append(result, view)
	}
	return result
}

// ErrInvalidNodeRegistration indicates that a node registration request was
// rejected as malformed or inconsistent.
var ErrInvalidNodeRegistration = errors.New("invalid node registration")

// RegisterNode adds or updates a cluster node.
func (s *Server) RegisterNode(ctx context.Context, nodeRegistration *NodeRegistration) error {
	if err := validateNodeEndpoint(nodeRegistration.Host, nodeRegistration.Port); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidNodeRegistration, err)
	}
	if err := validateNodeCapacity(nodeRegistration.CPUCapacity, nodeRegistration.MemoryCapacity, nodeRegistration.CPUAllocatable, nodeRegistration.MemoryAllocatable); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidNodeRegistration, err)
	}
	if nodeRegistration.WireGuardPublicKey != "" || nodeRegistration.WireGuardEndpoint != "" || nodeRegistration.WireGuardPortBase != 0 || nodeRegistration.WireGuardPortCount != 0 {
		if nodeRegistration.WireGuardPublicKey == "" || nodeRegistration.WireGuardEndpoint == "" {
			return fmt.Errorf("%w: WireGuard registration requires a public key and endpoint", ErrInvalidNodeRegistration)
		}
		if nodeRegistration.WireGuardPortCount != s.wireGuardPortCount {
			return fmt.Errorf("%w: WireGuard port count %d does not match cluster count %d", ErrInvalidNodeRegistration, nodeRegistration.WireGuardPortCount, s.wireGuardPortCount)
		}
		if nodeRegistration.WireGuardPortBase < 1 || nodeRegistration.WireGuardPortBase+nodeRegistration.WireGuardPortCount-1 > 65535 {
			return fmt.Errorf("%w: WireGuard port range is outside 1-65535", ErrInvalidNodeRegistration)
		}
	}
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	s.mu.RLock()
	existing := s.nodes[nodeRegistration.ID]
	status := NodeStatusHealthy
	next := &Node{ID: nodeRegistration.ID}
	if existing != nil && existing.Status == NodeStatusDraining {
		status = existing.Status
	}
	if existing != nil {
		next = existing.Clone()
	}
	s.mu.RUnlock()
	next.Host, next.Port, next.Status = nodeRegistration.Host, nodeRegistration.Port, status
	next.CPUCapacity, next.MemoryCapacity = nodeRegistration.CPUCapacity, nodeRegistration.MemoryCapacity
	next.CPUAllocatable, next.MemoryAllocatable = nodeRegistration.CPUAllocatable, nodeRegistration.MemoryAllocatable
	next.OS, next.Arch, next.Labels = nodeRegistration.OS, nodeRegistration.Arch, maps.Clone(nodeRegistration.Labels)
	next.Volumes, next.Capabilities = append([]string(nil), nodeRegistration.Volumes...), append([]spec.NodeCapability(nil), nodeRegistration.Capabilities...)
	next.WireGuardPublicKey, next.WireGuardEndpoint = nodeRegistration.WireGuardPublicKey, nodeRegistration.WireGuardEndpoint
	next.WireGuardPortBase, next.WireGuardPortCount = nodeRegistration.WireGuardPortBase, nodeRegistration.WireGuardPortCount
	next.RunsWorkloads = new(nodeRegistration.RunsWorkloads)
	registeredAt := s.now().UTC()
	if err := s.state.PutNode(ctx, nodeRegistration.ID.String(), nodeSummary(next)); err != nil {
		return fmt.Errorf("save node remotely: %w", stateUnavailable(err))
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	node := s.nodes[nodeRegistration.ID]
	if node == nil {
		node = &Node{}
		s.nodes[nodeRegistration.ID] = node
	}
	applyNodeSnapshot(node, next)
	// Registration is evidence of life, and it admits the node's heartbeats.
	s.liveness.register(nodeRegistration.ID, registeredAt)
	return nil
}

// Heartbeat records a node heartbeat. It stamps the node's liveness as soon
// as the report is validated and hands the report to the observation
// applier, which persists allocation and node observations asynchronously.
// Heartbeats never wait for reconciliation or a Raft commit, so neither can
// make a heartbeating node look silent. A heartbeat from a node that is not
// registered fails with ErrNodeNotFound so the agent registers again.
func (s *Server) Heartbeat(ctx context.Context, nodeID uuid.UUID, actual []nodeapi.AllocationStatus, version string, volumes []string, capabilities []spec.NodeCapability, resources nodeResourceObservation) error {
	ctx, release := s.bindTerm(ctx)
	defer release()
	receivedAt := s.now().UTC()
	observation, err := newNodeObservation(nodeID, receivedAt, actual, version, volumes, capabilities, resources)
	if err != nil {
		return err
	}
	s.termMu.RLock()
	defer s.termMu.RUnlock()
	if err := s.checkTermLocked(ctx); err != nil {
		return err
	}
	registration, registered := s.liveness.stamp(nodeID, receivedAt)
	if !registered {
		return fmt.Errorf("%w: %s", ErrNodeNotFound, nodeID)
	}
	observation.registration = registration
	if s.observations.submit(observation) && s.metrics != nil {
		s.metrics.ObservationsSuperseded.Inc()
	}
	return nil
}

// DrainNode marks a node for allocation evacuation.
func (s *Server) DrainNode(ctx context.Context, id uuid.UUID) error {
	s.mutationMu.Lock()
	s.mu.RLock()
	node := s.nodes[id]
	if node == nil {
		s.mu.RUnlock()
		s.mutationMu.Unlock()
		return ErrNodeNotFound
	}
	next := node.Clone()
	next.Status = NodeStatusDraining
	s.mu.RUnlock()
	if err := s.state.PutNode(ctx, id.String(), nodeSummary(next)); err != nil {
		s.mutationMu.Unlock()
		return stateUnavailable(err)
	}
	s.mu.Lock()
	applyNodeSnapshot(node, next)
	s.mu.Unlock()
	s.mutationMu.Unlock()
	s.Reconcile(ctx)
	return nil
}

// UndrainNode makes a drained node schedulable.
func (s *Server) UndrainNode(ctx context.Context, id uuid.UUID) error {
	s.reconcileMu.Lock()
	resumes, err := s.resumeNodeAllocations(ctx, id)
	s.reconcileMu.Unlock()
	if err != nil {
		return err
	}
	// Deliver outside reconcileMu: an unreachable agent costs a full request
	// timeout per allocation and must not stall cluster-wide planning.
	s.deliverResumes(ctx, id, resumes)
	s.Reconcile(ctx)
	return nil
}

// resumeNodeAllocations durably undrains the node and its node-drained
// allocations and returns the resumes to deliver to the agent. The caller
// holds reconcileMu.
func (s *Server) resumeNodeAllocations(ctx context.Context, id uuid.UUID) ([]resumeDelivery, error) {
	s.mutationMu.Lock()
	mutationLocked := true
	defer func() {
		if mutationLocked {
			s.mutationMu.Unlock()
		}
	}()
	s.mu.RLock()
	node := s.nodes[id]
	if node == nil {
		s.mu.RUnlock()
		return nil, fmt.Errorf("%w: %s", ErrNodeNotFound, id)
	}
	nextNode := node.Clone()
	nextNode.Status = livenessStatus(NodeStatusHealthy, s.liveness.lastHeartbeat(id), s.now())
	var resumes []resumeDelivery
	var updates []*Allocation
	for _, allocation := range s.allocations {
		allocation.mu.Lock()
		if allocation.Node == nil || allocation.Node.ID != id || !allocation.Draining || allocation.DrainReason != "node" ||
			(allocation.Phase != lifecycle.PhaseRunning && allocation.Phase != lifecycle.PhaseStarting && allocation.Phase != lifecycle.PhasePlaced) {
			allocation.mu.Unlock()
			continue
		}
		job := s.jobs[jobKey(allocation.Namespace, allocation.JobName)]
		if job == nil || allocation.JobIncarnation != job.Incarnation || allocation.JobRevision != job.Revision {
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
			allocation.mu.Unlock()
			continue
		}
		address := fmt.Sprintf("%s:%d", node.Host, node.Port)
		request := &nodeapi.DrainAllocationRequest{AllocationID: allocation.ID, Generation: allocation.Generation, Epoch: s.controlEpoch, Sequence: allocation.DrainSequence + 1}
		next := allocation.cloneOnto(nextNode)
		allocation.mu.Unlock()
		next.Draining = false
		next.DrainReason = ""
		next.DrainSequence = request.Sequence
		updates = append(updates, next)
		resumes = append(resumes, resumeDelivery{allocation: allocation, address: address, request: request})
	}
	s.mu.RUnlock()
	if err := s.state.PutNodeAndAllocations(ctx, nodeSummary(nextNode), updates); err != nil {
		return nil, stateUnavailable(err)
	}
	s.mu.Lock()
	applyNodeSnapshot(node, nextNode)
	for i, resume := range resumes {
		resume.allocation.mu.Lock()
		applyAllocationSnapshot(resume.allocation, updates[i])
		resume.allocation.mu.Unlock()
	}
	s.mu.Unlock()
	s.mutationMu.Unlock()
	mutationLocked = false
	return resumes, nil
}

// deliverResumes sends durable resumes to the agent without holding any state
// lock. A resume the agent does not acknowledge now is redelivered by
// reconciliation, and the agent rejects a resume that a later drain has
// superseded by its sequence, so a drain racing with delivery stays correct.
func (s *Server) deliverResumes(ctx context.Context, id uuid.UUID, resumes []resumeDelivery) {
	for _, resume := range resumes {
		allocation := resume.allocation
		allocation.mu.Lock()
		superseded := allocation.Draining || allocation.DrainSequence != resume.request.Sequence
		allocation.mu.Unlock()
		if superseded {
			continue
		}
		if err := s.client.ResumeAllocation(ctx, id, resume.address, resume.request); err != nil {
			s.log.Warn("deliver allocation resume; reconciliation will retry", "allocation", allocation.ID, "node", id, "error", err)
		} else {
			s.recordResumeDelivered(resume.request)
		}
	}
}

type resumeDelivery struct {
	allocation *Allocation
	address    string
	request    *nodeapi.DrainAllocationRequest
}

type resumeDeliveryKey struct {
	allocation string
	generation uint64
}

// deliveredResumes copies the resume sequence each agent acknowledged per
// allocation generation during the given leadership term. The record is
// renewable delivery state, not desired state: a new term starts empty and
// redelivers each resume once.
func (s *Server) deliveredResumes(epoch uint64) map[resumeDeliveryKey]uint64 {
	s.resumeMu.Lock()
	defer s.resumeMu.Unlock()
	if s.resumeEpoch != epoch {
		return nil
	}
	delivered := make(map[resumeDeliveryKey]uint64, len(s.resumes))
	maps.Copy(delivered, s.resumes)
	return delivered
}

func (s *Server) recordResumeDelivered(request *nodeapi.DrainAllocationRequest) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if request.Epoch != s.controlEpoch {
		return
	}
	s.resumeMu.Lock()
	defer s.resumeMu.Unlock()
	if s.resumes == nil || s.resumeEpoch != request.Epoch {
		s.resumes = make(map[resumeDeliveryKey]uint64)
		s.resumeEpoch = request.Epoch
	}
	s.resumes[resumeDeliveryKey{allocation: request.AllocationID, generation: request.Generation}] = request.Sequence
}
