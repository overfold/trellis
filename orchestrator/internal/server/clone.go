package server

import (
	"maps"
	"reflect"
	"slices"
	"time"

	"github.com/overfold/trellis/internal/api"
	"github.com/overfold/trellis/internal/spec"
)

func clonePointer[T any](value *T) *T {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

// Clone returns a deep copy of the node, including its leader-local
// observations. The copy shares no maps, slices, or pointers with n.
func (n *Node) Clone() *Node {
	if n == nil {
		return nil
	}
	clone := *n
	clone.CPUUsage = clonePointer(n.CPUUsage)
	clone.MemoryUsed = clonePointer(n.MemoryUsed)
	clone.MemoryAvailable = clonePointer(n.MemoryAvailable)
	clone.MetricsAt = clonePointer(n.MetricsAt)
	clone.Labels = maps.Clone(n.Labels)
	clone.Volumes = slices.Clone(n.Volumes)
	clone.Capabilities = slices.Clone(n.Capabilities)
	clone.observedAllocations = slices.Clone(n.observedAllocations)
	return &clone
}

func cloneEndpoints(endpoints []api.AllocationEndpoint) []api.AllocationEndpoint {
	if endpoints == nil {
		return nil
	}
	clone := make([]api.AllocationEndpoint, len(endpoints))
	for i, endpoint := range endpoints {
		clone[i] = endpoint
		clone[i].Ports = slices.Clone(endpoint.Ports)
	}
	return clone
}

// Clone returns a deep copy of the allocation whose Node is a detached copy
// of the placement node. The caller must hold whatever lock guards a and its
// node: allocation.mu for a canonical allocation and s.mu for its node.
func (a *Allocation) Clone() *Allocation {
	return a.cloneOnto(a.Node.Clone())
}

// cloneOnto deep-copies the allocation record and places the copy on node.
// Copies taken together, such as one reconciliation snapshot, share one
// detached copy of each node instead of copying it per allocation.
func (a *Allocation) cloneOnto(node *Node) *Allocation {
	diagnostic := a.Diagnostic
	diagnostic.NextRetryAt = clonePointer(a.NextRetryAt)
	return &Allocation{
		Namespace:      a.Namespace,
		JobName:        a.JobName,
		TaskGroupName:  a.TaskGroupName,
		ID:             a.ID,
		Generation:     a.Generation,
		JobIncarnation: a.JobIncarnation,
		JobRevision:    a.JobRevision,
		Tasks:          spec.CloneTasks(a.Tasks),
		Phase:          a.Phase,
		Health:         a.Health,
		Diagnostic:     diagnostic,
		Node:           node,
		Endpoints:      cloneEndpoints(a.Endpoints),
		Ports:          slices.Clone(a.Ports),
		Draining:       a.Draining,
		DrainSequence:  a.DrainSequence,
		DrainReason:    a.DrainReason,
		Events:         a.Events.Clone(),
	}
}

func equalTimePointers(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Equal(*b)
}

// sameRecord reports whether two allocations have the same durable record,
// the fields MarshalJSON persists. Events are leader-local and not compared;
// the placement node is compared by ID.
func (a *Allocation) sameRecord(b *Allocation) bool {
	aNode, bNode := a.Node != nil, b.Node != nil
	if aNode != bNode || aNode && a.Node.ID != b.Node.ID {
		return false
	}
	return a.Namespace == b.Namespace &&
		a.JobName == b.JobName &&
		a.TaskGroupName == b.TaskGroupName &&
		a.ID == b.ID &&
		a.Generation == b.Generation &&
		a.JobIncarnation == b.JobIncarnation &&
		a.JobRevision == b.JobRevision &&
		a.Phase == b.Phase &&
		a.Health == b.Health &&
		a.CreatedAt.Equal(b.CreatedAt) &&
		a.TransitionedAt.Equal(b.TransitionedAt) &&
		a.Reason == b.Reason &&
		a.Message == b.Message &&
		a.Attempt == b.Attempt &&
		equalTimePointers(a.NextRetryAt, b.NextRetryAt) &&
		a.Draining == b.Draining &&
		a.DrainSequence == b.DrainSequence &&
		a.DrainReason == b.DrainReason &&
		slices.Equal(a.Ports, b.Ports) &&
		slices.EqualFunc(a.Endpoints, b.Endpoints, func(x, y api.AllocationEndpoint) bool {
			return x.Task == y.Task && x.Address == y.Address && slices.Equal(x.Ports, y.Ports)
		}) &&
		// Tasks are copied from the job specification and never edited in
		// place, so a structural comparison is exact.
		reflect.DeepEqual(a.Tasks, b.Tasks)
}

// equal reports whether two node summaries hold the same durable facts.
func (s *NodeSummary) equal(other *NodeSummary) bool {
	return s.ID == other.ID &&
		s.Host == other.Host &&
		s.Port == other.Port &&
		s.CPUCapacity == other.CPUCapacity &&
		s.MemoryCapacity == other.MemoryCapacity &&
		s.CPUAllocatable == other.CPUAllocatable &&
		s.MemoryAllocatable == other.MemoryAllocatable &&
		s.OS == other.OS &&
		s.Arch == other.Arch &&
		maps.Equal(s.Labels, other.Labels) &&
		slices.Equal(s.Volumes, other.Volumes) &&
		slices.Equal(s.Capabilities, other.Capabilities) &&
		s.Draining == other.Draining &&
		s.WireGuardPublicKey == other.WireGuardPublicKey &&
		s.WireGuardEndpoint == other.WireGuardEndpoint &&
		s.WireGuardPortBase == other.WireGuardPortBase &&
		s.WireGuardPortCount == other.WireGuardPortCount &&
		s.Version == other.Version
}
