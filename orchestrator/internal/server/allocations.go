// Package server implements Trellis orchestration and HTTP APIs.
package server

import (
	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/spec"
)

// AllocationListFilter restricts allocation query results.
type AllocationListFilter struct {
	Job   string
	Label string // "key:value" or key-existence format
}

// ListAllocations exposes allocations as the public query surface for running
// work. Labels are task-group metadata; address and ports are derived runtime
// details. A blank namespace lists allocations across namespaces for
// cluster-scoped callers.
func (s *Server) ListAllocations(namespace string, filter *AllocationListFilter) api.AllocationListResponse {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(api.AllocationListResponse, 0, len(s.allocations))
	for _, allocation := range s.allocations {
		allocation.mu.Lock()
		if namespace != "" && allocation.Namespace != namespace {
			allocation.mu.Unlock()
			continue
		}
		if filter != nil && filter.Job != "" && allocation.JobName != filter.Job {
			allocation.mu.Unlock()
			continue
		}

		labels := s.allocationLabelsLocked(allocation)
		if filter != nil && filter.Label != "" && !matchAllocationLabel(labels, filter.Label) {
			allocation.mu.Unlock()
			continue
		}

		response := api.AllocationResponse{
			ID:               allocation.ID,
			Job:              allocation.JobName,
			Group:            allocation.TaskGroupName,
			Namespace:        allocation.Namespace,
			Phase:            allocation.Phase,
			Health:           allocation.Health,
			Draining:         allocation.Draining,
			Generation:       allocation.Generation,
			JobRevision:      allocation.JobRevision,
			CreatedAt:        allocation.CreatedAt,
			LastTransitionAt: allocation.TransitionedAt,
			Reason:           allocation.Reason,
			Message:          allocation.Message,
			Attempt:          allocation.Attempt,
			NextRetryAt:      allocation.NextRetryAt,
			Labels:           labels,
			Ports:            allocation.Ports,
		}
		if allocation.Node != nil {
			response.NodeID = allocation.Node.ID
		}
		response.Address = allocationEndpointAddressLocked(allocation)
		result = append(result, response)
		allocation.mu.Unlock()
	}
	return result
}

// AllocationEvents returns the recent lifecycle event history for a single
// allocation. The second return value is false when no matching allocation
// exists. Events are in chronological order and capped at EventRingSize; they
// are not persisted and reset on leader failover.
func (s *Server) AllocationEvents(namespace, id string) (api.AllocationEventListResponse, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, a := range s.allocations {
		a.mu.Lock()
		if a.ID != id {
			a.mu.Unlock()
			continue
		}
		if namespace != "" && a.Namespace != namespace {
			a.mu.Unlock()
			continue
		}
		if a.Events == nil {
			a.mu.Unlock()
			return api.AllocationEventListResponse{}, true
		}
		entries := a.Events.Entries()
		result := make(api.AllocationEventListResponse, len(entries))
		for i, e := range entries {
			result[i] = api.AllocationEventResponse{Phase: e.Phase, Reason: e.Reason, Message: e.Message, At: e.At}
		}
		a.mu.Unlock()
		return result, true
	}
	return nil, false
}

func allocationEndpointAddressLocked(allocation *Allocation) string {
	if allocation == nil {
		return ""
	}

	namespaceTasks := 0
	hostTasks := 0
	for i := range allocation.Tasks {
		if allocation.Tasks[i].Networking == nil {
			continue
		}
		switch allocation.Tasks[i].Networking.Mode {
		case spec.TaskNetworkWireGuard:
			namespaceTasks++
		case spec.TaskNetworkHost:
			hostTasks++
		}
	}

	switch {
	case namespaceTasks == 1 && hostTasks == 0:
		return allocation.Address
	case namespaceTasks > 0:
		// One AllocationResponse has only one address. Multiple namespace
		// endpoints, or a namespace/host mixture, cannot be represented safely.
		return ""
	case hostTasks > 0:
		if allocation.Node != nil {
			return allocation.Node.Host
		}
		return ""
	case len(allocation.Tasks) == 0:
		// Preserve the legacy node-address behavior for old/recovered allocation
		// records that do not carry task metadata.
		if allocation.Address != "" {
			return allocation.Address
		}
		if allocation.Node != nil {
			return allocation.Node.Host
		}
	}
	return ""
}

func (s *Server) allocationLabelsLocked(allocation *Allocation) map[string]string {
	job := s.jobs[jobKey(allocation.Namespace, allocation.JobName)]
	if job == nil {
		return nil
	}
	for _, group := range job.Spec.TaskGroups {
		if group.Name == allocation.TaskGroupName {
			return group.Labels
		}
	}
	return nil
}

func matchAllocationLabel(labels map[string]string, filter string) bool {
	for i := range filter {
		if filter[i] == ':' {
			key, value := filter[:i], filter[i+1:]
			return labels[key] == value
		}
	}
	_, ok := labels[filter]
	return ok
}
