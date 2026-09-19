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

		response := s.allocationResponseLocked(allocation)
		response.Labels = labels
		result = append(result, response)
		allocation.mu.Unlock()
	}
	return result
}

func allocationTaskEndpoints(allocation *Allocation) []api.AllocationEndpoint {
	observed := make(map[string]api.AllocationEndpoint, len(allocation.Endpoints))
	for _, endpoint := range allocation.Endpoints {
		if endpoint.Task != "" {
			observed[endpoint.Task] = endpoint
		}
	}

	// Older persisted allocations may not have task specs. Preserve the
	// historical node-address behavior for those records only.
	if len(allocation.Tasks) == 0 {
		if len(allocation.Endpoints) > 0 {
			return append([]api.AllocationEndpoint(nil), allocation.Endpoints...)
		}
		if allocation.Node != nil {
			return []api.AllocationEndpoint{{Address: allocation.Node.Host, Ports: append([]api.PortMapping(nil), allocation.Ports...)}}
		}
		return nil
	}

	endpoints := make([]api.AllocationEndpoint, 0, len(allocation.Tasks))
	for _, task := range allocation.Tasks {
		mode := spec.TaskNetworkDefault
		if task.Networking != nil {
			mode = task.Networking.Mode
		}
		endpoint := observed[task.Name]
		endpoint.Task = task.Name
		switch mode {
		case spec.TaskNetworkHost:
			if allocation.Node == nil {
				continue
			}
			endpoint.Address = allocation.Node.Host
			if len(endpoint.Ports) == 0 && task.Networking != nil {
				for _, port := range task.Networking.Ports {
					endpoint.Ports = append(endpoint.Ports, api.PortMapping{HostPort: port.Port, ContainerPort: port.Port})
				}
			}
			endpoints = append(endpoints, endpoint)
		case spec.TaskNetworkWireGuard:
			// Namespace addresses are valid only when observed from the agent.
			// Never substitute the node host for a missing workload address.
			endpoints = append(endpoints, endpoint)
		}
	}
	return endpoints
}

func allocationEndpointAddress(allocation *Allocation) string {
	var address string
	for _, endpoint := range allocationTaskEndpoints(allocation) {
		if endpoint.Address == "" {
			return ""
		}
		if address == "" {
			address = endpoint.Address
			continue
		}
		if address != endpoint.Address {
			return ""
		}
	}
	return address
}

func (s *Server) allocationResponseLocked(allocation *Allocation) api.AllocationResponse {
	response := api.AllocationResponse{
		ID:               allocation.ID,
		Job:              allocation.JobName,
		Group:            allocation.TaskGroupName,
		Namespace:        allocation.Namespace,
		Address:          allocationEndpointAddress(allocation),
		Endpoints:        allocationTaskEndpoints(allocation),
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
		Labels:           s.allocationLabelsLocked(allocation),
		Ports:            allocation.Ports,
	}
	if allocation.Node != nil {
		response.NodeID = allocation.Node.ID
	}
	return response
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
