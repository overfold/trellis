package server

import (
	"sort"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/internal/catalog"
	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
)

// ListServices returns discoverable service instances.
func (s *Server) ListServices(namespace string, filter *catalog.ListFilter) nodeapi.ServiceListResponse {
	return s.catalog.List(namespace, filter)
}

// ListServicesForNode returns services only for namespaces with active
// allocations assigned to the authenticated node.
func (s *Server) ListServicesForNode(nodeID uuid.UUID, filter *catalog.ListFilter) nodeapi.ServiceListResponse {
	s.mu.RLock()
	namespaces := make(map[string]struct{})
	for _, allocation := range s.allocations {
		allocation.mu.Lock()
		if allocation.Node != nil && allocation.Node.ID == nodeID &&
			allocation.Phase != lifecycle.PhaseStopped && allocation.Phase != lifecycle.PhaseFailed && allocation.Phase != lifecycle.PhaseLost {
			namespaces[allocation.Namespace] = struct{}{}
		}
		allocation.mu.Unlock()
	}
	s.mu.RUnlock()

	names := make([]string, 0, len(namespaces))
	for namespace := range namespaces {
		names = append(names, namespace)
	}
	sort.Strings(names)
	var result nodeapi.ServiceListResponse
	for _, namespace := range names {
		result = append(result, s.catalog.List(namespace, filter)...)
	}
	return result
}

// Catalog returns the service catalog.
func (s *Server) Catalog() *catalog.ServiceCatalog {
	return s.catalog
}

func (s *Server) refreshCatalog() {
	s.mu.RLock()
	defer s.mu.RUnlock()

	namespaced := make(map[string][]catalog.ServiceInstance)
	for _, a := range s.allocations {
		a.mu.Lock()
		if a.Phase != lifecycle.PhaseRunning || a.Health != lifecycle.HealthHealthy {
			a.mu.Unlock()
			continue
		}
		var labels map[string]string
		job := s.jobs[jobKey(a.Namespace, a.JobName)]
		if job == nil || a.JobIncarnation != job.Incarnation {
			a.mu.Unlock()
			continue
		}
		for _, g := range job.Spec.TaskGroups {
			if g.Name == a.TaskGroupName {
				labels = g.Labels
				break
			}
		}
		address := allocationEndpointAddress(a)
		if address == "" {
			a.mu.Unlock()
			continue
		}
		namespaced[a.Namespace] = append(namespaced[a.Namespace], catalog.ServiceInstance{
			ID:      a.ID,
			Job:     a.JobName,
			Group:   a.TaskGroupName,
			Address: address,
			Ports:   a.Ports,
			Labels:  labels,
		})
		a.mu.Unlock()
	}

	s.catalog.Replace(namespaced)
}

func (s *Server) refreshCatalogAllocations(allocations []*Allocation) {
	ids := make(map[string]bool, len(allocations))
	replacements := make(map[string][]catalog.ServiceInstance)
	s.mu.RLock()
	for _, allocation := range allocations {
		allocation.mu.Lock()
		ids[allocation.ID] = true
		if allocation.Phase == lifecycle.PhaseRunning && allocation.Health == lifecycle.HealthHealthy {
			var labels map[string]string
			job := s.jobs[jobKey(allocation.Namespace, allocation.JobName)]
			if job == nil || allocation.JobIncarnation != job.Incarnation {
				allocation.mu.Unlock()
				continue
			}
			for _, group := range job.Spec.TaskGroups {
				if group.Name == allocation.TaskGroupName {
					labels = group.Labels
					break
				}
			}
			if address := allocationEndpointAddress(allocation); address != "" {
				replacements[allocation.Namespace] = append(replacements[allocation.Namespace], catalog.ServiceInstance{
					ID: allocation.ID, Job: allocation.JobName, Group: allocation.TaskGroupName,
					Address: address, Ports: allocation.Ports, Labels: labels,
				})
			}
		}
		allocation.mu.Unlock()
	}
	s.mu.RUnlock()
	s.catalog.ReplaceInstances(ids, replacements)
}
