package server

import (
	"sort"

	"github.com/overfold/trellis/internal/api"
)

// ListNamespaces returns namespace names currently referenced by desired jobs.
// Namespaces are not lifecycle-managed resources; an operator may apply a job to
// a new namespace at any time, after which it becomes discoverable here.
func (s *Server) ListNamespaces() api.NamespaceListResponse {
	s.mu.RLock()
	seen := make(map[string]struct{}, len(s.jobs))
	for _, job := range s.jobs {
		if job != nil && job.Spec != nil && job.Spec.Namespace != "" {
			seen[job.Spec.Namespace] = struct{}{}
		}
	}
	s.mu.RUnlock()

	result := make(api.NamespaceListResponse, 0, len(seen))
	for namespace := range seen {
		result = append(result, namespace)
	}
	sort.Strings(result)
	return result
}
