package server

import (
	"context"
	"fmt"
	"sort"

	"github.com/overfold/trellis/orchestrator/api"
)

// ListNamespaces returns the sorted union of namespaces referenced by desired
// jobs, stored secrets, and namespace-scoped credentials. Namespaces are not
// lifecycle-managed resources: a namespace exists while something is stored in
// it, so an operator may mint a credential for, store a secret in, or apply a
// job to a new namespace at any time, after which it is discoverable here.
func (s *Server) ListNamespaces(ctx context.Context) (api.NamespaceListResponse, error) {
	s.mu.RLock()
	seen := make(map[string]struct{}, len(s.jobs))
	for _, job := range s.jobs {
		if job != nil && job.Spec != nil && job.Spec.Namespace != "" {
			seen[job.Spec.Namespace] = struct{}{}
		}
	}
	s.mu.RUnlock()

	if s.secrets != nil {
		namespaces, err := s.secrets.Namespaces(ctx)
		if err != nil {
			return nil, fmt.Errorf("list secret namespaces: %w", err)
		}
		for _, namespace := range namespaces {
			seen[namespace] = struct{}{}
		}
	}
	if s.tokenManager != nil {
		namespaces, err := s.tokenManager.CredentialNamespaces(ctx)
		if err != nil {
			return nil, fmt.Errorf("list credential namespaces: %w", err)
		}
		for _, namespace := range namespaces {
			seen[namespace] = struct{}{}
		}
	}

	result := make(api.NamespaceListResponse, 0, len(seen))
	for namespace := range seen {
		result = append(result, namespace)
	}
	sort.Strings(result)
	return result, nil
}
