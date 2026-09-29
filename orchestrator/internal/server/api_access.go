package server

import (
	"context"
	"fmt"

	"github.com/overfold/trellis/internal/auth"
	"github.com/overfold/trellis/internal/spec"
)

func (s *Server) apiAccessToken(ctx context.Context, access *spec.APIAccessSpec, namespace string) (string, error) {
	if access == nil {
		return "", nil
	}
	if s.tokenManager == nil {
		return "", fmt.Errorf("scoped API access is unavailable")
	}

	scope := auth.AccessScope(access.Scope)
	level := auth.AccessLevel(access.Access)
	token, err := s.tokenManager.GetOrCreateWorkloadToken(ctx, scope, level, namespace)
	if err != nil {
		return "", fmt.Errorf("create %s/%s workload API token: %w", access.Scope, access.Access, err)
	}
	return token, nil
}
