package client

import (
	"context"
	"fmt"
	"net/http"

	"github.com/overfold/trellis/internal/api"
)

// ListNamespaces returns namespace names visible to the caller.
func (s *ServerClient) ListNamespaces(ctx context.Context) (*api.NamespaceListResponse, error) {
	var response api.NamespaceListResponse
	if err := s.client.request(ctx, http.MethodGet, s.address()+"/v1/namespaces", nil, &response); err != nil {
		return nil, fmt.Errorf("list namespaces: %w", err)
	}
	return &response, nil
}
