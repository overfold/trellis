package client

import (
	"context"
	"fmt"
	"net/http"

	"github.com/clofour/trellis/internal/api"
)

// CreateCredential asks the administrator API to mint a scoped credential.
func (s *ServerClient) CreateCredential(ctx context.Context, request *api.CredentialCreateRequest) (*api.CredentialCreateResponse, error) {
	var response api.CredentialCreateResponse
	if err := s.client.request(ctx, http.MethodPost, s.address()+"/v1/credentials", request, &response); err != nil {
		return nil, fmt.Errorf("create credential: %w", err)
	}
	return &response, nil
}

// CredentialInfo returns the identity and effective authorization of this client's bearer credential.
func (s *ServerClient) CredentialInfo(ctx context.Context) (*api.CredentialInfoResponse, error) {
	var response api.CredentialInfoResponse
	if err := s.client.request(ctx, http.MethodGet, s.address()+"/v1/auth/whoami", nil, &response); err != nil {
		return nil, fmt.Errorf("inspect credential: %w", err)
	}
	return &response, nil
}
