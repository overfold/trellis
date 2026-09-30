package client

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	"github.com/overfold/trellis/internal/api"
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

// ListCredentials lists operator credential metadata with the administrator
// credential. Listings never include bearer tokens.
func (s *ServerClient) ListCredentials(ctx context.Context) (api.CredentialListResponse, error) {
	var response api.CredentialListResponse
	if err := s.client.request(ctx, http.MethodGet, s.address()+"/v1/credentials", nil, &response); err != nil {
		return nil, fmt.Errorf("list credentials: %w", err)
	}
	return response, nil
}

// RevokeCredential deletes an operator credential by its listed ID with the
// administrator credential.
func (s *ServerClient) RevokeCredential(ctx context.Context, id string) error {
	if err := s.client.request(ctx, http.MethodDelete, s.address()+"/v1/credentials/"+url.PathEscape(id), nil, nil); err != nil {
		return fmt.Errorf("revoke credential: %w", err)
	}
	return nil
}

// CreateJoinToken mints a node join token with the administrator credential.
func (s *ServerClient) CreateJoinToken(ctx context.Context, request *api.JoinTokenCreateRequest) (*api.JoinTokenCreateResponse, error) {
	var response api.JoinTokenCreateResponse
	if err := s.client.request(ctx, http.MethodPost, s.address()+"/v1/nodes/join-tokens", request, &response); err != nil {
		return nil, fmt.Errorf("create join token: %w", err)
	}
	return &response, nil
}

// ListJoinTokens lists unexpired join token metadata with the administrator
// credential. Listings never include tokens.
func (s *ServerClient) ListJoinTokens(ctx context.Context) (api.JoinTokenListResponse, error) {
	var response api.JoinTokenListResponse
	if err := s.client.request(ctx, http.MethodGet, s.address()+"/v1/nodes/join-tokens", nil, &response); err != nil {
		return nil, fmt.Errorf("list join tokens: %w", err)
	}
	return response, nil
}

// RevokeJoinToken deletes a join token by its listed ID with the administrator
// credential.
func (s *ServerClient) RevokeJoinToken(ctx context.Context, id string) error {
	if err := s.client.request(ctx, http.MethodDelete, s.address()+"/v1/nodes/join-tokens/"+url.PathEscape(id), nil, nil); err != nil {
		return fmt.Errorf("revoke join token: %w", err)
	}
	return nil
}
