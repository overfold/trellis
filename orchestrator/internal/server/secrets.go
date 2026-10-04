package server

import (
	"context"
	"fmt"

	"github.com/overfold/trellis/orchestrator/api"
	secretstore "github.com/overfold/trellis/orchestrator/internal/secrets"
)

func secretMetadata(meta *secretstore.Metadata) *api.SecretMetadata {
	if meta == nil {
		return nil
	}
	return &api.SecretMetadata{Namespace: meta.Namespace, Name: meta.Name, Version: meta.Version, CreatedAt: meta.CreatedAt, UpdatedAt: meta.UpdatedAt, CiphertextSize: meta.CiphertextSize, KeyID: meta.KeyID}
}

// SetSecret creates or updates an encrypted namespace secret.
func (s *Server) SetSecret(ctx context.Context, namespace, name string, value []byte, expected *uint64) (*api.SecretMetadata, error) {
	if s.secrets == nil {
		return nil, fmt.Errorf("secrets are not configured")
	}
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	meta, err := s.secrets.Set(ctx, namespace, name, value, expected)
	if err != nil {
		return nil, err
	}
	s.log.Info("secret set", "namespace", namespace, "secret", name, "version", meta.Version, "bytes", len(value))
	return secretMetadata(meta), nil
}

// ListSecrets returns secret metadata for a namespace.
func (s *Server) ListSecrets(ctx context.Context, namespace string) (api.SecretListResponse, error) {
	if s.secrets == nil {
		return nil, fmt.Errorf("secrets are not configured")
	}
	items, err := s.secrets.List(ctx, namespace)
	if err != nil {
		return nil, err
	}
	result := make(api.SecretListResponse, 0, len(items))
	for i := range items {
		result = append(result, *secretMetadata(&items[i]))
	}
	return result, nil
}

// GetSecretMetadata returns metadata for a namespace secret.
func (s *Server) GetSecretMetadata(ctx context.Context, namespace, name string) (*api.SecretMetadata, error) {
	if s.secrets == nil {
		return nil, fmt.Errorf("secrets are not configured")
	}
	meta, err := s.secrets.GetMetadata(ctx, namespace, name)
	return secretMetadata(meta), err
}

// DeleteSecret removes a namespace secret.
func (s *Server) DeleteSecret(ctx context.Context, namespace, name string) error {
	if s.secrets == nil {
		return fmt.Errorf("secrets are not configured")
	}
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	if err := s.secrets.Delete(ctx, namespace, name); err != nil {
		return err
	}
	s.log.Info("secret deleted", "namespace", namespace, "secret", name)
	return nil
}

// SetSecretStore configures encrypted secret storage.
func (s *Server) SetSecretStore(store *secretstore.Store) { s.secrets = store }
