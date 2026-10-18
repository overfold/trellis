package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/overfold/trellis/orchestrator/internal/auth"
)

// ErrInvalidCredentialRequest reports a minted credential request that
// cannot be honored as asked.
var ErrInvalidCredentialRequest = errors.New("invalid credential request")

// CreateCredential mints a minted credential. A positive ttl makes the
// credential expire that long after creation; zero means no expiry. The HTTP
// layer restricts this operation to the administrator credential.
func (s *Server) CreateCredential(ctx context.Context, role auth.Role, ttl time.Duration) (string, auth.MintedCredential, error) {
	if s.tokenManager == nil {
		return "", auth.MintedCredential{}, fmt.Errorf("credential management is unavailable")
	}
	if ttl < 0 {
		return "", auth.MintedCredential{}, fmt.Errorf("%w: ttl must not be negative", ErrInvalidCredentialRequest)
	}
	principal := auth.Principal{
		Role:      role,
		CreatedAt: s.now().UTC(),
	}
	if ttl > 0 {
		principal.ExpiresAt = principal.CreatedAt.Add(ttl)
	}
	return s.tokenManager.CreateMintedToken(ctx, principal)
}

// ListCredentials returns minted credential metadata. Tokens are stored only
// as hashes, so no listing can reveal one.
func (s *Server) ListCredentials(ctx context.Context) ([]auth.MintedCredential, error) {
	if s.tokenManager == nil {
		return nil, fmt.Errorf("credential management is unavailable")
	}
	return s.tokenManager.ListMintedCredentials(ctx)
}

// RevokeCredential deletes one minted credential by its listed ID.
func (s *Server) RevokeCredential(ctx context.Context, id string) error {
	if s.tokenManager == nil {
		return fmt.Errorf("credential management is unavailable")
	}
	return s.tokenManager.RevokeMintedCredential(ctx, id)
}
