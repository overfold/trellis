package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/overfold/trellis/internal/auth"
)

// ErrInvalidCredentialRequest reports an operator credential request that
// cannot be honored as asked.
var ErrInvalidCredentialRequest = errors.New("invalid credential request")

// CreateCredential mints a scoped operator credential. A positive ttl makes the
// credential expire that long after creation; zero means no expiry. The HTTP
// layer restricts this operation to the administrator credential.
func (s *Server) CreateCredential(ctx context.Context, scope auth.AccessScope, access auth.AccessLevel, namespace string, ttl time.Duration) (string, auth.OperatorCredential, error) {
	if s.tokenManager == nil {
		return "", auth.OperatorCredential{}, fmt.Errorf("credential management is unavailable")
	}
	if ttl < 0 {
		return "", auth.OperatorCredential{}, fmt.Errorf("%w: ttl must not be negative", ErrInvalidCredentialRequest)
	}
	principal := auth.Principal{
		Kind:      auth.CredentialOperator,
		Scope:     scope,
		Access:    access,
		Namespace: namespace,
		CreatedAt: s.now().UTC(),
	}
	if ttl > 0 {
		principal.ExpiresAt = principal.CreatedAt.Add(ttl)
	}
	return s.tokenManager.CreateOperatorToken(ctx, principal)
}

// ListCredentials returns operator credential metadata. Tokens are stored only
// as hashes, so no listing can reveal one.
func (s *Server) ListCredentials(ctx context.Context) ([]auth.OperatorCredential, error) {
	if s.tokenManager == nil {
		return nil, fmt.Errorf("credential management is unavailable")
	}
	return s.tokenManager.ListOperatorCredentials(ctx)
}

// RevokeCredential deletes one operator credential by its listed ID.
func (s *Server) RevokeCredential(ctx context.Context, id string) error {
	if s.tokenManager == nil {
		return fmt.Errorf("credential management is unavailable")
	}
	return s.tokenManager.RevokeOperatorCredential(ctx, id)
}
