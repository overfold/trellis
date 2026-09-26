package server

import (
	"context"
	"fmt"

	"github.com/clofour/trellis/internal/auth"
)

// CreateCredential mints a scoped operator credential. The HTTP layer restricts
// this operation to the administrator credential.
func (s *Server) CreateCredential(ctx context.Context, scope auth.AccessScope, access auth.AccessLevel, namespace string) (string, error) {
	if s.tokenManager == nil {
		return "", fmt.Errorf("credential management is unavailable")
	}
	return s.tokenManager.CreateToken(ctx, auth.Principal{
		Kind:      auth.CredentialOperator,
		Scope:     scope,
		Access:    access,
		Namespace: namespace,
	})
}
