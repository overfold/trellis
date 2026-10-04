package server

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v5"
	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/internal/auth"
)

// PrincipalContextKey stores the authenticated credential principal in request context.
const PrincipalContextKey contextKey = "trellis-principal"

// HandleWhoAmI returns the identity and effective authorization of the current credential.
func HandleWhoAmI(c *echo.Context) error {
	principal, ok := c.Request().Context().Value(PrincipalContextKey).(auth.Principal)
	if !ok {
		return echo.NewHTTPError(http.StatusUnauthorized, "authenticated principal is unavailable")
	}
	response := api.CredentialInfoResponse{
		Kind:   string(principal.Kind),
		Scope:  string(principal.Scope),
		Access: string(principal.Access),
	}
	if !principal.CreatedAt.IsZero() {
		createdAt := principal.CreatedAt
		response.CreatedAt = &createdAt
	}
	if !principal.ExpiresAt.IsZero() {
		expiresAt := principal.ExpiresAt
		response.ExpiresAt = &expiresAt
	}
	if principal.Subject != nil {
		response.Subject = &api.CredentialSubjectResponse{
			Namespace: principal.Subject.Namespace,
			Job:       principal.Subject.Job,
			TaskGroup: principal.Subject.TaskGroup,
		}
	}
	c.Response().Header().Set("Cache-Control", "no-store")
	return c.JSON(http.StatusOK, response)
}

// AdministratorVerification returns the replicated administrator public key and current leadership epoch.
func (s *Server) AdministratorVerification() (ed25519.PublicKey, uint64, bool) {
	s.mu.RLock()
	encoded := ""
	epoch := uint64(0)
	if s.cluster != nil {
		encoded = s.cluster.AdministratorPublicKey
		epoch = s.cluster.ControlEpoch
	}
	s.mu.RUnlock()
	publicKey, err := parseAdministratorPublicKey(encoded)
	if err != nil {
		return nil, 0, false
	}
	return publicKey, epoch, true
}

func parseAdministratorPublicKey(encoded string) (ed25519.PublicKey, error) {
	der, err := base64.RawStdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decode base64: %w", err)
	}
	parsed, err := x509.ParsePKIXPublicKey(der)
	if err != nil {
		return nil, fmt.Errorf("parse PKIX key: %w", err)
	}
	publicKey, ok := parsed.(ed25519.PublicKey)
	if !ok {
		return nil, fmt.Errorf("key must be Ed25519")
	}
	return publicKey, nil
}
