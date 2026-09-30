package server

import (
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
		Kind:      string(principal.Kind),
		Scope:     string(principal.Scope),
		Access:    string(principal.Access),
		Namespace: principal.Namespace,
	}
	if !principal.CreatedAt.IsZero() {
		createdAt := principal.CreatedAt
		response.CreatedAt = &createdAt
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
