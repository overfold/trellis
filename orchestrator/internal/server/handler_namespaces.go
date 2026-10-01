package server

import (
	"net/http"

	"github.com/labstack/echo/v5"
	"github.com/overfold/trellis/orchestrator/internal/auth"
)

func (h *Handler) handleListNamespaces(c *echo.Context) error {
	authz := authorization(c)
	if !authz.root && authz.scope != auth.AccessCluster {
		return echo.NewHTTPError(http.StatusForbidden, "namespace discovery requires an authenticated scoped credential")
	}
	namespaces, err := h.server.ListNamespaces(c.Request().Context())
	if err != nil {
		h.server.log.Error("list namespaces", "error", err)
		return echo.NewHTTPError(http.StatusServiceUnavailable, "unable to list namespaces")
	}
	return c.JSON(http.StatusOK, namespaces)
}
