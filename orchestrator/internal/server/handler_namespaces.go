package server

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleListNamespaces(c *echo.Context) error {
	if err := requireObserver(c, "namespace discovery requires an authenticated credential"); err != nil {
		return err
	}
	namespaces, err := h.server.ListNamespaces(c.Request().Context())
	if err != nil {
		h.server.log.Error("list namespaces", "error", err)
		return echo.NewHTTPError(http.StatusServiceUnavailable, "unable to list namespaces")
	}
	return c.JSON(http.StatusOK, namespaces)
}
