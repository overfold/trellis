package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

func nodeRequest(nodeID uuid.UUID, method, target string) *http.Request {
	req := httptest.NewRequest(method, target, http.NoBody)
	return req.WithContext(context.WithValue(req.Context(), NodeContextKey, nodeID))
}

func TestNodeIdentityCannotImpersonateOrGainAdministratorAuthority(t *testing.T) {
	nodeA, nodeB := uuid.New(), uuid.New()
	e := echo.New()
	NewHandler(&Server{}).Register(e)

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{name: "other node heartbeat", method: http.MethodPost, path: "/v1/nodes/" + nodeB.String() + "/heartbeat"},
		{name: "credential minting", method: http.MethodPost, path: "/v1/credentials"},
		{name: "raft removal", method: http.MethodDelete, path: "/v1/raft/members/node-b"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, nodeRequest(nodeA, tt.method, tt.path))
			if rec.Code != http.StatusForbidden {
				t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusForbidden, rec.Body.String())
			}
		})
	}
}
