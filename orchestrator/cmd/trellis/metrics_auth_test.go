package main

import (
	"crypto/ed25519"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/overfold/trellis/orchestrator/internal/auth"
	"github.com/overfold/trellis/orchestrator/internal/server"
	"github.com/overfold/trellis/orchestrator/internal/state"
)

func TestMetricsRequireClusterScopedCredential(t *testing.T) {
	store, err := state.NewBoltStore(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	tokens := auth.NewTokenManager(store, "test")
	token := func(scope auth.AccessScope, access auth.AccessLevel, namespace string) string {
		t.Helper()
		value, err := tokens.CreateToken(t.Context(), auth.Principal{Kind: auth.CredentialOperator, Scope: scope, Access: access, Namespace: namespace})
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	control := server.NewServer(slog.New(slog.NewTextHandler(io.Discard, nil)), nil, server.NewStateController(store, "test"), store, "test", "")
	e := echo.New()
	e.Use(leaderAuthMiddleware(auth.NewAdministratorAuthenticator(), func() (ed25519.PublicKey, uint64, bool) { return nil, 0, false }, "", tokens, nil))
	server.NewHandler(control).Register(e)

	for _, tc := range []struct {
		name   string
		token  string
		status int
	}{
		{name: "anonymous", status: http.StatusUnauthorized},
		{name: "unknown token", token: "trls_unknown", status: http.StatusUnauthorized},
		{name: "namespace read", token: token(auth.AccessNamespace, auth.AccessRead, "team"), status: http.StatusForbidden},
		{name: "namespace write", token: token(auth.AccessNamespace, auth.AccessWrite, "team"), status: http.StatusForbidden},
		{name: "cluster read", token: token(auth.AccessCluster, auth.AccessRead, ""), status: http.StatusOK},
		{name: "cluster write", token: token(auth.AccessCluster, auth.AccessWrite, ""), status: http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
			if tc.token != "" {
				request.Header.Set("Authorization", "Bearer "+tc.token)
			}
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, request)
			if rec.Code != tc.status {
				t.Fatalf("status = %d, want %d; body: %s", rec.Code, tc.status, rec.Body.String())
			}
			if tc.status == http.StatusOK && !strings.Contains(rec.Body.String(), "go_goroutines") {
				t.Fatalf("metrics body lacks Prometheus metrics: %s", rec.Body.String())
			}
			if tc.status != http.StatusOK && strings.Contains(rec.Body.String(), "trellis_") {
				t.Fatalf("rejected scrape exposed metrics: %s", rec.Body.String())
			}
		})
	}
}
