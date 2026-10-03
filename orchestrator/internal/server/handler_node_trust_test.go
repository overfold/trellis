package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/internal/auth"
)

func nodeTrustHandler(t *testing.T) (*nodeTrustFixture, *echo.Echo) {
	t.Helper()
	f := newNodeTrustFixture(t)
	f.server.tokenManager = auth.NewTokenManager(f.store, "test")
	f.server.tokenManager.SetClock(func() time.Time { return f.now })
	e := echo.New()
	NewHandler(f.server).Register(e)
	return f, e
}

func serveAs(t *testing.T, e *echo.Echo, method, path string, body any, admin bool) *httptest.ResponseRecorder {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("encode request body: %v", err)
		}
		reader = bytes.NewReader(raw)
	} else {
		reader = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("Content-Type", "application/json")
	if admin {
		req = req.WithContext(context.WithValue(req.Context(), AdminContextKey, true))
	} else {
		req = req.WithContext(context.WithValue(req.Context(), NamespaceContextKey, auth.EncodeScope(auth.AccessCluster, auth.AccessWrite)))
	}
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	return rec
}

func TestCredentialLifecycleEndpoints(t *testing.T) {
	f, e := nodeTrustHandler(t)
	rec := serveAs(t, e, http.MethodPost, "/v1/credentials", api.CredentialCreateRequest{Scope: "cluster", Access: "read", TTLSeconds: 3600}, true)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create status = %d; body: %s", rec.Code, rec.Body.String())
	}
	var created api.CredentialCreateResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.Token == "" || created.ID == "" || created.ExpiresAt == nil || !created.ExpiresAt.Equal(f.now.Add(time.Hour)) {
		t.Fatalf("created credential = %+v", created)
	}
	if rec := serveAs(t, e, http.MethodPost, "/v1/credentials", api.CredentialCreateRequest{Scope: "cluster", Access: "read", TTLSeconds: -1}, true); rec.Code != http.StatusBadRequest {
		t.Fatalf("negative ttl status = %d", rec.Code)
	}

	for _, request := range []struct{ method, path string }{
		{http.MethodGet, "/v1/credentials"},
		{http.MethodDelete, "/v1/credentials/" + created.ID},
	} {
		if rec := serveAs(t, e, request.method, request.path, nil, false); rec.Code != http.StatusForbidden {
			t.Fatalf("%s %s with cluster/write status = %d, want 403", request.method, request.path, rec.Code)
		}
	}

	rec = serveAs(t, e, http.MethodGet, "/v1/credentials", nil, true)
	if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), created.Token) {
		t.Fatalf("list status = %d, body exposes token or failed: %s", rec.Code, rec.Body.String())
	}
	var listed api.CredentialListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &listed); err != nil || len(listed) != 1 || listed[0].ID != created.ID {
		t.Fatalf("listed credentials = %+v, %v", listed, err)
	}

	if rec := serveAs(t, e, http.MethodDelete, "/v1/credentials/"+created.ID, nil, true); rec.Code != http.StatusNoContent {
		t.Fatalf("revoke status = %d; body: %s", rec.Code, rec.Body.String())
	}
	if principal, _ := f.server.tokenManager.ValidateToken(context.Background(), created.Token); principal != nil {
		t.Fatal("revoked credential still authenticates")
	}
	if rec := serveAs(t, e, http.MethodDelete, "/v1/credentials/"+created.ID, nil, true); rec.Code != http.StatusNotFound {
		t.Fatalf("second revoke status = %d, want 404", rec.Code)
	}
}

func TestJoinTokenEndpoints(t *testing.T) {
	_, e := nodeTrustHandler(t)
	if rec := serveAs(t, e, http.MethodPost, "/v1/nodes/join-tokens", api.JoinTokenCreateRequest{}, false); rec.Code != http.StatusForbidden {
		t.Fatalf("create with cluster/write status = %d, want 403", rec.Code)
	}
	if rec := serveAs(t, e, http.MethodPost, "/v1/nodes/join-tokens", api.JoinTokenCreateRequest{TTLSeconds: int64(MaxJoinTokenTTL/time.Second) + 1}, true); rec.Code != http.StatusBadRequest {
		t.Fatalf("over-long ttl status = %d, want 400", rec.Code)
	}
	rec := serveAs(t, e, http.MethodPost, "/v1/nodes/join-tokens", api.JoinTokenCreateRequest{TTLSeconds: 600, MaxUses: 1}, true)
	if rec.Code != http.StatusCreated || rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("create status = %d, cache-control %q; body: %s", rec.Code, rec.Header().Get("Cache-Control"), rec.Body.String())
	}
	var created api.JoinTokenCreateResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil || created.Token == "" || created.MaxUses != 1 {
		t.Fatalf("created join token = %+v, %v", created, err)
	}

	enroll := func(token string) int {
		req := httptest.NewRequest(http.MethodPost, "/v1/nodes/enroll", strings.NewReader(`{"server_advertise":"n:8128","agent_advertise":"n:8127","raft_advertise":"n:8129"}`))
		req.Header.Set("Content-Type", "application/json")
		req = req.WithContext(context.WithValue(req.Context(), JoinTokenContextKey, token))
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		return rec.Code
	}
	if code := enroll(""); code != http.StatusUnauthorized {
		t.Fatalf("enroll without token status = %d, want 401", code)
	}
	if code := enroll(created.Token); code != http.StatusCreated {
		t.Fatalf("enroll status = %d, want 201", code)
	}
	if code := enroll(created.Token); code != http.StatusUnauthorized {
		t.Fatalf("enroll with exhausted token status = %d, want 401", code)
	}

	rec = serveAs(t, e, http.MethodGet, "/v1/nodes/join-tokens", nil, true)
	var listed api.JoinTokenListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &listed); err != nil || len(listed) != 1 || listed[0].Uses != 1 || strings.Contains(rec.Body.String(), created.Token) {
		t.Fatalf("listed join tokens = %s, %v", rec.Body.String(), err)
	}
	if rec := serveAs(t, e, http.MethodDelete, "/v1/nodes/join-tokens/"+created.ID, nil, true); rec.Code != http.StatusNoContent {
		t.Fatalf("revoke status = %d", rec.Code)
	}
	if rec := serveAs(t, e, http.MethodDelete, "/v1/nodes/join-tokens/"+created.ID, nil, true); rec.Code != http.StatusNotFound {
		t.Fatalf("second revoke status = %d, want 404", rec.Code)
	}
}
