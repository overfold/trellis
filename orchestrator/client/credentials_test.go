package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/overfold/trellis/orchestrator/api"
)

func TestCredentialAndJoinTokenRoutes(t *testing.T) {
	type call struct{ method, path, body string }
	var calls []call
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		raw, _ := json.Marshal(body)
		calls = append(calls, call{r.Method, r.URL.EscapedPath(), string(raw)})
		switch r.Method {
		case http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte("[]"))
		default:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"token":"t","id":"0123456789abcdef"}`))
		}
	}))
	defer server.Close()

	ctx := context.Background()
	c := mustNew(t, Config{Address: server.URL})
	if _, err := c.CreateCredential(ctx, &api.CredentialCreateRequest{Scope: "cluster", Access: "read", TTLSeconds: 60}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ListCredentials(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.RevokeCredential(ctx, "0123456789abcdef"); err != nil {
		t.Fatal(err)
	}
	created, err := c.CreateJoinToken(ctx, &api.JoinTokenCreateRequest{TTLSeconds: 3600, MaxUses: 2})
	if err != nil || created.Token != "t" || created.ID != "0123456789abcdef" {
		t.Fatalf("created join token = %+v, %v", created, err)
	}
	if _, err := c.ListJoinTokens(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.RevokeJoinToken(ctx, "../x"); err != nil {
		t.Fatal(err)
	}
	want := []call{
		{http.MethodPost, "/v1/credentials", `{"access":"read","scope":"cluster","ttl_seconds":60}`},
		{http.MethodGet, "/v1/credentials", "null"},
		{http.MethodDelete, "/v1/credentials/0123456789abcdef", "null"},
		{http.MethodPost, "/v1/nodes/join-tokens", `{"max_uses":2,"ttl_seconds":3600}`},
		{http.MethodGet, "/v1/nodes/join-tokens", "null"},
		{http.MethodDelete, "/v1/nodes/join-tokens/..%2Fx", "null"},
	}
	if len(calls) != len(want) {
		t.Fatalf("calls = %+v", calls)
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Errorf("call %d = %+v, want %+v", i, calls[i], want[i])
		}
	}
}
