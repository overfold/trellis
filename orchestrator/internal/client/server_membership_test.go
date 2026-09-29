package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRemoveRaftMember(t *testing.T) {
	var method, path string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method, path = r.Method, r.URL.Path
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := NewServerClient("token", server.URL, nil)
	if err := client.RemoveRaftMember(context.Background(), "node-2.example:8128"); err != nil {
		t.Fatal(err)
	}
	if method != http.MethodDelete {
		t.Fatalf("method = %q, want %q", method, http.MethodDelete)
	}
	if path != "/v1/raft/members/node-2.example:8128" {
		t.Fatalf("path = %q, want %q", path, "/v1/raft/members/node-2.example:8128")
	}
}

func TestResetReplacementBackoff(t *testing.T) {
	var method, path, namespace string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method, path, namespace = r.Method, r.URL.EscapedPath(), r.Header.Get("X-Trellis-Namespace")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := NewNamespaceServerClient("token", server.URL, "payments", nil)
	if err := client.ResetReplacementBackoff(context.Background(), "web", "api"); err != nil {
		t.Fatal(err)
	}
	if method != http.MethodPost || path != "/v1/jobs/web/groups/api/replacement-backoff/reset" || namespace != "payments" {
		t.Fatalf("request = %s %s namespace %q", method, path, namespace)
	}
}
