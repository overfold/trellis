package client

import (
	"context"
	"errors"
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
	var method, path string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method, path = r.Method, r.URL.EscapedPath()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := NewNamespaceServerClient("token", server.URL, "payments", nil)
	if err := client.ResetReplacementBackoff(context.Background(), "web", "api"); err != nil {
		t.Fatal(err)
	}
	if method != http.MethodPost || path != "/v1/namespaces/payments/jobs/web/groups/api/replacement-backoff/reset" {
		t.Fatalf("request = %s %s", method, path)
	}
}

func TestNamespacedRequestsRequireNamespace(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
	}))
	defer server.Close()

	if _, err := NewServerClient("token", server.URL, nil).ListJobs(context.Background()); !errors.Is(err, ErrNamespaceRequired) {
		t.Fatalf("ListJobs error = %v, want ErrNamespaceRequired", err)
	}
}

func TestClusterAllocationsUseClusterWideRoute(t *testing.T) {
	var path, query string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path, query = r.URL.EscapedPath(), r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("[]"))
	}))
	defer server.Close()

	c := NewNamespaceServerClient("token", server.URL, "payments", nil)
	if _, err := c.ListClusterAllocations(context.Background(), "tier:web"); err != nil {
		t.Fatal(err)
	}
	if path != "/v1/allocations" || query != "label=tier%3Aweb" {
		t.Fatalf("cluster request = %s?%s", path, query)
	}
	if _, err := c.ListAllocations(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	if path != "/v1/namespaces/payments/allocations" || query != "" {
		t.Fatalf("namespaced request = %s?%s", path, query)
	}
}
