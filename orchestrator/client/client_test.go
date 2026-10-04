package client

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/overfold/trellis/orchestrator/api"
)

func mustNew(t *testing.T, config Config) *Client {
	t.Helper()
	c, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestNewValidatesConfig(t *testing.T) {
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	for name, config := range map[string]Config{
		"missing address":     {},
		"token and key":       {Address: "control:8124", Token: "token", AdministratorKey: key},
		"short administrator": {Address: "control:8124", AdministratorKey: key[:10]},
	} {
		if _, err := New(config); err == nil {
			t.Errorf("%s: New succeeded", name)
		}
	}
	c := mustNew(t, Config{Address: "control:8124/", Namespace: "team"})
	if c.baseURL != "https://control:8124" || c.Namespace() != "team" {
		t.Fatalf("client = %q in %q", c.baseURL, c.Namespace())
	}
	other := c.WithNamespace("payments")
	if other.Namespace() != "payments" || c.Namespace() != "team" || other.transport != c.transport {
		t.Fatal("WithNamespace must address another namespace over the same connection")
	}
}

func TestRemoveRaftMember(t *testing.T) {
	var method, path string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method, path = r.Method, r.URL.Path
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	if err := mustNew(t, Config{Address: server.URL, Token: "token"}).RemoveRaftMember(context.Background(), "node-2.example:8128"); err != nil {
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

	c := mustNew(t, Config{Address: server.URL, Token: "token", Namespace: "payments"})
	if err := c.ResetReplacementBackoff(context.Background(), "web", "api"); err != nil {
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

	if _, err := mustNew(t, Config{Address: server.URL, Token: "token"}).ListJobs(context.Background()); !errors.Is(err, ErrNamespaceRequired) {
		t.Fatalf("ListJobs error = %v, want ErrNamespaceRequired", err)
	}
}

func TestAllocationListingsUseFiltersAndRoutes(t *testing.T) {
	var path, query string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path, query = r.URL.EscapedPath(), r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("[]"))
	}))
	defer server.Close()

	c := mustNew(t, Config{Address: server.URL, Token: "token", Namespace: "payments"})
	if _, err := c.ListClusterAllocations(context.Background(), AllocationFilter{Label: "tier:web"}); err != nil {
		t.Fatal(err)
	}
	if path != "/v1/allocations" || query != "label=tier%3Aweb" {
		t.Fatalf("cluster request = %s?%s", path, query)
	}
	if _, err := c.ListAllocations(context.Background(), AllocationFilter{}); err != nil {
		t.Fatal(err)
	}
	if path != "/v1/namespaces/payments/allocations" || query != "" {
		t.Fatalf("namespaced request = %s?%s", path, query)
	}
	if _, err := c.ListAllocations(context.Background(), AllocationFilter{Job: "web", Label: "trellis.expose"}); err != nil {
		t.Fatal(err)
	}
	if query != "job=web&label=trellis.expose" {
		t.Fatalf("filtered request = %s?%s", path, query)
	}
}

func TestApplyJobSendsSpecAndPreconditions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/namespaces/default/jobs" || r.Header.Get("Authorization") != "Bearer token" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		var request map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		if string(request["spec"]) != `{"namespace":"default","name":"web"}` || string(request["expected_version"]) != "3" || string(request["expected_incarnation"]) != `"b7c2"` {
			t.Errorf("request = %s", request)
		}
		if string(request["resolved_images"]) != `{"app:main":"app:main@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}` {
			t.Errorf("resolved images = %s", request["resolved_images"])
		}
		w.WriteHeader(http.StatusConflict)
		_, _ = io.WriteString(w, `{"message":"job version conflict: the job was deleted and recreated"}`)
	}))
	defer server.Close()

	version := 3
	_, err := mustNew(t, Config{Address: server.URL, Token: "token", Namespace: "default"}).ApplyJob(context.Background(), &api.JobRegistrationRequest{
		Spec:                json.RawMessage(`{"namespace":"default","name":"web"}`),
		ResolvedImages:      map[string]string{"app:main": "app:main@sha256:" + strings.Repeat("a", 64)},
		ExpectedVersion:     &version,
		ExpectedIncarnation: "b7c2",
	})
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.Status != http.StatusConflict || httpErr.Message() != "job version conflict: the job was deleted and recreated" {
		t.Fatalf("apply error = %v", err)
	}
}

func TestAllocationLogsSelectsTask(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/namespaces/default/allocations/web-1/logs" || r.URL.RawQuery != "follow=true&tail=0&task=api" {
			http.Error(w, "unexpected request "+r.URL.String(), http.StatusBadRequest)
			return
		}
		_, _ = io.WriteString(w, "line\n")
	}))
	defer server.Close()

	logs, err := mustNew(t, Config{Address: server.URL, Namespace: "default"}).AllocationLogs(context.Background(), "web-1", LogOptions{Task: "api", Follow: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = logs.Close() }()
	body, err := io.ReadAll(logs)
	if err != nil || string(body) != "line\n" {
		t.Fatalf("logs = %q, %v", body, err)
	}
}

func TestEventsDecodesServerSentEvents(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/namespaces/default/events" {
			http.Error(w, "unexpected request", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"type\":\"job.registered\",\"namespace\":\"default\",\"job\":\"web\",\"version\":2}\n\n")
		_, _ = io.WriteString(w, ": keepalive\n\n")
		_, _ = io.WriteString(w, "data: {\"type\":\"job.deleted\",\"namespace\":\"default\",\"job\":\"web\"}\n\n")
	}))
	defer server.Close()

	stream, err := mustNew(t, Config{Address: server.URL, Namespace: "default"}).Events(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.Close() }()
	var got []string
	for {
		event, err := stream.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, string(event.Type)+" "+event.JobName)
	}
	if strings.Join(got, ", ") != "job.registered web, job.deleted web" {
		t.Fatalf("events = %v", got)
	}
}

func TestStreamErrorsAreHTTPErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `{"message":"streaming events across namespaces requires cluster scope"}`)
	}))
	defer server.Close()

	_, err := mustNew(t, Config{Address: server.URL, Token: "token"}).ClusterEvents(context.Background())
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.Status != http.StatusForbidden {
		t.Fatalf("ClusterEvents error = %v", err)
	}
}

func TestEventsRejectsOversizedEvent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		line := "data: " + strings.Repeat("x", 64<<10) + "\n"
		for range maxEventBytes/(64<<10) + 1 {
			_, _ = io.WriteString(w, line)
		}
	}))
	defer server.Close()

	stream, err := mustNew(t, Config{Address: server.URL, Namespace: "default"}).Events(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.Close() }()
	if _, err := stream.Next(); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("Next error = %v, want size limit", err)
	}
}
