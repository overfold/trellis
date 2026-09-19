package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/clofour/trellis/internal/election"
)

type fixedElector struct{ leader *election.Leader }

func (e fixedElector) Run(context.Context, chan<- election.Event) error  { return nil }
func (e fixedElector) Current(context.Context) (*election.Leader, error) { return e.leader, nil }

func TestAcquireNodeIDIsStable(t *testing.T) {
	dir := t.TempDir()
	first, err := acquireNodeID(dir)
	if err != nil {
		t.Fatal(err)
	}
	second, err := acquireNodeID(dir)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("node ID changed from %s to %s", first, second)
	}
	info, err := os.Stat(dir + "/node-id")
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("node ID mode is %o", info.Mode().Perm())
	}
}

func TestSplitAddress(t *testing.T) {
	host, port, err := splitAddress("node.example:8127")
	if err != nil {
		t.Fatal(err)
	}
	if host != "node.example" || port != 8127 {
		t.Fatalf("got %s:%d", host, port)
	}
}

func TestDiscardEnrollmentCredential(t *testing.T) {
	path := t.TempDir() + "/trellis.yaml"
	if err := os.WriteFile(path, []byte("cluster: demo\nbootstrap_token: secret\njoin: node-a:8128\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := discardEnrollmentCredential(path); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(raw), "cluster: demo\njoin: node-a:8128\n"; got != want {
		t.Fatalf("config = %q, want %q", got, want)
	}
}

func TestControlPlaneFollowerProxiesToLeader(t *testing.T) {
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer workload-token" {
			t.Errorf("authorization header = %q", got)
		}
		if r.Method != http.MethodGet || r.URL.Path != "/v1/jobs" {
			t.Errorf("proxied request = %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("X-Executed-By", "leader")
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer leader.Close()

	proxy := newControlPlaneProxy(
		fixedElector{leader: &election.Leader{Address: leader.URL}},
		"https://follower.example:8128",
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Fatal("follower executed request locally") }),
		http.DefaultTransport,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	req := httptest.NewRequest(http.MethodGet, "https://follower.example/v1/jobs", nil)
	req.Header.Set("Authorization", "Bearer workload-token")
	recorder := httptest.NewRecorder()
	proxy.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK || recorder.Header().Get("X-Executed-By") != "leader" {
		t.Fatalf("response = %d, headers %v", recorder.Code, recorder.Header())
	}
}

func TestControlPlaneExecutesLocallyOnlyWhenLeaderIsActive(t *testing.T) {
	localCalls := 0
	proxy := newControlPlaneProxy(
		fixedElector{leader: &election.Leader{Address: "node.example:8128"}},
		"node.example:8128",
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { localCalls++; w.WriteHeader(http.StatusNoContent) }),
		http.DefaultTransport,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)

	request := func() *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		proxy.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/jobs", strings.NewReader("{}")))
		return recorder
	}
	if got := request().Code; got != http.StatusServiceUnavailable {
		t.Fatalf("inactive leader status = %d", got)
	}
	proxy.SetLeaderActive(true)
	if got := request().Code; got != http.StatusNoContent {
		t.Fatalf("active leader status = %d", got)
	}
	if localCalls != 1 {
		t.Fatalf("local handler calls = %d", localCalls)
	}
}
