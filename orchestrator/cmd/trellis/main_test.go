package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/clofour/trellis/internal/election"
	"github.com/clofour/trellis/internal/storage"
	"github.com/clofour/trellis/internal/tlsutil"
	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

type fixedElector struct{ leader *election.Leader }

func (e fixedElector) Run(context.Context, chan<- election.Event) error  { return nil }
func (e fixedElector) Current(context.Context) (*election.Leader, error) { return e.leader, nil }
func (e fixedElector) CurrentID() (uuid.UUID, error) {
	if e.leader == nil {
		return uuid.Nil, nil
	}
	return e.leader.NodeID, nil
}

type mutableElector struct{ leaderID uuid.UUID }

func (e *mutableElector) Run(context.Context, chan<- election.Event) error { return nil }
func (e *mutableElector) Current(context.Context) (*election.Leader, error) {
	if e.leaderID == uuid.Nil {
		return nil, nil
	}
	return &election.Leader{NodeID: e.leaderID}, nil
}
func (e *mutableElector) CurrentID() (uuid.UUID, error) { return e.leaderID, nil }

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

func TestManagedSigningBootstrapsNodeIdentityAndCAKey(t *testing.T) {
	dir := t.TempDir()
	local := storage.NewLocalStorage(dir)
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	id := uuid.New()
	cfg := &config{SigningMode: "managed", ServerAdvertise: "node-a:8128", AgentAdvertise: "node-a:8127"}
	m, err := loadOrBootstrapTLS(context.Background(), slog.Default(), cfg, local, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.CAKey) == 0 {
		t.Fatal("managed mode did not retain the CA private key")
	}
	if err := tlsutil.ValidateMaterials(m, id); err != nil {
		t.Fatal(err)
	}
}

func TestExternalSigningDoesNotPersistCAKey(t *testing.T) {
	dir := t.TempDir()
	id := uuid.New()
	caCert, caKey, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	cert, key, err := tlsutil.GenerateNodeCert(caCert, caKey, id)
	if err != nil {
		t.Fatal(err)
	}
	write := func(name string, data []byte) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	local := storage.NewLocalStorage(filepath.Join(dir, "data"))
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	cfg := &config{SigningMode: "external", CACert: write("ca.crt", caCert), Cert: write("node.crt", cert), Key: write("node.key", key)}
	m, err := loadOrBootstrapTLS(context.Background(), slog.Default(), cfg, local, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.CAKey) != 0 {
		t.Fatal("external mode loaded a CA private key")
	}
	var persisted string
	if err := local.Get("tls/ca-key", &persisted); err == nil {
		t.Fatal("external mode persisted a CA private key")
	}
}

func TestExternalSigningRejectsCertificateFromUntrustedCA(t *testing.T) {
	dir := t.TempDir()
	id := uuid.New()
	trustedCert, _, _ := tlsutil.GenerateCA()
	untrustedCert, untrustedKey, _ := tlsutil.GenerateCA()
	cert, key, _ := tlsutil.GenerateNodeCert(untrustedCert, untrustedKey, id)
	write := func(name string, data []byte) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	local := storage.NewLocalStorage(filepath.Join(dir, "data"))
	_ = local.Init()
	cfg := &config{SigningMode: "external", CACert: write("ca.crt", trustedCert), Cert: write("node.crt", cert), Key: write("node.key", key)}
	if _, err := loadOrBootstrapTLS(context.Background(), slog.Default(), cfg, local, id); err == nil {
		t.Fatal("accepted node certificate signed by an untrusted CA")
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

func TestAgentAuthorizationFollowsLocallyKnownLeader(t *testing.T) {
	oldLeader, newLeader := uuid.New(), uuid.New()
	elector := &mutableElector{leaderID: oldLeader}
	authorize := currentLeaderAuthorizer(elector)
	if !authorize(oldLeader) || authorize(newLeader) {
		t.Fatal("initial Raft leader identity was not enforced")
	}
	elector.leaderID = newLeader
	if authorize(oldLeader) || !authorize(newLeader) {
		t.Fatal("agent authorization did not follow the Raft leadership change")
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

func TestEnrollmentCredentialIsNotAdministratorCredential(t *testing.T) {
	e := echo.New()
	e.Use(leaderAuthMiddleware(func(token string) bool { return token == "admin-secret" }, "enroll-secret", nil))
	e.POST("/v1/jobs", func(c *echo.Context) error { return c.NoContent(http.StatusNoContent) })

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", nil)
	req.Header.Set("Authorization", "Bearer enroll-secret")
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("enrollment credential status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}

	req = httptest.NewRequest(http.MethodPost, "/v1/jobs", nil)
	req.Header.Set("Authorization", "Bearer admin-secret")
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("administrator credential status = %d, want %d", rec.Code, http.StatusNoContent)
	}
}
