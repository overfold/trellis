package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/auth"
	"github.com/clofour/trellis/internal/election"
	"github.com/clofour/trellis/internal/server"
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
		if r.Header.Get(auth.AdministratorChallengeHeader) != "challenge" || r.Header.Get(auth.AdministratorSignatureHeader) != "signature" {
			t.Error("administrator signing headers were not proxied unchanged")
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
	req.Header.Set(auth.AdministratorChallengeHeader, "challenge")
	req.Header.Set(auth.AdministratorSignatureHeader, "signature")
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
	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	e := echo.New()
	e.Use(leaderAuthMiddleware(auth.NewAdministratorAuthenticator(), func() (ed25519.PublicKey, uint64, bool) {
		return publicKey, 1, true
	}, "enroll-secret", nil))
	e.POST("/v1/jobs", func(c *echo.Context) error { return c.NoContent(http.StatusNoContent) })

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", nil)
	req.Header.Set("Authorization", "Bearer enroll-secret")
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("enrollment credential status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}

	req = httptest.NewRequest(http.MethodPost, "/v1/jobs", nil)
	req.Header.Set("Authorization", "Bearer former-admin-secret")
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("former administrator bearer status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestAdministratorRequestSignatures(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, wrongKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	epoch := uint64(7)
	e := echo.New()
	e.Use(leaderAuthMiddleware(auth.NewAdministratorAuthenticator(), func() (ed25519.PublicKey, uint64, bool) {
		return publicKey, epoch, true
	}, "", nil))
	e.POST("/v1/root", func(c *echo.Context) error {
		if admin, _ := c.Request().Context().Value(server.AdminContextKey).(bool); !admin {
			t.Fatal("valid signature did not grant administrator context")
		}
		return c.NoContent(http.StatusNoContent)
	})

	challenge := func() string {
		req := httptest.NewRequest(http.MethodPost, "/v1/auth/administrator/challenge", nil)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		if rec.Code != http.StatusCreated {
			t.Fatalf("challenge status = %d", rec.Code)
		}
		var response api.AdministratorChallengeResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		return response.Challenge
	}
	signedRequest := func(actualMethod, actualTarget string, actualBody []byte, signedMethod, signedTarget string, signedBody []byte, key ed25519.PrivateKey, challenge string) *http.Request {
		req := httptest.NewRequest(actualMethod, actualTarget, strings.NewReader(string(actualBody)))
		payload := auth.AdministratorSigningPayload(challenge, signedMethod, signedTarget, signedBody)
		req.Header.Set(auth.AdministratorChallengeHeader, challenge)
		req.Header.Set(auth.AdministratorSignatureHeader, base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, payload)))
		return req
	}

	validChallenge := challenge()
	valid := signedRequest(http.MethodPost, "/v1/root?mode=safe", []byte(`{"value":1}`), http.MethodPost, "/v1/root?mode=safe", []byte(`{"value":1}`), privateKey, validChallenge)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, valid)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("valid signature status = %d: %s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, signedRequest(http.MethodPost, "/v1/root?mode=safe", []byte(`{"value":1}`), http.MethodPost, "/v1/root?mode=safe", []byte(`{"value":1}`), privateKey, validChallenge))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("replay status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}

	oldTermChallenge := challenge()
	epoch++
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, signedRequest(http.MethodPost, "/v1/root", nil, http.MethodPost, "/v1/root", nil, privateKey, oldTermChallenge))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("old leadership term challenge status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}

	tests := []struct {
		name         string
		actualMethod string
		actualTarget string
		actualBody   []byte
		signedMethod string
		signedTarget string
		signedBody   []byte
		key          ed25519.PrivateKey
	}{
		{name: "method", actualMethod: http.MethodPost, actualTarget: "/v1/root", signedMethod: http.MethodDelete, signedTarget: "/v1/root", key: privateKey},
		{name: "path and query", actualMethod: http.MethodPost, actualTarget: "/v1/root?mode=unsafe", signedMethod: http.MethodPost, signedTarget: "/v1/root?mode=safe", key: privateKey},
		{name: "body", actualMethod: http.MethodPost, actualTarget: "/v1/root", actualBody: []byte("changed"), signedMethod: http.MethodPost, signedTarget: "/v1/root", signedBody: []byte("original"), key: privateKey},
		{name: "wrong key", actualMethod: http.MethodPost, actualTarget: "/v1/root", signedMethod: http.MethodPost, signedTarget: "/v1/root", key: wrongKey},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, signedRequest(test.actualMethod, test.actualTarget, test.actualBody, test.signedMethod, test.signedTarget, test.signedBody, test.key, challenge()))
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
			}
		})
	}
}
