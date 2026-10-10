package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/client"
	"github.com/overfold/trellis/orchestrator/internal/adminsign"
	"github.com/overfold/trellis/orchestrator/internal/auth"
	"github.com/overfold/trellis/orchestrator/internal/election"
	"github.com/overfold/trellis/orchestrator/internal/execstream"
	"github.com/overfold/trellis/orchestrator/internal/execwebsocket"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	"github.com/overfold/trellis/orchestrator/internal/server"
	"github.com/overfold/trellis/orchestrator/internal/storage"
	"github.com/overfold/trellis/orchestrator/internal/tlsutil"
	"github.com/spf13/pflag"
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

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

type countedReader struct {
	reader io.Reader
	reads  int
	bytes  int64
}

func (r *countedReader) Read(p []byte) (int, error) {
	r.reads++
	if r.reader != nil {
		n, err := r.reader.Read(p)
		r.bytes += int64(n)
		return n, err
	}
	return 0, io.ErrUnexpectedEOF
}

func TestAdministratorProofRejectedBeforeBodyRead(t *testing.T) {
	t.Parallel()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	a := auth.NewAdministratorAuthenticator()
	e := echo.New()
	e.Use(leaderAuthMiddleware(a, func() (ed25519.PublicKey, uint64, bool) { return publicKey, 7, true }, nil, nil))
	e.POST("/v1/credentials", func(c *echo.Context) error { return c.NoContent(http.StatusNoContent) })
	value, expiresAt, err := a.Issue(7)
	if err != nil {
		t.Fatal(err)
	}
	oldEpoch, _, err := a.Issue(6)
	if err != nil {
		t.Fatal(err)
	}
	proof := base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, adminsign.Payload(value, "POST", "/v1/credentials", nil)))
	used, _, err := a.Issue(7)
	if err != nil {
		t.Fatal(err)
	}
	usedProof := base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, adminsign.Payload(used, "POST", "/v1/credentials", nil)))
	valid := httptest.NewRequest(http.MethodPost, "/v1/credentials", nil)
	valid.Header.Set(adminsign.ChallengeHeader, used)
	valid.Header.Set(adminsign.SignatureHeader, usedProof)
	validResponse := httptest.NewRecorder()
	e.ServeHTTP(validResponse, valid)
	if validResponse.Code != http.StatusNoContent {
		t.Fatalf("legitimate request status=%d", validResponse.Code)
	}
	for _, test := range []struct{ name, challenge, signature string }{
		{"incomplete", value, ""},
		{"signature only", "", proof},
		{"malformed signature", value, "invalid"},
		{"malformed signature encoding", value, strings.Repeat("!", 86)},
		{"malformed challenge", "invalid", proof},
		{"invalid challenge", strings.Repeat("A", 107), proof},
		{"old epoch", oldEpoch, proof},
		{"replay", used, usedProof},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			body := &countedReader{}
			req := httptest.NewRequest(http.MethodPost, "/v1/credentials", nil)
			req.Body = io.NopCloser(body)
			req.Header.Set(adminsign.ChallengeHeader, test.challenge)
			req.Header.Set(adminsign.SignatureHeader, test.signature)
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req)
			t.Logf("status=%d body reads=%d", rec.Code, body.reads)
			if rec.Code != http.StatusUnauthorized || body.reads != 0 {
				t.Fatalf("want 401 with zero body reads")
			}
		})
	}
	t.Run("expired challenge", func(t *testing.T) {
		t.Parallel()
		time.Sleep(time.Until(expiresAt))
		body := &countedReader{}
		req := httptest.NewRequest(http.MethodPost, "/v1/credentials", nil)
		req.Body = io.NopCloser(body)
		req.Header.Set(adminsign.ChallengeHeader, value)
		req.Header.Set(adminsign.SignatureHeader, proof)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		t.Logf("expired challenge status=%d body reads=%d", rec.Code, body.reads)
		if rec.Code != http.StatusUnauthorized || body.reads != 0 {
			t.Fatal("expired challenge read body")
		}
	})
}

type endlessReader struct{}

func (endlessReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	return len(p), nil
}

type heldRequestBody struct {
	entered chan<- struct{}
	release <-chan struct{}
}

func (r heldRequestBody) Read([]byte) (int, error) {
	r.entered <- struct{}{}
	<-r.release
	return 0, io.EOF
}

type signalingBody struct {
	io.ReadCloser
	entered chan<- struct{}
}

func (r signalingBody) Read(p []byte) (int, error) {
	select {
	case r.entered <- struct{}{}:
	default:
	}
	return r.ReadCloser.Read(p)
}

func TestAdministratorVerificationBudgets(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	a := auth.NewAdministratorAuthenticator()
	var epoch atomic.Uint64
	epoch.Store(7)
	e := echo.New()
	e.Use(leaderAuthMiddleware(a, func() (ed25519.PublicKey, uint64, bool) { return publicKey, epoch.Load(), true }, nil, nil))
	for _, route := range []struct{ method, path string }{
		{"POST", "/v1/credentials"}, {"POST", "/v1/namespaces/:namespace/jobs"},
		{"POST", "/v1/namespaces/:namespace/jobs/plan"}, {"PUT", "/v1/namespaces/:namespace/secrets/:name"},
		{"POST", "/v1/nodes"}, {"POST", "/v1/nodes/:id/heartbeat"},
	} {
		e.Add(route.method, route.path, func(c *echo.Context) error { return c.NoContent(http.StatusNoContent) })
	}
	request := func(method, target string, body []byte, valid bool) *http.Request {
		value, _, err := a.Issue(epoch.Load())
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(method, target, bytes.NewReader(body))
		if len(body) == 0 {
			req.Body = http.NoBody
		}
		req.Header.Set(adminsign.ChallengeHeader, value)
		proof := make([]byte, ed25519.SignatureSize)
		if valid {
			proof = ed25519.Sign(privateKey, adminsign.Payload(value, method, target, body))
		}
		req.Header.Set(adminsign.SignatureHeader, base64.RawURLEncoding.EncodeToString(proof))
		return req
	}
	t.Run("route boundaries without digest", func(t *testing.T) {
		for _, route := range []struct {
			method, target string
			limit          int
		}{
			{"POST", "/v1/credentials", 64 << 10}, {"POST", "/v1/namespaces/default/jobs", 4 << 20},
			{"POST", "/v1/namespaces/default/jobs/plan", 4 << 20}, {"PUT", "/v1/namespaces/default/secrets/key", 96 << 10},
			{"POST", "/v1/nodes", 1 << 20}, {"POST", "/v1/nodes/node/heartbeat", 32 << 20},
		} {
			body := bytes.Repeat([]byte("x"), route.limit)
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, request(route.method, route.target, body, true))
			if rec.Code != http.StatusNoContent {
				t.Fatalf("legitimate boundary %s: %d", route.target, rec.Code)
			}
			req := request(route.method, route.target, nil, false)
			incoming := &countedReader{reader: io.LimitReader(endlessReader{}, int64(route.limit+4096))}
			req.Body = io.NopCloser(incoming)
			rec = httptest.NewRecorder()
			e.ServeHTTP(rec, req)
			t.Logf("%s streamed overflow: status=%d bytes read=%d budget=%d", route.target, rec.Code, incoming.bytes, route.limit)
			if rec.Code != http.StatusRequestEntityTooLarge || incoming.bytes != int64(route.limit+1) {
				t.Errorf("well-formed invalid signature exceeded route budget")
			}
		}
	})
	t.Run("tampered body does not consume challenge", func(t *testing.T) {
		for _, withDigest := range []bool{false, true} {
			original := []byte("original")
			req := request("POST", "/v1/credentials", original, true)
			if withDigest {
				digest := sha256.Sum256(original)
				req.Header.Set(adminsign.DigestHeader, hex.EncodeToString(digest[:]))
			}
			req.Body = io.NopCloser(strings.NewReader("tampered"))
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("tampered request status=%d", rec.Code)
			}
			req.Body = io.NopCloser(bytes.NewReader(original))
			rec = httptest.NewRecorder()
			e.ServeHTTP(rec, req)
			if rec.Code != http.StatusNoContent {
				t.Fatalf("valid retry after tampering status=%d", rec.Code)
			}
		}
	})
	t.Run("concurrent replay admits one request", func(t *testing.T) {
		req := request("POST", "/v1/credentials", nil, true)
		results := make(chan int, 8)
		for range 8 {
			go func() {
				rec := httptest.NewRecorder()
				e.ServeHTTP(rec, req.Clone(context.Background()))
				results <- rec.Code
			}()
		}
		admitted := 0
		for range 8 {
			switch status := <-results; status {
			case http.StatusNoContent:
				admitted++
			case http.StatusUnauthorized, http.StatusServiceUnavailable:
			default:
				t.Errorf("unexpected replay status=%d", status)
			}
		}
		if admitted != 1 {
			t.Fatalf("concurrent replay admitted %d requests", admitted)
		}
	})
	t.Run("invalid signature with digest and declared oversize read nothing", func(t *testing.T) {
		for _, withDigest := range []bool{false, true} {
			req := request("POST", "/v1/credentials", nil, false)
			body := &countedReader{}
			req.Body = io.NopCloser(body)
			want := http.StatusRequestEntityTooLarge
			if withDigest {
				digest := sha256.Sum256(nil)
				req.Header.Set(adminsign.DigestHeader, hex.EncodeToString(digest[:]))
				want = http.StatusUnauthorized
			} else {
				req.ContentLength = (64 << 10) + 1
			}
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req)
			if rec.Code != want || body.reads != 0 {
				t.Fatalf("status=%d reads=%d, want %d and zero", rec.Code, body.reads, want)
			}
		}
	})
	t.Run("concurrent upload admission", func(t *testing.T) {
		entered := make(chan struct{}, 4)
		release := make(chan struct{})
		results := make(chan int, 4)
		t.Cleanup(func() {
			close(release)
			for range 4 {
				<-results
			}
		})
		for range 4 {
			req := request("POST", "/v1/namespaces/default/jobs", nil, false)
			req.Body = io.NopCloser(heldRequestBody{entered, release})
			go func() {
				rec := httptest.NewRecorder()
				e.ServeHTTP(rec, req)
				results <- rec.Code
			}()
		}
		for range 4 {
			select {
			case <-entered:
			case <-time.After(2 * time.Second):
				t.Fatal("upload did not enter")
			}
		}
		body := &countedReader{}
		req := request("POST", "/v1/namespaces/default/jobs", nil, false)
		req.Body = io.NopCloser(body)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		t.Logf("fifth concurrent request status=%d reads=%d retry=%s", rec.Code, body.reads, rec.Header().Get("Retry-After"))
		if rec.Code != http.StatusServiceUnavailable || body.reads != 0 || rec.Header().Get("Retry-After") != "1" {
			t.Error("aggregate admission did not reject fifth request before reading")
		}
		for _, payload := range [][]byte{nil, []byte("small")} {
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, request("POST", "/v1/credentials", payload, true))
			if rec.Code != http.StatusNoContent {
				t.Errorf("lightweight operation blocked by large uploads: %d", rec.Code)
			}
		}
	})
	t.Run("downstream requests do not occupy upload slots", func(t *testing.T) {
		entered, release := make(chan struct{}, 8), make(chan struct{})
		results := make(chan int, 8)
		e.POST("/v1/held", func(c *echo.Context) error {
			entered <- struct{}{}
			<-release
			return c.NoContent(http.StatusNoContent)
		})
		launched := 0
		t.Cleanup(func() {
			close(release)
			for range launched {
				<-results
			}
		})
		for i := range 8 {
			payload := []byte("body")
			if i >= 4 {
				payload = nil
			}
			req := request("POST", "/v1/held", payload, true)
			launched++
			go func() { rec := httptest.NewRecorder(); e.ServeHTTP(rec, req); results <- rec.Code }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("downstream request occupied upload admission")
			}
		}
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, request("POST", "/v1/credentials", []byte("small"), true))
		if rec.Code != http.StatusNoContent {
			t.Fatalf("lightweight operation status=%d", rec.Code)
		}
	})
	t.Run("retained buffer budget and recovery", func(t *testing.T) {
		entered, release := make(chan struct{}, 15), make(chan struct{})
		results := make(chan int, 15)
		e.POST("/v1/retained", func(c *echo.Context) error {
			entered <- struct{}{}
			<-release
			return c.NoContent(http.StatusNoContent)
		})
		launched := 0
		closed := false
		t.Cleanup(func() {
			if !closed {
				close(release)
			}
			for range launched {
				<-results
			}
		})
		// Fifteen 64 KiB + 1 buffers fit in the reserved 1 MiB lane.
		for range 15 {
			req := request("POST", "/v1/retained", []byte("body"), true)
			launched++
			go func() { rec := httptest.NewRecorder(); e.ServeHTTP(rec, req); results <- rec.Code }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("buffer budget exhausted too early")
			}
		}
		req := request("POST", "/v1/credentials", []byte("body"), true)
		incoming := &countedReader{reader: strings.NewReader("body")}
		req.Body = io.NopCloser(incoming)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		if rec.Code != http.StatusServiceUnavailable || incoming.reads != 0 || !strings.Contains(rec.Body.String(), "memory limit") {
			t.Fatalf("retained memory budget status=%d reads=%d: %s", rec.Code, incoming.reads, rec.Body.String())
		}
		empty := httptest.NewRecorder()
		e.ServeHTTP(empty, request("POST", "/v1/credentials", nil, true))
		if empty.Code != http.StatusNoContent {
			t.Fatal("empty operation used buffer budget")
		}
		close(release)
		closed = true
		for range launched {
			<-results
		}
		launched = 0
		req.Body = io.NopCloser(strings.NewReader("body"))
		rec = httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		if rec.Code != http.StatusNoContent {
			t.Fatalf("memory recovery burned proof or retained buffers: %d", rec.Code)
		}
	})
	t.Run("recovered slot and canceled proof not consumed", func(t *testing.T) {
		req := request("POST", "/v1/credentials", nil, true)
		ctx, cancel := context.WithCancel(req.Context())
		cancel()
		body := &countedReader{}
		req.Body = io.NopCloser(body)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req.WithContext(ctx))
		if rec.Code != http.StatusServiceUnavailable || body.reads != 0 {
			t.Fatalf("canceled request status=%d reads=%d", rec.Code, body.reads)
		}
		req.Body = http.NoBody
		rec = httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		if rec.Code != http.StatusNoContent {
			t.Fatalf("recovered admission/reused unconsumed proof: %d", rec.Code)
		}
	})
	t.Run("epoch changes during upload", func(t *testing.T) {
		req := request("POST", "/v1/credentials", nil, true)
		entered, release := make(chan struct{}, 1), make(chan struct{})
		req.Body = io.NopCloser(heldRequestBody{entered, release})
		done := make(chan int, 1)
		go func() { rec := httptest.NewRecorder(); e.ServeHTTP(rec, req); done <- rec.Code }()
		<-entered
		epoch.Add(1)
		close(release)
		if got := <-done; got != http.StatusServiceUnavailable {
			t.Fatalf("epoch changed during successful upload: status=%d", got)
		}
	})
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprintf("slow wire upload canceled=%v", canceled), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered := make(chan struct{}, 1)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				r.Body = signalingBody{r.Body, entered}
				e.ServeHTTP(w, r.WithContext(ctx))
			}))
			defer srv.Close()
			conn, err := net.DialTimeout("tcp", srv.Listener.Addr().String(), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = conn.Close() }()
			req := request("POST", "/v1/credentials", nil, false)
			if err := conn.SetDeadline(time.Now().Add(20 * time.Second)); err != nil {
				t.Fatal(err)
			}
			start := time.Now()
			_, err = fmt.Fprintf(conn, "POST /v1/credentials HTTP/1.1\r\nHost: test\r\nContent-Length: 2\r\nConnection: close\r\n%s: %s\r\n%s: %s\r\n\r\nx", adminsign.ChallengeHeader, req.Header.Get(adminsign.ChallengeHeader), adminsign.SignatureHeader, req.Header.Get(adminsign.SignatureHeader))
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-time.After(2 * time.Second):
				t.Fatal("wire body read did not enter")
			}
			if canceled {
				cancel()
			}
			response, err := http.ReadResponse(bufio.NewReader(conn), req)
			if err != nil {
				t.Fatalf("slow upload not bounded: %v", err)
			}
			defer func() { _ = response.Body.Close() }()
			t.Logf("slow upload status=%d elapsed=%s", response.StatusCode, time.Since(start).Round(time.Millisecond))
			if response.StatusCode != http.StatusBadRequest {
				t.Fatalf("slow upload status=%d", response.StatusCode)
			}
			if canceled && time.Since(start) > 2*time.Second {
				t.Fatal("cancellation did not unblock wire upload promptly")
			}
		})
	}
}

func TestRunValidatesClusterNameBeforeStorage(t *testing.T) {
	tests := []struct {
		name  string
		valid bool
	}{
		{name: "a", valid: true},
		{name: strings.Repeat("a", 63), valid: true},
		{name: "Production_1.eu-west", valid: true},
		{name: ""},
		{name: strings.Repeat("a", 64)},
		{name: "/production"},
		{name: "production/primary"},
		{name: `production\primary`},
		{name: " production"},
		{name: "production "},
		{name: "prod:primary"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataDir := filepath.Join(t.TempDir(), "data")
			err := run(t.Context(), &config{Cluster: tt.name, DataDir: dataDir})
			if tt.valid {
				if err == nil || !strings.Contains(err.Error(), "administrator_public_key or --administrator-public-key is required") {
					t.Fatalf("run error = %v, want next startup validation error", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "cluster or --cluster must be a safe identifier") {
				t.Fatalf("run error = %v, want cluster name validation error", err)
			}
			if _, err := os.Stat(dataDir); !os.IsNotExist(err) {
				t.Fatalf("data directory was touched during startup validation: %v", err)
			}
		})
	}
}

func TestRunRejectsUnsafeClusterBeforeStorageForFlagAndConfigFile(t *testing.T) {
	for _, source := range []string{"flag", "config file"} {
		t.Run(source, func(t *testing.T) {
			dataDir := filepath.Join(t.TempDir(), "data")
			cfg := &config{Cluster: "default", DataDir: dataDir}
			flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
			flags.StringVar(&cfg.Cluster, "cluster", cfg.Cluster, "")
			if source == "flag" {
				if err := flags.Parse([]string{"--cluster", "production/primary"}); err != nil {
					t.Fatal(err)
				}
			} else {
				path := filepath.Join(t.TempDir(), "trellis.yaml")
				if err := os.WriteFile(path, []byte("cluster: production/primary\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := loadNodeConfig(path, cfg, flags); err != nil {
					t.Fatal(err)
				}
			}

			err := run(t.Context(), cfg)
			if err == nil || !strings.Contains(err.Error(), "cluster or --cluster must be a safe identifier") {
				t.Fatalf("run error = %v, want cluster name validation error", err)
			}
			if _, err := os.Stat(dataDir); !os.IsNotExist(err) {
				t.Fatalf("data directory was touched before cluster validation: %v", err)
			}
		})
	}
}

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

func TestDecodeSecretsKeyClearsInputBuffer(t *testing.T) {
	raw := []byte(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32)))
	key, err := decodeSecretsKey(raw)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(key)
	if !bytes.Equal(key, bytes.Repeat([]byte{7}, 32)) {
		t.Fatalf("decoded key = %x", key)
	}
	if !bytes.Equal(raw, make([]byte, len(raw))) {
		t.Fatal("raw key-loading buffer was not cleared")
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
	m, enrolledID, err := loadOrBootstrapTLS(context.Background(), slog.Default(), cfg, local, id)
	if err != nil {
		t.Fatal(err)
	}
	if enrolledID != id {
		t.Fatalf("node ID = %s, want %s", enrolledID, id)
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
	m, enrolledID, err := loadOrBootstrapTLS(context.Background(), slog.Default(), cfg, local, id)
	if err != nil {
		t.Fatal(err)
	}
	if enrolledID != id {
		t.Fatalf("node ID = %s, want %s", enrolledID, id)
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
	if _, _, err := loadOrBootstrapTLS(context.Background(), slog.Default(), cfg, local, id); err == nil {
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
	authorize := currentLeaderAuthorizer(elector, nil)
	if !authorize(context.Background(), oldLeader, nil) || authorize(context.Background(), newLeader, nil) {
		t.Fatal("initial Raft leader identity was not enforced")
	}
	elector.leaderID = newLeader
	if authorize(context.Background(), oldLeader, nil) || !authorize(context.Background(), newLeader, nil) {
		t.Fatal("agent authorization did not follow the Raft leadership change")
	}
}

func TestControlPlaneFollowerProxiesToLeader(t *testing.T) {
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer workload-token" {
			t.Errorf("authorization header = %q", got)
		}
		if r.Method != http.MethodGet || r.URL.Path != "/v1/namespaces/default/jobs" {
			t.Errorf("proxied request = %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get(adminsign.ChallengeHeader) != "challenge" || r.Header.Get(adminsign.SignatureHeader) != "signature" {
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
	req := httptest.NewRequest(http.MethodGet, "https://follower.example/v1/namespaces/default/jobs", nil)
	req.Header.Set("Authorization", "Bearer workload-token")
	req.Header.Set(adminsign.ChallengeHeader, "challenge")
	req.Header.Set(adminsign.SignatureHeader, "signature")
	recorder := httptest.NewRecorder()
	proxy.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK || recorder.Header().Get("X-Executed-By") != "leader" {
		t.Fatalf("response = %d, headers %v", recorder.Code, recorder.Header())
	}
}

// A follower forwards an exec WebSocket to the leader and then carries its
// messages in both directions.
func TestControlPlaneFollowerProxiesExecStream(t *testing.T) {
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !execwebsocket.IsRequest(r) || r.Header.Get("Authorization") != "Bearer operator-token" {
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		conn, err := execwebsocket.Accept(w, r)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		frame, err := execstream.NewReader(conn).Next()
		if err != nil {
			t.Errorf("read: %v", err)
			return
		}
		writer := execstream.NewWriter(conn, 0)
		_ = writer.WriteData(execstream.FrameStdout, frame.Payload)
		_ = writer.WriteJSON(execstream.FrameExit, api.ExecExit{ExitCode: 2})
	}))
	defer leader.Close()
	follower := httptest.NewUnstartedServer(newControlPlaneProxy(
		fixedElector{leader: &election.Leader{Address: leader.URL}},
		"https://follower.example:8128",
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Fatal("follower executed request locally") }),
		http.DefaultTransport,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	))
	// The API server's read timeout must not end a stream that outlives it.
	follower.Config.ReadTimeout = 200 * time.Millisecond
	follower.Start()
	defer follower.Close()

	operator, err := client.New(client.Config{Address: follower.URL, Token: "operator-token", Namespace: "team"})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := operator.Exec(context.Background(), "alloc-1", api.ExecRequest{Command: []string{"cat"}, Stdin: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.Close() }()
	time.Sleep(3 * follower.Config.ReadTimeout)
	if _, err := stream.Write([]byte("through the follower")); err != nil {
		t.Fatal(err)
	}
	var stdout strings.Builder
	if code, err := stream.Wait(&stdout, io.Discard); err != nil || code != 2 || stdout.String() != "through the follower" {
		t.Fatalf("wait = %d, %v; stdout %q", code, err, stdout.String())
	}
}

func TestControlPlaneFollowerRedirectsCertificateBoundNodeRoutes(t *testing.T) {
	proxy := newControlPlaneProxy(
		fixedElector{leader: &election.Leader{Address: "leader.example:8128"}},
		"follower.example:8128",
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Fatal("follower executed request locally") }),
		roundTripperFunc(func(*http.Request) (*http.Response, error) {
			t.Fatal("follower proxied a certificate-bound node request with its own identity")
			return nil, nil
		}),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)

	for _, test := range []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/v1/nodes"},
		{http.MethodPost, "/v1/nodes/" + uuid.NewString() + "/heartbeat"},
		{http.MethodGet, "/v1/internal/discovery"},
		{http.MethodPost, "/v1/raft/join"},
	} {
		t.Run(test.method+" "+test.path, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			proxy.ServeHTTP(recorder, httptest.NewRequest(test.method, "https://follower.example"+test.path, nil))
			if recorder.Code != http.StatusTemporaryRedirect {
				t.Fatalf("status = %d, want %d", recorder.Code, http.StatusTemporaryRedirect)
			}
			if got := recorder.Header().Get("Location"); got != "https://leader.example:8128"+test.path {
				t.Fatalf("Location = %q", got)
			}
		})
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
		proxy.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/namespaces/default/jobs", strings.NewReader("{}")))
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

func TestJoinTokenIsOnlyAnEnrollmentCredential(t *testing.T) {
	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	const joinToken = "trls_join_0123456789abcdef.secret"
	e := echo.New()
	e.Use(leaderAuthMiddleware(auth.NewAdministratorAuthenticator(), func() (ed25519.PublicKey, uint64, bool) {
		return publicKey, 1, true
	}, nil, nil))
	e.POST("/v1/namespaces/default/jobs", func(c *echo.Context) error { return c.NoContent(http.StatusNoContent) })
	e.POST("/v1/nodes/enroll", func(c *echo.Context) error {
		if admin, _ := c.Request().Context().Value(server.AdminContextKey).(bool); admin {
			t.Fatal("enrollment request carried administrator authority")
		}
		if got, _ := c.Request().Context().Value(server.JoinTokenContextKey).(string); got != joinToken {
			t.Fatalf("enrollment join token = %q, want the presented bearer", got)
		}
		return c.NoContent(http.StatusCreated)
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/namespaces/default/jobs", nil)
	req.Header.Set("Authorization", "Bearer "+joinToken)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("join token status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}

	req = httptest.NewRequest(http.MethodPost, "/v1/nodes/enroll", nil)
	req.Header.Set("Authorization", "Bearer "+joinToken)
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("enrollment status = %d, want the join token passed to enrollment", rec.Code)
	}

	req = httptest.NewRequest(http.MethodPost, "/v1/namespaces/default/jobs", nil)
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
	verificationAvailable := true
	var retainedBody []byte
	e := echo.New()
	e.Use(leaderAuthMiddleware(auth.NewAdministratorAuthenticator(), func() (ed25519.PublicKey, uint64, bool) {
		return publicKey, epoch, verificationAvailable
	}, nil, nil))
	e.POST("/v1/root", func(c *echo.Context) error {
		if admin, _ := c.Request().Context().Value(server.AdminContextKey).(bool); !admin {
			t.Fatal("valid signature did not grant administrator context")
		}
		return c.NoContent(http.StatusNoContent)
	})
	e.POST("/v1/secrets", func(c *echo.Context) error {
		// bytes.Reader.WriteTo passes the middleware's actual backing slice.
		_, err := io.Copy(captureBodyWriter{capture: &retainedBody}, c.Request().Body)
		return err
	})
	e.POST("/v1/backup/restore", func(c *echo.Context) error {
		if admin, _ := c.Request().Context().Value(server.AdminContextKey).(bool); !admin {
			t.Fatal("restore not authenticated")
		}
		count, err := io.Copy(io.Discard, c.Request().Body)
		if err != nil || count != (64<<20)+1 {
			t.Fatalf("staged body = %d bytes, %v", count, err)
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
		payload := adminsign.Payload(challenge, signedMethod, signedTarget, signedBody)
		digest := sha256.Sum256(signedBody)
		req.Header.Set(adminsign.DigestHeader, hex.EncodeToString(digest[:]))
		req.Header.Set(adminsign.ChallengeHeader, challenge)
		req.Header.Set(adminsign.SignatureHeader, base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, payload)))
		return req
	}
	t.Run("secret request buffer wiped after downstream", func(t *testing.T) {
		body := []byte(`{"value_base64":"c2VjcmV0"}`)
		req := signedRequest("POST", "/v1/secrets", body, "POST", "/v1/secrets", body, privateKey, challenge())
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK || len(retainedBody) != len(body) || !bytes.Equal(retainedBody, make([]byte, len(body))) {
			t.Fatal("redundant signed request body was retained", rec.Code)
		}
	})
	t.Run("large restore signature and spool cleanup", func(t *testing.T) {
		t.Setenv("TMPDIR", t.TempDir())
		body := bytes.Repeat([]byte("x"), (64<<20)+1)
		for _, valid := range []bool{false, true} {
			value := challenge()
			signedBody := body
			if !valid {
				signedBody = []byte("different")
			}
			req := signedRequest(http.MethodPost, "/v1/backup/restore", body, http.MethodPost, "/v1/backup/restore", signedBody, privateKey, value)
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req)
			want := http.StatusUnauthorized
			if valid {
				want = http.StatusNoContent
			}
			if rec.Code != want {
				t.Fatalf("restore signature status = %d: %s", rec.Code, rec.Body.String())
			}
			files, err := os.ReadDir(os.Getenv("TMPDIR"))
			if err != nil || len(files) != 0 {
				t.Fatalf("restore spool leaked: %v, %v", files, err)
			}
		}
	})

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

	assertNotBurned := func(t *testing.T, challenge string, attempt *http.Request, wantStatus int) {
		t.Helper()
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, attempt)
		if rec.Code != wantStatus {
			t.Fatalf("malformed attempt status = %d, want %d", rec.Code, wantStatus)
		}
		rec = httptest.NewRecorder()
		e.ServeHTTP(rec, signedRequest(http.MethodPost, "/v1/root", nil, http.MethodPost, "/v1/root", nil, privateKey, challenge))
		if rec.Code != http.StatusNoContent {
			t.Fatalf("challenge retry status = %d, want %d", rec.Code, http.StatusNoContent)
		}
	}

	t.Run("incomplete headers do not burn challenge", func(t *testing.T) {
		value := challenge()
		req := httptest.NewRequest(http.MethodPost, "/v1/root", nil)
		req.Header.Set(adminsign.ChallengeHeader, value)
		assertNotBurned(t, value, req, http.StatusUnauthorized)
	})
	t.Run("unreadable body does not burn challenge", func(t *testing.T) {
		value := challenge()
		req := signedRequest(http.MethodPost, "/v1/root", nil, http.MethodPost, "/v1/root", nil, privateKey, value)
		req.Body = io.NopCloser(failingReader{})
		assertNotBurned(t, value, req, http.StatusBadRequest)
	})
	t.Run("oversized body does not burn challenge", func(t *testing.T) {
		value := challenge()
		req := signedRequest(http.MethodPost, "/v1/root", nil, http.MethodPost, "/v1/root", nil, privateKey, value)
		req.Body = io.NopCloser(io.LimitReader(endlessReader{}, (64<<20)+1))
		assertNotBurned(t, value, req, http.StatusRequestEntityTooLarge)
	})
	t.Run("unavailable verification does not burn challenge", func(t *testing.T) {
		value := challenge()
		verificationAvailable = false
		attempt := signedRequest(http.MethodPost, "/v1/root", nil, http.MethodPost, "/v1/root", nil, privateKey, value)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, attempt)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("unavailable attempt status = %d, want %d", rec.Code, http.StatusUnauthorized)
		}
		verificationAvailable = true
		rec = httptest.NewRecorder()
		e.ServeHTTP(rec, signedRequest(http.MethodPost, "/v1/root", nil, http.MethodPost, "/v1/root", nil, privateKey, value))
		if rec.Code != http.StatusNoContent {
			t.Fatalf("challenge retry status = %d, want %d", rec.Code, http.StatusNoContent)
		}
	})

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

type captureBodyWriter struct{ capture *[]byte }

func (w captureBodyWriter) Write(p []byte) (int, error) {
	*w.capture = p
	return len(p), nil
}

func TestSecretsKeyLoaderRejectsUnsafeOwnershipAndSymlink(t *testing.T) {
	path := filepath.Join(t.TempDir(), "key")
	if err := os.WriteFile(path, bytes.Repeat([]byte{7}, 32), 0o600); err != nil {
		t.Fatal(err)
	}
	key, _, err := loadSecretsKey(path, "")
	if err != nil || len(key) != 32 {
		t.Fatal("private owned key rejected", err)
	}
	clear(key)
	link := path + "-link"
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadSecretsKey(link, ""); err == nil {
		t.Fatal("symlink key accepted")
	}
	if os.Geteuid() == 0 {
		if err := os.Chown(path, 65534, 65534); err != nil {
			t.Fatal(err)
		}
		if _, _, err := loadSecretsKey(path, ""); err == nil {
			t.Fatal("foreign-owned mode-0600 key accepted")
		}
	} else {
		t.Log("foreign ownership test requires root; symlink rejection verified")
	}
}

func TestSecureJoinAddress(t *testing.T) {
	for _, input := range []string{"node-a:8128", "https://node-a:8128/", " [::1]:8128 "} {
		base, err := secureJoinAddress(input)
		if err != nil || !strings.HasPrefix(base, "https://") || strings.HasSuffix(base, "/") {
			t.Fatalf("secureJoinAddress(%q) = %q, %v", input, base, err)
		}
	}
	for _, input := range []string{"", "http://node-a:8128", "ftp://node-a:8128", "https://user:password@node-a:8128", "https://node-a:8128/path", "https://node-a:8128?secret", "https://node-a:8128#fragment", "https://node-a:bad"} {
		if _, err := secureJoinAddress(input); err == nil {
			t.Errorf("accepted insecure or malformed address %q", input)
		}
	}
}

func TestEnrollmentRejectsHTTPWithoutSendingToken(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer server.Close()
	_, err := joinClusterTLS(context.Background(), slog.Default(), server.URL, "secret", nil, "", "", "", api.NodeRoleWorker, nil)
	if err == nil || !strings.Contains(err.Error(), "HTTPS") || requests.Load() != 0 {
		t.Fatalf("HTTP enrollment: error=%v, requests=%d", err, requests.Load())
	}
}

func TestEnrollmentRedirectSecurity(t *testing.T) {
	ca, caKey, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	cert, key, err := tlsutil.GenerateAPICert(ca, caKey)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair(cert, key)
	if err != nil {
		t.Fatal(err)
	}
	for _, status := range []int{301, 302, 307, 308} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var leaked atomic.Int32
			plain := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { leaked.Add(1) }))
			defer plain.Close()
			redirect := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer join-secret" {
					t.Error("missing bearer token at pinned TLS endpoint")
				}
				http.Redirect(w, r, plain.URL+"/v1/nodes/enroll", status)
			}))
			redirect.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
			redirect.StartTLS()
			defer redirect.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer cancel()
			if _, err := joinClusterTLS(ctx, slog.Default(), redirect.URL, "join-secret", ca, "", "", "", api.NodeRoleWorker, nil); err == nil {
				t.Fatal("accepted downgrade redirect")
			}
			if leaked.Load() != 0 {
				t.Fatalf("sent %d plaintext requests", leaked.Load())
			}
		})
	}
	// Legitimate same-CA leader redirects still work and preserve POST + token.
	leader := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer join-secret" {
			t.Error("redirect lost enrollment method or authorization")
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"role":"worker"}`))
	}))
	leader.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
	leader.StartTLS()
	defer leader.Close()
	follower := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, leader.URL+"/v1/nodes/enroll", http.StatusTemporaryRedirect)
	}))
	follower.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
	follower.StartTLS()
	defer follower.Close()
	if _, err := joinClusterTLS(context.Background(), slog.Default(), follower.URL, "join-secret", ca, "", "", "", api.NodeRoleWorker, nil); err != nil {
		t.Fatal(err)
	}
}

func TestEnrollmentJournalResumesPartialPublication(t *testing.T) {
	ca, caKey, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.New()
	cert, key, err := tlsutil.GenerateNodeCert(ca, caKey, id)
	if err != nil {
		t.Fatal(err)
	}
	for _, obstruction := range []string{"tls/node-key", "tls/role", "node-id"} {
		t.Run(obstruction, func(t *testing.T) {
			dir := t.TempDir()
			local := storage.NewLocalStorage(dir)
			pending := pendingEnrollment{NodeID: id, Role: api.NodeRoleWorker, CACert: string(ca), Cert: string(cert), Key: string(key)}
			if err := local.Put("tls/pending-enrollment", &pending); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, obstruction)
			if err := os.MkdirAll(path, 0o700); err != nil {
				t.Fatal(err)
			}
			worker := false
			cfg := &config{DataDir: dir, Join: "must-not-contact.invalid:8128", ControlPlane: &worker, SigningMode: "managed"}
			if _, _, err := loadOrBootstrapTLS(context.Background(), slog.Default(), cfg, local, uuid.New()); err == nil {
				t.Fatal("publication unexpectedly succeeded")
			}
			if err := local.Get("tls/pending-enrollment", &pending); err != nil {
				t.Fatal("lost enrollment journal", err)
			}
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "node-id"), []byte("interrupted"), 0o600); err != nil {
				t.Fatal(err)
			}
			acquired, err := acquireNodeID(dir)
			if err != nil || acquired != id {
				t.Fatalf("acquired %s, %v", acquired, err)
			}
			m, enrolled, err := loadOrBootstrapTLS(context.Background(), slog.Default(), cfg, local, acquired)
			if err != nil || enrolled != id {
				t.Fatalf("resume = %s, %v", enrolled, err)
			}
			if !bytes.Equal(m.Key, key) || !bytes.Equal(m.Cert, cert) {
				t.Fatal("resume changed enrolled key pair")
			}
			if err := local.Get("tls/pending-enrollment", &pending); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("journal retained after publication", err)
			}
			stored, err := acquireNodeID(dir)
			if err != nil || stored != id {
				t.Fatalf("published ID = %s, %v", stored, err)
			}
			info, err := os.Stat(filepath.Join(dir, "tls/node-key"))
			if err != nil || info.Mode().Perm() != 0o600 {
				t.Fatal("private key permissions", err)
			}
		})
	}
}

func TestEnrollmentPublishesJournalBeforeKeyProjection(t *testing.T) {
	ca, caKey, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	apiCert, apiKey, err := tlsutil.GenerateAPICert(ca, caKey)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair(apiCert, apiKey)
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.New()
	var enrollments atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		enrollments.Add(1)
		var request nodeapi.NodeEnrollmentRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		cert, err := tlsutil.SignNodeCSR(ca, caKey, []byte(request.CSR), id)
		if err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		w.WriteHeader(http.StatusCreated)
		if err := json.NewEncoder(w).Encode(nodeapi.NodeEnrollmentResponse{NodeID: id, Role: api.NodeRoleWorker, Cert: string(cert)}); err != nil {
			t.Error(err)
		}
	}))
	server.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
	server.StartTLS()
	defer server.Close()
	dir := t.TempDir()
	caPath := filepath.Join(dir, "pinned-ca")
	if err := os.WriteFile(caPath, ca, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "tls/node-key"), 0o700); err != nil {
		t.Fatal(err)
	}
	worker := false
	cfg := &config{DataDir: dir, Join: server.URL, JoinToken: "single-use", CACert: caPath, SigningMode: "managed", ControlPlane: &worker}
	local := storage.NewLocalStorage(dir)
	if _, _, err := loadOrBootstrapTLS(context.Background(), slog.Default(), cfg, local, uuid.New()); err == nil {
		t.Fatal("key projection unexpectedly succeeded")
	}
	var pending pendingEnrollment
	if err := local.Get("tls/pending-enrollment", &pending); err != nil {
		t.Fatal("enrollment identity not durable before projection", err)
	}
	info, err := os.Stat(filepath.Join(dir, "tls/pending-enrollment"))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatal("journal permissions", err)
	}
	if err := os.Remove(filepath.Join(dir, "tls/node-key")); err != nil {
		t.Fatal(err)
	}
	cfg.JoinToken = "" // Consumed token is not needed to resume.
	m, gotID, err := loadOrBootstrapTLS(context.Background(), slog.Default(), cfg, local, uuid.New())
	if err != nil || gotID != id {
		t.Fatalf("resume ID = %s, %v", gotID, err)
	}
	if string(m.Key) != pending.Key || enrollments.Load() != 1 {
		t.Fatal("re-enrolled or changed private key on resume")
	}
}

func TestDetectRunscRequiresContainerdShim(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("PATH", dir)
	for _, name := range []string{"runsc", "containerd-shim-runsc-v1"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
			t.Fatal(err)
		}
		got := detectNodeCapabilities()
		if name == "runsc" && len(got) != 0 {
			t.Fatal("Docker-only runsc advertised")
		}
		if name != "runsc" && (len(got) != 1 || got[0] != "runtime.runsc") {
			t.Fatalf("containerd shim capabilities = %v", got)
		}
	}
}

type restoreCountingReader struct {
	reader io.Reader
	bytes  int64
}

func (r *restoreCountingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.bytes += int64(n)
	return n, err
}

func TestRestoreAdmissionBoundsAndClient(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	a := auth.NewAdministratorAuthenticator()
	e := echo.New()
	e.Use(leaderAuthMiddleware(a, func() (ed25519.PublicKey, uint64, bool) { return publicKey, 7, true }, nil, nil))
	entered, release := make(chan struct{}, 2), make(chan struct{})
	e.POST("/v1/backup/restore", func(c *echo.Context) error {
		entered <- struct{}{}
		<-release
		return c.NoContent(http.StatusNoContent)
	})
	sign := func() *http.Request {
		value, _, err := a.Issue(7)
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/v1/backup/restore", nil)
		digest := sha256.Sum256(nil)
		req.Header.Set(adminsign.DigestHeader, hex.EncodeToString(digest[:]))
		req.Header.Set(adminsign.ChallengeHeader, value)
		req.Header.Set(adminsign.SignatureHeader, base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, adminsign.Payload(value, req.Method, req.URL.RequestURI(), nil))))
		return req
	}
	for _, name := range []string{"missing digest", "invalid signature", "forged challenge", "different process challenge", "known oversize", "stream oversize"} {
		t.Run(name, func(t *testing.T) {
			req := sign()
			reader := &restoreCountingReader{reader: endlessReader{}}
			req.Body = io.NopCloser(reader)
			want, wantBytes := http.StatusUnauthorized, int64(0)
			switch name {
			case "missing digest":
				req.Header.Del(adminsign.DigestHeader)
			case "invalid signature":
				req.Header.Set(adminsign.SignatureHeader, "invalid")
			case "forged challenge":
				req.Header.Set(adminsign.ChallengeHeader, strings.Repeat("x", 107))
			case "different process challenge":
				old := auth.NewAdministratorAuthenticator() // Different process key.
				value, _, _ := old.Issue(7)
				req.Header.Set(adminsign.ChallengeHeader, value)
			case "known oversize":
				req.ContentLength = server.MaxRestoreRequestBytes + 1
				want = http.StatusRequestEntityTooLarge
			case "stream oversize":
				req.ContentLength = -1
				want, wantBytes = http.StatusRequestEntityTooLarge, server.MaxRestoreRequestBytes+1
			}
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req)
			if rec.Code != want || reader.bytes != wantBytes {
				t.Fatalf("status %d, read %d bytes; want %d, %d: %s", rec.Code, reader.bytes, want, wantBytes, rec.Body.String())
			}
			files, err := os.ReadDir(os.Getenv("TMPDIR"))
			if err != nil || len(files) != 0 {
				t.Fatalf("spools leaked: %v, %v", files, err)
			}
		})
	}
	// Keep two admitted restores in downstream execution. The third must
	// fail without reading its body; slots cover the entire spool lifetime.
	done := make(chan *httptest.ResponseRecorder, 2)
	for range 2 {
		req := sign()
		go func() {
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req)
			done <- rec
		}()
	}
	for range 2 {
		<-entered
	}
	req := sign()
	reader := &restoreCountingReader{reader: endlessReader{}}
	req.Body = io.NopCloser(reader)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	close(release)
	for range 2 {
		if result := <-done; result.Code != http.StatusNoContent {
			t.Fatalf("admitted restore status = %d", result.Code)
		}
	}
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "1" || reader.bytes != 0 {
		t.Fatalf("overload status %d, read %d bytes: %s", rec.Code, reader.bytes, rec.Body.String())
	}
	// Exercise the public client against real authentication after releasing
	// capacity: it must send the signed digest automatically.
	httpServer := httptest.NewServer(e)
	defer httpServer.Close()
	c, err := client.New(client.Config{Address: httpServer.URL, AdministratorKey: privateKey})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.RestoreBackup(t.Context(), &api.BackupSnapshot{}); err != nil {
		t.Fatal(err)
	}
	files, err := os.ReadDir(os.Getenv("TMPDIR"))
	if err != nil || len(files) != 0 {
		t.Fatalf("spools leaked: %v, %v", files, err)
	}
}
