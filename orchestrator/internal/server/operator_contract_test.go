package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/overfold/trellis/orchestrator/api"
	publicclient "github.com/overfold/trellis/orchestrator/client"
	agentclient "github.com/overfold/trellis/orchestrator/internal/client"
	"github.com/overfold/trellis/orchestrator/internal/execstream"
	"github.com/overfold/trellis/orchestrator/internal/spec"
	"github.com/overfold/trellis/orchestrator/internal/state"
	"github.com/overfold/trellis/orchestrator/internal/storage"
	"github.com/overfold/trellis/orchestrator/internal/tlsutil"
)

// Authentication is injected here so these tests exercise public requests and
// client error types without duplicating the administrator signing tests.
func operatorContractClient(t *testing.T, s *Server) *publicclient.Client {
	t.Helper()
	e := echo.New()
	e.Pre(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			ctx := context.WithValue(c.Request().Context(), AdminContextKey, true)
			c.SetRequest(c.Request().WithContext(ctx))
			return next(c)
		}
	})
	NewHandler(s).Register(e)
	server := httptest.NewServer(e)
	t.Cleanup(server.Close)
	c, err := publicclient.New(publicclient.Config{Address: server.URL, Namespace: "team"})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func requirePublicStatus(t *testing.T, err error, status int) string {
	t.Helper()
	var response *publicclient.HTTPError
	if !errors.As(err, &response) || response.Status != status {
		t.Fatalf("error = %v, want public HTTP status %d", err, status)
	}
	return response.Message()
}

type identityFaultStore struct {
	*auditStore
	fail        atomic.Bool
	readKey     string
	commit      bool
	err         error
	afterCommit func()
}

func (s *identityFaultStore) Get(ctx context.Context, key string) ([]byte, error) {
	if s.fail.Load() && key == s.readKey {
		return nil, s.err
	}
	return s.auditStore.Get(ctx, key)
}

func (s *identityFaultStore) Batch(ctx context.Context, changes []state.Mutation) error {
	if s.fail.Load() && s.readKey == "" {
		if s.commit {
			if err := s.auditStore.Batch(ctx, changes); err != nil {
				return err
			}
			if s.afterCommit != nil {
				s.afterCommit()
			}
		}
		return s.err
	}
	return s.auditStore.Batch(ctx, changes)
}

func TestExternalEnrollmentInfrastructureFailures(t *testing.T) {
	for _, stage := range []string{"CA certificate", "CA key", "tombstone read", "fingerprint read", "role read", "commit", "ambiguous commit", "cancellation"} {
		t.Run(stage, func(t *testing.T) {
			f := newNodeTrustFixture(t)
			if err := f.server.storage.Delete("tls/ca-key"); err != nil {
				t.Fatal(err)
			}
			id := uuid.New()
			cert := f.certificateFor(id)
			request := &api.NodeIdentityCreateRequest{Certificate: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})), Role: api.NodeRoleWorker}
			store := &identityFaultStore{auditStore: &auditStore{memoryStore: f.store}, err: errors.New("storage unavailable")}
			f.server.state = NewStateController(store, "test")
			switch stage {
			case "tombstone read":
				store.readKey = f.server.state.nodeTombstoneKey(id.String())
			case "fingerprint read":
				store.readKey = f.server.state.nodeCertificateFingerprintKey(id.String())
			case "role read":
				if err := f.server.BindNodeCertificateRole(t.Context(), id, cert, request.Role); err != nil {
					t.Fatal(err)
				}
				store.readKey = f.server.state.nodeRoleKey(id.String())
			case "ambiguous commit":
				store.commit = true
			case "cancellation":
				store.err = context.Canceled
			}
			client := operatorContractClient(t, f.server)
			var brokenCAPath string
			if strings.HasPrefix(stage, "CA ") {
				root := t.TempDir()
				f.server.storage = storage.NewLocalStorage(root)
				if err := f.server.storage.Put("tls/ca-cert", string(f.caCert)); err != nil {
					t.Fatal(err)
				}
				key := "tls/ca-cert"
				if stage == "CA key" {
					key = "tls/ca-key"
				}
				// A directory where a stored JSON file belongs fails the actual read.
				if err := f.server.storage.Delete(key); err != nil && !os.IsNotExist(err) {
					t.Fatal(err)
				}
				brokenCAPath = filepath.Join(root, key)
				if err := os.Mkdir(brokenCAPath, 0o700); err != nil {
					t.Fatal(err)
				}
			} else {
				store.fail.Store(true)
			}
			_, err := client.EnrollNodeIdentity(t.Context(), request)
			requirePublicStatus(t, err, http.StatusServiceUnavailable)
			store.fail.Store(false)
			bound := f.server.AuthorizeNodeCertificate(t.Context(), id, cert)
			if want := stage == "role read" || stage == "ambiguous commit"; bound != want {
				t.Fatalf("binding after failure = %t, want %t", bound, want)
			}
			if strings.HasPrefix(stage, "CA ") {
				if err := os.Remove(brokenCAPath); err != nil {
					t.Fatal(err)
				}
				if err := f.server.storage.Put("tls/ca-cert", string(f.caCert)); err != nil {
					t.Fatal(err)
				}
			}
			for range 2 {
				response, err := client.EnrollNodeIdentity(t.Context(), request)
				if err != nil || response.ID != id {
					t.Fatalf("idempotent retry: %v, %v", response, err)
				}
			}
			request.Role = "invalid"
			_, err = client.EnrollNodeIdentity(t.Context(), request)
			requirePublicStatus(t, err, http.StatusBadRequest)
			request.Role = api.NodeRoleControlPlane
			_, err = client.EnrollNodeIdentity(t.Context(), request)
			requirePublicStatus(t, err, http.StatusBadRequest)
			other := f.certificateFor(id)
			request.Certificate = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: other.Raw}))
			_, err = client.EnrollNodeIdentity(t.Context(), request)
			requirePublicStatus(t, err, http.StatusBadRequest)
			request.Certificate = "not a certificate"
			_, err = client.EnrollNodeIdentity(t.Context(), request)
			requirePublicStatus(t, err, http.StatusBadRequest)
			if !f.server.AuthorizeNodeCertificate(t.Context(), id, cert) {
				t.Fatal("invalid enrollment replaced the existing binding")
			}
		})
	}
}

type membershipReadFault struct {
	*fakeMembership
	fail atomic.Bool
}

func (m *membershipReadFault) Membership() ([]state.RaftMember, error) {
	if m.fail.Load() {
		return nil, errors.New("membership unavailable")
	}
	return m.fakeMembership.Membership()
}

func TestRemovalInfrastructureFailuresAndRetry(t *testing.T) {
	for _, stage := range []string{"membership read", "tombstone commit", "ambiguous tombstone", "Raft removal", "term cancellation"} {
		t.Run(stage, func(t *testing.T) {
			leader, target := uuid.New(), uuid.New()
			joiner := &membershipReadFault{fakeMembership: newFakeMembership(fakeMember(leader, true), fakeMember(target, false))}
			store := &identityFaultStore{auditStore: &auditStore{memoryStore: memoryStore{}}, err: errors.New("commit unavailable")}
			s := &Server{joiner: joiner, nodeID: leader, state: NewStateController(store, "test"), now: time.Now}
			cert := newNodeTrustFixture(t).certificateFor(target)
			if err := s.BindNodeCertificate(t.Context(), target, cert); err != nil {
				t.Fatal(err)
			}
			switch stage {
			case "membership read":
				joiner.fail.Store(true)
			case "tombstone commit", "ambiguous tombstone":
				store.commit = stage == "ambiguous tombstone"
				store.fail.Store(true)
			case "Raft removal":
				joiner.err = errors.New("not the leader")
			case "term cancellation":
				term, cancel := context.WithCancel(t.Context())
				t.Cleanup(cancel)
				s.term = term
				store.commit, store.err, store.afterCommit = true, nil, cancel
				store.fail.Store(true)
			}
			client := operatorContractClient(t, s)
			err := client.RemoveRaftMember(t.Context(), target.String())
			message := requirePublicStatus(t, err, http.StatusServiceUnavailable)
			if (stage == "Raft removal" || stage == "term cancellation") && (!strings.Contains(message, "tombstoned") || !strings.Contains(message, "retry")) {
				t.Fatalf("missing partial-removal diagnostic: %s", message)
			}
			store.fail.Store(false)
			joiner.fail.Store(false)
			joiner.err = nil
			s.termMu.Lock()
			s.term = nil
			s.termMu.Unlock()
			if stage == "term cancellation" {
				// A new serving term uses a fresh connection, not the HTTP
				// connection whose reads the canceled term interrupted.
				client = operatorContractClient(t, s)
			}
			removed, err := s.state.NodeRemoved(t.Context(), target.String())
			wantRemoved := stage == "Raft removal" || stage == "ambiguous tombstone" || stage == "term cancellation"
			if err != nil || removed != wantRemoved || s.AuthorizeNodeCertificate(t.Context(), target, cert) == wantRemoved {
				t.Fatalf("removed=%t, err=%v; want removed and revoked=%t", removed, err, wantRemoved)
			}
			for range 2 {
				if err := client.RemoveRaftMember(t.Context(), target.String()); err != nil {
					t.Fatalf("safe retry: %v", err)
				}
			}
			members, err := joiner.Membership()
			if err != nil || len(members) != 1 || members[0].ID != leader.String() || s.AuthorizeNodeCertificate(t.Context(), target, cert) {
				t.Fatalf("retry did not complete removal and revocation: %v, %v", members, err)
			}
		})
	}
}

func TestScopedAgentAddressPreservesZone(t *testing.T) {
	s := newExecTestServer(t)
	s.allocations = []*Allocation{{Namespace: "team", ID: "scoped", Node: &Node{ID: uuid.New(), Host: "fe80::1%lo", Port: 8127}}}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	// There is no link-local peer on loopback. The failed dial must still
	// address the requested zone, not fail URL parsing or silently drop it.
	_, err := s.AllocationMetrics(ctx, "team", "scoped")
	var dial *net.OpError
	if !errors.As(err, &dial) {
		t.Fatalf("expected scoped dial failure, got %v", err)
	}
	address, ok := dial.Addr.(*net.TCPAddr)
	if !ok || address.Zone != "lo" || !address.IP.Equal(net.ParseIP("fe80::1")) || address.Port != 8127 {
		t.Fatalf("dial target = %v; want [fe80::1%%lo]:8127", dial.Addr)
	}
}

func TestOperatorRelaysAgentAddresses(t *testing.T) {
	for _, host := range []string{"127.0.0.1", "localhost", "::1", "::1%lo"} {
		t.Run(host, func(t *testing.T) {
			listen := "127.0.0.1:0"
			if strings.Contains(host, ":") {
				listen = "[::1]:0"
			}
			listener, err := net.Listen("tcp", listen)
			if err != nil {
				t.Fatal(err)
			}
			id := uuid.New()
			ca, key, err := tlsutil.GenerateCA()
			if err != nil {
				t.Fatal(err)
			}
			certPEM, keyPEM, err := tlsutil.GenerateNodeCert(ca, key, id)
			if err != nil {
				t.Fatal(err)
			}
			cert, err := tls.X509KeyPair(certPEM, keyPEM)
			if err != nil {
				t.Fatal(err)
			}
			agent := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/allocations/alloc-1/exec":
					conn, err := execstream.Accept(w)
					if err != nil {
						t.Error(err)
						return
					}
					defer func() { _ = conn.Close() }()
					writer := execstream.NewWriter(conn, 0)
					_ = writer.WriteData(execstream.FrameStdout, []byte("IPv6 exec output"))
					_ = writer.WriteJSON(execstream.FrameExit, api.ExecExit{ExitCode: 7})
				case "/v1/allocations/alloc-1/metrics":
					w.Header().Set("Content-Type", "application/json")
					_, _ = io.WriteString(w, `[{"task":"web","cpu_usage_nanoseconds":123,"memory_usage_bytes":456}]`)
				case "/v1/allocations/alloc-1/logs":
					_, _ = io.WriteString(w, "agent logs\n")
				default:
					http.NotFound(w, r)
				}
			}))
			_ = agent.Listener.Close()
			agent.Listener = listener
			agent.TLS = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}
			agent.StartTLS()
			t.Cleanup(agent.Close)
			roots := x509.NewCertPool()
			roots.AppendCertsFromPEM(ca)
			s := newExecTestServer(t)
			s.client = agentclient.NewAgentClient("", &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS13})
			_, portText, _ := net.SplitHostPort(listener.Addr().String())
			port, _ := strconv.Atoi(portText)
			s.allocations = []*Allocation{{Namespace: "team", ID: "alloc-1", Tasks: []spec.TaskSpec{{Name: "web"}}, Node: &Node{ID: id, Host: host, Port: port}}}
			client := operatorContractClient(t, s)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			t.Run("exec", func(t *testing.T) {
				stream, err := client.Exec(ctx, "alloc-1", api.ExecRequest{Command: []string{"true"}})
				if err != nil {
					t.Fatal(err)
				}
				var output strings.Builder
				code, err := stream.Wait(&output, io.Discard)
				_ = stream.Close()
				if err != nil || code != 7 || output.String() != "IPv6 exec output" {
					t.Fatalf("exec: exit=%d, output=%q, err=%v", code, output.String(), err)
				}
			})
			t.Run("metrics", func(t *testing.T) {
				metrics, err := client.AllocationMetrics(ctx, "alloc-1")
				if err != nil || len(metrics) != 1 || metrics[0].CPUUsageNanoseconds != 123 || metrics[0].MemoryUsageBytes != 456 {
					t.Fatalf("metrics: %v, %v", metrics, err)
				}
			})
			for _, task := range []string{"", "web"} {
				t.Run("logs/task="+task, func(t *testing.T) {
					logs, err := client.AllocationLogs(ctx, "alloc-1", publicclient.LogOptions{Task: task})
					if err != nil {
						t.Fatal(err)
					}
					body, err := io.ReadAll(logs)
					_ = logs.Close()
					if err != nil || string(body) != "agent logs\n" {
						t.Fatalf("logs: %q, %v", body, err)
					}
				})
			}
			t.Run("allocation logs", func(t *testing.T) {
				logs, err := s.AllocationLogsForNamespace(ctx, "team", "alloc-1", false, 0)
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(logs)
				_ = logs.Close()
				if err != nil || string(body) != "agent logs\n" {
					t.Fatalf("allocation logs: %q, %v", body, err)
				}
			})
		})
	}
}
