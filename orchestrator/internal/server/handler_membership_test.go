package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/storage"
	"github.com/clofour/trellis/internal/tlsutil"
	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

type recordingClusterJoiner struct {
	removedID string
	addedID   string
	addedAddr string
	err       error
}

func (j *recordingClusterJoiner) AddVoter(id, address string) error {
	j.addedID, j.addedAddr = id, address
	return j.err
}
func (*recordingClusterJoiner) LeadershipTransfer() error { return nil }

func (j *recordingClusterJoiner) RemoveServer(id string) error {
	j.removedID = id
	return j.err
}

func TestHandleRaftMemberRemove(t *testing.T) {
	joiner := &recordingClusterJoiner{}
	control := &Server{joiner: joiner}
	e := echo.New()
	NewHandler(control).Register(e)

	req := httptest.NewRequest(http.MethodDelete, "/v1/raft/members/node-2.example:8128", nil)
	req = req.WithContext(context.WithValue(req.Context(), AdminContextKey, true))
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusNoContent, rec.Body.String())
	}
	if joiner.removedID != "node-2.example:8128" {
		t.Fatalf("removed ID = %q, want %q", joiner.removedID, "node-2.example:8128")
	}
}

func TestHandleRaftMemberRemoveFailure(t *testing.T) {
	joiner := &recordingClusterJoiner{err: errors.New("not the leader")}
	control := &Server{joiner: joiner}
	e := echo.New()
	NewHandler(control).Register(e)

	req := httptest.NewRequest(http.MethodDelete, "/v1/raft/members/node-2", nil)
	req = req.WithContext(context.WithValue(req.Context(), AdminContextKey, true))
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusInternalServerError, rec.Body.String())
	}
}

func TestHandleRaftMemberRemoveRequiresClusterAuthorization(t *testing.T) {
	joiner := &recordingClusterJoiner{}
	control := &Server{joiner: joiner}
	e := echo.New()
	NewHandler(control).Register(e)

	req := httptest.NewRequest(http.MethodDelete, "/v1/raft/members/node-2", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusForbidden, rec.Body.String())
	}
	if joiner.removedID != "" {
		t.Fatalf("unexpected removal of %q", joiner.removedID)
	}
}

func TestHandleRaftJoinBindsMembershipToCertificateIdentity(t *testing.T) {
	caCert, caKey, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	nodeID := uuid.New()
	certPEM, _, err := tlsutil.GenerateNodeCert(caCert, caKey, nodeID, "node-b")
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(certPEM)
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	local := storage.NewLocalStorage(t.TempDir())
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	if err := local.Put("tls/ca-cert", string(caCert)); err != nil {
		t.Fatal(err)
	}
	if err := local.Put("tls/ca-key", string(caKey)); err != nil {
		t.Fatal(err)
	}
	joiner := &recordingClusterJoiner{}
	store := memoryStore{}
	control := &Server{storage: local, joiner: joiner, state: NewStateController(store, "test")}
	e := echo.New()
	NewHandler(control).Register(e)
	body, _ := json.Marshal(api.RaftJoinRequest{RaftAddress: "node-b:8129", ServerAddress: "node-b:8128"})
	req := httptest.NewRequest(http.MethodPost, "/v1/raft/join", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{certificate}}
	req = req.WithContext(context.WithValue(req.Context(), NodeContextKey, nodeID))
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var joinResponse api.RaftJoinResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &joinResponse); err != nil || joinResponse.CAKey != string(caKey) {
		t.Fatalf("join response did not return managed CA key after admission: %v", err)
	}
	if joiner.addedID != nodeID.String() || joiner.addedAddr != "node-b:8129" {
		t.Fatalf("added voter = (%q, %q), want (%q, %q)", joiner.addedID, joiner.addedAddr, nodeID, "node-b:8129")
	}
	address, err := control.NodeServerAddress(context.Background(), nodeID.String())
	if err != nil || address != "node-b:8128" {
		t.Fatalf("stored server address = %q, %v", address, err)
	}
}

func TestManagedNodeEnrollmentIssuesCertificateForServerAssignedIdentity(t *testing.T) {
	local := storage.NewLocalStorage(t.TempDir())
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	caCert, caKey, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	if err := local.Put("tls/ca-cert", string(caCert)); err != nil {
		t.Fatal(err)
	}
	if err := local.Put("tls/ca-key", string(caKey)); err != nil {
		t.Fatal(err)
	}
	control := &Server{storage: local, state: NewStateController(memoryStore{}, "test")}
	response, err := control.EnrollNode(context.Background(), "node-b:8128", "node-b:8127")
	if err != nil {
		t.Fatal(err)
	}
	m := &tlsutil.Materials{CACert: []byte(response.CACert), Cert: []byte(response.Cert), Key: []byte(response.Key)}
	if err := tlsutil.ValidateMaterials(m, response.NodeID); err != nil {
		t.Fatalf("enrolled certificate: %v", err)
	}
	second, err := control.EnrollNode(context.Background(), "node-b:8128", "node-b:8127")
	if err != nil {
		t.Fatal(err)
	}
	if response.NodeID == uuid.Nil || second.NodeID == uuid.Nil || response.NodeID == second.NodeID {
		t.Fatalf("enrollment identities = %s and %s; want distinct server-assigned UUIDs", response.NodeID, second.NodeID)
	}
}

func TestManagedEnrollmentCannotRequestExistingIdentityOrReceiveCAKey(t *testing.T) {
	local := storage.NewLocalStorage(t.TempDir())
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	caCert, caKey, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	if err := local.Put("tls/ca-cert", string(caCert)); err != nil {
		t.Fatal(err)
	}
	if err := local.Put("tls/ca-key", string(caKey)); err != nil {
		t.Fatal(err)
	}
	leaderID := uuid.New()
	control := &Server{storage: local, state: NewStateController(memoryStore{}, "test"), nodeID: leaderID}
	e := echo.New()
	NewHandler(control).Register(e)
	body := []byte(`{"node_id":"` + leaderID.String() + `","server_advertise":"node-b:8128","agent_advertise":"node-b:8127","raft_advertise":"node-b:8129"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/nodes/enroll", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(context.WithValue(req.Context(), EnrollmentContextKey, true))
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusCreated, rec.Body.String())
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	if _, ok := raw["ca_key"]; ok {
		t.Fatal("enrollment returned the managed CA key before Raft admission")
	}
	var response api.NodeEnrollmentResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.NodeID == uuid.Nil || response.NodeID == leaderID {
		t.Fatalf("server-assigned identity = %s, leader identity = %s", response.NodeID, leaderID)
	}
}

func TestNodeCertificateBindingRejectsDuplicateCertificateForLeaderUUID(t *testing.T) {
	caCert, caKey, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	leaderID := uuid.New()
	parse := func(certPEM []byte) *x509.Certificate {
		t.Helper()
		block, _ := pem.Decode(certPEM)
		certificate, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			t.Fatal(err)
		}
		return certificate
	}
	firstPEM, _, err := tlsutil.GenerateNodeCert(caCert, caKey, leaderID, "leader.example")
	if err != nil {
		t.Fatal(err)
	}
	duplicatePEM, _, err := tlsutil.GenerateNodeCert(caCert, caKey, leaderID, "leader.example")
	if err != nil {
		t.Fatal(err)
	}
	first, duplicate := parse(firstPEM), parse(duplicatePEM)
	control := &Server{state: NewStateController(memoryStore{}, "test")}
	if err := control.BindNodeCertificate(context.Background(), leaderID, first); err != nil {
		t.Fatal(err)
	}
	if err := control.BindNodeCertificate(context.Background(), leaderID, duplicate); err == nil {
		t.Fatal("duplicate certificate replaced the leader UUID binding")
	}
	if !control.AuthorizeNodeCertificate(context.Background(), leaderID, first) {
		t.Fatal("bound leader certificate was rejected")
	}
	if control.AuthorizeNodeCertificate(context.Background(), leaderID, duplicate) {
		t.Fatal("duplicate leader certificate was authorized")
	}
}

func TestExternalSigningCannotEnrollWithoutCAKey(t *testing.T) {
	local := storage.NewLocalStorage(t.TempDir())
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	caCert, _, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	if err := local.Put("tls/ca-cert", string(caCert)); err != nil {
		t.Fatal(err)
	}
	if _, err := (&Server{storage: local}).EnrollNode(context.Background()); err == nil {
		t.Fatal("external-signing node unexpectedly enrolled another node")
	}
}
