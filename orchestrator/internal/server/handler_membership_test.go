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
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/overfold/trellis/internal/api"
	"github.com/overfold/trellis/internal/state"
	"github.com/overfold/trellis/internal/storage"
	"github.com/overfold/trellis/internal/tlsutil"
)

func TestHandleRaftMemberRemove(t *testing.T) {
	leader, removed := uuid.New(), uuid.New()
	joiner := newFakeMembership(fakeMember(leader, true), fakeMember(removed, false))
	control := &Server{joiner: joiner, nodeID: leader, now: time.Now, state: NewStateController(memoryStore{}, "test")}
	e := echo.New()
	NewHandler(control).Register(e)

	req := httptest.NewRequest(http.MethodDelete, "/v1/raft/members/"+removed.String(), nil)
	req = req.WithContext(context.WithValue(req.Context(), AdminContextKey, true))
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusNoContent, rec.Body.String())
	}
	if got := joiner.operations(); len(got) != 1 || got[0] != "remove "+removed.String() {
		t.Fatalf("operations = %v, want [remove %s]", got, removed)
	}
	if gone, err := control.state.NodeRemoved(context.Background(), removed.String()); err != nil || !gone {
		t.Fatalf("removed node tombstone = %v, %v; want recorded", gone, err)
	}
}

func TestHandleRaftMemberRemoveRejectsNonUUID(t *testing.T) {
	leader := uuid.New()
	joiner := newFakeMembership(fakeMember(leader, true), state.RaftMember{ID: "node-2", Address: "node-2:8129"})
	control := &Server{joiner: joiner, nodeID: leader, now: time.Now, state: NewStateController(memoryStore{}, "test")}
	e := echo.New()
	NewHandler(control).Register(e)

	req := httptest.NewRequest(http.MethodDelete, "/v1/raft/members/node-2", nil)
	req = req.WithContext(context.WithValue(req.Context(), AdminContextKey, true))
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if got := joiner.operations(); len(got) != 0 {
		t.Fatalf("operations = %v, want none", got)
	}
}

func TestHandleRaftMemberRemoveFailure(t *testing.T) {
	leader, removed := uuid.New(), uuid.New()
	joiner := newFakeMembership(fakeMember(leader, true), fakeMember(removed, false))
	joiner.err = errors.New("not the leader")
	control := &Server{joiner: joiner, nodeID: leader, now: time.Now, state: NewStateController(memoryStore{}, "test")}
	e := echo.New()
	NewHandler(control).Register(e)

	req := httptest.NewRequest(http.MethodDelete, "/v1/raft/members/"+removed.String(), nil)
	req = req.WithContext(context.WithValue(req.Context(), AdminContextKey, true))
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusInternalServerError, rec.Body.String())
	}
}

func TestHandleRaftMemberRemoveRefusesQuorumLoss(t *testing.T) {
	leader, live, silent := uuid.New(), uuid.New(), uuid.New()
	now := time.Now()
	joiner := newFakeMembership(fakeMember(leader, true), fakeMember(live, true), fakeMember(silent, true))
	control := &Server{joiner: joiner, nodeID: leader, now: func() time.Time { return now }, state: NewStateController(memoryStore{}, "test"), nodes: map[uuid.UUID]*Node{
		live:   {ID: live, Status: NodeStatusHealthy, LastHeartbeat: now},
		silent: {ID: silent, Status: NodeStatusUnhealthy, LastHeartbeat: now.Add(-time.Hour)},
	}}
	e := echo.New()
	NewHandler(control).Register(e)

	req := httptest.NewRequest(http.MethodDelete, "/v1/raft/members/"+live.String(), nil)
	req = req.WithContext(context.WithValue(req.Context(), AdminContextKey, true))
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusConflict, rec.Body.String())
	}
	if got := joiner.operations(); len(got) != 0 {
		t.Fatalf("unsafe removal changed membership: %v", got)
	}
	if gone, _ := control.state.NodeRemoved(context.Background(), live.String()); gone {
		t.Fatal("refused removal recorded a tombstone")
	}
}

func TestHandleRaftMemberRemoveRequiresClusterAuthorization(t *testing.T) {
	leader := uuid.New()
	joiner := newFakeMembership(fakeMember(leader, true), state.RaftMember{ID: "node-2", Address: "node-2:8129"})
	control := &Server{joiner: joiner, nodeID: leader, now: time.Now}
	e := echo.New()
	NewHandler(control).Register(e)

	req := httptest.NewRequest(http.MethodDelete, "/v1/raft/members/node-2", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusForbidden, rec.Body.String())
	}
	if got := joiner.operations(); len(got) != 0 {
		t.Fatalf("unexpected membership changes %v", got)
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
	joiner := newFakeMembership(fakeMember(uuid.New(), true))
	store := memoryStore{}
	control := &Server{storage: local, joiner: joiner, state: NewStateController(store, "test")}
	e := echo.New()
	NewHandler(control).Register(e)
	join := func() *httptest.ResponseRecorder {
		body, _ := json.Marshal(api.RaftJoinRequest{RaftAddress: "node-b:8129", ServerAddress: "node-b:8128"})
		req := httptest.NewRequest(http.MethodPost, "/v1/raft/join", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{certificate}}
		req = req.WithContext(context.WithValue(req.Context(), NodeContextKey, nodeID))
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		return rec
	}
	// In managed mode a CA-signed certificate for an identity that never
	// enrolled with a join token cannot join.
	if rec := join(); rec.Code != http.StatusForbidden {
		t.Fatalf("unenrolled join status = %d, want 403; body: %s", rec.Code, rec.Body.String())
	}
	if got := joiner.operations(); len(got) != 0 {
		t.Fatalf("unenrolled join changed membership: %v", got)
	}
	if err := control.BindNodeCertificate(context.Background(), nodeID, certificate); err != nil {
		t.Fatal(err)
	}
	rec := join()
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var joinResponse api.RaftJoinResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &joinResponse); err != nil || joinResponse.CAKey != string(caKey) {
		t.Fatalf("join response did not return managed CA key after admission: %v", err)
	}
	if len(joinResponse.Members) != 2 || !slices.Contains(joinResponse.Members, nodeID.String()) {
		t.Fatalf("join response members = %v, want the admitting configuration including the joiner", joinResponse.Members)
	}
	if got := joiner.operations(); len(got) != 1 || got[0] != "add-nonvoter "+nodeID.String()+" node-b:8129" {
		t.Fatalf("join operations = %v, want the node added as a non-voter", got)
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
	control := &Server{storage: local, state: NewStateController(memoryStore{}, "test"), now: time.Now}
	token, _, err := control.CreateJoinToken(context.Background(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	response, err := control.EnrollNode(context.Background(), token, "node-b:8128", "node-b:8127")
	if err != nil {
		t.Fatal(err)
	}
	m := &tlsutil.Materials{CACert: []byte(response.CACert), Cert: []byte(response.Cert), Key: []byte(response.Key)}
	if err := tlsutil.ValidateMaterials(m, response.NodeID); err != nil {
		t.Fatalf("enrolled certificate: %v", err)
	}
	second, err := control.EnrollNode(context.Background(), token, "node-b:8128", "node-b:8127")
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
	control := &Server{storage: local, state: NewStateController(memoryStore{}, "test"), nodeID: leaderID, now: time.Now}
	token, _, err := control.CreateJoinToken(context.Background(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	e := echo.New()
	NewHandler(control).Register(e)
	enroll := func(body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/nodes/enroll", bytes.NewReader([]byte(body)))
		req.Header.Set("Content-Type", "application/json")
		req = req.WithContext(context.WithValue(req.Context(), JoinTokenContextKey, token))
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		return rec
	}
	// The enrollment request has no identity field, so asking for one is
	// rejected rather than ignored.
	if rec := enroll(`{"node_id":"` + leaderID.String() + `","server_advertise":"node-b:8128","agent_advertise":"node-b:8127","raft_advertise":"node-b:8129"}`); rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "node_id") {
		t.Fatalf("identity request status = %d, want 400 naming node_id; body: %s", rec.Code, rec.Body.String())
	}
	rec := enroll(`{"server_advertise":"node-b:8128","agent_advertise":"node-b:8127","raft_advertise":"node-b:8129"}`)
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
	control := &Server{storage: local, state: NewStateController(memoryStore{}, "test"), now: time.Now}
	token, _, err := control.CreateJoinToken(context.Background(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := control.EnrollNode(context.Background(), token); err == nil {
		t.Fatal("external-signing node unexpectedly enrolled another node")
	}
}

func TestHandleListNodesShowsControlPlaneMembership(t *testing.T) {
	leader, nonvoter, removed := uuid.New(), uuid.New(), uuid.New()
	joiner := newFakeMembership(fakeMember(leader, true), fakeMember(nonvoter, false))
	control := &Server{joiner: joiner, nodeID: leader, now: time.Now, nodes: map[uuid.UUID]*Node{
		leader: {ID: leader}, nonvoter: {ID: nonvoter}, removed: {ID: removed},
	}}
	e := echo.New()
	NewHandler(control).Register(e)
	req := httptest.NewRequest(http.MethodGet, "/v1/nodes", nil)
	req = req.WithContext(context.WithValue(req.Context(), AdminContextKey, true))
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; body: %s", rec.Code, rec.Body.String())
	}
	var nodes api.NodeListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &nodes); err != nil {
		t.Fatal(err)
	}
	want := map[uuid.UUID]api.ControlPlaneMembership{leader: api.ControlPlaneVoter, nonvoter: api.ControlPlaneNonvoter, removed: ""}
	for _, node := range nodes {
		if node.ControlPlane != want[node.ID] {
			t.Errorf("node %s control plane = %q, want %q", node.ID, node.ControlPlane, want[node.ID])
		}
	}
	if len(nodes) != len(want) {
		t.Fatalf("listed %d nodes, want %d", len(nodes), len(want))
	}
}
