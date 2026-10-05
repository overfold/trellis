package server

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/internal/state"
	"github.com/overfold/trellis/orchestrator/internal/storage"
	"github.com/overfold/trellis/orchestrator/internal/tlsutil"
)

type nodeTrustFixture struct {
	server         *Server
	store          memoryStore
	now            time.Time
	caCert, caKey  []byte
	certificateFor func(id uuid.UUID) *x509.Certificate
}

func newNodeTrustFixture(t *testing.T) *nodeTrustFixture {
	t.Helper()
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
	f := &nodeTrustFixture{store: memoryStore{}, now: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC), caCert: caCert, caKey: caKey}
	// Enrollment reads tokens before taking the mutation lock, so the fixture
	// needs a store that synchronizes reads with concurrent batch writes.
	f.server = &Server{storage: local, state: NewStateController(&auditStore{memoryStore: f.store}, "test"), now: func() time.Time { return f.now }}
	f.certificateFor = func(id uuid.UUID) *x509.Certificate {
		t.Helper()
		certPEM, _, err := tlsutil.GenerateNodeCert(caCert, caKey, id)
		if err != nil {
			t.Fatal(err)
		}
		return parseCertificatePEM(t, []byte(certPEM))
	}
	return f
}

func parseCertificatePEM(t *testing.T, certPEM []byte) *x509.Certificate {
	t.Helper()
	block, _ := pem.Decode(certPEM)
	if block == nil {
		t.Fatal("certificate PEM did not decode")
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return certificate
}

func testCSR(t *testing.T) (string, string) {
	t.Helper()
	csr, key, err := tlsutil.GenerateCSR()
	if err != nil {
		t.Fatal(err)
	}
	return string(csr), string(key)
}

func TestJoinTokenBounds(t *testing.T) {
	f := newNodeTrustFixture(t)
	ctx := context.Background()
	for _, tc := range []struct {
		ttl     time.Duration
		maxUses int
	}{
		{ttl: -time.Second},
		{ttl: time.Millisecond},
		{ttl: MaxJoinTokenTTL + time.Second},
		{maxUses: -1},
		{maxUses: MaxJoinTokenUses + 1},
	} {
		if _, _, err := f.server.CreateJoinToken(ctx, tc.ttl, tc.maxUses); !errors.Is(err, ErrInvalidJoinTokenRequest) {
			t.Errorf("CreateJoinToken(%s, %d) error = %v, want ErrInvalidJoinTokenRequest", tc.ttl, tc.maxUses, err)
		}
	}
	token, record, err := f.server.CreateJoinToken(ctx, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !record.ExpiresAt.Equal(f.now.Add(DefaultJoinTokenTTL)) {
		t.Fatalf("default expiry = %s, want %s", record.ExpiresAt, f.now.Add(DefaultJoinTokenTTL))
	}
	if id, ok := parseJoinTokenID(token); !ok || id != record.ID {
		t.Fatalf("token %q does not embed its ID %s", token, record.ID)
	}
	for key, value := range f.store {
		if strings.Contains(string(value), token) || strings.Contains(key, token) {
			t.Fatal("replicated state contains the join token itself")
		}
	}
	if _, _, err := f.server.CreateJoinToken(ctx, time.Hour, 2, api.NodeRoleControlPlane); !errors.Is(err, ErrInvalidJoinTokenRequest) {
		t.Fatalf("repeatable control-plane token error = %v, want ErrInvalidJoinTokenRequest", err)
	}
}

func TestWorkerEnrollmentIgnoresRequestedNamesAndReturnsNoPrivateKey(t *testing.T) {
	f := newNodeTrustFixture(t)
	ctx := context.Background()
	token, _, err := f.server.CreateJoinToken(ctx, time.Hour, 1, api.NodeRoleWorker)
	if err != nil {
		t.Fatal(err)
	}
	csr, _ := testCSR(t)
	response, err := f.server.EnrollNodeCSR(ctx, token, csr, api.NodeRoleWorker)
	if err != nil {
		t.Fatal(err)
	}
	certificate := parseCertificatePEM(t, []byte(response.Cert))
	if len(certificate.DNSNames) != 1 || certificate.DNSNames[0] != response.NodeID.String()+".node.trellis" {
		t.Fatalf("certificate names = %v, want only assigned UUID identity", certificate.DNSNames)
	}
	raw, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if _, found := fields["key"]; found {
		t.Fatal("worker enrollment returned a private key")
	}
}

func TestWorkerTokenCannotEnrollControlPlaneAndIsNotConsumed(t *testing.T) {
	f := newNodeTrustFixture(t)
	ctx := context.Background()
	token, record, err := f.server.CreateJoinToken(ctx, time.Hour, 1, api.NodeRoleWorker)
	if err != nil {
		t.Fatal(err)
	}
	csr, _ := testCSR(t)
	if _, err := f.server.EnrollNodeCSR(ctx, token, csr, api.NodeRoleControlPlane); !errors.Is(err, ErrInvalidJoinToken) {
		t.Fatalf("role escalation error = %v, want ErrInvalidJoinToken", err)
	}
	stored, found, err := f.server.state.GetJoinToken(ctx, record.ID)
	if err != nil || !found || stored.Uses != 0 {
		t.Fatalf("worker token after rejected enrollment = %+v, found %v, error %v", stored, found, err)
	}
}

func TestAPICertificateExpiryAndRemovalAuthorization(t *testing.T) {
	f := newNodeTrustFixture(t)
	ctx := context.Background()
	id := uuid.New()
	certificate := f.certificateFor(id)
	if err := f.server.BindNodeCertificate(ctx, id, certificate); err != nil {
		t.Fatal(err)
	}
	f.server.joiner = newFakeMembership(fakeMember(id, true))
	csr, _ := testCSR(t)
	certPEM, err := f.server.IssueAPICertificate(ctx, id, csr)
	if err != nil {
		t.Fatal(err)
	}
	apiCertificate := parseCertificatePEM(t, certPEM)
	if got := apiCertificate.NotAfter.Sub(apiCertificate.NotBefore); got != tlsutil.APICertificateLifetime {
		t.Fatalf("API certificate lifetime = %v, want %v", got, tlsutil.APICertificateLifetime)
	}
	if err := f.server.state.PutNodeTombstone(ctx, id.String(), NodeTombstone{RemovedAt: f.now}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.server.IssueAPICertificate(ctx, id, csr); err == nil {
		t.Fatal("removed node received an API certificate")
	}
}

func TestWorkerCannotReceiveAPICertificate(t *testing.T) {
	f := newNodeTrustFixture(t)
	ctx := context.Background()
	id := uuid.New()
	if err := f.server.BindNodeCertificateRole(ctx, id, f.certificateFor(id), api.NodeRoleWorker); err != nil {
		t.Fatal(err)
	}
	f.server.joiner = newFakeMembership(fakeMember(id, true))
	csr, _ := testCSR(t)
	if _, err := f.server.IssueAPICertificate(ctx, id, csr); err == nil {
		t.Fatal("worker received an API certificate")
	}
}

func TestEnrollmentConsumesJoinToken(t *testing.T) {
	f := newNodeTrustFixture(t)
	ctx := context.Background()
	token, record, err := f.server.CreateJoinToken(ctx, time.Hour, 2, api.NodeRoleWorker)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		csr, _ := testCSR(t)
		response, err := f.server.EnrollNodeCSR(ctx, token, csr, api.NodeRoleWorker)
		if err != nil {
			t.Fatal(err)
		}
		// The enrolled identity is bound in the same transaction.
		if !f.server.AuthorizeNodeCertificate(ctx, response.NodeID, parseCertificatePEM(t, []byte(response.Cert))) {
			t.Fatal("enrolled certificate is not bound to its identity")
		}
	}
	csr, _ := testCSR(t)
	if _, err := f.server.EnrollNodeCSR(ctx, token, csr, api.NodeRoleWorker); !errors.Is(err, ErrInvalidJoinToken) {
		t.Fatalf("third enrollment error = %v, want an exhausted token rejected", err)
	}
	listed, err := f.server.ListJoinTokens(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].ID != record.ID || listed[0].Uses != 2 || listed[0].Hash == "" {
		t.Fatalf("listed tokens = %+v, want the exhausted token with two uses", listed)
	}
	if api := listed[0].API(); api.ID != record.ID || api.Uses != 2 || api.MaxUses != 2 {
		t.Fatalf("token metadata = %+v", api)
	}
}

func TestEnrollmentRejectsInvalidJoinTokens(t *testing.T) {
	f := newNodeTrustFixture(t)
	ctx := context.Background()
	token, record, err := f.server.CreateJoinToken(ctx, time.Hour, 0)
	if err != nil {
		t.Fatal(err)
	}
	forged := token[:strings.Index(token, ".")+1] + "forged"
	for _, candidate := range []string{"", "trls_join_", "not-a-token", forged, "trls_join_ffffffffffffffff.secret"} {
		csr, _ := testCSR(t)
		if _, err := f.server.EnrollNodeCSR(ctx, candidate, csr, api.NodeRoleControlPlane); !errors.Is(err, ErrInvalidJoinToken) {
			t.Errorf("EnrollNode(%q) error = %v, want ErrInvalidJoinToken", candidate, err)
		}
	}
	f.now = record.ExpiresAt
	csr, _ := testCSR(t)
	if _, err := f.server.EnrollNodeCSR(ctx, token, csr, api.NodeRoleControlPlane); !errors.Is(err, ErrInvalidJoinToken) {
		t.Fatalf("expired token error = %v, want ErrInvalidJoinToken", err)
	}
	if listed, _ := f.server.ListJoinTokens(ctx); len(listed) != 0 {
		t.Fatalf("expired tokens listed: %+v", listed)
	}
	// Creating a token prunes expired records.
	if _, _, err := f.server.CreateJoinToken(ctx, time.Hour, 0); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := f.server.state.GetJoinToken(ctx, record.ID); found {
		t.Fatal("expired join token record was not pruned")
	}
}

func TestRevokedJoinTokenCannotEnroll(t *testing.T) {
	f := newNodeTrustFixture(t)
	ctx := context.Background()
	token, record, err := f.server.CreateJoinToken(ctx, time.Hour, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.server.RevokeJoinToken(ctx, record.ID); err != nil {
		t.Fatal(err)
	}
	csr, _ := testCSR(t)
	if _, err := f.server.EnrollNodeCSR(ctx, token, csr, api.NodeRoleControlPlane); !errors.Is(err, ErrInvalidJoinToken) {
		t.Fatalf("revoked token error = %v, want ErrInvalidJoinToken", err)
	}
	if err := f.server.RevokeJoinToken(ctx, record.ID); !errors.Is(err, ErrJoinTokenNotFound) {
		t.Fatalf("second revoke error = %v, want ErrJoinTokenNotFound", err)
	}
	if err := f.server.RevokeJoinToken(ctx, "../meta"); !errors.Is(err, ErrJoinTokenNotFound) {
		t.Fatalf("malformed ID error = %v, want ErrJoinTokenNotFound", err)
	}
}

func TestSingleUseJoinTokenEnrollsOnceUnderConcurrency(t *testing.T) {
	f := newNodeTrustFixture(t)
	ctx := context.Background()
	token, _, err := f.server.CreateJoinToken(ctx, time.Hour, 1)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	enrolled := 0
	for range 8 {
		wg.Go(func() {
			csr, _ := testCSR(t)
			if _, err := f.server.EnrollNodeCSR(ctx, token, csr, api.NodeRoleControlPlane); err == nil {
				mu.Lock()
				enrolled++
				mu.Unlock()
			} else if !errors.Is(err, ErrInvalidJoinToken) {
				t.Errorf("enrollment error = %v, want ErrInvalidJoinToken", err)
			}
		})
	}
	wg.Wait()
	if enrolled != 1 {
		t.Fatalf("single-use token enrolled %d nodes", enrolled)
	}
}

func TestRemovedNodeIdentityIsRevoked(t *testing.T) {
	f := newNodeTrustFixture(t)
	ctx := context.Background()
	leader, removed := uuid.New(), uuid.New()
	f.server.joiner = newFakeMembership(fakeMember(leader, true), fakeMember(removed, false))
	f.server.nodeID = leader
	certificate := f.certificateFor(removed)
	if err := f.server.BindNodeCertificate(ctx, removed, certificate); err != nil {
		t.Fatal(err)
	}
	if !f.server.AuthorizeNodeCertificate(ctx, removed, certificate) {
		t.Fatal("bound member certificate rejected before removal")
	}
	if err := f.server.RemoveMember(ctx, removed.String()); err != nil {
		t.Fatal(err)
	}
	if f.server.AuthorizeNodeCertificate(ctx, removed, certificate) {
		t.Fatal("removed node certificate still authorized")
	}
	if err := f.server.BindNodeCertificate(ctx, removed, certificate); !errors.Is(err, ErrNodeRemoved) {
		t.Fatalf("rebinding removed node error = %v, want ErrNodeRemoved", err)
	}
	if _, err := f.server.JoinMember(ctx, removed, "removed:8129"); !errors.Is(err, ErrNodeRemoved) {
		t.Fatalf("rejoining removed node error = %v, want ErrNodeRemoved", err)
	}
	if members, _ := f.server.MemberVoters(); len(members) != 1 {
		t.Fatalf("members after refused rejoin = %v", members)
	}
}

func TestRemovingUnknownNodeStillRevokesIt(t *testing.T) {
	f := newNodeTrustFixture(t)
	ctx := context.Background()
	leader := uuid.New()
	f.server.joiner = newFakeMembership(fakeMember(leader, true))
	f.server.nodeID = leader
	token, _, err := f.server.CreateJoinToken(ctx, time.Hour, 0)
	if err != nil {
		t.Fatal(err)
	}
	// An enrolled node that never joined Raft can still be removed.
	csr, _ := testCSR(t)
	response, err := f.server.EnrollNodeCSR(ctx, token, csr, api.NodeRoleControlPlane)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.server.RemoveMember(ctx, response.NodeID.String()); err != nil {
		t.Fatal(err)
	}
	if f.server.AuthorizeNodeCertificate(ctx, response.NodeID, parseCertificatePEM(t, []byte(response.Cert))) {
		t.Fatal("removed enrolled identity still authorized")
	}
}

func TestRaftPeerAuthorizer(t *testing.T) {
	f := newNodeTrustFixture(t)
	ctx := context.Background()
	member, joiner, outsider, removed := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	bound := map[uuid.UUID]*x509.Certificate{}
	for _, id := range []uuid.UUID{member, joiner, removed} {
		bound[id] = f.certificateFor(id)
		if err := f.server.BindNodeCertificate(ctx, id, bound[id]); err != nil {
			t.Fatal(err)
		}
	}
	configuration := []state.RaftMember{{ID: member.String(), Voter: true}, {ID: removed.String(), Voter: true}}
	authorizer := NewRaftPeerAuthorizer()
	if err := authorizer.Authorize(bound[member]); err == nil {
		t.Fatal("authorizer admitted a peer before it could read replicated state")
	}
	authorizer.Bind(f.server.state, func() ([]state.RaftMember, error) { return configuration, nil })

	if err := authorizer.Authorize(bound[member]); err != nil {
		t.Fatalf("bound member rejected: %v", err)
	}
	// Another CA-signed certificate for the member UUID is not the bound one.
	if err := authorizer.Authorize(f.certificateFor(member)); err == nil {
		t.Fatal("certificate with the wrong fingerprint for a member UUID was admitted")
	}
	if err := authorizer.Authorize(f.certificateFor(outsider)); err == nil {
		t.Fatal("CA-signed non-member was admitted")
	}
	// A bound identity is not a peer until it is admitted to Raft.
	if err := authorizer.Authorize(bound[joiner]); err == nil {
		t.Fatal("bound non-member was admitted")
	}
	configuration = append(configuration, state.RaftMember{ID: joiner.String()})
	if err := authorizer.Authorize(bound[joiner]); err != nil {
		t.Fatalf("admitted joiner rejected: %v", err)
	}

	if err := authorizer.Authorize(bound[removed]); err != nil {
		t.Fatalf("member rejected before removal: %v", err)
	}
	// The tombstone applies before, and regardless of, the configuration change.
	if err := f.server.state.PutNodeTombstone(ctx, removed.String(), NodeTombstone{RemovedAt: f.now}); err != nil {
		t.Fatal(err)
	}
	if err := authorizer.Authorize(bound[removed]); !errors.Is(err, ErrNodeRemoved) {
		t.Fatalf("removed member error = %v, want ErrNodeRemoved", err)
	}
}

// A new member has no replicated state until the leader first replicates to
// it, so it trusts the members that admitted it, subject to the same
// revocation and binding checks once it learns them.
func TestRaftPeerAuthorizerTrustsAdmittingMembersBeforeReplication(t *testing.T) {
	f := newNodeTrustFixture(t)
	ctx := context.Background()
	leader, outsider := uuid.New(), uuid.New()
	authorizer := NewRaftPeerAuthorizer()
	authorizer.Bind(f.server.state, func() ([]state.RaftMember, error) { return nil, nil })
	leaderCertificate := f.certificateFor(leader)
	if err := authorizer.Authorize(leaderCertificate); err == nil {
		t.Fatal("empty member admitted a peer before learning who admitted it")
	}
	authorizer.TrustJoinMembers([]string{leader.String()})
	if err := authorizer.Authorize(leaderCertificate); err != nil {
		t.Fatalf("admitting leader rejected: %v", err)
	}
	if err := authorizer.Authorize(f.certificateFor(outsider)); err == nil {
		t.Fatal("peer outside the admitting membership was admitted")
	}
	worker := uuid.New()
	workerCertificate := f.certificateFor(worker)
	authorizer.TrustJoinMembers([]string{worker.String()})
	if err := f.server.BindNodeCertificateRole(ctx, worker, workerCertificate, api.NodeRoleWorker); err != nil {
		t.Fatal(err)
	}
	if err := authorizer.Authorize(workerCertificate); err == nil {
		t.Fatal("bootstrap trust admitted a worker peer")
	}
	// Once replicated, the leader's binding pins its certificate.
	if err := f.server.BindNodeCertificate(ctx, leader, leaderCertificate); err != nil {
		t.Fatal(err)
	}
	if err := authorizer.Authorize(f.certificateFor(leader)); err == nil {
		t.Fatal("second certificate for the admitting leader was admitted")
	}
	if err := f.server.state.PutNodeTombstone(ctx, leader.String(), NodeTombstone{RemovedAt: f.now}); err != nil {
		t.Fatal(err)
	}
	if err := authorizer.Authorize(leaderCertificate); !errors.Is(err, ErrNodeRemoved) {
		t.Fatalf("removed admitting member error = %v, want ErrNodeRemoved", err)
	}
}

func TestLeaderCannotRemoveItself(t *testing.T) {
	leader, b, c := uuid.New(), uuid.New(), uuid.New()
	joiner := newFakeMembership(fakeMember(leader, true), fakeMember(b, true), fakeMember(c, true))
	s := membershipTestServer(joiner, leader, b, c)
	if err := s.RemoveMember(context.Background(), leader.String()); !errors.Is(err, ErrMembershipUnsafe) {
		t.Fatalf("leader self-removal error = %v, want ErrMembershipUnsafe", err)
	}
	if got := joiner.operations(); len(got) != 0 {
		t.Fatalf("refused self-removal changed membership: %v", got)
	}
	if removed, _ := s.state.NodeRemoved(context.Background(), leader.String()); removed {
		t.Fatal("refused self-removal recorded a tombstone")
	}
}
