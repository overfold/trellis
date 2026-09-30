package server

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/internal/api"
	"github.com/overfold/trellis/internal/state"
	"github.com/overfold/trellis/internal/tlsutil"
)

const (
	// DefaultJoinTokenTTL is how long a join token is valid when its creator
	// does not choose a lifetime.
	DefaultJoinTokenTTL = time.Hour
	// MaxJoinTokenTTL bounds every join token's lifetime, so a leaked token
	// cannot enroll nodes indefinitely.
	MaxJoinTokenTTL = 7 * 24 * time.Hour
	// MaxJoinTokenUses bounds an explicit use limit.
	MaxJoinTokenUses = 10000

	joinTokenPrefix   = "trls_join_"
	joinTokenIDLength = 16
)

var (
	// ErrInvalidJoinToken rejects enrollment with a join token that is
	// unknown, revoked, expired, or exhausted. The cases are deliberately not
	// distinguished to unauthenticated callers.
	ErrInvalidJoinToken = errors.New("invalid, expired, or exhausted join token")
	// ErrInvalidJoinTokenRequest reports a join token request outside its bounds.
	ErrInvalidJoinTokenRequest = errors.New("invalid join token request")
	// ErrJoinTokenNotFound reports that no unexpired join token has the given ID.
	ErrJoinTokenNotFound = errors.New("join token not found")
	// ErrNodeRemoved reports that a node identity was removed from the
	// cluster. Removed identities are never admitted again.
	ErrNodeRemoved = errors.New("node identity has been removed from the cluster")
	// ErrInvalidNodeID reports a node reference that is not a node UUID.
	ErrInvalidNodeID = errors.New("invalid node ID")
)

// JoinToken is the replicated record of an administrator-minted node join
// token. Only the SHA-256 hash of the token is stored.
type JoinToken struct {
	ID        string    `json:"id"`
	Hash      string    `json:"hash"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
	// MaxUses limits enrollments with the token; zero means unlimited.
	MaxUses int `json:"max_uses,omitempty"`
	Uses    int `json:"uses"`
}

// API returns the listable metadata of the token.
func (t *JoinToken) API() api.JoinTokenResponse {
	return api.JoinTokenResponse{ID: t.ID, CreatedAt: t.CreatedAt, ExpiresAt: t.ExpiresAt, MaxUses: t.MaxUses, Uses: t.Uses}
}

func (t *JoinToken) expired(now time.Time) bool { return !now.Before(t.ExpiresAt) }

func (t *JoinToken) usable(now time.Time) bool {
	return !t.expired(now) && (t.MaxUses == 0 || t.Uses < t.MaxUses)
}

// NodeTombstone durably records that a node identity was removed. Every node
// authentication path rejects a tombstoned UUID, whatever certificate it
// presents.
type NodeTombstone struct {
	RemovedAt time.Time `json:"removed_at"`
}

func joinTokenHash(token string) string {
	digest := sha256.Sum256([]byte(token))
	return hex.EncodeToString(digest[:])
}

// parseJoinTokenID returns the ID embedded in a join token.
func parseJoinTokenID(token string) (string, bool) {
	rest, ok := strings.CutPrefix(token, joinTokenPrefix)
	if !ok {
		return "", false
	}
	id, secret, ok := strings.Cut(rest, ".")
	if !ok || secret == "" || !validJoinTokenID(id) {
		return "", false
	}
	return id, true
}

func validJoinTokenID(id string) bool {
	if len(id) != joinTokenIDLength {
		return false
	}
	for _, c := range id {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func (s *StateController) joinTokenPrefix() string {
	return fmt.Sprintf("%s/%s/join-tokens/", trellisNamespace, s.cluster)
}

func (s *StateController) nodeTombstoneKey(id string) string {
	return fmt.Sprintf("%s/%s/node-tombstones/%s", trellisNamespace, s.cluster, id)
}

func (s *StateController) nodeCertificateFingerprintKey(id string) string {
	return fmt.Sprintf("%s/%s/node-certificate-fingerprints/%s", trellisNamespace, s.cluster, id)
}

// GetJoinToken loads one join token record.
func (s *StateController) GetJoinToken(ctx context.Context, id string) (*JoinToken, bool, error) {
	var token JoinToken
	found, err := s.get(ctx, s.joinTokenPrefix()+id, &token)
	if err != nil {
		return nil, false, fmt.Errorf("get join token: %w", err)
	}
	return &token, found, nil
}

// ListJoinTokens loads every join token record, ordered by creation time and ID.
func (s *StateController) ListJoinTokens(ctx context.Context) ([]*JoinToken, error) {
	values, err := listValues[JoinToken](ctx, s.store, s.joinTokenPrefix())
	if err != nil {
		return nil, err
	}
	result := make([]*JoinToken, 0, len(values))
	for _, token := range values {
		result = append(result, token)
	}
	sort.Slice(result, func(i, j int) bool {
		if !result[i].CreatedAt.Equal(result[j].CreatedAt) {
			return result[i].CreatedAt.Before(result[j].CreatedAt)
		}
		return result[i].ID < result[j].ID
	})
	return result, nil
}

// NodeRemoved reports whether a node UUID carries a removal tombstone.
func (s *StateController) NodeRemoved(ctx context.Context, id string) (bool, error) {
	var tombstone NodeTombstone
	found, err := s.get(ctx, s.nodeTombstoneKey(id), &tombstone)
	if err != nil {
		return false, fmt.Errorf("get node tombstone: %w", err)
	}
	return found, nil
}

// PutNodeTombstone records that a node UUID was removed.
func (s *StateController) PutNodeTombstone(ctx context.Context, id string, tombstone NodeTombstone) error {
	if err := s.put(ctx, s.nodeTombstoneKey(id), tombstone); err != nil {
		return fmt.Errorf("put node tombstone: %w", err)
	}
	return nil
}

func (s *StateController) batch(ctx context.Context, mutations []state.Mutation) error {
	atomic, ok := s.store.(state.AtomicStore)
	if !ok {
		return fmt.Errorf("state store does not support atomic updates")
	}
	return atomic.Batch(ctx, mutations)
}

func jsonMutation(key string, value any) (state.Mutation, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return state.Mutation{}, fmt.Errorf("marshal %s: %w", key, err)
	}
	return state.Mutation{Key: key, Value: raw}, nil
}

// CreateJoinToken mints a node join token. A zero ttl selects
// DefaultJoinTokenTTL; maxUses zero allows unlimited enrollments until expiry.
// The token is returned exactly once; replicated state keeps only its hash.
// Expired token records are pruned in the same transaction.
func (s *Server) CreateJoinToken(ctx context.Context, ttl time.Duration, maxUses int) (string, *JoinToken, error) {
	if ttl == 0 {
		ttl = DefaultJoinTokenTTL
	}
	if ttl < time.Second || ttl > MaxJoinTokenTTL {
		return "", nil, fmt.Errorf("%w: ttl must be between 1s and %s", ErrInvalidJoinTokenRequest, MaxJoinTokenTTL)
	}
	if maxUses < 0 || maxUses > MaxJoinTokenUses {
		return "", nil, fmt.Errorf("%w: max_uses must be between 0 and %d", ErrInvalidJoinTokenRequest, MaxJoinTokenUses)
	}
	idBytes := make([]byte, joinTokenIDLength/2)
	secret := make([]byte, 32)
	if _, err := rand.Read(idBytes); err != nil {
		return "", nil, fmt.Errorf("generate join token: %w", err)
	}
	if _, err := rand.Read(secret); err != nil {
		return "", nil, fmt.Errorf("generate join token: %w", err)
	}
	id := hex.EncodeToString(idBytes)
	token := joinTokenPrefix + id + "." + base64.RawURLEncoding.EncodeToString(secret)
	clear(secret)
	now := s.now().UTC()
	record := &JoinToken{ID: id, Hash: joinTokenHash(token), CreatedAt: now, ExpiresAt: now.Add(ttl), MaxUses: maxUses}

	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	existing, err := s.state.ListJoinTokens(ctx)
	if err != nil {
		return "", nil, err
	}
	var mutations []state.Mutation
	for _, old := range existing {
		if old.ID == id {
			return "", nil, fmt.Errorf("generate join token: ID collision")
		}
		if old.expired(now) {
			mutations = append(mutations, state.Mutation{Key: s.state.joinTokenPrefix() + old.ID})
		}
	}
	put, err := jsonMutation(s.state.joinTokenPrefix()+id, record)
	if err != nil {
		return "", nil, err
	}
	if err := s.state.batch(ctx, append(mutations, put)); err != nil {
		return "", nil, fmt.Errorf("store join token: %w", err)
	}
	return token, record, nil
}

// ListJoinTokens returns the metadata of every unexpired join token.
func (s *Server) ListJoinTokens(ctx context.Context) ([]*JoinToken, error) {
	tokens, err := s.state.ListJoinTokens(ctx)
	if err != nil {
		return nil, err
	}
	now := s.now()
	result := tokens[:0]
	for _, token := range tokens {
		if !token.expired(now) {
			result = append(result, token)
		}
	}
	return result, nil
}

// RevokeJoinToken deletes a join token so no further node can enroll with it.
// Nodes that already enrolled keep their identities.
func (s *Server) RevokeJoinToken(ctx context.Context, id string) error {
	if !validJoinTokenID(id) {
		return ErrJoinTokenNotFound
	}
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	token, found, err := s.state.GetJoinToken(ctx, id)
	if err != nil {
		return err
	}
	if !found || token.expired(s.now()) {
		return ErrJoinTokenNotFound
	}
	if err := s.state.batch(ctx, []state.Mutation{{Key: s.state.joinTokenPrefix() + id}}); err != nil {
		return fmt.Errorf("revoke join token: %w", err)
	}
	return nil
}

// EnrollNode issues a server-assigned node identity and certificate in managed
// signing mode to a caller presenting a usable join token. The token's use
// and the new identity's certificate binding commit in one transaction, so a
// use limit holds however many enrollments race. The CA key is withheld until
// the identity joins Raft, so a join token alone cannot mint or duplicate an
// existing identity.
func (s *Server) EnrollNode(ctx context.Context, joinToken string, advertised ...string) (*api.NodeEnrollmentResponse, error) {
	id, ok := parseJoinTokenID(joinToken)
	if !ok {
		return nil, ErrInvalidJoinToken
	}
	hash := joinTokenHash(joinToken)
	checkToken := func() (*JoinToken, error) {
		record, found, err := s.state.GetJoinToken(ctx, id)
		if err != nil {
			return nil, err
		}
		if !found || subtle.ConstantTimeCompare([]byte(record.Hash), []byte(hash)) != 1 || !record.usable(s.now()) {
			return nil, ErrInvalidJoinToken
		}
		return record, nil
	}
	// Reject invalid tokens before taking mutationMu, which every durable
	// mutation shares, so unauthenticated callers cannot contend it. The
	// check is repeated under the lock, where the use is consumed.
	if _, err := checkToken(); err != nil {
		return nil, err
	}
	caCert, caKey, err := s.ClusterCA()
	if err != nil || caKey == "" {
		return nil, fmt.Errorf("managed node signer is unavailable")
	}

	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	record, err := checkToken()
	if err != nil {
		return nil, err
	}
	nodeID := uuid.New()
	cert, key, err := tlsutil.GenerateNodeCert([]byte(caCert), []byte(caKey), nodeID, advertised...)
	if err != nil {
		return nil, fmt.Errorf("sign node certificate: %w", err)
	}
	block, _ := pem.Decode(cert)
	if block == nil {
		return nil, fmt.Errorf("decode signed node certificate")
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse signed node certificate: %w", err)
	}
	if _, bound, err := s.state.GetNodeCertificateFingerprint(ctx, nodeID.String()); err != nil || bound {
		return nil, fmt.Errorf("reserve node identity: identity %s is unavailable", nodeID)
	}
	record.Uses++
	useToken, err := jsonMutation(s.state.joinTokenPrefix()+id, record)
	if err != nil {
		return nil, err
	}
	bind, err := jsonMutation(s.state.nodeCertificateFingerprintKey(nodeID.String()), nodeCertificateFingerprint(certificate))
	if err != nil {
		return nil, err
	}
	if err := s.state.batch(ctx, []state.Mutation{useToken, bind}); err != nil {
		return nil, fmt.Errorf("reserve node identity: %w", err)
	}
	return &api.NodeEnrollmentResponse{
		NodeID: nodeID,
		CACert: caCert,
		Cert:   string(cert),
		Key:    string(key),
	}, nil
}

// RaftPeerAuthorizer decides which peers may open inbound Raft streams to this
// member. A certificate chaining to the cluster CA only proves that it was
// issued for some node UUID; a peer is admitted only when that UUID is not
// removed, its certificate is the one durably bound to the UUID, and it is a
// current Raft member. All of this is read from the local replicated state and
// Raft configuration.
//
// A new member has neither until the leader first replicates to it, so it
// also trusts the members it was admitted by (TrustJoinMembers) for as long as
// their UUIDs are not removed and not bound to another certificate.
type RaftPeerAuthorizer struct {
	mu        sync.RWMutex
	state     *StateController
	members   func() ([]state.RaftMember, error)
	bootstrap map[string]struct{}
}

// NewRaftPeerAuthorizer returns an authorizer that rejects every peer until
// Bind supplies the local replicated state.
func NewRaftPeerAuthorizer() *RaftPeerAuthorizer { return &RaftPeerAuthorizer{} }

// Bind supplies the local replicated state and Raft configuration.
func (a *RaftPeerAuthorizer) Bind(stateCtl *StateController, members func() ([]state.RaftMember, error)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.state, a.members = stateCtl, members
}

// TrustJoinMembers records the Raft members reported when this node was
// admitted, which it trusts before it has replicated the cluster's own view.
func (a *RaftPeerAuthorizer) TrustJoinMembers(ids []string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.bootstrap == nil {
		a.bootstrap = make(map[string]struct{}, len(ids))
	}
	for _, id := range ids {
		a.bootstrap[id] = struct{}{}
	}
}

// Authorize admits or rejects the peer that presented certificate, which the
// TLS layer has already verified against the cluster CA.
func (a *RaftPeerAuthorizer) Authorize(certificate *x509.Certificate) error {
	id, err := tlsutil.NodeID(certificate)
	if err != nil {
		return fmt.Errorf("peer certificate has no node identity: %w", err)
	}
	a.mu.RLock()
	stateCtl, members := a.state, a.members
	_, trusted := a.bootstrap[id.String()]
	a.mu.RUnlock()
	if stateCtl == nil || members == nil {
		return fmt.Errorf("raft peer authorization is not ready")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	removed, err := stateCtl.NodeRemoved(ctx, id.String())
	if err != nil {
		return err
	}
	if removed {
		return fmt.Errorf("node %s: %w", id, ErrNodeRemoved)
	}
	fingerprint, bound, err := stateCtl.GetNodeCertificateFingerprint(ctx, id.String())
	if err != nil {
		return err
	}
	if bound && subtle.ConstantTimeCompare([]byte(fingerprint), []byte(nodeCertificateFingerprint(certificate))) != 1 {
		return fmt.Errorf("node %s presented a certificate other than its bound certificate", id)
	}
	if trusted {
		return nil
	}
	if !bound {
		return fmt.Errorf("node %s has no certificate binding", id)
	}
	configuration, err := members()
	if err != nil {
		return fmt.Errorf("read Raft membership: %w", err)
	}
	for _, member := range configuration {
		if member.ID == id.String() {
			return nil
		}
	}
	return fmt.Errorf("node %s is not a Raft member", id)
}
