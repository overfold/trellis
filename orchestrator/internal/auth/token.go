// Package auth implements Trellis bearer credentials and administrator request signing.
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/overfold/trellis/internal/state"
)

// AccessScope controls where a generated API credential may operate.
type AccessScope string

const (
	// AccessNamespace restricts a generated credential to one namespace.
	AccessNamespace AccessScope = "namespace"
	// AccessCluster permits cluster-wide operations allowed by the access level.
	AccessCluster AccessScope = "cluster"
)

// AccessLevel controls whether a generated API credential may mutate state.
type AccessLevel string

const (
	// AccessRead grants observation-only API access.
	AccessRead AccessLevel = "read"
	// AccessWrite grants ordinary mutation API access within the credential scope.
	AccessWrite AccessLevel = "write"
)

// CredentialKind identifies why a credential exists.
type CredentialKind string

const (
	// CredentialAdministrator is the root operator credential.
	CredentialAdministrator CredentialKind = "administrator"
	// CredentialOperator is an explicitly minted human or external-client credential.
	CredentialOperator CredentialKind = "operator"
	// CredentialWorkload is injected into a task group through api_access.
	CredentialWorkload CredentialKind = "workload"
)

// CredentialSubject identifies the workload that owns an injected credential.
type CredentialSubject struct {
	Namespace string `json:"namespace"`
	Job       string `json:"job"`
	TaskGroup string `json:"task_group"`
}

// Principal is the authoritative identity and authorization attached to a credential.
type Principal struct {
	Kind      CredentialKind     `json:"kind"`
	Scope     AccessScope        `json:"scope"`
	Access    AccessLevel        `json:"access"`
	Namespace string             `json:"namespace,omitempty"`
	Subject   *CredentialSubject `json:"subject,omitempty"`
	CreatedAt time.Time          `json:"created_at,omitempty"`
	// ExpiresAt is when an operator credential stops authenticating. The zero
	// time means the credential does not expire.
	ExpiresAt time.Time `json:"expires_at,omitzero"`
}

// AdministratorPrincipal returns the effective principal for the administrator credential.
func AdministratorPrincipal() Principal {
	return Principal{Kind: CredentialAdministrator, Scope: AccessCluster, Access: AccessWrite}
}

// Validate checks that a persisted principal is internally consistent.
func (p Principal) Validate() error {
	if p.Kind != CredentialOperator && p.Kind != CredentialWorkload {
		return fmt.Errorf("invalid credential kind %q", p.Kind)
	}
	if p.Scope != AccessNamespace && p.Scope != AccessCluster {
		return fmt.Errorf("invalid credential scope %q", p.Scope)
	}
	if p.Access != AccessRead && p.Access != AccessWrite {
		return fmt.Errorf("invalid credential access %q", p.Access)
	}
	if p.Scope == AccessNamespace && p.Namespace == "" {
		return fmt.Errorf("namespace credential requires a namespace")
	}
	if p.Scope == AccessCluster && p.Namespace != "" {
		return fmt.Errorf("cluster credential must not include a namespace")
	}
	if p.Kind == CredentialWorkload && !p.ExpiresAt.IsZero() {
		return fmt.Errorf("workload credential must not include an expiry")
	}
	if p.Kind == CredentialOperator && p.Subject != nil {
		return fmt.Errorf("operator credential must not include a workload subject")
	}
	if p.Kind == CredentialWorkload && p.Subject == nil {
		return fmt.Errorf("workload credential requires a workload subject")
	}
	if p.Subject != nil {
		if p.Subject.Namespace == "" || p.Subject.Job == "" || p.Subject.TaskGroup == "" {
			return fmt.Errorf("workload subject requires namespace, job, and task group")
		}
		if p.Kind != CredentialWorkload {
			return fmt.Errorf("only workload credentials may include a workload subject")
		}
		if p.Scope == AccessNamespace && p.Namespace != p.Subject.Namespace {
			return fmt.Errorf("workload credential namespace must match its subject")
		}
	}
	return nil
}

// TokenManager creates and validates persisted scoped tokens.
type TokenManager struct {
	store            state.Store
	cluster          string
	now              func() time.Time
	workloadTokensMu sync.Mutex
}

// NewTokenManager creates a token manager backed by state storage.
func NewTokenManager(store state.Store, cluster string) *TokenManager {
	return &TokenManager{store: store, cluster: cluster, now: time.Now}
}

// SetClock replaces the clock used for credential creation and expiry.
func (m *TokenManager) SetClock(now func() time.Time) { m.now = now }

// ErrCredentialNotFound reports that no operator credential has the given ID.
var ErrCredentialNotFound = errors.New("credential not found")

// credentialIDLength is the number of token-hash hex digits that identify an
// operator credential in listings. The hash of a 256-bit random token reveals
// nothing about the token, and 64 bits make collisions negligible.
const credentialIDLength = 16

// CredentialID returns the public identifier of the credential whose token
// hash is hashHex.
func CredentialID(hashHex string) string {
	if len(hashHex) < credentialIDLength {
		return hashHex
	}
	return hashHex[:credentialIDLength]
}

// ValidCredentialID reports whether id has the form returned by CredentialID.
func ValidCredentialID(id string) bool {
	if len(id) != credentialIDLength {
		return false
	}
	for _, c := range id {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// OperatorCredential is the listable metadata of one operator credential. It
// never contains the bearer token.
type OperatorCredential struct {
	ID        string
	Principal Principal
}

// CreateOperatorToken mints an operator credential and returns the bearer
// token together with its listable metadata.
func (m *TokenManager) CreateOperatorToken(ctx context.Context, principal Principal) (string, OperatorCredential, error) {
	principal.Kind = CredentialOperator
	token, key, data, err := m.prepareToken(principal)
	if err != nil {
		return "", OperatorCredential{}, err
	}
	var stored Principal
	if err := json.Unmarshal(data, &stored); err != nil {
		return "", OperatorCredential{}, fmt.Errorf("decode operator principal: %w", err)
	}
	if err := m.store.Put(ctx, key, data); err != nil {
		return "", OperatorCredential{}, fmt.Errorf("store token: %w", err)
	}
	return token, OperatorCredential{ID: CredentialID(strings.TrimPrefix(key, m.tokenKey(""))), Principal: stored}, nil
}

// ListOperatorCredentials returns the metadata of every stored operator
// credential, including expired ones, ordered by creation time and ID.
// Workload credentials are managed with their allocations and are omitted.
func (m *TokenManager) ListOperatorCredentials(ctx context.Context) ([]OperatorCredential, error) {
	prefix := m.tokenKey("")
	values, err := m.store.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("list credentials: %w", err)
	}
	result := make([]OperatorCredential, 0, len(values))
	for key, data := range values {
		var principal Principal
		if err := json.Unmarshal(data, &principal); err != nil {
			return nil, fmt.Errorf("unmarshal principal: %w", err)
		}
		if principal.Kind != CredentialOperator {
			continue
		}
		result = append(result, OperatorCredential{ID: CredentialID(strings.TrimPrefix(key, prefix)), Principal: principal})
	}
	sort.Slice(result, func(i, j int) bool {
		if !result[i].Principal.CreatedAt.Equal(result[j].Principal.CreatedAt) {
			return result[i].Principal.CreatedAt.Before(result[j].Principal.CreatedAt)
		}
		return result[i].ID < result[j].ID
	})
	return result, nil
}

// RevokeOperatorCredential deletes the operator credential identified by id.
// Workload credentials cannot be revoked this way.
func (m *TokenManager) RevokeOperatorCredential(ctx context.Context, id string) error {
	if !ValidCredentialID(id) {
		return ErrCredentialNotFound
	}
	values, err := m.store.List(ctx, m.tokenKey(id))
	if err != nil {
		return fmt.Errorf("list credentials: %w", err)
	}
	var match string
	for key, data := range values {
		var principal Principal
		if err := json.Unmarshal(data, &principal); err != nil || principal.Kind != CredentialOperator {
			continue
		}
		if match != "" {
			return fmt.Errorf("credential ID %s is ambiguous", id)
		}
		match = key
	}
	if match == "" {
		return ErrCredentialNotFound
	}
	if err := m.store.Delete(ctx, match); err != nil {
		return fmt.Errorf("revoke credential: %w", err)
	}
	return nil
}

// CreateToken creates and persists a token for principal.
func (m *TokenManager) CreateToken(ctx context.Context, principal Principal) (string, error) {
	token, key, data, err := m.prepareToken(principal)
	if err != nil {
		return "", err
	}
	if err := m.store.Put(ctx, key, data); err != nil {
		return "", fmt.Errorf("store token: %w", err)
	}
	return token, nil
}

func (m *TokenManager) prepareToken(principal Principal) (string, string, []byte, error) {
	if principal.Scope == AccessCluster {
		principal.Namespace = ""
	}
	if principal.CreatedAt.IsZero() {
		principal.CreatedAt = m.now().UTC()
	}
	if err := principal.Validate(); err != nil {
		return "", "", nil, err
	}

	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", "", nil, fmt.Errorf("generate token: %w", err)
	}
	prefix := "trls_op_"
	if principal.Kind == CredentialWorkload {
		prefix = "trls_wl_"
	}
	token := prefix + base64.RawURLEncoding.EncodeToString(raw)
	hash := sha256.Sum256([]byte(token))
	data, err := json.Marshal(&principal)
	if err != nil {
		return "", "", nil, fmt.Errorf("marshal principal: %w", err)
	}
	return token, m.tokenKey(hex.EncodeToString(hash[:])), data, nil
}

// ValidateToken returns the principal for a valid generated token.
func (m *TokenManager) ValidateToken(ctx context.Context, rawToken string) (*Principal, error) {
	hash := sha256.Sum256([]byte(rawToken))
	data, err := m.store.Get(ctx, m.tokenKey(hex.EncodeToString(hash[:])))
	if err != nil {
		return nil, fmt.Errorf("lookup token: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var principal Principal
	if err := json.Unmarshal(data, &principal); err != nil {
		return nil, fmt.Errorf("unmarshal principal: %w", err)
	}
	if err := principal.Validate(); err != nil {
		return nil, fmt.Errorf("stored token has invalid principal: %w", err)
	}
	if !principal.ExpiresAt.IsZero() && !m.now().Before(principal.ExpiresAt) {
		return nil, nil
	}
	return &principal, nil
}

// CredentialNamespaces returns the sorted unique namespaces named by stored,
// unexpired namespace-scoped credentials.
func (m *TokenManager) CredentialNamespaces(ctx context.Context) ([]string, error) {
	values, err := m.store.List(ctx, m.tokenKey(""))
	if err != nil {
		return nil, fmt.Errorf("list credentials: %w", err)
	}
	seen := make(map[string]struct{})
	for _, data := range values {
		var principal Principal
		if err := json.Unmarshal(data, &principal); err != nil {
			return nil, fmt.Errorf("unmarshal principal: %w", err)
		}
		expired := !principal.ExpiresAt.IsZero() && !m.now().Before(principal.ExpiresAt)
		if principal.Scope == AccessNamespace && principal.Namespace != "" && !expired {
			seen[principal.Namespace] = struct{}{}
		}
	}
	result := make([]string, 0, len(seen))
	for namespace := range seen {
		result = append(result, namespace)
	}
	sort.Strings(result)
	return result, nil
}

// Sealer encrypts persisted workload credentials so replicated state never
// contains a usable bearer token.
type Sealer interface {
	Seal(plaintext, associated []byte) ([]byte, error)
	Open(sealed, associated []byte) ([]byte, error)
}

// WorkloadCredential binds an injected credential to one allocation generation.
// Only the token hash and its sealed form are persisted.
type WorkloadCredential struct {
	AllocationID string    `json:"allocation_id"`
	Generation   uint64    `json:"generation"`
	TokenHash    string    `json:"token_hash"`
	Principal    Principal `json:"principal"`
	SealedToken  []byte    `json:"sealed_token"`
}

func (m *TokenManager) tokenKey(hashHex string) string {
	return fmt.Sprintf("trellis/%s/tokens/%s", m.cluster, hashHex)
}

func (m *TokenManager) workloadCredentialPrefix() string {
	return fmt.Sprintf("trellis/%s/workload-credentials/", m.cluster)
}

func (m *TokenManager) workloadCredentialKey(allocationID string) string {
	return m.workloadCredentialPrefix() + url.PathEscape(allocationID)
}

func (m *TokenManager) workloadTokenAAD(allocationID string, generation uint64, tokenHash string) []byte {
	return []byte(fmt.Sprintf("trellis-workload-token\x00%s\x00%s\x00%d\x00%s", m.cluster, allocationID, generation, tokenHash))
}

func samePrincipalGrant(a, b Principal) bool {
	if a.Kind != b.Kind || a.Scope != b.Scope || a.Access != b.Access || a.Namespace != b.Namespace {
		return false
	}
	if a.Subject == nil || b.Subject == nil {
		return a.Subject == b.Subject
	}
	return *a.Subject == *b.Subject
}

// WorkloadToken returns the credential for one allocation generation, minting
// it on first use. Retries of the same generation receive the same token, so
// the allocation execution hash stays stable across retries and leader
// changes. A new generation or a changed grant replaces and revokes the
// previous credential.
func (m *TokenManager) WorkloadToken(ctx context.Context, sealer Sealer, allocationID string, generation uint64, principal Principal) (string, error) {
	if sealer == nil {
		return "", fmt.Errorf("workload credentials require the secrets encryption key")
	}
	if allocationID == "" || generation == 0 {
		return "", fmt.Errorf("workload credential requires an allocation identity and generation")
	}
	principal.Kind = CredentialWorkload
	if principal.Scope == AccessCluster {
		principal.Namespace = ""
	}
	principal.CreatedAt = time.Time{}
	if err := principal.Validate(); err != nil {
		return "", err
	}
	atomic, ok := m.store.(state.AtomicStore)
	if !ok {
		return "", fmt.Errorf("state store does not support atomic workload tokens")
	}
	key := m.workloadCredentialKey(allocationID)

	// The active leader owns one TokenManager. Serializing its read and batch
	// prevents concurrent allocation starts from minting competing credentials;
	// leader activation's Raft barrier makes committed batches visible before a
	// successor starts serving this path.
	m.workloadTokensMu.Lock()
	defer m.workloadTokensMu.Unlock()

	existing, err := m.loadWorkloadCredential(ctx, key)
	if err != nil {
		return "", err
	}
	if existing != nil && existing.Generation > generation {
		return "", fmt.Errorf("allocation generation %d was superseded by generation %d", generation, existing.Generation)
	}
	if existing != nil && existing.Generation == generation && samePrincipalGrant(existing.Principal, principal) {
		token, ok, err := m.openWorkloadToken(ctx, sealer, existing)
		if err != nil {
			return "", err
		}
		if ok {
			return token, nil
		}
	}

	token, tokenKey, tokenData, err := m.prepareToken(principal)
	if err != nil {
		return "", err
	}
	stored := principal
	if err := json.Unmarshal(tokenData, &stored); err != nil {
		return "", fmt.Errorf("decode workload principal: %w", err)
	}
	hash := sha256.Sum256([]byte(token))
	hashHex := hex.EncodeToString(hash[:])
	sealed, err := sealer.Seal([]byte(token), m.workloadTokenAAD(allocationID, generation, hashHex))
	if err != nil {
		return "", fmt.Errorf("seal workload token: %w", err)
	}
	record, err := json.Marshal(&WorkloadCredential{AllocationID: allocationID, Generation: generation, TokenHash: hashHex, Principal: stored, SealedToken: sealed})
	if err != nil {
		return "", fmt.Errorf("marshal workload credential: %w", err)
	}
	mutations := []state.Mutation{{Key: tokenKey, Value: tokenData}, {Key: key, Value: record}}
	if existing != nil && existing.TokenHash != "" && existing.TokenHash != hashHex {
		mutations = append(mutations, state.Mutation{Key: m.tokenKey(existing.TokenHash)})
	}
	if err := atomic.Batch(ctx, mutations); err != nil {
		return "", fmt.Errorf("store workload token: %w", err)
	}
	return token, nil
}

func (m *TokenManager) loadWorkloadCredential(ctx context.Context, key string) (*WorkloadCredential, error) {
	data, err := m.store.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("lookup workload credential: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var credential WorkloadCredential
	if err := json.Unmarshal(data, &credential); err != nil {
		return nil, fmt.Errorf("decode workload credential: %w", err)
	}
	return &credential, nil
}

// openWorkloadToken recovers a persisted token. It reports false when the
// token cannot be recovered (for example after the secrets key changed) or was
// revoked, so the caller replaces it. Storage errors are returned instead, so a
// transient failure never rotates a token that running tasks still hold.
func (m *TokenManager) openWorkloadToken(ctx context.Context, sealer Sealer, credential *WorkloadCredential) (string, bool, error) {
	principal, err := m.store.Get(ctx, m.tokenKey(credential.TokenHash))
	if err != nil {
		return "", false, fmt.Errorf("lookup workload token: %w", err)
	}
	if principal == nil {
		return "", false, nil
	}
	raw, err := sealer.Open(credential.SealedToken, m.workloadTokenAAD(credential.AllocationID, credential.Generation, credential.TokenHash))
	if err != nil {
		return "", false, nil
	}
	defer clear(raw)
	hash := sha256.Sum256(raw)
	if hex.EncodeToString(hash[:]) != credential.TokenHash {
		return "", false, nil
	}
	return string(raw), true, nil
}

// RevokeWorkloadCredentials deletes every workload credential for which the
// keep function returned by snapshot reports false, and returns how many were
// revoked. snapshot runs after issuance is locked out, so the desired state it
// captures is no older than any credential it judges.
func (m *TokenManager) RevokeWorkloadCredentials(ctx context.Context, snapshot func() func(WorkloadCredential) bool) (int, error) {
	m.workloadTokensMu.Lock()
	defer m.workloadTokensMu.Unlock()
	keep := snapshot()

	records, err := m.store.List(ctx, m.workloadCredentialPrefix())
	if err != nil {
		return 0, fmt.Errorf("list workload credentials: %w", err)
	}
	keys := make([]string, 0, len(records))
	for key := range records {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var mutations []state.Mutation
	revoked := 0
	for _, key := range keys {
		var credential WorkloadCredential
		if err := json.Unmarshal(records[key], &credential); err == nil && keep(credential) {
			continue
		}
		// An undecodable record cannot be matched to a live allocation.
		mutations = append(mutations, state.Mutation{Key: key})
		if credential.TokenHash != "" {
			mutations = append(mutations, state.Mutation{Key: m.tokenKey(credential.TokenHash)})
		}
		revoked++
	}
	if len(mutations) == 0 {
		return 0, nil
	}
	atomic, ok := m.store.(state.AtomicStore)
	if !ok {
		return 0, fmt.Errorf("state store does not support atomic workload tokens")
	}
	if err := atomic.Batch(ctx, mutations); err != nil {
		return 0, fmt.Errorf("revoke workload credentials: %w", err)
	}
	return revoked, nil
}
