// Package auth implements Trellis bearer credentials and administrator request signing.
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
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
	workloadTokensMu sync.Mutex
}

// NewTokenManager creates a token manager backed by state storage.
func NewTokenManager(store state.Store, cluster string) *TokenManager {
	return &TokenManager{store: store, cluster: cluster}
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
		principal.CreatedAt = time.Now().UTC()
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
	return &principal, nil
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
	if existing != nil && existing.Generation == generation && samePrincipalGrant(existing.Principal, principal) {
		if token, ok := m.openWorkloadToken(ctx, sealer, existing); ok {
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
// token cannot be recovered or is no longer valid, so the caller replaces it.
func (m *TokenManager) openWorkloadToken(ctx context.Context, sealer Sealer, credential *WorkloadCredential) (string, bool) {
	raw, err := sealer.Open(credential.SealedToken, m.workloadTokenAAD(credential.AllocationID, credential.Generation, credential.TokenHash))
	if err != nil {
		return "", false
	}
	defer clear(raw)
	hash := sha256.Sum256(raw)
	if hex.EncodeToString(hash[:]) != credential.TokenHash {
		return "", false
	}
	token := string(raw)
	if validated, err := m.ValidateToken(ctx, token); err != nil || validated == nil {
		return "", false
	}
	return token, true
}

// RevokeWorkloadCredentials deletes every workload credential for which keep
// returns false and returns how many were revoked.
func (m *TokenManager) RevokeWorkloadCredentials(ctx context.Context, keep func(WorkloadCredential) bool) (int, error) {
	m.workloadTokensMu.Lock()
	defer m.workloadTokensMu.Unlock()

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
