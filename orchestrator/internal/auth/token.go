// Package auth implements Trellis bearer credentials and administrator request signing.
package auth

import (
	"bytes"
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

	"github.com/overfold/trellis/orchestrator/internal/state"
)

// Role is the authority a credential grants. Roles are ordered: each includes
// the authority of the roles before it.
type Role string

const (
	// RoleObserver grants observation-only API access across the cluster.
	RoleObserver Role = "observer"
	// RoleOperator grants cluster-wide workload management. Submitting jobs
	// runs code on nodes, so an operator is trusted with every node its
	// workloads can be placed on.
	RoleOperator Role = "operator"
	// RoleAdministrator is the root-of-trust authority granted only by an
	// administrator-signed request. Generated credentials never carry it.
	RoleAdministrator Role = "administrator"
)

// rank orders roles; an unknown role has no authority.
func (r Role) rank() int {
	switch r {
	case RoleObserver:
		return 1
	case RoleOperator:
		return 2
	case RoleAdministrator:
		return 3
	default:
		return 0
	}
}

// Includes reports whether r grants at least the authority of other.
func (r Role) Includes(other Role) bool { return r.rank() > 0 && r.rank() >= other.rank() }

// Generated reports whether r may be carried by a generated bearer credential.
func (r Role) Generated() bool { return r == RoleObserver || r == RoleOperator }

// CredentialSubject identifies the workload that owns an injected credential.
type CredentialSubject struct {
	Namespace string `json:"namespace"`
	Job       string `json:"job"`
	TaskGroup string `json:"task_group"`
	Task      string `json:"task"`
}

// Principal is the authoritative identity and authorization attached to a
// generated credential. A principal with a Subject is a workload credential
// injected through api_access; one without is a minted operator-held credential.
type Principal struct {
	Role      Role               `json:"role"`
	Subject   *CredentialSubject `json:"subject,omitempty"`
	CreatedAt time.Time          `json:"created_at"`
	// ExpiresAt is when a minted credential stops authenticating. The zero
	// time means the credential does not expire.
	ExpiresAt time.Time `json:"expires_at,omitzero"`
}

// AdministratorPrincipal returns the effective principal of an
// administrator-signed request. It is never persisted.
func AdministratorPrincipal() Principal { return Principal{Role: RoleAdministrator} }

// Workload reports whether p belongs to a workload credential.
func (p Principal) Workload() bool { return p.Subject != nil }

// Validate checks that a persisted principal is internally consistent.
func (p Principal) Validate() error {
	if !p.Role.Generated() {
		return fmt.Errorf("invalid credential role %q", p.Role)
	}
	if p.Subject != nil {
		if !p.ExpiresAt.IsZero() {
			return fmt.Errorf("workload credential must not include an expiry")
		}
		if p.Subject.Namespace == "" || p.Subject.Job == "" || p.Subject.TaskGroup == "" || p.Subject.Task == "" {
			return fmt.Errorf("workload subject requires namespace, job, task group, and task")
		}
	}
	return nil
}

// tokenRecord adds the allocation lookup to the stored principal, not the
// public principal. Workload authentication never scans other sealed tokens.
type tokenRecord struct {
	Principal    `json:",inline"`
	AllocationID string `json:"allocation_id,omitempty"`
}

// TokenManager creates and validates persisted tokens.
type TokenManager struct {
	store             state.Store
	cluster           string
	now               func() time.Time
	workloadTokensMu  sync.Mutex
	workloadAuthorize func(context.Context, WorkloadCredential) bool
}

// NewTokenManager creates a token manager backed by state storage.
func NewTokenManager(store state.Store, cluster string) *TokenManager {
	return &TokenManager{store: store, cluster: cluster, now: time.Now}
}

// SetClock replaces the clock used for credential creation and expiry.
func (m *TokenManager) SetClock(now func() time.Time) { m.now = now }

// SetWorkloadAuthorizer installs the coordinator's live credential-authority
// check before serving requests. It is immutable once requests are served.
func (m *TokenManager) SetWorkloadAuthorizer(authorize func(context.Context, WorkloadCredential) bool) {
	m.workloadAuthorize = authorize
}

// ErrCredentialNotFound reports that no minted credential has the given ID.
var ErrCredentialNotFound = errors.New("credential not found")

// credentialIDLength is the number of token-hash hex digits that identify an
// minted credential in listings. The hash of a 256-bit random token reveals
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

// MintedCredential is the listable metadata of one minted credential. It
// never contains the bearer token.
type MintedCredential struct {
	ID        string
	Principal Principal
}

// CreateMintedToken mints a minted credential and returns the bearer
// token together with its listable metadata.
func (m *TokenManager) CreateMintedToken(ctx context.Context, principal Principal) (string, MintedCredential, error) {
	if principal.Workload() {
		return "", MintedCredential{}, fmt.Errorf("minted credential must not include a workload subject")
	}
	token, key, data, err := m.prepareToken(principal)
	if err != nil {
		return "", MintedCredential{}, err
	}
	var stored Principal
	if err := json.Unmarshal(data, &stored); err != nil {
		return "", MintedCredential{}, fmt.Errorf("decode operator principal: %w", err)
	}
	if err := m.store.Put(ctx, key, data); err != nil {
		return "", MintedCredential{}, fmt.Errorf("store token: %w", err)
	}
	return token, MintedCredential{ID: CredentialID(strings.TrimPrefix(key, m.tokenKey(""))), Principal: stored}, nil
}

// ListMintedCredentials returns the metadata of every stored operator
// credential, including expired ones, ordered by creation time and ID.
// Workload credentials are managed with their allocations and are omitted.
func (m *TokenManager) ListMintedCredentials(ctx context.Context) ([]MintedCredential, error) {
	prefix := m.tokenKey("")
	values, err := m.store.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("list credentials: %w", err)
	}
	result := make([]MintedCredential, 0, len(values))
	for key, data := range values {
		var record tokenRecord
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&record); err != nil {
			return nil, fmt.Errorf("unmarshal principal: %w", err)
		}
		principal := record.Principal
		if principal.Workload() {
			continue
		}
		result = append(result, MintedCredential{ID: CredentialID(strings.TrimPrefix(key, prefix)), Principal: principal})
	}
	sort.Slice(result, func(i, j int) bool {
		if !result[i].Principal.CreatedAt.Equal(result[j].Principal.CreatedAt) {
			return result[i].Principal.CreatedAt.Before(result[j].Principal.CreatedAt)
		}
		return result[i].ID < result[j].ID
	})
	return result, nil
}

// RevokeMintedCredential deletes the minted credential identified by id.
// Workload credentials cannot be revoked this way.
func (m *TokenManager) RevokeMintedCredential(ctx context.Context, id string) error {
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
		if err := json.Unmarshal(data, &principal); err != nil || principal.Workload() {
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
	if principal.CreatedAt.IsZero() {
		principal.CreatedAt = m.now().UTC()
	}
	if err := principal.Validate(); err != nil {
		return "", "", nil, err
	}

	raw := make([]byte, 32)
	defer clear(raw)
	if _, err := rand.Read(raw); err != nil {
		return "", "", nil, fmt.Errorf("generate token: %w", err)
	}
	// Prefixes are descriptive only; authorization uses the stored principal.
	prefix := "trls_op_"
	switch {
	case principal.Workload():
		prefix = "trls_wl_"
	case principal.Role == RoleObserver:
		prefix = "trls_ob_"
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
	var record tokenRecord
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return nil, fmt.Errorf("unmarshal principal: %w", err)
	}
	principal := record.Principal
	if err := principal.Validate(); err != nil {
		return nil, fmt.Errorf("stored token has invalid principal: %w", err)
	}
	if !principal.ExpiresAt.IsZero() && !m.now().Before(principal.ExpiresAt) {
		return nil, nil
	}
	if principal.Workload() && m.workloadAuthorize != nil {
		if record.AllocationID == "" {
			return nil, nil
		}
		credential, err := m.loadWorkloadCredential(ctx, m.workloadCredentialKey(record.AllocationID, principal.Subject.Task))
		if err != nil {
			return nil, fmt.Errorf("lookup workload authority: %w", err)
		}
		if credential != nil && credential.TokenHash == hex.EncodeToString(hash[:]) && samePrincipalGrant(credential.Principal, principal) && m.workloadAuthorize(ctx, *credential) {
			return &principal, nil
		}
		return nil, nil
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

func (m *TokenManager) workloadCredentialKey(allocationID, task string) string {
	return m.workloadCredentialPrefix() + url.PathEscape(allocationID) + "/" + url.PathEscape(task)
}

func (m *TokenManager) workloadTokenAAD(allocationID, task string, generation uint64, tokenHash string) []byte {
	return fmt.Appendf(nil, "trellis-workload-token\x00%s\x00%s\x00%s\x00%d\x00%s", m.cluster, allocationID, task, generation, tokenHash)
}

func samePrincipalGrant(a, b Principal) bool {
	if a.Role != b.Role {
		return false
	}
	if a.Subject == nil || b.Subject == nil {
		return a.Subject == b.Subject
	}
	return *a.Subject == *b.Subject
}

// WorkloadToken returns the credential for one task in an allocation generation,
// minting it on first use. Retries of the same generation receive the same token, so
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
	if !principal.Workload() {
		return "", fmt.Errorf("workload credential requires a workload subject")
	}
	principal.CreatedAt = time.Time{}
	if err := principal.Validate(); err != nil {
		return "", err
	}
	atomic, ok := m.store.(state.AtomicStore)
	if !ok {
		return "", fmt.Errorf("state store does not support atomic workload tokens")
	}
	key := m.workloadCredentialKey(allocationID, principal.Subject.Task)

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
	tokenData, err = json.Marshal(tokenRecord{Principal: stored, AllocationID: allocationID})
	if err != nil {
		return "", fmt.Errorf("marshal workload token: %w", err)
	}
	hash := sha256.Sum256([]byte(token))
	hashHex := hex.EncodeToString(hash[:])
	raw := []byte(token)
	defer clear(raw)
	sealed, err := sealer.Seal(raw, m.workloadTokenAAD(allocationID, principal.Subject.Task, generation, hashHex))
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
	raw, err := sealer.Open(credential.SealedToken, m.workloadTokenAAD(credential.AllocationID, credential.Principal.Subject.Task, credential.Generation, credential.TokenHash))
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
