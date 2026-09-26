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
	"time"

	"github.com/clofour/trellis/internal/state"
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

// CredentialSubject optionally identifies the workload that owns an injected credential.
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
	store   state.Store
	cluster string
}

// NewTokenManager creates a token manager backed by state storage.
func NewTokenManager(store state.Store, cluster string) *TokenManager {
	return &TokenManager{store: store, cluster: cluster}
}

// CreateToken creates and persists a token for principal.
func (m *TokenManager) CreateToken(ctx context.Context, principal Principal) (string, error) {
	if principal.Scope == AccessCluster {
		principal.Namespace = ""
	}
	if principal.CreatedAt.IsZero() {
		principal.CreatedAt = time.Now().UTC()
	}
	if err := principal.Validate(); err != nil {
		return "", err
	}

	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate token: %w", err)
	}
	prefix := "trls_op_"
	if principal.Kind == CredentialWorkload {
		prefix = "trls_wl_"
	}
	token := prefix + base64.RawURLEncoding.EncodeToString(raw)
	hash := sha256.Sum256([]byte(token))
	hashHex := hex.EncodeToString(hash[:])
	data, err := json.Marshal(&principal)
	if err != nil {
		return "", fmt.Errorf("marshal principal: %w", err)
	}
	key := fmt.Sprintf("trellis/%s/tokens/%s", m.cluster, hashHex)
	if err := m.store.Put(ctx, key, data); err != nil {
		return "", fmt.Errorf("store token: %w", err)
	}
	return token, nil
}

// ValidateToken returns the principal for a valid generated token.
func (m *TokenManager) ValidateToken(ctx context.Context, rawToken string) (*Principal, error) {
	hash := sha256.Sum256([]byte(rawToken))
	hashHex := hex.EncodeToString(hash[:])
	key := fmt.Sprintf("trellis/%s/tokens/%s", m.cluster, hashHex)
	data, err := m.store.Get(ctx, key)
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

// GetOrCreateWorkloadToken returns the persistent workload credential for a scope/access pair.
func (m *TokenManager) GetOrCreateWorkloadToken(ctx context.Context, scope AccessScope, access AccessLevel, namespace string) (string, error) {
	principal := Principal{Kind: CredentialWorkload, Scope: scope, Access: access, Namespace: namespace}
	if scope == AccessCluster {
		principal.Namespace = ""
	}
	if err := principal.Validate(); err != nil {
		return "", err
	}
	mapping := fmt.Sprintf("%s/%s/%s", scope, access, namespace)
	mappingHash := sha256.Sum256([]byte(mapping))
	rawKey := fmt.Sprintf("trellis/%s/workload-tokens/%s", m.cluster, hex.EncodeToString(mappingHash[:]))
	existing, err := m.store.Get(ctx, rawKey)
	if err != nil {
		return "", fmt.Errorf("lookup existing workload token: %w", err)
	}
	if existing != nil {
		token := string(existing)
		if validated, err := m.ValidateToken(ctx, token); err == nil && validated != nil {
			return token, nil
		}
	}
	token, err := m.CreateToken(ctx, principal)
	if err != nil {
		return "", err
	}
	if err := m.store.Put(ctx, rawKey, []byte(token)); err != nil {
		return "", fmt.Errorf("store workload token mapping: %w", err)
	}
	return token, nil
}
