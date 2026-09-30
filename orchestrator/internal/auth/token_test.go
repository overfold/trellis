package auth

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/overfold/trellis/orchestrator/internal/secrets"
	"github.com/overfold/trellis/orchestrator/internal/state"
)

type memStore struct {
	mu         sync.Mutex
	values     map[string][]byte
	puts       int
	batches    int
	batchError error
}

func newMemStore() *memStore { return &memStore{values: map[string][]byte{}} }

func (m *memStore) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.values[key], nil
}
func (m *memStore) List(_ context.Context, prefix string) (map[string][]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r := map[string][]byte{}
	for k, v := range m.values {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			r[k] = v
		}
	}
	return r, nil
}
func (m *memStore) Put(_ context.Context, key string, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.puts++
	m.values[key] = value
	return nil
}
func (m *memStore) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.values, key)
	return nil
}
func (m *memStore) Batch(_ context.Context, mutations []state.Mutation) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.batches++
	if m.batchError != nil {
		return m.batchError
	}
	for _, mutation := range mutations {
		if mutation.Value == nil {
			delete(m.values, mutation.Key)
			continue
		}
		m.values[mutation.Key] = mutation.Value
	}
	return nil
}

func TestTokenRoundTrip(t *testing.T) {
	ctx := context.Background()
	mgr := NewTokenManager(newMemStore(), "test")
	principal := Principal{Kind: CredentialOperator, Scope: AccessNamespace, Access: AccessRead, Namespace: "acme"}

	token, err := mgr.CreateToken(ctx, principal)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(token, "trls_op_") {
		t.Fatalf("operator token has unexpected prefix: %q", token)
	}
	saved, err := mgr.ValidateToken(ctx, token)
	if err != nil {
		t.Fatal(err)
	}
	if saved == nil || saved.Kind != CredentialOperator || saved.Scope != AccessNamespace || saved.Access != AccessRead || saved.Namespace != "acme" || saved.CreatedAt.IsZero() {
		t.Fatalf("unexpected principal: %#v", saved)
	}

	saved, err = mgr.ValidateToken(ctx, "invalid-token")
	if err != nil {
		t.Fatal(err)
	}
	if saved != nil {
		t.Fatalf("expected nil principal for invalid token, got %#v", saved)
	}
}

func testSealer(t *testing.T, store state.Store) Sealer {
	t.Helper()
	sealer, err := secrets.NewStore(store, "test", "k1", bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}
	return sealer
}

func workloadPrincipal(scope AccessScope, access AccessLevel) Principal {
	return Principal{Kind: CredentialWorkload, Scope: scope, Access: access, Namespace: "acme", Subject: &CredentialSubject{Namespace: "acme", Job: "api", TaskGroup: "web"}}
}

func TestWorkloadTokenStableAndSealed(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	mgr := NewTokenManager(store, "test")
	sealer := testSealer(t, store)

	token1, err := mgr.WorkloadToken(ctx, sealer, "alloc-1", 1, workloadPrincipal(AccessNamespace, AccessRead))
	if err != nil {
		t.Fatal(err)
	}
	token2, err := mgr.WorkloadToken(ctx, sealer, "alloc-1", 1, workloadPrincipal(AccessNamespace, AccessRead))
	if err != nil {
		t.Fatal(err)
	}
	if token1 != token2 {
		t.Fatalf("expected stable workload token for one allocation generation")
	}
	if !strings.HasPrefix(token1, "trls_wl_") {
		t.Fatalf("workload token has unexpected prefix")
	}
	store.mu.Lock()
	for key, value := range store.values {
		if bytes.Contains(value, []byte(token1)) || strings.Contains(key, token1) {
			t.Fatalf("state key %q contains the plaintext workload token", key)
		}
	}
	store.mu.Unlock()

	saved, err := mgr.ValidateToken(ctx, token1)
	if err != nil {
		t.Fatal(err)
	}
	if saved == nil || saved.Kind != CredentialWorkload || saved.Scope != AccessNamespace || saved.Access != AccessRead || saved.Namespace != "acme" {
		t.Fatalf("unexpected workload principal: %#v", saved)
	}
	if saved.Subject == nil || *saved.Subject != (CredentialSubject{Namespace: "acme", Job: "api", TaskGroup: "web"}) {
		t.Fatalf("workload principal subject = %#v", saved.Subject)
	}

	other, err := mgr.WorkloadToken(ctx, sealer, "alloc-2", 1, workloadPrincipal(AccessNamespace, AccessRead))
	if err != nil {
		t.Fatal(err)
	}
	if other == token1 {
		t.Fatalf("distinct allocations share one workload token")
	}
}

func TestWorkloadTokenRotatesAndRevokesPrevious(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	mgr := NewTokenManager(store, "test")
	sealer := testSealer(t, store)

	first, err := mgr.WorkloadToken(ctx, sealer, "alloc-1", 1, workloadPrincipal(AccessNamespace, AccessRead))
	if err != nil {
		t.Fatal(err)
	}
	for name, next := range map[string]struct {
		generation uint64
		principal  Principal
	}{
		"generation": {generation: 2, principal: workloadPrincipal(AccessNamespace, AccessRead)},
		"grant":      {generation: 2, principal: workloadPrincipal(AccessNamespace, AccessWrite)},
	} {
		second, err := mgr.WorkloadToken(ctx, sealer, "alloc-1", next.generation, next.principal)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if second == first {
			t.Fatalf("%s change reused the previous token", name)
		}
		if saved, err := mgr.ValidateToken(ctx, first); err != nil || saved != nil {
			t.Fatalf("%s change left the previous token valid: %#v %v", name, saved, err)
		}
		first = second
	}
}

func TestWorkloadTokenRejectsSupersededGeneration(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	mgr := NewTokenManager(store, "test")
	sealer := testSealer(t, store)
	current, err := mgr.WorkloadToken(ctx, sealer, "alloc-1", 2, workloadPrincipal(AccessNamespace, AccessRead))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.WorkloadToken(ctx, sealer, "alloc-1", 1, workloadPrincipal(AccessNamespace, AccessRead)); err == nil {
		t.Fatal("stale generation minted a workload token")
	}
	if saved, _ := mgr.ValidateToken(ctx, current); saved == nil {
		t.Fatal("stale generation revoked the current workload token")
	}
}

func TestWorkloadTokenLookupFailureDoesNotRotate(t *testing.T) {
	ctx := context.Background()
	store := &failingGetStore{memStore: newMemStore()}
	mgr := NewTokenManager(store, "test")
	sealer := testSealer(t, store.memStore)
	first, err := mgr.WorkloadToken(ctx, sealer, "alloc-1", 1, workloadPrincipal(AccessNamespace, AccessRead))
	if err != nil {
		t.Fatal(err)
	}
	store.failPrefix = "trellis/test/tokens/"
	if _, err := mgr.WorkloadToken(ctx, sealer, "alloc-1", 1, workloadPrincipal(AccessNamespace, AccessRead)); err == nil {
		t.Fatal("transient token lookup failure was not returned")
	}
	store.failPrefix = ""
	if saved, _ := mgr.ValidateToken(ctx, first); saved == nil {
		t.Fatal("transient lookup failure rotated the workload token")
	}
}

type failingGetStore struct {
	*memStore
	failPrefix string
}

func (s *failingGetStore) Get(ctx context.Context, key string) ([]byte, error) {
	if s.failPrefix != "" && strings.HasPrefix(key, s.failPrefix) {
		return nil, errors.New("injected lookup failure")
	}
	return s.memStore.Get(ctx, key)
}

func TestWorkloadTokenReplacesUnrecoverableCredential(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	mgr := NewTokenManager(store, "test")

	first, err := mgr.WorkloadToken(ctx, testSealer(t, store), "alloc-1", 1, workloadPrincipal(AccessNamespace, AccessRead))
	if err != nil {
		t.Fatal(err)
	}
	rotated, err := secrets.NewStore(store, "test", "k2", bytes.Repeat([]byte{9}, 32))
	if err != nil {
		t.Fatal(err)
	}
	second, err := mgr.WorkloadToken(ctx, rotated, "alloc-1", 1, workloadPrincipal(AccessNamespace, AccessRead))
	if err != nil {
		t.Fatal(err)
	}
	if second == first {
		t.Fatalf("token sealed with an unavailable key was reused")
	}
	if saved, _ := mgr.ValidateToken(ctx, first); saved != nil {
		t.Fatalf("unrecoverable token remains valid")
	}
}

func TestWorkloadTokenRequiresSealerAndSubject(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	mgr := NewTokenManager(store, "test")
	if _, err := mgr.WorkloadToken(ctx, nil, "alloc-1", 1, workloadPrincipal(AccessNamespace, AccessRead)); err == nil {
		t.Fatalf("workload token issued without a sealer")
	}
	principal := workloadPrincipal(AccessNamespace, AccessRead)
	principal.Subject = nil
	if _, err := mgr.WorkloadToken(ctx, testSealer(t, store), "alloc-1", 1, principal); err == nil {
		t.Fatalf("workload token issued without a subject")
	}
	if _, err := mgr.WorkloadToken(ctx, testSealer(t, store), "alloc-1", 0, workloadPrincipal(AccessNamespace, AccessRead)); err == nil {
		t.Fatalf("workload token issued without a generation")
	}
	if len(store.values) != 0 {
		t.Fatalf("rejected workload tokens stored %d records", len(store.values))
	}
}

func TestWorkloadTokenConcurrent(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	mgr := NewTokenManager(store, "test")
	sealer := testSealer(t, store)
	const callers = 32
	tokens := make(chan string, callers)
	errs := make(chan error, callers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			token, err := mgr.WorkloadToken(ctx, sealer, "alloc-1", 1, workloadPrincipal(AccessNamespace, AccessWrite))
			tokens <- token
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(tokens)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var want string
	for token := range tokens {
		if want == "" {
			want = token
		} else if token != want {
			t.Fatalf("concurrent calls returned different tokens")
		}
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.batches != 1 || store.puts != 0 {
		t.Fatalf("credential was not created by one atomic batch: batches=%d puts=%d", store.batches, store.puts)
	}
	if len(store.values) != 2 {
		t.Fatalf("atomic token creation stored %d records, want 2", len(store.values))
	}
}

func TestWorkloadTokenBatchFailureLeavesNoCredential(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	store.batchError = errors.New("injected batch failure")
	mgr := NewTokenManager(store, "test")

	if _, err := mgr.WorkloadToken(ctx, testSealer(t, store), "alloc-1", 1, workloadPrincipal(AccessNamespace, AccessRead)); err == nil || !strings.Contains(err.Error(), "injected batch failure") {
		t.Fatalf("WorkloadToken error = %v, want injected failure", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.batches != 1 || store.puts != 0 {
		t.Fatalf("failed creation used non-atomic writes: batches=%d puts=%d", store.batches, store.puts)
	}
	if len(store.values) != 0 {
		t.Fatalf("failed atomic creation left %d records", len(store.values))
	}
}

func TestRevokeWorkloadCredentials(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	mgr := NewTokenManager(store, "test")
	sealer := testSealer(t, store)
	kept, err := mgr.WorkloadToken(ctx, sealer, "alloc-keep", 1, workloadPrincipal(AccessNamespace, AccessRead))
	if err != nil {
		t.Fatal(err)
	}
	dropped, err := mgr.WorkloadToken(ctx, sealer, "alloc-drop", 1, workloadPrincipal(AccessCluster, AccessWrite))
	if err != nil {
		t.Fatal(err)
	}
	revoked, err := mgr.RevokeWorkloadCredentials(ctx, func() func(WorkloadCredential) bool {
		return func(credential WorkloadCredential) bool { return credential.AllocationID == "alloc-keep" }
	})
	if err != nil || revoked != 1 {
		t.Fatalf("RevokeWorkloadCredentials = %d, %v; want 1", revoked, err)
	}
	if saved, _ := mgr.ValidateToken(ctx, dropped); saved != nil {
		t.Fatalf("revoked workload token remains valid")
	}
	if saved, _ := mgr.ValidateToken(ctx, kept); saved == nil {
		t.Fatalf("kept workload token was revoked")
	}
	if len(store.values) != 2 {
		t.Fatalf("revocation left %d records, want 2", len(store.values))
	}
	reissued, err := mgr.WorkloadToken(ctx, sealer, "alloc-drop", 1, workloadPrincipal(AccessCluster, AccessWrite))
	if err != nil {
		t.Fatal(err)
	}
	if reissued == dropped {
		t.Fatalf("revoked workload token was re-issued")
	}
}

func TestPrincipalValidation(t *testing.T) {
	for _, principal := range []Principal{
		{Kind: CredentialOperator, Scope: AccessNamespace, Access: AccessRead},
		{Kind: CredentialOperator, Scope: AccessCluster, Access: AccessWrite, Namespace: "acme"},
		{Kind: CredentialOperator, Scope: AccessNamespace, Access: AccessRead, Namespace: "acme", Subject: &CredentialSubject{Namespace: "acme", Job: "api", TaskGroup: "web"}},
		{Kind: CredentialWorkload, Scope: AccessNamespace, Access: AccessRead, Namespace: "acme"},
		{Kind: CredentialWorkload, Scope: AccessNamespace, Access: AccessRead, Namespace: "other", Subject: &CredentialSubject{Namespace: "acme", Job: "api", TaskGroup: "web"}},
	} {
		if err := principal.Validate(); err == nil {
			t.Fatalf("expected principal to be invalid: %#v", principal)
		}
	}
}

func TestOperatorCredentialExpires(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	mgr := NewTokenManager(newMemStore(), "test")
	mgr.SetClock(func() time.Time { return now })
	token, credential, err := mgr.CreateOperatorToken(ctx, Principal{Scope: AccessCluster, Access: AccessRead, ExpiresAt: now.Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if !credential.Principal.CreatedAt.Equal(now) || !credential.Principal.ExpiresAt.Equal(now.Add(time.Hour)) || !ValidCredentialID(credential.ID) {
		t.Fatalf("credential metadata = %+v", credential)
	}
	if principal, err := mgr.ValidateToken(ctx, token); err != nil || principal == nil {
		t.Fatalf("unexpired credential rejected: %v", err)
	}
	now = now.Add(time.Hour)
	if principal, err := mgr.ValidateToken(ctx, token); err != nil || principal != nil {
		t.Fatalf("expired credential = %+v, %v; want rejected", principal, err)
	}
}

func TestWorkloadCredentialCannotExpire(t *testing.T) {
	principal := workloadPrincipal(AccessNamespace, AccessRead)
	principal.ExpiresAt = time.Now()
	if err := principal.Validate(); err == nil {
		t.Fatal("workload credential with an expiry validated")
	}
}

func TestListAndRevokeOperatorCredentials(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	mgr := NewTokenManager(store, "test")
	first, firstCredential, err := mgr.CreateOperatorToken(ctx, Principal{Scope: AccessCluster, Access: AccessWrite})
	if err != nil {
		t.Fatal(err)
	}
	second, secondCredential, err := mgr.CreateOperatorToken(ctx, Principal{Scope: AccessNamespace, Access: AccessRead, Namespace: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.WorkloadToken(ctx, testSealer(t, store), "alloc-1", 1, workloadPrincipal(AccessNamespace, AccessRead)); err != nil {
		t.Fatal(err)
	}

	listed, err := mgr.ListOperatorCredentials(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 2 {
		t.Fatalf("listed %d credentials, want the two operator credentials only: %+v", len(listed), listed)
	}
	ids := map[string]bool{listed[0].ID: true, listed[1].ID: true}
	if !ids[firstCredential.ID] || !ids[secondCredential.ID] {
		t.Fatalf("listed IDs = %v, want %s and %s", ids, firstCredential.ID, secondCredential.ID)
	}
	for _, credential := range listed {
		if strings.Contains(credential.ID, first) || strings.Contains(credential.ID, second) {
			t.Fatal("listing exposed a bearer token")
		}
	}

	if err := mgr.RevokeOperatorCredential(ctx, firstCredential.ID); err != nil {
		t.Fatal(err)
	}
	if principal, _ := mgr.ValidateToken(ctx, first); principal != nil {
		t.Fatal("revoked credential still authenticates")
	}
	if principal, _ := mgr.ValidateToken(ctx, second); principal == nil {
		t.Fatal("revoking one credential revoked another")
	}
	if err := mgr.RevokeOperatorCredential(ctx, firstCredential.ID); !errors.Is(err, ErrCredentialNotFound) {
		t.Fatalf("second revoke error = %v, want ErrCredentialNotFound", err)
	}
	if err := mgr.RevokeOperatorCredential(ctx, "not-an-id"); !errors.Is(err, ErrCredentialNotFound) {
		t.Fatalf("malformed ID error = %v, want ErrCredentialNotFound", err)
	}
}

func TestRevokeOperatorCredentialIgnoresWorkloadCredentials(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	mgr := NewTokenManager(store, "test")
	token, err := mgr.WorkloadToken(ctx, testSealer(t, store), "alloc-1", 1, workloadPrincipal(AccessNamespace, AccessRead))
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(token))
	if err := mgr.RevokeOperatorCredential(ctx, CredentialID(hex.EncodeToString(digest[:]))); !errors.Is(err, ErrCredentialNotFound) {
		t.Fatalf("revoke workload credential error = %v, want ErrCredentialNotFound", err)
	}
	if principal, _ := mgr.ValidateToken(ctx, token); principal == nil {
		t.Fatal("operator revocation removed a workload credential")
	}
}

func TestCredentialNamespacesIgnoreExpiredCredentials(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	mgr := NewTokenManager(newMemStore(), "test")
	mgr.SetClock(func() time.Time { return now })
	if _, _, err := mgr.CreateOperatorToken(ctx, Principal{Scope: AccessNamespace, Access: AccessRead, Namespace: "tmp", ExpiresAt: now.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if got, _ := mgr.CredentialNamespaces(ctx); len(got) != 1 || got[0] != "tmp" {
		t.Fatalf("namespaces = %v, want [tmp]", got)
	}
	now = now.Add(time.Hour)
	if got, _ := mgr.CredentialNamespaces(ctx); len(got) != 0 {
		t.Fatalf("namespaces after expiry = %v, want none", got)
	}
}
