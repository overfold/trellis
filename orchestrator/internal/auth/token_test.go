package auth

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/clofour/trellis/internal/state"
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

func TestWorkloadTokenCarriesWorkloadKind(t *testing.T) {
	ctx := context.Background()
	mgr := NewTokenManager(newMemStore(), "test")

	token1, err := mgr.GetOrCreateWorkloadToken(ctx, AccessNamespace, AccessRead, "acme")
	if err != nil {
		t.Fatal(err)
	}
	token2, err := mgr.GetOrCreateWorkloadToken(ctx, AccessNamespace, AccessRead, "acme")
	if err != nil {
		t.Fatal(err)
	}
	if token1 != token2 {
		t.Fatalf("expected stable workload token, got %q and %q", token1, token2)
	}
	if !strings.HasPrefix(token1, "trls_wl_") {
		t.Fatalf("workload token has unexpected prefix: %q", token1)
	}

	saved, err := mgr.ValidateToken(ctx, token1)
	if err != nil {
		t.Fatal(err)
	}
	if saved == nil || saved.Kind != CredentialWorkload || saved.Scope != AccessNamespace || saved.Access != AccessRead || saved.Namespace != "acme" {
		t.Fatalf("unexpected workload principal: %#v", saved)
	}
}

func TestGetOrCreateWorkloadTokenConcurrent(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	mgr := NewTokenManager(store, "test")
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
			token, err := mgr.GetOrCreateWorkloadToken(ctx, AccessNamespace, AccessWrite, "acme")
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

func TestGetOrCreateWorkloadTokenBatchFailureLeavesNoCredential(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	store.batchError = errors.New("injected batch failure")
	mgr := NewTokenManager(store, "test")

	if _, err := mgr.GetOrCreateWorkloadToken(ctx, AccessNamespace, AccessRead, "acme"); err == nil || !strings.Contains(err.Error(), "injected batch failure") {
		t.Fatalf("GetOrCreateWorkloadToken error = %v, want injected failure", err)
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

func TestPrincipalValidation(t *testing.T) {
	for _, principal := range []Principal{
		{Kind: CredentialOperator, Scope: AccessNamespace, Access: AccessRead},
		{Kind: CredentialOperator, Scope: AccessCluster, Access: AccessWrite, Namespace: "acme"},
		{Kind: CredentialOperator, Scope: AccessNamespace, Access: AccessRead, Namespace: "acme", Subject: &CredentialSubject{Namespace: "acme", Job: "api", TaskGroup: "web"}},
		{Kind: CredentialWorkload, Scope: AccessNamespace, Access: AccessRead, Namespace: "other", Subject: &CredentialSubject{Namespace: "acme", Job: "api", TaskGroup: "web"}},
	} {
		if err := principal.Validate(); err == nil {
			t.Fatalf("expected principal to be invalid: %#v", principal)
		}
	}
}
