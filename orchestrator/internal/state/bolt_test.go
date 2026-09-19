package state

import (
	"context"
	"path/filepath"
	"testing"
)

func TestBoltStoreRoundTrip(t *testing.T) {
	store, err := NewBoltStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	ctx := context.Background()

	val, err := store.Get(ctx, "missing")
	if err != nil {
		t.Fatal(err)
	}
	if val != nil {
		t.Fatalf("expected nil for missing key, got %q", val)
	}

	if err := store.Put(ctx, "key1", []byte("value1")); err != nil {
		t.Fatal(err)
	}
	val, err = store.Get(ctx, "key1")
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "value1" {
		t.Fatalf("expected value1, got %q", val)
	}
}

func TestBoltStoreListPrefix(t *testing.T) {
	store, err := NewBoltStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	ctx := context.Background()

	_ = store.Put(ctx, "trellis/default/jobs/web", []byte("web"))
	_ = store.Put(ctx, "trellis/default/jobs/api", []byte("api"))
	_ = store.Put(ctx, "trellis/default/nodes/n1", []byte("node1"))
	if err := store.Put(ctx, "trellis/other/jobs/x", []byte("x")); err != nil {
		t.Fatal(err)
	}

	result, err := store.List(ctx, "trellis/default/jobs/")
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 results, got %d: %v", len(result), result)
	}
	if string(result["trellis/default/jobs/web"]) != "web" {
		t.Fatal("missing web entry")
	}
	if string(result["trellis/default/jobs/api"]) != "api" {
		t.Fatal("missing api entry")
	}
}

func TestBoltStoreDelete(t *testing.T) {
	store, err := NewBoltStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	ctx := context.Background()

	if err := store.Put(ctx, "key", []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, "key"); err != nil {
		t.Fatal(err)
	}
	val, err := store.Get(ctx, "key")
	if err != nil {
		t.Fatal(err)
	}
	if val != nil {
		t.Fatalf("expected nil after delete, got %q", val)
	}

	if err := store.Delete(ctx, "nonexistent"); err != nil {
		t.Fatal("delete of missing key should not error")
	}
}

func TestRestoreDesiredKeepsRuntimeStateAndRequiresFreshTarget(t *testing.T) {
	store, err := NewBoltStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	ctx := context.Background()
	if err := store.Put(ctx, "trellis/new/nodes/local", []byte(`{"id":"local"}`)); err != nil {
		t.Fatal(err)
	}
	snapshot := &DesiredSnapshot{
		Jobs:                     map[string][]byte{"web": []byte(`{"revision":3}`)},
		Secrets:                  map[string][]byte{"prod/token": []byte(`{"ciphertext":"encrypted"}`)},
		NetworkPortRegistrations: map[string][]byte{"acme": []byte(`{"namespace":"acme","slot":7}`)},
	}
	if err := store.RestoreDesired("new", snapshot); err != nil {
		t.Fatal(err)
	}
	all, err := store.List(ctx, "trellis/new/")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 4 ||
		all["trellis/new/nodes/local"] == nil ||
		all["trellis/new/jobs/web"] == nil ||
		all["trellis/new/secrets/prod/token"] == nil ||
		all["trellis/new/network-port-registrations/acme"] == nil {
		t.Fatalf("unexpected restored state: %#v", all)
	}
	if err := store.RestoreDesired("new", snapshot); err == nil {
		t.Fatal("expected restore into non-fresh desired state to fail")
	}
}

var _ Store = (*BoltStore)(nil)
