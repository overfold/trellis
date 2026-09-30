package secrets

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"path/filepath"
	"testing"

	"github.com/overfold/trellis/orchestrator/internal/state"
)

func TestStoreIsWriteOnlyAndEncryptedAtRest(t *testing.T) {
	bolt, err := state.NewBoltStore(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bolt.Close() }()
	store, err := NewStore(bolt, "cluster", "key-1", bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	value := []byte("sentinel-plaintext-value")
	zero := uint64(0)
	meta, err := store.Set(ctx, "acme", "database", value, &zero)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Version != 1 || meta.Name != "database" {
		t.Fatalf("unexpected metadata: %#v", meta)
	}
	raw, err := bolt.List(ctx, "trellis/cluster/secrets/")
	if err != nil {
		t.Fatal(err)
	}
	for _, encoded := range raw {
		if bytes.Contains(encoded, value) {
			t.Fatal("persisted record contains plaintext")
		}
	}
	got, version, err := store.Resolve(ctx, "acme", "database")
	if err != nil {
		t.Fatal(err)
	}
	if version != 1 || !bytes.Equal(got, value) {
		t.Fatalf("resolved %q version %d", got, version)
	}
	wrong := uint64(0)
	if _, err := store.Set(ctx, "acme", "database", []byte("new"), &wrong); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("expected version conflict, got %v", err)
	}
	one := uint64(1)
	if _, err := store.Set(ctx, "acme", "database", []byte("new"), &one); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, "acme", "database"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetMetadata(ctx, "acme", "database"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestStoreRejectsWrongKeyAndOversizedValues(t *testing.T) {
	bolt, err := state.NewBoltStore(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bolt.Close() }()
	ctx := context.Background()
	store, _ := NewStore(bolt, "cluster", "key-1", bytes.Repeat([]byte{1}, 32))
	if _, err := store.Set(ctx, "acme", "boundary", make([]byte, MaxValueSize), nil); err != nil {
		t.Fatalf("exact size boundary: %v", err)
	}
	if _, err := store.Set(ctx, "acme", "secret", make([]byte, MaxValueSize+1), nil); err == nil {
		t.Fatal("expected size error")
	}
	if _, err := store.Set(ctx, "acme", "secret", []byte("value"), nil); err != nil {
		t.Fatal(err)
	}
	other, _ := NewStore(bolt, "cluster", "key-2", bytes.Repeat([]byte{2}, 32))
	if _, _, err := other.Resolve(ctx, "acme", "secret"); err == nil {
		t.Fatal("expected unavailable key error")
	}
}

func TestStoreAADPreventsReplayAcrossSecretIdentity(t *testing.T) {
	bolt, err := state.NewBoltStore(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bolt.Close() }()
	ctx := context.Background()
	store, _ := NewStore(bolt, "cluster", "key-1", bytes.Repeat([]byte{1}, 32))
	if _, err := store.Set(ctx, "source", "token", []byte("source-value"), nil); err != nil {
		t.Fatal(err)
	}
	raw, err := bolt.Get(ctx, store.key("source", "token"))
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []struct{ namespace, name string }{{"other", "token"}, {"source", "other"}} {
		var replay record
		if err := json.Unmarshal(raw, &replay); err != nil {
			t.Fatal(err)
		}
		replay.Namespace, replay.Name = target.namespace, target.name
		encoded, _ := json.Marshal(replay)
		key := "trellis/cluster/secrets/" + url.PathEscape(target.namespace) + "/" + url.PathEscape(target.name)
		if err := bolt.Put(ctx, key, encoded); err != nil {
			t.Fatal(err)
		}
		if _, _, err := store.Resolve(ctx, target.namespace, target.name); err == nil {
			t.Fatalf("replayed ciphertext resolved as %s/%s", target.namespace, target.name)
		}
	}
	value, _, err := store.Resolve(ctx, "source", "token")
	if err != nil || string(value) != "source-value" {
		t.Fatalf("matching identity resolution = %q, %v", value, err)
	}
}

func TestSealBindsAssociatedDataAndKey(t *testing.T) {
	store, err := NewStore(nil, "cluster", "key-1", bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}
	value := []byte("sentinel-plaintext-value")
	sealed, err := store.Seal(value, []byte("alloc-1"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(sealed, value) {
		t.Fatal("sealed value contains plaintext")
	}
	opened, err := store.Open(sealed, []byte("alloc-1"))
	if err != nil || !bytes.Equal(opened, value) {
		t.Fatalf("Open = %q, %v", opened, err)
	}
	if _, err := store.Open(sealed, []byte("alloc-2")); err == nil {
		t.Fatal("sealed value opened with different associated data")
	}
	other, err := NewStore(nil, "cluster", "key-2", bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.Open(sealed, []byte("alloc-1")); err == nil {
		t.Fatal("sealed value opened under a different key ID")
	}
}
