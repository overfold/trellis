package server

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"log/slog"
	"testing"

	"github.com/clofour/trellis/internal/storage"
)

func encodedAdministratorPublicKey(t *testing.T) (ed25519.PublicKey, string) {
	t.Helper()
	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	return publicKey, base64.RawStdEncoding.EncodeToString(der)
}

func TestJoiningServerUsesReplicatedAdministratorPublicKeyWithoutPrivateKey(t *testing.T) {
	ctx := context.Background()
	store := memoryStore{}
	state := NewStateController(store, "test")
	publicKey, encoded := encodedAdministratorPublicKey(t)
	if err := state.PutCluster(ctx, &Cluster{AdministratorPublicKey: encoded, ControlEpoch: 4}); err != nil {
		t.Fatal(err)
	}
	local := storage.NewLocalStorage(t.TempDir())
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	control := NewServer(slog.Default(), local, state, store, "test", "node-b:8128")
	if err := control.Init(ctx, ""); err != nil {
		t.Fatalf("initialize joining server without administrator private key: %v", err)
	}
	got, epoch, ok := control.AdministratorVerification()
	if !ok || !publicKey.Equal(got) || epoch != 4 {
		t.Fatalf("replicated administrator verification = (%v, %d, %t)", got, epoch, ok)
	}
}

func TestInitialServerStoresOnlyAdministratorPublicKey(t *testing.T) {
	store := memoryStore{}
	local := storage.NewLocalStorage(t.TempDir())
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	publicKey, encoded := encodedAdministratorPublicKey(t)
	control := NewServer(slog.Default(), local, NewStateController(store, "test"), store, "test", "node-a:8128")
	if err := control.Init(context.Background(), encoded); err != nil {
		t.Fatal(err)
	}
	got, _, ok := control.AdministratorVerification()
	if !ok || !publicKey.Equal(got) {
		t.Fatal("initial administrator public key was not retained")
	}
}

func TestInitialServerRequiresValidAdministratorPublicKey(t *testing.T) {
	store := memoryStore{}
	local := storage.NewLocalStorage(t.TempDir())
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	control := NewServer(slog.Default(), local, NewStateController(store, "test"), store, "test", "node-a:8128")
	if err := control.Init(context.Background(), ""); err == nil {
		t.Fatal("initialized a new cluster without an administrator public key")
	}
}
