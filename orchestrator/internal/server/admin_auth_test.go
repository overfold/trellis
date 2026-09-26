package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"os"
	"testing"

	"github.com/clofour/trellis/internal/storage"
)

func TestJoiningServerUsesReplicatedAdminVerificationWithoutCredential(t *testing.T) {
	ctx := context.Background()
	store := memoryStore{}
	state := NewStateController(store, "test")
	token := "operator-side-secret"
	digest := sha256.Sum256([]byte(token))
	if err := state.PutCluster(ctx, &Cluster{Hash: hex.EncodeToString(digest[:])}); err != nil {
		t.Fatal(err)
	}
	local := storage.NewLocalStorage(t.TempDir())
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	if err := local.Put("token", "legacy-raw-token"); err != nil {
		t.Fatal(err)
	}
	control := NewServer(slog.Default(), local, state, store, "test", "node-b:8128")
	if err := control.Init(ctx, ""); err != nil {
		t.Fatalf("initialize joining server without administrator material: %v", err)
	}
	if !control.ValidateAPIToken(token) {
		t.Fatal("replicated administrator verification material did not authenticate the operator token")
	}
	var retained string
	if err := local.Get("token", &retained); !os.IsNotExist(unwrapPathError(err)) {
		t.Fatalf("legacy raw administrator token was retained: value=%q err=%v", retained, err)
	}
}

func TestInitialServerRequiresAdminVerificationHash(t *testing.T) {
	store := memoryStore{}
	local := storage.NewLocalStorage(t.TempDir())
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	control := NewServer(slog.Default(), local, NewStateController(store, "test"), store, "test", "node-a:8128")
	if err := control.Init(context.Background(), ""); err == nil {
		t.Fatal("initialized a new cluster without administrator verification material")
	}
}
