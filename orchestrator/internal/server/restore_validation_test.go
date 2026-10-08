package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"maps"
	"net/http"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	secretstore "github.com/overfold/trellis/orchestrator/internal/secrets"
	"github.com/overfold/trellis/orchestrator/internal/state"
)

// Exercise the operator preflight against both atomic Bolt installation and
// actual Raft submission. A rejected backup must not consume a fresh target.
func TestRestoreSecretKeyAvailability(t *testing.T) {
	source := &backupStore{data: memoryStore{}, snapshot: &state.DesiredSnapshot{}}
	sourceServer := newBackupTestServer(t, source, DefaultClusterSettings())
	key := bytes.Repeat([]byte{7}, 32)
	owner, err := secretstore.NewStore(source, "test", "original", key)
	if err != nil {
		t.Fatal(err)
	}
	source.snapshot.Secrets = map[string][]byte{}
	for _, name := range []string{"first", "second"} {
		if _, err := owner.Set(t.Context(), "default", name, []byte("recover-"+name), nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"first", "second"} {
		source.snapshot.Secrets["default/"+name] = source.data["trellis/test/secrets/default/"+name]
	}
	backup := mustBackup(t, sourceServer)
	for _, backend := range []string{"bolt", "raft"} {
		for _, tc := range []struct {
			name, id, message string
			key               []byte
		}{
			{"matching", "original", "", key},
			{"missing-key", "different", "unavailable", bytes.Repeat([]byte{8}, 32)},
			{"wrong-bytes-same-id", "original", "unwrap data encryption key", bytes.Repeat([]byte{8}, 32)},
			{"wrong-explicit-id", "different", "unavailable", key},
			{"no-store", "", "secrets store is unavailable", nil},
		} {
			for _, fresh := range []bool{true, false} {
				t.Run(backend+"/"+tc.name+"/fresh="+map[bool]string{true: "true", false: "false"}[fresh], func(t *testing.T) {
					var store interface {
						state.Store
						desiredStore
						Close() error
					}
					if backend == "bolt" {
						bolt, openErr := state.NewBoltStore(filepath.Join(t.TempDir(), "state.db"))
						if openErr != nil {
							t.Fatal(openErr)
						}
						store = &restoreBoltStore{bolt}
					} else {
						raft, openErr := state.NewRaftStore(state.RaftConfig{DataDir: t.TempDir(), BindAddr: "127.0.0.1:0", ServerID: "restore-test", Bootstrap: true})
						if openErr != nil {
							t.Fatal(openErr)
						}
						store = raft
						deadline := time.Now().Add(10 * time.Second)
						for leader, _ := raft.Raft().LeaderWithID(); leader == "" && time.Now().Before(deadline); leader, _ = raft.Raft().LeaderWithID() {
							time.Sleep(20 * time.Millisecond)
						}
					}
					t.Cleanup(func() { _ = store.Close() })
					s := NewServer(slog.Default(), nil, NewStateController(store, "test"), store, "test", "")
					_, public := encodedAdministratorPublicKey(t)
					if err := s.Init(t.Context(), ClusterBootstrap{AdministratorPublicKey: public, Settings: DefaultClusterSettings()}); err != nil {
						t.Fatal(err)
					}
					if tc.key != nil {
						s.secrets, err = secretstore.NewStore(store, "test", tc.id, tc.key)
						if err != nil {
							t.Fatal(err)
						}
					}
					if !fresh {
						if err := store.Put(t.Context(), "trellis/test/replacement-backoffs/retained", []byte("{}")); err != nil {
							t.Fatal(err)
						}
					}
					before, err := store.List(t.Context(), "trellis/")
					if err != nil {
						t.Fatal(err)
					}
					response := requestAdmin(t.Context(), t, s, http.MethodPost, "/v1/backup/restore", backup)
					if tc.message != "" || !fresh {
						if response.Code != http.StatusConflict {
							t.Fatalf("status %d: %s", response.Code, response.Body.String())
						}
						message := tc.message
						if message == "" {
							message = "fresh cluster"
						}
						if !strings.Contains(response.Body.String(), message) || (tc.message != "" && !strings.Contains(response.Body.String(), "original secrets_key")) {
							t.Fatalf("not actionable: %s", response.Body.String())
						}
						after, err := store.List(t.Context(), "trellis/")
						if err != nil || !reflect.DeepEqual(before, after) {
							t.Fatalf("rejected restore changed state: %v", err)
						}
						if !fresh {
							return
						}
						// Fixing local key configuration must make the same backup
						// recoverable without replacing an already-consumed target.
						s.secrets, err = secretstore.NewStore(store, "test", "original", key)
						if err != nil {
							t.Fatal(err)
						}
						response = requestAdmin(t.Context(), t, s, http.MethodPost, "/v1/backup/restore", backup)
					}
					if response.Code != http.StatusNoContent {
						t.Fatalf("status %d: %s", response.Code, response.Body.String())
					}
					for _, name := range []string{"first", "second"} {
						value, version, err := s.secrets.Resolve(t.Context(), "default", name)
						if err != nil || version != 1 || string(value) != "recover-"+name {
							t.Fatalf("restored secret %s version %d: %v", name, version, err)
						}
						clear(value)
					}
				})
			}
		}
	}
}

type restoreBoltStore struct{ *state.BoltStore }

func (b *restoreBoltStore) BackupDesired(cluster string) (*state.DesiredSnapshot, error) {
	return b.DesiredSnapshot(cluster)
}

type restoreFailureStore struct {
	*backupStore
	stage string
}

func (b *restoreFailureStore) RestoreDesired(cluster string, snapshot *state.DesiredSnapshot) error {
	if b.stage == "barrier" {
		return errors.New("raft barrier failed")
	}
	if b.stage == "commit" {
		return errors.New("raft commit failed")
	}
	return b.backupStore.RestoreDesired(cluster, snapshot)
}

func (b *restoreFailureStore) Get(ctx context.Context, key string) ([]byte, error) {
	if b.stage == "read" {
		return nil, errors.New("storage read failed")
	}
	return b.backupStore.Get(ctx, key)
}

func (b *restoreFailureStore) List(ctx context.Context, prefix string) (map[string][]byte, error) {
	if b.stage == "reload" {
		return nil, errors.New("reload read failed")
	}
	return b.backupStore.List(ctx, prefix)
}

func TestRestoreInfrastructureFailuresRemainUnavailable(t *testing.T) {
	for _, stage := range []string{"read", "barrier", "commit", "reload", "no-backup-store"} {
		t.Run(stage, func(t *testing.T) {
			base := &backupStore{data: memoryStore{}, snapshot: &state.DesiredSnapshot{}}
			s := newBackupTestServer(t, base, DefaultClusterSettings())
			backup := mustBackup(t, s)
			failing := &restoreFailureStore{backupStore: base, stage: stage}
			s.state = NewStateController(failing, "test")
			s.backupStore = failing
			if stage == "no-backup-store" {
				s.backupStore = nil
			}
			response := requestAdmin(t.Context(), t, s, http.MethodPost, "/v1/backup/restore", backup)
			if response.Code != http.StatusServiceUnavailable {
				t.Fatalf("status %d: %s", response.Code, response.Body.String())
			}
		})
	}
}

func TestRestoreAuthenticatesEverySecretRecord(t *testing.T) {
	base := &backupStore{data: memoryStore{}, snapshot: &state.DesiredSnapshot{}}
	s := newBackupTestServer(t, base, DefaultClusterSettings())
	owner, err := secretstore.NewStore(base, "test", "key", bytes.Repeat([]byte{1}, 32))
	if err != nil {
		t.Fatal(err)
	}
	s.secrets = owner
	for _, name := range []string{"first", "token"} {
		if _, err := owner.Set(t.Context(), "default", name, []byte("plaintext"), nil); err != nil {
			t.Fatal(err)
		}
	}
	backup := mustBackup(t, s)
	before := base.snapshot
	var record map[string]any
	if err := json.Unmarshal(base.data["trellis/test/secrets/default/token"], &record); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"ciphertext", "wrapped_dek", "nonce", "wrap_nonce", "short-nonce", "short-wrap-nonce"} {
		t.Run(field, func(t *testing.T) {
			tampered := map[string]any{}
			maps.Copy(tampered, record)
			switch field {
			case "short-nonce":
				tampered["nonce"] = "AA"
			case "short-wrap-nonce":
				tampered["wrap_nonce"] = "AA"
			default:
				encoded := tampered[field].(string)
				if encoded[0] == 'A' {
					tampered[field] = "B" + encoded[1:]
				} else {
					tampered[field] = "A" + encoded[1:]
				}
			}
			raw, err := json.Marshal(tampered)
			if err != nil {
				t.Fatal(err)
			}
			backup.Secrets = map[string]json.RawMessage{"default/first": base.data["trellis/test/secrets/default/first"], "default/token": raw}
			if err := s.Restore(t.Context(), backup); err == nil || !strings.Contains(err.Error(), "validate secret") {
				t.Fatalf("tampered %s accepted: %v", field, err)
			}
			if base.snapshot != before {
				t.Fatal("rejected restore reached installation")
			}
		})
	}
}
