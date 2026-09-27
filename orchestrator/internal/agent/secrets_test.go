package agent

import (
	"context"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/spec"
	"github.com/clofour/trellis/internal/storage"
)

func TestMaterializeSecretsDeliversEnvAndMemoryBackedFile(t *testing.T) {
	delivered := []api.DeliveredSecret{
		{Task: "api", Name: "password", Target: spec.SecretTargetEnv, Env: "PASSWORD", Value: []byte("env-value")},
		{Task: "api", Name: "key", Target: spec.SecretTargetFile, Path: "/run/trellis-secrets/key", Mode: 0o400, Value: []byte("file-value")},
		{Task: "other", Name: "ignored", Target: spec.SecretTargetEnv, Env: "IGNORED", Value: []byte("ignored")},
	}
	dir := filepath.Join(t.TempDir(), "alloc")
	env, mounts, err := materializeSecrets(dir, "api", delivered)
	if err != nil {
		t.Fatal(err)
	}
	if env["PASSWORD"] != "env-value" || env["IGNORED"] != "" {
		t.Fatalf("unexpected env: %#v", env)
	}
	if len(mounts) != 1 || !mounts[0].ReadOnly || mounts[0].ContainerPath != "/run/trellis-secrets/key" || filepath.Dir(mounts[0].HostPath) != dir {
		t.Fatalf("unexpected mounts: %#v", mounts)
	}
	value, err := os.ReadFile(mounts[0].HostPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(value) != "file-value" {
		t.Fatalf("got %q", value)
	}
	info, err := os.Stat(mounts[0].HostPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o400 {
		t.Fatalf("mode is %o", info.Mode().Perm())
	}
	if info, err := os.Stat(dir); err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("secret directory = %v, %v", info, err)
	}
}

func TestMaterializeSecretsReplacesStaleDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "alloc")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "stale"), []byte("old"), 0o400); err != nil {
		t.Fatal(err)
	}
	delivered := []api.DeliveredSecret{{Task: "api", Name: "key", Target: spec.SecretTargetFile, Path: "/run/trellis-secrets/key", Value: []byte("new")}}
	if _, _, err := materializeSecrets(dir, "api", delivered); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "stale")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale secret file survived: %v", err)
	}
}

func TestMaterializeSecretsEnvOnlyNeedsNoDirectory(t *testing.T) {
	delivered := []api.DeliveredSecret{{Task: "api", Name: "password", Target: spec.SecretTargetEnv, Env: "PASSWORD", Value: []byte("value")}}
	if taskHasFileSecrets("api", delivered) {
		t.Fatal("environment-only task reported file secrets")
	}
	env, mounts, err := materializeSecrets("", "api", delivered)
	if err != nil || env["PASSWORD"] != "value" || len(mounts) != 0 {
		t.Fatalf("env = %#v, mounts = %#v, error = %v", env, mounts, err)
	}
}

func TestSecretDirForIsRecordedAndDeterministicAcrossRestart(t *testing.T) {
	local := storage.NewLocalStorage(t.TempDir())
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	agent := newOperationTestAgent(t, &reconcilerRuntime{})
	agent.ConfigureDurability(local, "test")
	dir, err := agent.secretDirFor("allocation-g2-first")
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Dir(dir)
	if filepath.Base(dir) != base64.RawURLEncoding.EncodeToString([]byte("allocation-g2-first")) {
		t.Fatalf("secret dir = %q", dir)
	}
	if info, err := os.Lstat(root); err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
		t.Fatalf("secret root = %v, %v", info, err)
	}
	var recorded string
	if err := local.Get(secretRootKey, &recorded); err != nil || recorded != root {
		t.Fatalf("recorded secret root = %q, %v, want %q", recorded, err, root)
	}

	restarted := newOperationTestAgent(t, &reconcilerRuntime{})
	restarted.ConfigureDurability(local, "test")
	again, err := restarted.secretDirFor("allocation-g2-first")
	if err != nil || again != dir {
		t.Fatalf("secret dir after restart = %q, %v, want %q", again, err, dir)
	}
}

func TestSecretDirForReplacesUntrustworthyRecordedRoot(t *testing.T) {
	for name, prepare := range map[string]func(t *testing.T, path string){
		"missing": func(*testing.T, string) {},
		"symlink": func(t *testing.T, path string) {
			if err := os.Symlink(t.TempDir(), path); err != nil {
				t.Fatal(err)
			}
		},
		"shared": func(t *testing.T, path string) {
			if err := os.Mkdir(path, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(path, 0o755); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			local := storage.NewLocalStorage(t.TempDir())
			if err := local.Init(); err != nil {
				t.Fatal(err)
			}
			agent := newOperationTestAgent(t, &reconcilerRuntime{})
			agent.ConfigureDurability(local, "test")
			untrusted := filepath.Join(agent.secretBase, "trellis-secrets-untrusted")
			prepare(t, untrusted)
			if err := local.Put(secretRootKey, untrusted); err != nil {
				t.Fatal(err)
			}
			dir, err := agent.secretDirFor("allocation")
			if err != nil {
				t.Fatal(err)
			}
			if root := filepath.Dir(dir); root == untrusted {
				t.Fatalf("reused untrustworthy secret root %q", root)
			}
			var recorded string
			if err := local.Get(secretRootKey, &recorded); err != nil || recorded != filepath.Dir(dir) {
				t.Fatalf("recorded secret root = %q, %v", recorded, err)
			}
		})
	}
}

type pullHookRuntime struct {
	*reconcilerRuntime
	onPull func() error
}

func (r *pullHookRuntime) Pull(context.Context, string) error { return r.onPull() }

func TestRunAllocationRecordsSecretDirBeforeWritingSecrets(t *testing.T) {
	rt := &pullHookRuntime{reconcilerRuntime: &reconcilerRuntime{}}
	agent := newOperationTestAgent(t, rt)
	root := t.TempDir()
	local := storage.NewLocalStorage(root)
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	agent.ConfigureDurability(local, "test")
	recordDir := filepath.Join(root, "agent", "allocations")
	// Fail the first durable write after image pull, which must be the
	// secret-location record rather than anything after plaintext exists.
	rt.onPull = func() error {
		if err := os.RemoveAll(recordDir); err != nil {
			return err
		}
		return os.WriteFile(recordDir, []byte("blocked"), 0o600)
	}
	request := operationTestRequest()
	request.Tasks = []spec.TaskSpec{{Name: "first", Image: "image"}}
	request.Secrets = []api.DeliveredSecret{{Task: "first", Name: "key", Target: spec.SecretTargetFile, Path: "/run/trellis-secrets/key", Value: []byte("secret")}}
	err := agent.RunGroup(context.Background(), request)
	if err == nil || !strings.Contains(err.Error(), "persist secret metadata") {
		t.Fatalf("run error = %v, want secret metadata persistence failure", err)
	}
	entries, err := os.ReadDir(agent.secretRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("secret material written before its location was durable: %v", entries)
	}
}

func TestRunAllocationUsesRecoverableSecretDir(t *testing.T) {
	agent := newOperationTestAgent(t, &reconcilerRuntime{})
	root := t.TempDir()
	local := storage.NewLocalStorage(root)
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	agent.ConfigureDurability(local, "test")
	request := operationTestRequest()
	request.Tasks = []spec.TaskSpec{{Name: "first", Image: "image"}}
	request.Secrets = []api.DeliveredSecret{{Task: "first", Name: "key", Target: spec.SecretTargetFile, Path: "/run/trellis-secrets/key", Value: []byte("secret")}}
	if err := agent.RunGroup(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	id := "allocation-g2-first"
	want := filepath.Join(agent.secretRoot, base64.RawURLEncoding.EncodeToString([]byte(id)))
	var recorded Allocation
	if err := local.Get(allocationRecordKey(id), &recorded); err != nil || recorded.SecretDir != want {
		t.Fatalf("record = %+v, error = %v, want secret dir %q", recorded, err, want)
	}
	if _, err := os.Stat(filepath.Join(want, "secret-0")); err != nil {
		t.Fatalf("secret file: %v", err)
	}
}

func TestRemoveOrphanedSecretDirsKeepsOwnedDirectories(t *testing.T) {
	agent := newOperationTestAgent(t, &reconcilerRuntime{})
	local := storage.NewLocalStorage(t.TempDir())
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	agent.ConfigureDurability(local, "test")
	recorded := &Allocation{ID: "recorded", ContainerID: "recorded", Status: "running"}
	if err := agent.persistAllocation(recorded); err != nil {
		t.Fatal(err)
	}
	if err := local.Put(allocationRecordKey("malformed"), "not an allocation"); err != nil {
		t.Fatal(err)
	}
	agent.allocations["in-memory"] = &Allocation{ID: "in-memory"}
	dirs := map[string]string{}
	for _, id := range []string{"recorded", "malformed", "in-memory", "orphan"} {
		dir, err := agent.secretDirFor(id)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := materializeSecrets(dir, "task", []api.DeliveredSecret{{Task: "task", Target: spec.SecretTargetFile, Path: "/run/trellis-secrets/key", Value: []byte("secret")}}); err != nil {
			t.Fatal(err)
		}
		dirs[id] = dir
	}

	agent.removeOrphanedSecretDirs()

	for _, id := range []string{"recorded", "malformed", "in-memory"} {
		if _, err := os.Stat(dirs[id]); err != nil {
			t.Fatalf("owned secret directory %s removed: %v", id, err)
		}
	}
	if _, err := os.Stat(dirs["orphan"]); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphaned secret directory survived: %v", err)
	}
}

func TestRemoveOrphanedSecretDirsSkipsWithoutVerifiableOwnership(t *testing.T) {
	// Without durable records there is no recorded root or ownership to verify.
	agent := newOperationTestAgent(t, &reconcilerRuntime{})
	dir, err := agent.secretDirFor("orphan")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	agent.removeOrphanedSecretDirs()
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("secret directory removed without durable records: %v", err)
	}

	// An unreadable record may belong to any allocation.
	root := t.TempDir()
	local := storage.NewLocalStorage(root)
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	agent = newOperationTestAgent(t, &reconcilerRuntime{})
	agent.ConfigureDurability(local, "test")
	dir, err = agent.secretDirFor("orphan")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	recordDir := filepath.Join(root, "agent", "allocations")
	if err := os.MkdirAll(recordDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "missing"), filepath.Join(recordDir, "unreadable")); err != nil {
		t.Fatal(err)
	}
	agent.removeOrphanedSecretDirs()
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("secret directory removed with unreadable records: %v", err)
	}
}
