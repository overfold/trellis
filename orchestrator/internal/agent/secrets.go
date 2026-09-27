package agent

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/runtime"
	"github.com/clofour/trellis/internal/spec"
)

// defaultSecretRoot is the Trellis-owned, memory-backed directory holding
// per-allocation secret files. Directory names are derived from allocation
// IDs so a restarted agent can find files written before a crash even when
// the allocation record never learned about them.
const defaultSecretRoot = "/dev/shm/trellis-secrets"

func taskHasFileSecrets(taskName string, delivered []api.DeliveredSecret) bool {
	for _, secret := range delivered {
		if secret.Task == taskName && secret.Target == spec.SecretTargetFile {
			return true
		}
	}
	return false
}

// secretDirFor returns the deterministic secret directory for an allocation,
// creating the private secret root if needed. It does not create the
// allocation directory itself.
func (a *Agent) secretDirFor(allocID string) (string, error) {
	if err := os.Mkdir(a.secretRoot, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
		return "", fmt.Errorf("create memory-backed secret root: %w", err)
	}
	if err := checkSecretRoot(a.secretRoot); err != nil {
		return "", err
	}
	name := base64.RawURLEncoding.EncodeToString([]byte(allocID))
	if len(name) > 255 {
		return "", fmt.Errorf("allocation ID is too long for a secret directory name")
	}
	return filepath.Join(a.secretRoot, name), nil
}

// checkSecretRoot rejects a secret root that another user could have
// prepared or can read, such as a symlink planted in world-writable /dev/shm.
func checkSecretRoot(root string) error {
	info, err := os.Lstat(root)
	if err != nil {
		return fmt.Errorf("inspect secret root: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("secret root %s is not a directory", root)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("secret root %s is accessible to other users", root)
	}
	if stat, ok := info.Sys().(*syscall.Stat_t); ok && int(stat.Uid) != os.Geteuid() {
		return fmt.Errorf("secret root %s is not owned by the agent user", root)
	}
	return nil
}

// materializeSecrets returns the task's environment secrets and writes its
// file secrets below dir, which must be set when the task has file secrets.
// Any stale directory at dir is replaced.
func materializeSecrets(dir, taskName string, delivered []api.DeliveredSecret) (map[string]string, []*runtime.Mount, error) {
	env := map[string]string{}
	var taskSecrets []api.DeliveredSecret
	for _, secret := range delivered {
		if secret.Task == taskName {
			taskSecrets = append(taskSecrets, secret)
		}
	}
	if dir != "" {
		if err := os.RemoveAll(dir); err != nil {
			return nil, nil, fmt.Errorf("remove stale secret directory: %w", err)
		}
		if err := os.Mkdir(dir, 0o700); err != nil {
			return nil, nil, fmt.Errorf("create memory-backed secret directory: %w", err)
		}
		if err := os.Chmod(dir, 0o700); err != nil {
			_ = os.RemoveAll(dir)
			return nil, nil, err
		}
	}
	var mounts []*runtime.Mount
	for i, secret := range taskSecrets {
		switch secret.Target {
		case spec.SecretTargetEnv:
			env[secret.Env] = string(secret.Value)
		case spec.SecretTargetFile:
			if dir == "" {
				return nil, nil, fmt.Errorf("secret directory is required for file secrets")
			}
			hostPath := filepath.Join(dir, fmt.Sprintf("secret-%d", i))
			mode := os.FileMode(secret.Mode)
			if mode == 0 {
				mode = 0o400
			}
			file, err := os.OpenFile(hostPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, mode)
			if err != nil {
				_ = os.RemoveAll(dir)
				return nil, nil, fmt.Errorf("create secret file: %w", err)
			}
			if _, err = file.Write(secret.Value); err == nil {
				err = file.Sync()
			}
			closeErr := file.Close()
			if err == nil {
				err = closeErr
			}
			if err != nil {
				_ = os.RemoveAll(dir)
				return nil, nil, fmt.Errorf("write secret file: %w", err)
			}
			mounts = append(mounts, &runtime.Mount{HostPath: hostPath, ContainerPath: secret.Path, ReadOnly: true})
		default:
			if dir != "" {
				_ = os.RemoveAll(dir)
			}
			return nil, nil, fmt.Errorf("unsupported secret target")
		}
	}
	return env, mounts, nil
}

// removeOrphanedSecretDirs deletes secret directories that no allocation
// record or in-memory allocation owns. They are left behind when the agent
// stops after writing secrets but before the allocation record is durable.
// It runs after recovery and before the agent accepts allocation requests.
func (a *Agent) removeOrphanedSecretDirs() {
	if a.local == nil {
		return
	}
	entries, err := os.ReadDir(a.secretRoot)
	if errors.Is(err, fs.ErrNotExist) {
		return
	}
	if err != nil {
		a.log.Error("skip orphaned secret sweep: list secret root", "error", err)
		return
	}
	if err := checkSecretRoot(a.secretRoot); err != nil {
		a.log.Error("skip orphaned secret sweep", "error", err)
		return
	}
	// Record file names encode allocation IDs exactly as secret directory
	// names do. A record recovery could not parse still claims its directory.
	records, recordErrs := a.local.ListRaw("agent/allocations")
	if len(recordErrs) > 0 {
		a.log.Error("skip orphaned secret sweep: allocation records are unreadable", "error", errors.Join(recordErrs...))
		return
	}
	owned := make(map[string]bool, len(records))
	for name := range records {
		owned[name] = true
	}
	a.mu.RLock()
	for id := range a.allocations {
		owned[base64.RawURLEncoding.EncodeToString([]byte(id))] = true
	}
	a.mu.RUnlock()
	for _, entry := range entries {
		if owned[entry.Name()] {
			continue
		}
		path := filepath.Join(a.secretRoot, entry.Name())
		if err := os.RemoveAll(path); err != nil {
			a.log.Error("remove orphaned secret directory", "path", path, "error", err)
			continue
		}
		a.log.Info("removed orphaned secret directory", "path", path)
	}
}
