package agent

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	"github.com/overfold/trellis/orchestrator/internal/runtime"
	"github.com/overfold/trellis/orchestrator/internal/spec"
)

// defaultSecretBase is the memory-backed filesystem holding secret roots.
const defaultSecretBase = "/dev/shm"

const secretRootPrefix = "trellis-secrets-"
const tmpfsMagic = 0x01021994

// secretRootKey durably records this agent's secret root. The root name is
// random so other local users cannot predict it before first use and agents
// sharing a host never share a root. Recording it before first use lets a
// restarted agent find files written before a crash even when the allocation
// record never learned about them. A reboot empties /dev/shm, after which the
// recorded path is untrusted until checked again.
const secretRootKey = "agent/secret-root"

func taskHasSecrets(taskName string, delivered []nodeapi.DeliveredSecret) bool {
	for _, secret := range delivered {
		if secret.Task == taskName {
			return true
		}
	}
	return false
}

// secretDirFor returns the deterministic secret directory for an allocation
// below this agent's secret root. It refuses a path that already exists so a
// start never replaces or later removes files it did not write.
func (a *Agent) secretDirFor(allocID string) (string, error) {
	root, err := a.secretRootDir()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(root, allocationFileName(allocID))
	if _, err := os.Lstat(dir); err == nil {
		return "", fmt.Errorf("secret directory for %s already exists", allocID)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return "", fmt.Errorf("inspect secret directory: %w", err)
	}
	return dir, nil
}

// secretRootDir returns this agent's private secret root, reusing the
// recorded root and otherwise creating and recording a new one before use.
func (a *Agent) secretRootDir() (string, error) {
	statfs := a.secretStatfs
	if statfs == nil {
		statfs = syscall.Statfs
	}
	if err := verifyMemoryBackedFilesystem(a.secretBaseDir(), statfs); err != nil {
		return "", err
	}
	a.secretMu.Lock()
	defer a.secretMu.Unlock()
	if a.secretRoot != "" && checkSecretRoot(a.secretRoot) == nil {
		return a.secretRoot, nil
	}
	root, reuse, err := a.recordedSecretRoot()
	if err != nil {
		return "", err
	}
	if reuse {
		a.secretRoot = root
		return root, nil
	}
	root, err = os.MkdirTemp(a.secretBaseDir(), secretRootPrefix)
	if err != nil {
		return "", fmt.Errorf("create memory-backed secret root: %w", err)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		_ = os.Remove(root)
		return "", fmt.Errorf("restrict secret root: %w", err)
	}
	if a.local != nil {
		if err := a.local.Put(secretRootKey, root); err != nil {
			_ = os.Remove(root)
			return "", fmt.Errorf("persist secret root: %w", err)
		}
	}
	a.secretRoot = root
	return root, nil
}

func verifyMemoryBackedFilesystem(path string, statfs func(string, *syscall.Statfs_t) error) error {
	var stat syscall.Statfs_t
	if err := statfs(path, &stat); err != nil {
		return fmt.Errorf("inspect secret filesystem: %w", err)
	}
	if stat.Type != tmpfsMagic {
		return fmt.Errorf("secret base %s must be backed by tmpfs", path)
	}
	return nil
}

// recordedSecretRoot returns the recorded secret root and whether it is this
// agent's private root. A root that is gone (for example after a reboot) or
// was planted by another user holds none of this agent's secrets and may be
// replaced. Any other doubt is an error, so a root that may still hold
// secrets is never abandoned.
func (a *Agent) recordedSecretRoot() (root string, reuse bool, err error) {
	if a.local == nil {
		return "", false, nil
	}
	if err := a.local.Get(secretRootKey, &root); errors.Is(err, fs.ErrNotExist) {
		return "", false, nil
	} else if err != nil {
		return "", false, fmt.Errorf("read recorded secret root: %w", err)
	}
	if filepath.Dir(root) != filepath.Clean(a.secretBaseDir()) || !strings.HasPrefix(filepath.Base(root), secretRootPrefix) {
		return "", false, nil
	}
	info, err := os.Lstat(root)
	if errors.Is(err, fs.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("inspect secret root: %w", err)
	}
	if !info.IsDir() || !ownedByAgent(info) {
		return "", false, nil
	}
	if info.Mode().Perm()&0o077 != 0 {
		return "", false, fmt.Errorf("secret root %s is accessible to other users", root)
	}
	return root, true, nil
}

// removeSecretDir removes a recorded secret directory. After a reboot empties
// /dev/shm another user may recreate the root path, for example as a symlink
// that would redirect the removal. A root owned by another user cannot hold
// this agent's secrets, and only an agent-owned root is safe from being
// swapped in sticky /dev/shm between this check and the removal, so anything
// else is treated as already gone.
func removeSecretDir(dir string) error {
	if dir == "" {
		return nil
	}
	parent := filepath.Dir(dir)
	info, err := os.Lstat(parent)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect secret root: %w", err)
	}
	if !ownedByAgent(info) {
		return nil
	}
	if !info.IsDir() {
		return fmt.Errorf("refuse to remove %s: %s is not a directory", dir, parent)
	}
	// Environment directories are read/execute-only while mounted so the
	// workload UID cannot replace entries. Restore owner write access only as
	// part of cleanup; the agent-owned parent prevents an unprivileged swap.
	if err := os.Chmod(filepath.Join(dir, "env"), 0o700); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("prepare environment secret cleanup: %w", err)
	}
	return os.RemoveAll(dir)
}

func (a *Agent) secretBaseDir() string {
	if a.secretBase == "" {
		return defaultSecretBase
	}
	return a.secretBase
}

// ownedByAgent fails closed when the owner cannot be determined.
func ownedByAgent(info fs.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && int(stat.Uid) == os.Geteuid()
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
	if !ownedByAgent(info) {
		return fmt.Errorf("secret root %s is not owned by the agent user", root)
	}
	return nil
}

// createSecretDir creates an allocation's private secret directory. It fails
// if the directory exists, so a start only ever cleans up what it created.
func createSecretDir(dir string) error {
	if err := os.Mkdir(dir, 0o700); err != nil {
		return fmt.Errorf("create memory-backed secret directory: %w", err)
	}
	// Mkdir applies the umask, which could leave the owner unable to write.
	if err := os.Chmod(dir, 0o700); err != nil {
		_ = os.Remove(dir)
		return fmt.Errorf("restrict secret directory: %w", err)
	}
	return nil
}

// materializeSecrets writes the task's secrets into dir, which createSecretDir
// must already have created. Environment values remain files and are loaded by
// the runtime wrapper immediately before the image process is executed.
// The caller removes dir if this fails.
func materializeSecrets(dir, taskName string, delivered []nodeapi.DeliveredSecret) ([]*runtime.Mount, error) {
	var taskSecrets []nodeapi.DeliveredSecret
	for _, secret := range delivered {
		if secret.Task == taskName {
			taskSecrets = append(taskSecrets, secret)
		}
	}
	var mounts []*runtime.Mount
	hasEnv := false
	envDir := filepath.Join(dir, "env")
	for i, secret := range taskSecrets {
		hostPath := filepath.Join(dir, fmt.Sprintf("secret-%d", i))
		switch secret.Target {
		case spec.SecretTargetEnv:
			if !hasEnv {
				if err := os.Mkdir(envDir, 0o700); err != nil {
					return nil, fmt.Errorf("create environment secret directory: %w", err)
				}
			}
			hostPath = filepath.Join(envDir, secret.Env)
			hasEnv = true
		case spec.SecretTargetFile:
		default:
			return nil, fmt.Errorf("unsupported secret target %q", secret.Target)
		}
		if dir == "" {
			return nil, fmt.Errorf("secret directory is required")
		}
		// Environment values are private delivery files read only by the
		// runtime wrapper. File secrets carry their canonical mode.
		mode := os.FileMode(0o400)
		if secret.Target == spec.SecretTargetFile {
			if secret.Mode != 0o400 && secret.Mode != 0o600 {
				return nil, fmt.Errorf("secret %q file mode %#o must be 0400 or 0600", secret.Name, secret.Mode)
			}
			mode = os.FileMode(secret.Mode)
		}
		file, err := os.OpenFile(hostPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, mode)
		if err != nil {
			return nil, fmt.Errorf("create secret file: %w", err)
		}
		if _, err = file.Write(secret.Value); err == nil {
			err = file.Sync()
		}
		closeErr := file.Close()
		if err == nil {
			err = closeErr
		}
		if err != nil {
			return nil, fmt.Errorf("write secret file: %w", err)
		}
		if secret.Target == spec.SecretTargetFile {
			mounts = append(mounts, &runtime.Mount{HostPath: hostPath, ContainerPath: secret.Path, ReadOnly: true, Secret: true})
		}
	}
	if hasEnv {
		if err := os.Chmod(envDir, 0o500); err != nil {
			return nil, fmt.Errorf("restrict environment secret directory: %w", err)
		}
		mounts = append(mounts, &runtime.Mount{HostPath: envDir, ContainerPath: "/run/trellis/env-secrets", ReadOnly: true, Secret: true, SecretEnv: true})
	}
	return mounts, nil
}

// recoveredSecretDir returns where an allocation known only from runtime
// labels would keep its secret files, so stopping it removes them.
func (a *Agent) recoveredSecretDir(allocID string) string {
	root, reuse, err := a.recordedSecretRoot()
	if err != nil || !reuse {
		return ""
	}
	return filepath.Join(root, allocationFileName(allocID))
}

// removeOrphanedSecretDirs deletes secret directories that no allocation
// record or in-memory allocation owns. They are left behind when the agent
// stops after writing secrets but before the allocation record is durable.
// It runs once recovery has accounted for every container: at startup, or
// from the recovery retry after an incomplete listing. Starts register their
// allocation before writing secrets, so a concurrent start is never swept.
func (a *Agent) removeOrphanedSecretDirs() {
	root, reuse, err := a.recordedSecretRoot()
	if err != nil {
		a.log.Error("skip orphaned secret sweep", "error", err)
		return
	}
	if !reuse {
		return
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		a.log.Error("skip orphaned secret sweep: list secret root", "error", err)
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
		owned[allocationFileName(id)] = true
	}
	a.mu.RUnlock()
	for _, entry := range entries {
		if owned[entry.Name()] {
			continue
		}
		path := filepath.Join(root, entry.Name())
		if err := os.RemoveAll(path); err != nil {
			a.log.Error("remove orphaned secret directory", "path", path, "error", err)
			continue
		}
		a.log.Info("removed orphaned secret directory", "path", path)
	}
}
