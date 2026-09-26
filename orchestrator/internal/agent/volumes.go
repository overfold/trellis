package agent

import (
	"bufio"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/clofour/trellis/internal/runtime"
	"github.com/clofour/trellis/internal/spec"
	"golang.org/x/sys/unix"
)

// VolumeManager resolves task volume mounts and persists the namespace-scoped
// volume registrations that this node owns.
type VolumeManager struct {
	dataRootPath  string
	mu            sync.RWMutex
	registrations map[string]string
	stage         func(sourceFD int, target string) error
	unstage       func(target string) error
	stagingErr    error
}

// NewVolumeManager creates a volume manager.
func NewVolumeManager(dataRoot ...string) *VolumeManager {
	root := "/var/lib/trellis/data"
	if len(dataRoot) > 0 && dataRoot[0] != "" {
		root = dataRoot[0]
	}
	vm := &VolumeManager{dataRootPath: root, registrations: make(map[string]string)}
	vm.stage = stageDirectory
	vm.unstage = func(target string) error { return unix.Unmount(target, unix.MNT_DETACH) }
	vm.stagingErr = vm.cleanupStaging()
	_ = vm.loadRegistrations()
	return vm
}

// AvailableHostVolumes returns persisted volume registrations. The existing
// node-registration field name is retained for wire compatibility; entries are
// namespace/name identities rather than configured host-volume capabilities.
func (vm *VolumeManager) AvailableHostVolumes() []string {
	vm.mu.RLock()
	defer vm.mu.RUnlock()
	available := make([]string, 0, len(vm.registrations))
	for key := range vm.registrations {
		available = append(available, key)
	}
	slices.Sort(available)
	return available
}

// Create resolves and prepares an allocation volume mount. @/ is a path prefix
// for the current namespace below Trellis's volume root. Absolute host paths are
// operator-managed and must already exist. Once prepared, the namespace/name is
// persistently registered to this node; later path changes keep the same identity.
func (vm *VolumeManager) Create(namespace string, _ string, allocationID string, volume spec.VolumeSpec) (*runtime.Mount, error) {
	hostPath, managed, err := vm.resolveHostPath(namespace, volume.HostPath)
	if err != nil {
		return nil, err
	}
	if managed {
		if vm.stagingErr != nil {
			return nil, fmt.Errorf("cleaning stale volume staging mounts: %w", vm.stagingErr)
		}
		hostPath, err = vm.prepareManagedDirectory(namespace, allocationID, volume.Name, volume.HostPath)
		if err != nil {
			return nil, err
		}
	} else {
		info, err := os.Stat(hostPath)
		if err != nil {
			return nil, fmt.Errorf("checking host path %s: %w", hostPath, err)
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("host path %s is not a directory", hostPath)
		}
	}
	if err := vm.register(namespace, volume.Name, hostPath); err != nil {
		return nil, err
	}
	return &runtime.Mount{HostPath: hostPath, ContainerPath: volume.ContainerPath, ReadOnly: volume.ReadOnly}, nil
}

// Check reports whether a volume backing directory is available.
func (vm *VolumeManager) Check(namespace string, _ string, _ string, volume spec.VolumeSpec) (bool, error) {
	hostPath, managed, err := vm.resolveHostPath(namespace, volume.HostPath)
	if err != nil {
		return false, err
	}
	if managed {
		if err := vm.checkManagedDirectory(namespace, volume.HostPath); err != nil {
			return false, err
		}
		return true, nil
	}
	info, err := os.Stat(hostPath)
	if err != nil {
		return false, fmt.Errorf("checking volume dir %s: %w", hostPath, err)
	}
	return info.IsDir(), nil
}

// Delete intentionally does not remove a registration or its data. A named
// volume is namespace-scoped state whose lifetime is independent of an allocation.
func (vm *VolumeManager) Delete(_ string, _ string, _ string, _ spec.VolumeSpec) error { return nil }

func (vm *VolumeManager) resolveHostPath(namespace, hostPath string) (string, bool, error) {
	if hostPath == "" {
		return "", false, fmt.Errorf("host path is required")
	}
	if !strings.HasPrefix(hostPath, "@/") {
		if !filepath.IsAbs(hostPath) || filepath.Clean(hostPath) != hostPath {
			return "", false, fmt.Errorf("host path %q must be a clean absolute path or begin with @/", hostPath)
		}
		return hostPath, false, nil
	}
	if namespace == "" || namespace == "." || namespace == ".." || filepath.Base(namespace) != namespace || strings.ContainsAny(namespace, `/\\`) {
		return "", false, fmt.Errorf("invalid namespace path component %q", namespace)
	}
	rel := strings.TrimPrefix(hostPath, "@/")
	if rel == "" || filepath.IsAbs(rel) || filepath.Clean(rel) != rel || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", false, fmt.Errorf("invalid Trellis volume path %q", hostPath)
	}
	return filepath.Join(vm.dataRootPath, "volumes", "namespaces", namespace, filepath.FromSlash(rel)), true, nil
}

// prepareManagedDirectory creates each managed-volume component through a
// descriptor rooted at the namespace directory, then bind-mounts the resolved
// inode at a Trellis-controlled staging path. This preserves the resolved inode
// until containerd consumes the mount rather than returning an attacker-writable
// pathname for it to resolve again.
func (vm *VolumeManager) prepareManagedDirectory(namespace, allocationID, volumeName, hostPath string) (string, error) {
	if !spec.ValidIdentifier(volumeName) {
		return "", fmt.Errorf("invalid volume name %q", volumeName)
	}
	fd, err := vm.openManagedDirectory(namespace, hostPath, true)
	if err != nil {
		return "", err
	}
	defer func() { _ = unix.Close(fd) }()

	target := vm.stagingPath(allocationID, volumeName)
	if err := os.MkdirAll(target, 0o700); err != nil {
		return "", fmt.Errorf("creating managed volume staging directory: %w", err)
	}
	if err := vm.stage(fd, target); err != nil {
		return "", fmt.Errorf("staging managed volume: %w", err)
	}
	return target, nil
}

func (vm *VolumeManager) checkManagedDirectory(namespace, hostPath string) error {
	fd, err := vm.openManagedDirectory(namespace, hostPath, false)
	if err != nil {
		return err
	}
	return unix.Close(fd)
}

func (vm *VolumeManager) openManagedDirectory(namespace, hostPath string, create bool) (int, error) {
	if err := os.MkdirAll(vm.dataRootPath, 0o750); err != nil {
		return -1, fmt.Errorf("creating data root: %w", err)
	}
	fd, err := unix.Open(vm.dataRootPath, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, fmt.Errorf("opening data root: %w", err)
	}
	components := append([]string{"volumes", "namespaces", namespace}, strings.Split(strings.TrimPrefix(hostPath, "@/"), "/")...)
	for _, component := range components {
		next, err := openDirectoryAt(fd, component, create)
		closeErr := unix.Close(fd)
		if err != nil {
			return -1, fmt.Errorf("opening managed volume component %q: %w", component, err)
		}
		if closeErr != nil {
			_ = unix.Close(next)
			return -1, fmt.Errorf("closing managed volume component: %w", closeErr)
		}
		fd = next
	}
	return fd, nil
}

func openDirectoryAt(parentFD int, name string, create bool) (int, error) {
	fd, err := unix.Openat(parentFD, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err == nil || !create || err != unix.ENOENT {
		return fd, err
	}
	if err := unix.Mkdirat(parentFD, name, 0o750); err != nil && err != unix.EEXIST {
		return -1, err
	}
	return unix.Openat(parentFD, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
}

func stageDirectory(sourceFD int, target string) error {
	source := fmt.Sprintf("/proc/self/fd/%d", sourceFD)
	if err := unix.Mount(source, target, "", unix.MS_BIND|unix.MS_REC, ""); err != nil {
		return err
	}
	if err := unix.Mount("", target, "", unix.MS_PRIVATE|unix.MS_REC, ""); err != nil {
		_ = unix.Unmount(target, unix.MNT_DETACH)
		return err
	}
	return nil
}

// ReleaseStaging removes temporary bind-mount anchors after the runtime has
// consumed them. The container keeps its own bind mount after this point.
func (vm *VolumeManager) ReleaseStaging(allocationID string, volumes []spec.VolumeSpec) error {
	var errs []error
	for _, volume := range volumes {
		if !strings.HasPrefix(volume.HostPath, "@/") || !spec.ValidIdentifier(volume.Name) {
			continue
		}
		target := vm.stagingPath(allocationID, volume.Name)
		if err := vm.unstage(target); err != nil && err != unix.EINVAL && err != unix.ENOENT {
			errs = append(errs, fmt.Errorf("unstaging volume %s: %w", volume.Name, err))
		}
		if err := os.Remove(target); err != nil && !os.IsNotExist(err) {
			errs = append(errs, fmt.Errorf("removing volume staging directory %s: %w", volume.Name, err))
		}
	}
	if err := os.Remove(filepath.Dir(vm.stagingPath(allocationID, "placeholder"))); err != nil && !os.IsNotExist(err) {
		errs = append(errs, fmt.Errorf("removing allocation staging directory: %w", err))
	}
	return errors.Join(errs...)
}

func (vm *VolumeManager) stagingPath(allocationID, volumeName string) string {
	allocationKey := fmt.Sprintf("%x", sha256.Sum256([]byte(allocationID)))
	return filepath.Join(vm.dataRootPath, "volume-staging", allocationKey, volumeName)
}

// cleanupStaging removes orphaned staging bind mounts left behind when an agent
// exits before ReleaseStaging. Repeat because lazy-unmounting a stacked mount
// can reveal another mount at the same path.
func (vm *VolumeManager) cleanupStaging() error {
	root := filepath.Join(vm.dataRootPath, "volume-staging")
	for {
		mounts, err := stagingMounts(root)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if len(mounts) == 0 {
			return os.RemoveAll(root)
		}
		for _, mount := range mounts {
			if err := unix.Unmount(mount, unix.MNT_DETACH); err != nil && err != unix.EINVAL && err != unix.ENOENT {
				return fmt.Errorf("unstaging orphaned mount %s: %w", mount, err)
			}
		}
	}
}

func stagingMounts(root string) ([]string, error) {
	file, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	var mounts []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 5 {
			continue
		}
		mount := unescapeMountInfoPath(fields[4])
		if mount == root || strings.HasPrefix(mount, root+string(filepath.Separator)) {
			mounts = append(mounts, mount)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	sort.Slice(mounts, func(i, j int) bool { return len(mounts[i]) > len(mounts[j]) })
	return mounts, nil
}

func unescapeMountInfoPath(value string) string {
	return strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`).Replace(value)
}

func volumeRegistrationName(namespace, name string) string { return namespace + "/" + name }

func (vm *VolumeManager) register(namespace, name, hostPath string) error {
	if namespace == "" || name == "" {
		return fmt.Errorf("volume namespace and name are required")
	}
	vm.mu.Lock()
	defer vm.mu.Unlock()
	vm.registrations[volumeRegistrationName(namespace, name)] = hostPath
	return vm.persistRegistrationsLocked()
}

func (vm *VolumeManager) registrationsPath() string {
	return filepath.Join(vm.dataRootPath, "volume-registrations.json")
}

func (vm *VolumeManager) loadRegistrations() error {
	raw, err := os.ReadFile(vm.registrationsPath())
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var registrations map[string]string
	if err := json.Unmarshal(raw, &registrations); err != nil {
		return err
	}
	if registrations != nil {
		vm.registrations = registrations
	}
	return nil
}

func (vm *VolumeManager) persistRegistrationsLocked() error {
	if err := os.MkdirAll(vm.dataRootPath, 0o750); err != nil {
		return fmt.Errorf("creating data root: %w", err)
	}
	raw, err := json.MarshalIndent(vm.registrations, "", "  ")
	if err != nil {
		return err
	}
	path := vm.registrationsPath()
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return fmt.Errorf("writing volume registrations: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("committing volume registrations: %w", err)
	}
	return nil
}
