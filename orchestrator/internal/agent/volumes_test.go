package agent

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"

	"github.com/overfold/trellis/orchestrator/internal/spec"
	"golang.org/x/sys/unix"
)

func newTestVolumeManager(root string) *VolumeManager {
	manager := NewVolumeManager(root)
	manager.stage = func(int, string, bool) error { return nil }
	return manager
}

func TestVolumeManagerRejectsTraversal(t *testing.T) {
	manager := newTestVolumeManager(t.TempDir())
	invalid := []string{"@/../data", "@/data/../other", "relative/path"}
	for _, hostPath := range invalid {
		if _, err := manager.Create("ns", "job", "task", spec.VolumeSpec{Name: "data", HostPath: hostPath, ContainerPath: "/data"}); err == nil {
			t.Errorf("Create(%q) succeeded", hostPath)
		}
	}
}

func TestVolumeManagerRejectsManagedVolumeSymlinks(t *testing.T) {
	root := t.TempDir()
	namespaceRoot := filepath.Join(root, "volumes", "namespaces", "ns")
	if err := os.MkdirAll(filepath.Join(namespaceRoot, "data"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/", filepath.Join(namespaceRoot, "data", "escape")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("missing", filepath.Join(namespaceRoot, "dangling")); err != nil {
		t.Fatal(err)
	}

	manager := newTestVolumeManager(root)
	for _, hostPath := range []string{"@/data/escape", "@/data/escape/etc", "@/dangling", "@/dangling/child"} {
		if _, err := manager.Create("ns", "job", "task", spec.VolumeSpec{Name: "data", HostPath: hostPath, ContainerPath: "/data"}); err == nil {
			t.Errorf("Create(%q) succeeded through a symlink", hostPath)
		}
	}
}

func TestVolumeManagerStagesResolvedDirectoryBeforeRuntimeMount(t *testing.T) {
	for _, managed := range []bool{true, false} {
		t.Run(fmt.Sprintf("managed=%t", managed), func(t *testing.T) {
			root := t.TempDir()
			managedPath := filepath.Join(root, "volumes", "namespaces", "ns", "shared", "victim")
			if err := os.MkdirAll(managedPath, 0o750); err != nil {
				t.Fatal(err)
			}
			info, err := os.Stat(managedPath)
			if err != nil {
				t.Fatal(err)
			}

			manager := newTestVolumeManager(root)
			var staged unix.Stat_t
			manager.stage = func(fd int, _ string, _ bool) error {
				if err := os.Remove(managedPath); err != nil {
					return err
				}
				if err := os.Symlink("/", managedPath); err != nil {
					return err
				}
				return unix.Fstat(fd, &staged)
			}
			hostPath := managedPath
			if managed {
				hostPath = "@/shared/victim"
			}
			mount, err := manager.Create("ns", "job", "allocation", spec.VolumeSpec{Name: "data", HostPath: hostPath, ContainerPath: "/data"})
			if err != nil {
				t.Fatal(err)
			}

			if mount.HostPath != manager.stagingPath("allocation", "data") {
				t.Fatalf("runtime mount still uses replaceable path: %q", mount.HostPath)
			}
			if uint64(info.Sys().(*syscall.Stat_t).Ino) != staged.Ino {
				t.Fatalf("staged inode = %d, want %d", staged.Ino, info.Sys().(*syscall.Stat_t).Ino)
			}
		})
	}
}

func TestVolumeManagerCreatesNamespaceScopedAliasPath(t *testing.T) {
	root := t.TempDir()
	manager := newTestVolumeManager(root)
	mount, err := manager.Create("blog", "mysql", "db", spec.VolumeSpec{Name: "data", HostPath: "@/mysql/data", ContainerPath: "/var/lib/mysql"})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	want := manager.stagingPath("db", "data")
	if mount.HostPath != want || mount.ContainerPath != "/var/lib/mysql" {
		t.Errorf("unexpected mount: %#v", mount)
	}
	if _, err := os.Stat(filepath.Join(root, "volumes", "namespaces", "blog", "mysql", "data")); err != nil {
		t.Errorf("managed directory not created: %v", err)
	}
	if got := manager.AvailableHostVolumes(); !slices.Contains(got, "blog/data") {
		t.Fatalf("volume registration not advertised: %v", got)
	}
}

func TestVolumeManagerUsesExplicitHostPath(t *testing.T) {
	root := t.TempDir()
	host := filepath.Join(root, "postgres")
	if err := os.Mkdir(host, 0o750); err != nil {
		t.Fatal(err)
	}
	manager := newTestVolumeManager(root)
	mount, err := manager.Create("default", "app", "db", spec.VolumeSpec{Name: "database", HostPath: host, ContainerPath: "/data", ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if mount.HostPath != manager.stagingPath("db", "database") || !mount.ReadOnly {
		t.Fatalf("unexpected mount: %#v", mount)
	}
	if got := manager.AvailableHostVolumes(); !slices.Contains(got, "default/database") {
		t.Fatalf("explicit path registration not advertised: %v", got)
	}
}

func TestVolumeManagerChecksAbsolutePathsWithoutSymlinks(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "directory")
	if err := os.MkdirAll(filepath.Join(dir, "child"), 0o750); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(dir, link); err != nil {
		t.Fatal(err)
	}
	dangling := filepath.Join(root, "dangling")
	if err := os.Symlink(filepath.Join(root, "missing"), dangling); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(root, "file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	manager := newTestVolumeManager(filepath.Join(root, "agent"))
	manager.stage = func(int, string, bool) error { t.Fatal("unexpected staging mount"); return nil }
	for _, path := range []string{dir, "/"} {
		ok, err := manager.Check("ns", "job", "task", spec.VolumeSpec{HostPath: path})
		if err != nil || !ok {
			t.Fatalf("Check(%q) = %t, %v", path, ok, err)
		}
	}
	for _, path := range []string{link, filepath.Join(link, "child"), dangling, filepath.Join(dangling, "child"), file, filepath.Join(root, "missing", "child")} {
		volume := spec.VolumeSpec{Name: "data", HostPath: path, ContainerPath: "/data"}
		if ok, err := manager.Check("ns", "job", "task", volume); err == nil || ok {
			t.Errorf("Check(%q) = %t, %v; want rejection", path, ok, err)
		}
		if _, err := manager.Create("ns", "job", "task", volume); err == nil {
			t.Errorf("Create(%q) succeeded", path)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "missing")); !os.IsNotExist(err) {
		t.Fatalf("absolute volume created missing directories: %v", err)
	}
}

func TestVolumeManagerPersistsRegistrations(t *testing.T) {
	root := t.TempDir()
	manager := newTestVolumeManager(root)
	if _, err := manager.Create("acme", "app", "task", spec.VolumeSpec{Name: "uploads", HostPath: "@/uploads", ContainerPath: "/uploads"}); err != nil {
		t.Fatal(err)
	}
	reloaded := newTestVolumeManager(root)
	if got := reloaded.AvailableHostVolumes(); !slices.Contains(got, "acme/uploads") {
		t.Fatalf("registration did not survive reload: %v", got)
	}
}

func TestCleanupStagingKeepsMountsOfExistingContainers(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("staging bind mounts require root")
	}
	// mountinfo reports resolved paths; the data directory may be a symlink.
	root := filepath.Join(t.TempDir(), "data")
	if err := os.Symlink(t.TempDir(), root); err != nil {
		t.Fatal(err)
	}
	manager := NewVolumeManager(root)
	volume := spec.VolumeSpec{Name: "data", HostPath: "@/data", ContainerPath: "/data"}
	for _, id := range []string{"live", "orphan"} {
		if _, err := manager.Create("ns", "job", id, volume); err != nil {
			if errors.Is(err, unix.EPERM) {
				t.Skipf("bind mounts unavailable: %v", err)
			}
			t.Fatal(err)
		}
		// Unmount before TempDir removal can recurse into the volume.
		t.Cleanup(func() { _ = manager.ReleaseStaging(id) })
	}

	restarted := NewVolumeManager(root)
	if err := restarted.CleanupStaging([]string{"live"}); err != nil {
		t.Fatalf("cleanup staging: %v", err)
	}
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	mounts, err := stagingMounts(filepath.Join(resolvedRoot, "volume-staging"))
	if err != nil {
		t.Fatal(err)
	}
	resolved := &VolumeManager{dataRootPath: resolvedRoot}
	if want := []string{resolved.stagingPath("live", "data")}; !slices.Equal(mounts, want) {
		t.Fatalf("staging mounts after cleanup = %v, want %v", mounts, want)
	}
	if _, err := os.Stat(filepath.Dir(restarted.stagingPath("orphan", "data"))); !os.IsNotExist(err) {
		t.Fatalf("orphaned staging directory: %v, want not found", err)
	}
	for id, want := range map[string]bool{"live": true, "orphan": false} {
		if inUse, err := restarted.StagingInUse(id); err != nil || inUse != want {
			t.Fatalf("StagingInUse(%q) = %t, %v, want %t", id, inUse, err, want)
		}
	}
	if err := restarted.ReleaseStaging("live"); err != nil {
		t.Fatalf("release live staging: %v", err)
	}
}

func TestReleaseStagingIgnoresWrappedUnmountAbsence(t *testing.T) {
	manager := NewVolumeManager(t.TempDir())
	target := manager.stagingPath("allocation", "data")
	if err := os.MkdirAll(target, 0o750); err != nil {
		t.Fatal(err)
	}
	manager.unstage = func(string) error { return fmt.Errorf("unmount: %w", unix.EINVAL) }

	if err := manager.ReleaseStaging("allocation"); err != nil {
		t.Fatalf("release staging: %v", err)
	}
}

func TestKernelReadOnlyVolumeIncludesWritableSubmount(t *testing.T) {
	if os.Getenv("TRELLIS_VOLUME_E2E") != "1" {
		t.Skip("set TRELLIS_VOLUME_E2E=1 and run as root in a private mount namespace")
	}
	root := t.TempDir()
	backing := filepath.Join(root, "source")
	if err := os.Mkdir(backing, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mount("tmpfs", backing, "tmpfs", 0, "size=1m"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unix.Unmount(backing, unix.MNT_DETACH) })
	nested := filepath.Join(backing, "nested")
	if err := os.Mkdir(nested, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mount("tmpfs", nested, "tmpfs", 0, "size=1m"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unix.Unmount(nested, unix.MNT_DETACH) })
	for _, readOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("read-only=%t", readOnly), func(t *testing.T) {
			manager := NewVolumeManager(filepath.Join(root, fmt.Sprintf("data-%t", readOnly)))
			mount, err := manager.Create("ns", "job", "allocation", spec.VolumeSpec{Name: "data", HostPath: backing, ContainerPath: "/data", ReadOnly: readOnly})
			if readOnly && os.Getenv("TRELLIS_VOLUME_NO_MOUNT_SETATTR") == "1" {
				// Run under strace syscall fault injection to model an older
				// kernel without replacing the production mount implementation.
				if !errors.Is(err, unix.ENOSYS) || mount != nil {
					t.Fatalf("unsupported recursive readonly did not fail closed: mount=%+v err=%v", mount, err)
				}
				if inUse, err := manager.StagingInUse("allocation"); err != nil || inUse {
					t.Fatalf("failed readonly setup leaked staging mount: %t, %v", inUse, err)
				}
				if len(manager.AvailableHostVolumes()) != 0 {
					t.Fatal("failed readonly setup registered the volume")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := manager.ReleaseStaging("allocation"); err != nil {
					t.Error(err)
				}
			})
			for _, relative := range []string{"write", "nested/write"} {
				err := os.WriteFile(filepath.Join(mount.HostPath, relative), []byte("staging"), 0o600)
				if readOnly && !errors.Is(err, unix.EROFS) || !readOnly && err != nil {
					t.Fatalf("staging write %s: %v", relative, err)
				}
				// The recursive attribute changes only the staging clone, not
				// the host volume, even when both share the same filesystem.
				if err := os.WriteFile(filepath.Join(backing, relative), []byte("host"), 0o600); err != nil {
					t.Fatalf("host write %s: %v", relative, err)
				}
			}
		})
	}
}
