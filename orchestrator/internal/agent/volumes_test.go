package agent

import (
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"

	"github.com/clofour/trellis/internal/spec"
	"golang.org/x/sys/unix"
)

func newTestVolumeManager(root string) *VolumeManager {
	manager := NewVolumeManager(root)
	manager.stage = func(int, string) error { return nil }
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
	manager.stage = func(fd int, _ string) error { return unix.Fstat(fd, &staged) }
	mount, err := manager.Create("ns", "job", "allocation", spec.VolumeSpec{Name: "data", HostPath: "@/shared/victim", ContainerPath: "/data"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(managedPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/", managedPath); err != nil {
		t.Fatal(err)
	}

	if mount.HostPath != manager.stagingPath("allocation", "data") {
		t.Fatalf("runtime mount still uses managed path: %q", mount.HostPath)
	}
	if uint64(info.Sys().(*syscall.Stat_t).Ino) != staged.Ino {
		t.Fatalf("staged inode = %d, want %d", staged.Ino, info.Sys().(*syscall.Stat_t).Ino)
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
	manager := NewVolumeManager(root)
	mount, err := manager.Create("default", "app", "db", spec.VolumeSpec{Name: "database", HostPath: host, ContainerPath: "/data", ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if mount.HostPath != host || !mount.ReadOnly {
		t.Fatalf("unexpected mount: %#v", mount)
	}
	if got := manager.AvailableHostVolumes(); !slices.Contains(got, "default/database") {
		t.Fatalf("explicit path registration not advertised: %v", got)
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
