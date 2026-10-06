package runtime

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLogUsageCountsTaskLogsAndReportsFilesystem(t *testing.T) {
	dir := t.TempDir()
	r := &ContainerdRuntime{logDir: dir, legacyLogDir: filepath.Join(dir, "absent")}
	for name, size := range map[string]int{"running.log": 300, "retained.log": 40, "task-hosts": 1000, "task-resolv.conf": 1000} {
		if err := os.WriteFile(filepath.Join(dir, name), make([]byte, size), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(dir, "directory.log"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(dir, "running.log"), filepath.Join(dir, "link.log")); err != nil {
		t.Fatal(err)
	}

	usage, err := r.LogUsage()
	if err != nil {
		t.Fatal(err)
	}
	if usage.Bytes != 340 {
		t.Fatalf("log bytes = %d, want 340", usage.Bytes)
	}
	if usage.FilesystemCapacity <= 0 || usage.FilesystemAvailable < 0 || usage.FilesystemAvailable > usage.FilesystemCapacity {
		t.Fatalf("filesystem usage = %+v", usage)
	}
}

func TestLogUsageBeforeLogDirectoryExists(t *testing.T) {
	dir := t.TempDir()
	r := &ContainerdRuntime{logDir: filepath.Join(dir, "missing", "runtime")}

	usage, err := r.LogUsage()
	if err != nil {
		t.Fatal(err)
	}
	if usage.Bytes != 0 || usage.FilesystemCapacity <= 0 {
		t.Fatalf("usage without log directory = %+v", usage)
	}
}
