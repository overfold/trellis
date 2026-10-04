package runtime

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestCleanupRetainsLogsUntilPruning(t *testing.T) {
	dir := t.TempDir()
	r := &ContainerdRuntime{logDir: dir, legacyLogDir: filepath.Join(dir, "absent")}
	for _, suffix := range []string{".log", "-hosts", "-resolv.conf"} {
		if err := os.WriteFile(filepath.Join(dir, "task"+suffix), []byte("first\nlast\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "other.log"), []byte("unrelated"), 0o600); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := r.removeAllocationFiles("task", "-hosts", "-resolv.conf"); err != nil {
			t.Fatal(err)
		}
	}
	for _, suffix := range []string{"-hosts", "-resolv.conf"} {
		if _, err := os.Stat(filepath.Join(dir, "task"+suffix)); !os.IsNotExist(err) {
			t.Fatalf("ephemeral file %s: %v", suffix, err)
		}
	}
	stream, err := r.Logs(context.Background(), "task", false, 1)
	if err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(stream)
	_ = stream.Close()
	if err != nil || string(output) != "last\n" {
		t.Fatalf("retained tail=%q error=%v", output, err)
	}
	for range 2 {
		if err := r.RemoveRetainedLogs("task"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(r.logPath("task")); !os.IsNotExist(err) {
		t.Fatalf("pruned log: %v", err)
	}
	if data, err := os.ReadFile(r.logPath("other")); err != nil || string(data) != "unrelated" {
		t.Fatalf("other allocation log=%q error=%v", data, err)
	}
}
