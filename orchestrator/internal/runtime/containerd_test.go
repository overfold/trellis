package runtime

import (
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/opencontainers/runtime-spec/specs-go"
)

func TestOpenLogFileUsesLegacyPathOnlyWhenCurrentLogIsMissing(t *testing.T) {
	dir := t.TempDir()
	current := filepath.Join(dir, "current.log")
	legacy := filepath.Join(dir, "legacy.log")
	if err := os.WriteFile(legacy, []byte("legacy"), 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := openLogFile(current, legacy)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(file)
	_ = file.Close()
	if err != nil || string(data) != "legacy" {
		t.Fatalf("legacy log = %q, %v", data, err)
	}

	if err := os.WriteFile(current, []byte("current"), 0o600); err != nil {
		t.Fatal(err)
	}
	file, err = openLogFile(current, legacy)
	if err != nil {
		t.Fatal(err)
	}
	data, err = io.ReadAll(file)
	_ = file.Close()
	if err != nil || string(data) != "current" {
		t.Fatalf("current log = %q, %v", data, err)
	}
}

func TestRemoveRuntimeFilesCleansCurrentAndLegacyFiles(t *testing.T) {
	dir := t.TempDir()
	current := filepath.Join(dir, "current")
	legacy := filepath.Join(dir, "legacy")
	for _, path := range []string{current, legacy} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
		for _, suffix := range []string{".log", "-resolv.conf", "-hosts"} {
			if err := os.WriteFile(filepath.Join(path, "allocation"+suffix), []byte("data"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(filepath.Join(path, "other.log"), []byte("keep"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := removeRuntimeFiles("allocation", current, legacy); err != nil {
		t.Fatal(err)
	}
	if err := removeRuntimeFiles("allocation", current, legacy); err != nil {
		t.Fatalf("retry cleanup: %v", err)
	}
	for _, path := range []string{current, legacy} {
		for _, suffix := range []string{".log", "-resolv.conf", "-hosts"} {
			if _, err := os.Lstat(filepath.Join(path, "allocation"+suffix)); !os.IsNotExist(err) {
				t.Fatalf("runtime file %s remains: %v", filepath.Join(path, "allocation"+suffix), err)
			}
		}
		if _, err := os.Stat(filepath.Join(path, "other.log")); err != nil {
			t.Fatalf("unrelated log removed from %s: %v", path, err)
		}
	}
}

func TestWriteDNSConfigCreatesParentDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "logs", "allocation-resolv.conf")
	if err := writeDNSConfig(path, []string{"198.18.0.53"}); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if want := "nameserver 198.18.0.53\n"; string(got) != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestExecProcessSpecInheritsContainerContext(t *testing.T) {
	containerSpec := &specs.Spec{Process: &specs.Process{
		Env:  []string{"PATH=/usr/local/bin:/usr/bin", "TRELLIS_NAMESPACE=default"},
		Cwd:  "/app",
		User: specs.User{UID: 1000, GID: 1000},
	}}
	process := execProcessSpec(containerSpec, []string{"node", "-v"}, false)

	if !reflect.DeepEqual(process.Env, containerSpec.Process.Env) {
		t.Fatalf("env = %#v, want %#v", process.Env, containerSpec.Process.Env)
	}
	if process.Cwd != "/app" {
		t.Fatalf("cwd = %q, want /app", process.Cwd)
	}
	if process.User.UID != 1000 || process.User.GID != 1000 {
		t.Fatalf("user = %#v, want uid/gid 1000", process.User)
	}
	if process.Terminal {
		t.Fatal("non-interactive exec unexpectedly requested a terminal")
	}
}

func TestExecProcessSpecDefaultsWithoutContainerProcess(t *testing.T) {
	process := execProcessSpec(nil, []string{"/bin/true"}, true)
	if process.Cwd != "/" {
		t.Fatalf("cwd = %q, want /", process.Cwd)
	}
	if !process.Terminal {
		t.Fatal("terminal exec did not preserve terminal flag")
	}
}

func TestWriteHostsConfigAddsDeterministicAliases(t *testing.T) {
	path := filepath.Join(t.TempDir(), "logs", "allocation-hosts")
	if err := writeHostsConfig(path, map[string]string{"zeta": "10.0.0.2", "trellis": "127.0.0.1"}); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := "127.0.0.1 localhost\n::1 localhost ip6-localhost ip6-loopback\n127.0.0.1 trellis\n10.0.0.2 zeta\n"
	if string(got) != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestWriteDNSConfigRejectsNameserverPorts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "resolv.conf")
	if err := writeDNSConfig(path, []string{"127.0.0.1:8053"}); err == nil {
		t.Fatal("expected nameserver with port to be rejected")
	}
}

func TestRuntimeFilesRejectSymlinks(t *testing.T) {
	dir := t.TempDir()
	victim := filepath.Join(dir, "victim")
	if err := os.WriteFile(victim, []byte("unchanged"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "allocation-resolv.conf")
	if err := os.Symlink(victim, link); err != nil {
		t.Fatal(err)
	}
	if err := writeDNSConfig(link, []string{"198.18.0.53"}); err == nil {
		t.Fatal("expected symlink to be rejected")
	}
	got, err := os.ReadFile(victim)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "unchanged" {
		t.Fatalf("victim changed to %q", got)
	}
}

func TestRuntimeDirectoryRejectsUnsafePaths(t *testing.T) {
	dir := t.TempDir()
	info, err := os.Lstat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkRuntimeDir(dir, info); (err == nil) != (os.Geteuid() == 0) {
		t.Fatalf("root ownership check returned %v for uid %d", err, os.Geteuid())
	}
	if err := os.Chmod(dir, 0o777); err != nil {
		t.Fatal(err)
	}
	info, err = os.Lstat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkRuntimeDir(dir, info); err == nil {
		t.Fatal("expected writable directory to be rejected")
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(dir, link); err != nil {
		t.Fatal(err)
	}
	info, err = os.Lstat(link)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkRuntimeDir(link, info); err == nil {
		t.Fatal("expected symlink directory to be rejected")
	}
}
