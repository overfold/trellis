package runtime

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/opencontainers/runtime-spec/specs-go"
)

func TestWriteDNSConfigCreatesParentDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "logs", "allocation-resolv.conf")
	if err := writeDNSConfig(path, []string{"127.0.0.1:8053"}); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if want := "nameserver 127.0.0.1:8053\n"; string(got) != want {
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
