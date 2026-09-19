package runtime

import (
	"os"
	"path/filepath"
	"testing"
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
