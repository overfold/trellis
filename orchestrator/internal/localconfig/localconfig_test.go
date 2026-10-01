package localconfig

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWritePublishesCurrentPublicCA(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run", "local.yaml")
	caPath := filepath.Join(filepath.Dir(path), "ca.crt")
	for _, ca := range []string{"old-public-ca", "replacement-public-ca"} {
		cfg := &Config{ServerAddr: "localhost:8128", ClusterToken: "private-token", CACert: ca}
		if err := Write(path, cfg); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(caPath)
		if err != nil || string(data) != ca {
			t.Fatalf("public CA = %q, err = %v", data, err)
		}
		info, err := os.Stat(caPath)
		if err != nil || info.Mode().Perm() != 0o644 {
			t.Fatalf("public CA must be readable by local users: %v, %v", info, err)
		}
		loaded, err := Read(path)
		if err != nil || *loaded != *cfg {
			t.Fatalf("runtime config = %#v, err = %v", loaded, err)
		}
	}
}
