package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
)

func TestLoadConfigPreservesTLSFlags(t *testing.T) {
	previousConfig := config
	t.Cleanup(func() { config = previousConfig })

	t.Setenv("TRELLIS_CONFIG", filepath.Join(t.TempDir(), "missing.yaml"))
	config = CLIConfig{}
	root := testRootCommand()
	flags := root.PersistentFlags()
	if err := flags.Parse([]string{
		"--ca-cert", "cluster-ca.pem",
		"--cert", "client.pem",
		"--key", "client-key.pem",
		"--administrator-key", "administrator.pem",
	}); err != nil {
		t.Fatalf("parse flags: %v", err)
	}

	if err := loadConfig(root); err != nil {
		t.Fatalf("load config: %v", err)
	}
	if config.CACert != "cluster-ca.pem" {
		t.Fatalf("CA certificate flag was not preserved: got %q", config.CACert)
	}
	if config.Cert != "client.pem" {
		t.Fatalf("client certificate flag was not preserved: got %q", config.Cert)
	}
	if config.Key != "client-key.pem" {
		t.Fatalf("client key flag was not preserved: got %q", config.Key)
	}
	if config.AdminKey != "administrator.pem" {
		t.Fatalf("administrator key flag was not preserved: got %q", config.AdminKey)
	}
}

func TestLoadAdministratorPrivateKey(t *testing.T) {
	_, expected, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(expected)
	if err != nil {
		t.Fatal(err)
	}
	previousConfig := config
	t.Cleanup(func() { config = previousConfig })

	config = CLIConfig{AdminKeyData: base64.RawStdEncoding.EncodeToString(der)}
	got, err := loadAdministratorPrivateKey()
	if err != nil || !expected.Equal(got) {
		t.Fatalf("load base64 administrator key: equal=%t err=%v", expected.Equal(got), err)
	}

	path := filepath.Join(t.TempDir(), "administrator.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	config = CLIConfig{AdminKey: path}
	got, err = loadAdministratorPrivateKey()
	if err != nil || !expected.Equal(got) {
		t.Fatalf("load PEM administrator key: equal=%t err=%v", expected.Equal(got), err)
	}
}

func TestLoadConfigUsesNamedContextThenExplicitFlags(t *testing.T) {
	previousConfig := config
	t.Cleanup(func() { config = previousConfig })

	path := filepath.Join(t.TempDir(), "config.yaml")
	content := []byte(`current_context: production
contexts:
  production:
    server_addr: prod.example:8128
    token: prod-token
    namespace: payments
    ca_cert: prod-ca
`)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TRELLIS_CONFIG", path)
	config = CLIConfig{}
	root := testRootCommand()
	if err := root.PersistentFlags().Parse([]string{"--server-addr", "override.example:8128"}); err != nil {
		t.Fatalf("parse flags: %v", err)
	}

	if err := loadConfig(root); err != nil {
		t.Fatalf("load config: %v", err)
	}
	if config.Context != "production" {
		t.Fatalf("context = %q, want production", config.Context)
	}
	if config.ServerAddr != "override.example:8128" {
		t.Fatalf("server = %q", config.ServerAddr)
	}
	if config.ClusterToken != "prod-token" || config.Namespace != "payments" {
		t.Fatalf("context values not loaded: %#v", config)
	}
	if config.CACertPEM != "prod-ca" {
		t.Fatalf("CA = %q", config.CACertPEM)
	}
}

func TestWriteUserConfigProtectsTokenFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "config.yaml")
	file := fileConfig{CurrentContext: "prod", Contexts: map[string]contextFileConfig{
		"prod": {ServerAddr: "prod:8128", ClusterToken: "secret", Namespace: "default"},
	}}
	if err := writeUserConfig(path, file); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("config permissions = %o, want 600", got)
	}
	loaded, err := readUserConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Contexts["prod"].ClusterToken != "secret" {
		t.Fatal("saved context did not round-trip")
	}
}

func TestStructuredOutputFlagIsCommandLocal(t *testing.T) {
	previousConfig := config
	t.Cleanup(func() { config = previousConfig })
	config = CLIConfig{}

	root := newRootCmd()
	if root.PersistentFlags().Lookup("output") != nil {
		t.Fatal("--output must not be a persistent/global flag")
	}

	credentials, _, err := root.Find([]string{"credentials"})
	if err != nil {
		t.Fatalf("find credentials: %v", err)
	}
	if credentials.Hidden {
		t.Fatal("credentials command must be discoverable in CLI help")
	}

	for _, path := range [][]string{{"jobs", "status"}, {"namespaces", "list"}, {"nodes", "list"}, {"secrets", "describe"}, {"credentials", "create"}} {
		command, _, err := root.Find(path)
		if err != nil {
			t.Fatalf("find %v: %v", path, err)
		}
		if command.Flags().Lookup("output") == nil {
			t.Fatalf("%v does not expose --output", path)
		}
	}

	for _, path := range [][]string{{"jobs", "apply"}, {"jobs", "logs"}, {"nodes", "drain"}, {"secrets", "delete"}} {
		command, _, err := root.Find(path)
		if err != nil {
			t.Fatalf("find %v: %v", path, err)
		}
		if command.Flags().Lookup("output") != nil {
			t.Fatalf("%v unexpectedly exposes --output", path)
		}
	}
}

func testRootCommand() *cobra.Command {
	root := &cobra.Command{Use: "trellisctl"}
	flags := root.PersistentFlags()
	flags.StringVar(&config.Context, "context", "", "")
	flags.StringVar(&config.ServerAddr, "server-addr", "localhost:8128", "")
	flags.StringVar(&config.ClusterToken, "token", "", "")
	flags.StringVar(&config.Namespace, "namespace", "", "")
	flags.StringVar(&config.CACert, "ca-cert", "", "")
	flags.StringVar(&config.Cert, "cert", "", "")
	flags.StringVar(&config.Key, "key", "", "")
	flags.StringVar(&config.AdminKey, "administrator-key", "", "")
	return root
}
