package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/internal/tlsutil"
	"github.com/spf13/cobra"
)

func TestContextCAFileTracksReplacement(t *testing.T) {
	previousConfig := config
	t.Cleanup(func() { config = previousConfig })
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	caPath := filepath.Join(dir, "ca.crt")
	oldCA, _, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	newCA, newKey, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	certPEM, _, err := tlsutil.GenerateNodeCert(newCA, newKey, uuid.New())
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(certPEM)
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(caPath, oldCA, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Chdir(dir)
	config = CLIConfig{CACert: "ca.crt", CACertPEM: "must not embed this", ServerAddr: "localhost:8128", ClusterToken: "new-token"}
	ctx, err := effectiveContextFileConfig()
	if err != nil {
		t.Fatal(err)
	}
	if ctx.CACertFile != caPath || ctx.CACert != "" {
		t.Fatalf("saved CA file = %q, inline = %q", ctx.CACertFile, ctx.CACert)
	}
	if err := writeUserConfig(path, fileConfig{CurrentContext: "local", Contexts: map[string]contextFileConfig{
		"local": ctx, "remote": {CACert: string(oldCA), ClusterToken: "remote-token"},
	}}); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TRELLIS_CONFIG", path)
	config = CLIConfig{}
	if err := loadConfig(testRootCommand()); err != nil {
		t.Fatal(err)
	}
	verify := func() error {
		tlsConfig, err := buildCLITLSConfig()
		if err != nil {
			return err
		}
		_, err = cert.Verify(x509.VerifyOptions{Roots: tlsConfig.RootCAs, DNSName: tlsConfig.ServerName})
		return err
	}
	if err := verify(); err == nil || !strings.Contains(err.Error(), "ECDSA verification failure") {
		t.Fatalf("old CA should reject replacement certificate: %v", err)
	}
	if err := os.WriteFile(caPath, newCA, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := verify(); err != nil {
		t.Fatalf("live CA file did not trust replacement: %v", err)
	}
	loaded, err := readUserConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Contexts["remote"].CACert != string(oldCA) || loaded.Contexts["remote"].ClusterToken != "remote-token" {
		t.Fatal("unrelated remote context changed")
	}
	if err := os.Remove(caPath); err != nil {
		t.Fatal(err)
	}
	if _, err := buildCLITLSConfig(); err == nil || !strings.Contains(err.Error(), "read CA cert") {
		t.Fatalf("missing CA file must fail, not fall back: %v", err)
	}
}

func TestContextCASources(t *testing.T) {
	previousConfig := config
	t.Cleanup(func() { config = previousConfig })
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := writeUserConfig(path, fileConfig{CurrentContext: "local", Contexts: map[string]contextFileConfig{
		"local": {CACertFile: "ca.crt"},
	}}); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TRELLIS_CONFIG", path)
	config = CLIConfig{}
	if err := loadConfig(testRootCommand()); err != nil {
		t.Fatal(err)
	}
	if config.CACert != filepath.Join(dir, "ca.crt") {
		t.Fatalf("relative CA file not resolved against config directory: %q", config.CACert)
	}
	t.Setenv("TRELLIS_CA_CERT", "environment-ca")
	config = CLIConfig{}
	if err := loadConfig(testRootCommand()); err != nil {
		t.Fatal(err)
	}
	if config.CACert != "" || config.CACertPEM != "environment-ca" {
		t.Fatal("inline environment CA did not override file source")
	}
	ctx, err := effectiveContextFileConfig()
	if err != nil || ctx.CACert != "environment-ca" || ctx.CACertFile != "" {
		t.Fatalf("inline CA was not embedded: %#v, %v", ctx, err)
	}
	if err := writeUserConfig(path, fileConfig{Contexts: map[string]contextFileConfig{
		"ambiguous": {CACert: "inline-ca", CACertFile: "ca.crt"},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := readUserConfig(path); err == nil || !strings.Contains(err.Error(), "only one of ca_cert and ca_cert_file") {
		t.Fatalf("ambiguous CA sources were not rejected: %v", err)
	}
}

func TestLoadConfigPreservesTLSFlags(t *testing.T) {
	previousConfig := config
	t.Cleanup(func() { config = previousConfig })

	t.Setenv("TRELLIS_CONFIG", filepath.Join(t.TempDir(), "missing.yaml"))
	t.Setenv("TRELLIS_CA_CERT", "environment-inline-ca")
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
	if config.CACertPEM != "" {
		t.Fatalf("explicit CA certificate path did not override inline environment CA: got %q", config.CACertPEM)
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

func TestLoadConfigTreatsEnvironmentCACertAsInlinePEM(t *testing.T) {
	previousConfig := config
	t.Cleanup(func() { config = previousConfig })

	t.Setenv("TRELLIS_CONFIG", filepath.Join(t.TempDir(), "missing.yaml"))
	t.Setenv("TRELLIS_CA_CERT", "inline-ca-pem")
	config = CLIConfig{}
	root := testRootCommand()

	if err := loadConfig(root); err != nil {
		t.Fatalf("load config: %v", err)
	}
	if config.CACert != "" || config.CACertPEM != "inline-ca-pem" {
		t.Fatalf("CA path = %q, inline CA = %q", config.CACert, config.CACertPEM)
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

func TestCredentialsCreateRejectsNamespaceScope(t *testing.T) {
	cmd := newCredentialsCreateCmd()
	if cmd.Flags().Lookup("namespace-scope") != nil {
		t.Fatal("namespace-scope flag must not be available")
	}
	if err := cmd.ParseFlags([]string{"--scope", "namespace", "--access", "read"}); err != nil {
		t.Fatal(err)
	}
	if err := cmd.RunE(cmd, nil); err == nil || !strings.Contains(err.Error(), "--scope must be cluster") {
		t.Fatalf("error = %v, want rejection of namespace scope", err)
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
