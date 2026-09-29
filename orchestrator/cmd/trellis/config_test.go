package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/overfold/trellis/internal/nodecapacity"
	"github.com/overfold/trellis/internal/server"
	"github.com/spf13/pflag"
)

func TestLoadNodeConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trellis.yaml")
	if err := os.WriteFile(path, []byte(`cluster: production
administrator_public_key: test-public-key
enrollment_token: trls_enroll_test
node_signing_mode: managed
agent_advertise: node-a:8127
wireguard_port: 51900
wireguard_port_count: 64
labels:
  - storage=fast
resources:
  reserved:
    cpu: 500
    memory: 1GiB
  task_pids_limit: 2048
`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := &config{Cluster: "default", WireGuardPort: 51820, WireGuardPortCount: 256}
	if err := loadNodeConfig(path, cfg, pflag.NewFlagSet("test", pflag.ContinueOnError)); err != nil {
		t.Fatal(err)
	}
	if cfg.Cluster != "production" || cfg.AdminPublicKey != "test-public-key" || cfg.EnrollmentToken != "trls_enroll_test" || cfg.SigningMode != "managed" || cfg.AgentAdvertise != "node-a:8127" || cfg.WireGuardPort != 51900 || cfg.WireGuardPortCount != 64 {
		t.Fatalf("unexpected config: %#v", cfg)
	}
	if cfg.Explicit != (explicitClusterSettings{WireGuardPortCount: true}) {
		t.Fatalf("explicit cluster settings = %+v, want only the WireGuard port count", cfg.Explicit)
	}
	if cfg.TaskPidsLimit != 2048 {
		t.Fatalf("task pids limit = %d, want 2048", cfg.TaskPidsLimit)
	}
	if len(cfg.Labels) != 1 || cfg.Labels[0] != "storage=fast" {
		t.Fatalf("unexpected labels: %v", cfg.Labels)
	}
	cpu, memory, err := nodecapacity.Resolve(8000, 32<<30)
	if err != nil {
		t.Fatal(err)
	}
	if cpu != 7500 || memory != 31<<30 {
		t.Fatalf("unexpected allocatable resources: cpu=%d memory=%d", cpu, memory)
	}
}

func TestLoadNodeConfigFlagsOverrideFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trellis.yaml")
	if err := os.WriteFile(path, []byte("cluster: from-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	flags.String("cluster", "", "")
	if err := flags.Set("cluster", "from-flag"); err != nil {
		t.Fatal(err)
	}
	cfg := &config{Cluster: "from-flag"}
	if err := loadNodeConfig(path, cfg, flags); err != nil {
		t.Fatal(err)
	}
	if cfg.Cluster != "from-flag" {
		t.Fatalf("file overrode explicit flag: %q", cfg.Cluster)
	}
}

func TestLoadNodeConfigRejectsUnknownFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trellis.yaml")
	if err := os.WriteFile(path, []byte("administrator_public_key: key\nclustr: typo\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := loadNodeConfig(path, &config{}, pflag.NewFlagSet("test", pflag.ContinueOnError)); err == nil {
		t.Fatal("expected unknown config field to be rejected")
	}
}

func TestLoadNodeConfigKeepsDefaultsPerResource(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trellis.yaml")
	if err := os.WriteFile(path, []byte("resources:\n  reserved:\n    cpu: 250\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := loadNodeConfig(path, &config{}, pflag.NewFlagSet("test", pflag.ContinueOnError)); err != nil {
		t.Fatal(err)
	}
	cpu, memory, err := nodecapacity.Resolve(4000, 16<<30)
	if err != nil {
		t.Fatal(err)
	}
	if cpu != 3750 {
		t.Fatalf("allocatable CPU = %d, want 3750", cpu)
	}
	wantMemory := int64(16<<30) - int64(16<<30)/20
	if memory != wantMemory {
		t.Fatalf("allocatable memory = %d, want %d", memory, wantMemory)
	}
}

func TestLoadNodeConfigAllocationLossTimeout(t *testing.T) {
	write := func(t *testing.T, content string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "trellis.yaml")
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}

	cfg := &config{AllocationLossTimeout: server.DefaultAllocationLossTimeout}
	if err := loadNodeConfig(write(t, "cluster: default\n"), cfg, pflag.NewFlagSet("test", pflag.ContinueOnError)); err != nil {
		t.Fatal(err)
	}
	if cfg.AllocationLossTimeout != 45*time.Second {
		t.Fatalf("omitted allocation loss timeout = %s, want default 45s", cfg.AllocationLossTimeout)
	}

	cfg = &config{AllocationLossTimeout: server.DefaultAllocationLossTimeout}
	if err := loadNodeConfig(write(t, "allocation_loss_timeout: 5m\n"), cfg, pflag.NewFlagSet("test", pflag.ContinueOnError)); err != nil {
		t.Fatal(err)
	}
	if cfg.AllocationLossTimeout != 5*time.Minute {
		t.Fatalf("allocation loss timeout = %s, want 5m", cfg.AllocationLossTimeout)
	}

	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	flags.Duration("allocation-loss-timeout", 0, "")
	if err := flags.Set("allocation-loss-timeout", "2m"); err != nil {
		t.Fatal(err)
	}
	cfg = &config{AllocationLossTimeout: 2 * time.Minute}
	if err := loadNodeConfig(write(t, "allocation_loss_timeout: 5m\n"), cfg, flags); err != nil {
		t.Fatal(err)
	}
	if cfg.AllocationLossTimeout != 2*time.Minute {
		t.Fatalf("file overrode explicit allocation loss timeout flag: %s", cfg.AllocationLossTimeout)
	}

	if err := loadNodeConfig(write(t, "allocation_loss_timeout: soon\n"), &config{}, pflag.NewFlagSet("test", pflag.ContinueOnError)); err == nil || !strings.Contains(err.Error(), "allocation_loss_timeout") {
		t.Fatalf("invalid allocation loss timeout error = %v", err)
	}
}

func TestRunRejectsAllocationLossTimeoutBeforeStorage(t *testing.T) {
	for _, timeout := range []time.Duration{0, 10 * time.Second, 48 * time.Hour} {
		dataDir := filepath.Join(t.TempDir(), "data")
		err := run(t.Context(), &config{Cluster: "default", DataDir: dataDir, AdminPublicKey: "key", SigningMode: "external", Runtime: "containerd", WireGuardPort: 51820, WireGuardPortCount: 256, AllocationLossTimeout: timeout})
		if err == nil || !strings.Contains(err.Error(), "allocation_loss_timeout or --allocation-loss-timeout") {
			t.Fatalf("run error for %s = %v, want allocation loss timeout validation error", timeout, err)
		}
		if _, err := os.Stat(dataDir); !os.IsNotExist(err) {
			t.Fatalf("data directory was touched during startup validation: %v", err)
		}
	}
}
