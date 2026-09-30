package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/overfold/trellis/orchestrator/internal/nodecapacity"
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

func TestLoadNodeConfigRejectsClusterReconciliationSettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trellis.yaml")
	if err := os.WriteFile(path, []byte("allocation_loss_timeout: 5m\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// The loss timeout is a replicated cluster setting; a node-local value
	// could disagree between leaders.
	if err := loadNodeConfig(path, &config{}, pflag.NewFlagSet("test", pflag.ContinueOnError)); err == nil || !strings.Contains(err.Error(), "allocation_loss_timeout") {
		t.Fatalf("node config with allocation_loss_timeout error = %v, want unknown field", err)
	}
}
