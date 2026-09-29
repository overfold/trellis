package main

import (
	"bytes"
	"log/slog"
	"net/netip"
	"strings"
	"testing"

	"github.com/overfold/trellis/internal/server"
	"github.com/spf13/pflag"
)

func TestRecordExplicitClusterSettingFlags(t *testing.T) {
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	flags.String("max-task-memory", "", "")
	flags.String("wireguard-pool", "", "")
	flags.Int("wireguard-port-count", 0, "")
	if err := flags.Set("max-task-memory", "2TiB"); err != nil {
		t.Fatal(err)
	}
	cfg := &config{Explicit: explicitClusterSettings{WireGuardPortCount: true}}
	recordExplicitClusterSettingFlags(cfg, flags)
	if cfg.Explicit != (explicitClusterSettings{JobLimits: true, WireGuardPortCount: true}) {
		t.Fatalf("explicit cluster settings = %+v", cfg.Explicit)
	}
}

func TestApplyClusterSettingsUsesReplicatedValues(t *testing.T) {
	replicated := server.DefaultClusterSettings()
	replicated.JobLimits.MaxReplicasPerTaskGroup = 10
	replicated.WireGuardPool = netip.MustParsePrefix("10.128.0.0/10")
	replicated.WireGuardPortCount = 64
	configured := server.DefaultClusterSettings()

	t.Run("unset values are adopted silently", func(t *testing.T) {
		var logs bytes.Buffer
		cfg := &config{WireGuardPort: 51820, WireGuardPortCount: configured.WireGuardPortCount}
		if err := applyClusterSettings(slog.New(slog.NewTextHandler(&logs, nil)), cfg, configured, replicated); err != nil {
			t.Fatal(err)
		}
		if cfg.WireGuardPortCount != 64 {
			t.Fatalf("WireGuard port count = %d, want the cluster's 64", cfg.WireGuardPortCount)
		}
		if logs.Len() != 0 {
			t.Fatalf("unexpected warnings: %s", logs.String())
		}
	})

	t.Run("explicit job limits and pool are ignored with a warning", func(t *testing.T) {
		var logs bytes.Buffer
		cfg := &config{WireGuardPort: 51820, WireGuardPortCount: 64, Explicit: explicitClusterSettings{JobLimits: true, WireGuardPool: true}}
		configured := configured
		configured.WireGuardPortCount = 64
		if err := applyClusterSettings(slog.New(slog.NewTextHandler(&logs, nil)), cfg, configured, replicated); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(logs.String(), "job_limits") || !strings.Contains(logs.String(), "wireguard_pool") {
			t.Fatalf("missing warnings: %s", logs.String())
		}
	})

	t.Run("explicit conflicting port count is refused", func(t *testing.T) {
		cfg := &config{WireGuardPort: 51820, WireGuardPortCount: 256, Explicit: explicitClusterSettings{WireGuardPortCount: true}}
		err := applyClusterSettings(slog.New(slog.DiscardHandler), cfg, configured, replicated)
		if err == nil || !strings.Contains(err.Error(), "cluster uses 64") {
			t.Fatalf("err = %v, want a port count conflict", err)
		}
	})

	t.Run("adopted port count must fit the local base port", func(t *testing.T) {
		cfg := &config{WireGuardPort: 65500, WireGuardPortCount: 1}
		configured := configured
		configured.WireGuardPortCount = 1
		if err := applyClusterSettings(slog.New(slog.DiscardHandler), cfg, configured, replicated); err == nil {
			t.Fatal("adopted a port count that overflows the local port range")
		}
	})
}
