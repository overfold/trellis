package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/overfold/trellis/orchestrator/internal/api"
	"github.com/overfold/trellis/orchestrator/internal/spec"
)

func TestWriteClusterSettingsShowsReconciliation(t *testing.T) {
	settings := &api.ClusterSettings{
		JobLimits: spec.DefaultLimits(),
		Reconciliation: api.ReconciliationSettings{
			AllocationLossTimeout: 2 * time.Minute, ReplacementBackoffBase: 10 * time.Second, ReplacementBackoffMax: 5 * time.Minute,
			ReplacementStableAfter: 10 * time.Minute, TerminalAllocationRetention: 7,
		},
		Network: api.ClusterNetworkSettings{WireGuardPool: "10.64.0.0/10", WireGuardPortCount: 256},
	}
	var out bytes.Buffer
	if err := writeClusterSettings(&out, settings); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Allocation loss timeout", "2m0s", "Replacement backoff max", "5m0s", "Terminal allocation retention", "7"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("settings output missing %q:\n%s", want, out.String())
		}
	}
}

func TestSetReconciliationRequiresAFlag(t *testing.T) {
	cmd := newClusterSetReconciliationCmd()
	cmd.SetArgs(nil)
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	if err := cmd.Execute(); err == nil || !strings.Contains(err.Error(), "set at least one reconciliation flag") {
		t.Fatalf("error = %v, want a flag requirement", err)
	}
}
