package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestNetworkCleanupCommandReadsConfigAndHonorsFlags(t *testing.T) {
	original := cleanupNetworkAttachments
	t.Cleanup(func() { cleanupNetworkAttachments = original })
	var gotState, gotDNS string
	cleanupNetworkAttachments = func(_ context.Context, stateDir, dnsAddress string) error {
		gotState, gotDNS = stateDir, dnsAddress
		return nil
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("data_dir: /configured/data\ndns_listen: '198.18.1.53:53' # custom DNS\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, override := range []bool{false, true} {
		cmd := newNetworkCleanupCommand()
		args := []string{"--config", path}
		wantState := "/configured/data/network"
		if override {
			args = append(args, "--data-dir", "/explicit/data")
			wantState = "/explicit/data/network"
		}
		cmd.SetArgs(args)
		if err := cmd.Execute(); err != nil {
			t.Fatal(err)
		}
		if gotState != wantState || gotDNS != "198.18.1.53" {
			t.Fatalf("cleanup called with state=%q dns=%q", gotState, gotDNS)
		}
	}
}

func TestNetworkCleanupCommandWiresDataDirectoryAndDNSAddress(t *testing.T) {
	original := cleanupNetworkAttachments
	t.Cleanup(func() { cleanupNetworkAttachments = original })
	var gotState, gotDNS string
	cleanupNetworkAttachments = func(_ context.Context, stateDir, dnsAddress string) error {
		gotState, gotDNS = stateDir, dnsAddress
		return nil
	}
	dataDir := filepath.Join(t.TempDir(), "data")
	cmd := newNetworkCleanupCommand()
	cmd.SetArgs([]string{"--data-dir", dataDir, "--dns-listen", "198.18.1.53:53"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if gotState != filepath.Join(dataDir, "network") || gotDNS != "198.18.1.53" {
		t.Fatalf("cleanup called with state=%q dns=%q", gotState, gotDNS)
	}
}

func TestNetworkCleanupCommandUsesReservedAddressForWildcardListen(t *testing.T) {
	original := cleanupNetworkAttachments
	t.Cleanup(func() { cleanupNetworkAttachments = original })
	var gotDNS string
	cleanupNetworkAttachments = func(_ context.Context, _, dnsAddress string) error {
		gotDNS = dnsAddress
		return nil
	}
	cmd := newNetworkCleanupCommand()
	cmd.SetArgs([]string{"--dns-listen", ":53"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if gotDNS != "198.18.0.53" {
		t.Fatalf("cleanup DNS = %q", gotDNS)
	}
}
