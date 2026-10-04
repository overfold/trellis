package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/overfold/trellis/orchestrator/internal/agent"
	"github.com/overfold/trellis/orchestrator/internal/spec"
	"golang.org/x/sys/unix"
)

func TestLocalCleanupCommandReadsConfigAndHonorsFlags(t *testing.T) {
	original := cleanupNetworkAttachments
	originalVolumes := cleanupVolumeStaging
	t.Cleanup(func() {
		cleanupNetworkAttachments = original
		cleanupVolumeStaging = originalVolumes
	})
	var gotState, gotDNS, gotVolumes string
	cleanupNetworkAttachments = func(_ context.Context, stateDir, dnsAddress string) error {
		gotState, gotDNS = stateDir, dnsAddress
		return nil
	}
	cleanupVolumeStaging = func(dataDir string) error {
		gotVolumes = dataDir
		return nil
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("data_dir: /configured/data\ndns_listen: '198.18.1.53:53' # custom DNS\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, override := range []bool{false, true} {
		cmd := newLocalCleanupCommand()
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
		if gotVolumes != filepath.Dir(wantState) {
			t.Fatalf("volume cleanup data directory = %q, want %q", gotVolumes, filepath.Dir(wantState))
		}
	}
}

func TestLocalCleanupCommandWiresDataDirectoryAndDNSAddress(t *testing.T) {
	original := cleanupNetworkAttachments
	t.Cleanup(func() { cleanupNetworkAttachments = original })
	var gotState, gotDNS string
	cleanupNetworkAttachments = func(_ context.Context, stateDir, dnsAddress string) error {
		gotState, gotDNS = stateDir, dnsAddress
		return nil
	}
	dataDir := filepath.Join(t.TempDir(), "data")
	cmd := newLocalCleanupCommand()
	cmd.SetArgs([]string{"--data-dir", dataDir, "--dns-listen", "198.18.1.53:53"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if gotState != filepath.Join(dataDir, "network") || gotDNS != "198.18.1.53" {
		t.Fatalf("cleanup called with state=%q dns=%q", gotState, gotDNS)
	}
}

func TestLocalCleanupCommandUsesReservedAddressForWildcardListen(t *testing.T) {
	original := cleanupNetworkAttachments
	t.Cleanup(func() { cleanupNetworkAttachments = original })
	var gotDNS string
	cleanupNetworkAttachments = func(_ context.Context, _, dnsAddress string) error {
		gotDNS = dnsAddress
		return nil
	}
	cmd := newLocalCleanupCommand()
	cmd.SetArgs([]string{"--data-dir", t.TempDir(), "--dns-listen", ":53"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if gotDNS != "198.18.0.53" {
		t.Fatalf("cleanup DNS = %q", gotDNS)
	}
}

func TestLocalCleanupRemovesStagingWithoutDeletingVolumeData(t *testing.T) {
	original := cleanupNetworkAttachments
	t.Cleanup(func() { cleanupNetworkAttachments = original })
	cleanupNetworkAttachments = func(context.Context, string, string) error { return nil }
	root := t.TempDir()
	staging := filepath.Join(root, "volume-staging", "allocation", "pgdata")
	if err := os.MkdirAll(staging, 0o750); err != nil {
		t.Fatal(err)
	}
	volume := filepath.Join(root, "volumes", "namespaces", "platform", "postgres")
	if err := os.MkdirAll(volume, 0o750); err != nil {
		t.Fatal(err)
	}
	data := filepath.Join(volume, "data")
	if err := os.WriteFile(data, []byte("database contents"), 0o600); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		cmd := newLocalCleanupCommand()
		cmd.SetArgs([]string{"--data-dir", root})
		if err := cmd.Execute(); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(staging); !os.IsNotExist(err) {
			t.Fatalf("staging directory remains: %v", err)
		}
		if got, err := os.ReadFile(data); err != nil || string(got) != "database contents" {
			t.Fatalf("volume data = %q, err = %v", got, err)
		}
	}
}

func TestLocalCleanupUnmountsManagedVolume(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("staging bind mounts require root")
	}
	original := cleanupNetworkAttachments
	t.Cleanup(func() { cleanupNetworkAttachments = original })
	cleanupNetworkAttachments = func(context.Context, string, string) error { return nil }
	root := t.TempDir()
	manager := agent.NewVolumeManager(root)
	mount, err := manager.Create("platform", "db", "allocation", spec.VolumeSpec{
		Name: "pgdata", HostPath: "@/postgres", ContainerPath: "/data",
	})
	if err != nil {
		if errors.Is(err, unix.EPERM) {
			t.Skipf("bind mounts unavailable: %v", err)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.ReleaseStaging("allocation") })
	data := filepath.Join(root, "volumes", "namespaces", "platform", "postgres", "data")
	if err := os.WriteFile(data, []byte("database contents"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Staging exposes the backing data before cleanup, not an empty substitute.
	if got, err := os.ReadFile(filepath.Join(mount.HostPath, "data")); err != nil || string(got) != "database contents" {
		t.Fatalf("staged data = %q, err = %v", got, err)
	}
	cmd := newLocalCleanupCommand()
	cmd.SetArgs([]string{"--data-dir", root})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(mount.HostPath); !os.IsNotExist(err) {
		t.Fatalf("staging mount remains: %v", err)
	}
	if got, err := os.ReadFile(data); err != nil || string(got) != "database contents" {
		t.Fatalf("backing data = %q, err = %v", got, err)
	}
}

func TestLocalCleanupCommandReportsFailures(t *testing.T) {
	originalNetwork, originalVolumes := cleanupNetworkAttachments, cleanupVolumeStaging
	t.Cleanup(func() {
		cleanupNetworkAttachments, cleanupVolumeStaging = originalNetwork, originalVolumes
	})
	failure := errors.New("permission denied")
	for _, stage := range []string{"network", "volumes"} {
		t.Run(stage, func(t *testing.T) {
			volumeCalled := false
			cleanupNetworkAttachments = func(context.Context, string, string) error {
				if stage == "network" {
					return failure
				}
				return nil
			}
			cleanupVolumeStaging = func(string) error {
				volumeCalled = true
				return failure
			}
			cmd := newLocalCleanupCommand()
			cmd.SetArgs([]string{})
			err := cmd.Execute()
			if !errors.Is(err, failure) {
				t.Fatalf("cleanup error = %v, want %v", err, failure)
			}
			wantContext := "network resources"
			if stage == "volumes" {
				wantContext = "volume staging mounts"
			}
			if !strings.Contains(err.Error(), wantContext) || volumeCalled != (stage == "volumes") {
				t.Fatalf("error = %v, volumeCalled = %t", err, volumeCalled)
			}
		})
	}
}
