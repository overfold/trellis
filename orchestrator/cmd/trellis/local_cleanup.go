package main

import (
	"context"
	"fmt"
	"net"
	"path/filepath"

	"github.com/overfold/trellis/orchestrator/internal/agent"
	"github.com/overfold/trellis/orchestrator/internal/network"
	"github.com/spf13/cobra"
)

var cleanupNetworkAttachments = func(ctx context.Context, stateDir, dnsAddress string) error {
	return network.CleanupJournaledAttachments(ctx, stateDir, dnsAddress)
}

var cleanupVolumeStaging = func(dataDir string) error {
	return agent.NewVolumeManager(dataDir).CleanupStaging(nil)
}

// The caller must stop the daemon and remove all local containers first.
func newLocalCleanupCommand() *cobra.Command {
	cfg := &config{}
	cmd := &cobra.Command{
		Use:    "local-cleanup",
		Short:  "Remove journaled local resources and delivered secrets",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if cfg.ConfigFile != "" {
				if err := loadNodeConfig(cfg.ConfigFile, cfg, cmd.Flags()); err != nil {
					return err
				}
			}
			host, _, err := net.SplitHostPort(cfg.DNSListen)
			if err != nil {
				return fmt.Errorf("dns listen address: %w", err)
			}
			if host == "" || host == "0.0.0.0" {
				host = network.WorkloadDNSAddress
			}
			if err := cleanupNetworkAttachments(cmd.Context(), filepath.Join(cfg.DataDir, "network"), host); err != nil {
				return fmt.Errorf("cleaning local network resources: %w", err)
			}
			if err := cleanupVolumeStaging(cfg.DataDir); err != nil {
				return fmt.Errorf("cleaning volume staging mounts: %w", err)
			}
			if err := agent.CleanupDeliveredSecrets(cfg.DataDir); err != nil {
				return fmt.Errorf("cleaning delivered secrets: %w", err)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&cfg.ConfigFile, "config", "", "Path to Trellis node configuration YAML")
	cmd.Flags().StringVar(&cfg.DataDir, "data-dir", "/var/lib/trellis/data", "Directory containing local node state")
	cmd.Flags().StringVar(&cfg.DNSListen, "dns-listen", net.JoinHostPort(network.WorkloadDNSAddress, "53"), "Configured workload DNS listen address")
	return cmd
}
