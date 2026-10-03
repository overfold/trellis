package main

import (
	"context"
	"fmt"
	"net"
	"path/filepath"

	"github.com/overfold/trellis/orchestrator/internal/network"
	"github.com/spf13/cobra"
)

var cleanupNetworkAttachments = func(ctx context.Context, stateDir, dnsAddress string) error {
	return network.CleanupJournaledAttachments(ctx, stateDir, dnsAddress)
}

func newNetworkCleanupCommand() *cobra.Command {
	cfg := &config{}
	cmd := &cobra.Command{
		Use:    "network-cleanup",
		Short:  "Remove journaled local workload network resources",
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
			return cleanupNetworkAttachments(cmd.Context(), filepath.Join(cfg.DataDir, "network"), host)
		},
	}
	cmd.Flags().StringVar(&cfg.ConfigFile, "config", "", "Path to Trellis node configuration YAML")
	cmd.Flags().StringVar(&cfg.DataDir, "data-dir", "/var/lib/trellis/data", "Directory containing local node state")
	cmd.Flags().StringVar(&cfg.DNSListen, "dns-listen", net.JoinHostPort(network.WorkloadDNSAddress, "53"), "Configured workload DNS listen address")
	return cmd
}
