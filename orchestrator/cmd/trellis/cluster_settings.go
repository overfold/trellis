package main

import (
	"fmt"
	"log/slog"

	"github.com/overfold/trellis/orchestrator/internal/server"
	"github.com/spf13/pflag"
)

type explicitClusterSettings struct {
	JobLimits, WireGuardPool, WireGuardPortCount bool
}

var jobLimitFlags = []string{"max-replicas-per-task-group", "max-task-groups-per-job", "max-tasks-per-task-group", "max-desired-allocations", "max-desired-allocations-per-namespace", "default-task-cpu", "default-task-memory", "max-task-cpu", "max-task-memory"}

// recordExplicitClusterSettingFlags adds cluster settings set by flags to
// those the configuration file set.
func recordExplicitClusterSettingFlags(cfg *config, flags *pflag.FlagSet) {
	for _, name := range jobLimitFlags {
		cfg.Explicit.JobLimits = cfg.Explicit.JobLimits || flags.Changed(name)
	}
	cfg.Explicit.WireGuardPool = cfg.Explicit.WireGuardPool || flags.Changed("wireguard-pool")
	cfg.Explicit.WireGuardPortCount = cfg.Explicit.WireGuardPortCount || flags.Changed("wireguard-port-count")
}

// applyClusterSettings reconciles this node's configured cluster settings with
// the replicated ones once the control plane is initialized. Configured values
// only initialize a new cluster; afterwards every node, whether it created the
// cluster or joined it, uses the replicated values so leadership never changes
// cluster semantics.
//
// Job limits and the pool never affect the local node, so a differing
// configured value is ignored with a warning. The WireGuard port count also
// sizes this node's own namespace port range: a node that leaves it unset
// adopts the cluster's count, and one that configures a different count is
// refused because the leader would reject its registration.
func applyClusterSettings(log *slog.Logger, cfg *config, configured, replicated server.ClusterSettings) error {
	if cfg.Explicit.JobLimits && configured.JobLimits != replicated.JobLimits {
		log.Warn("configured job_limits differ from the cluster's replicated job limits and are ignored; view them with trellisctl cluster settings and change them with trellisctl cluster set-job-limits")
	}
	if cfg.Explicit.WireGuardPool && configured.WireGuardPool != replicated.WireGuardPool {
		log.Warn("configured wireguard_pool differs from the cluster's replicated pool and is ignored", "configured", configured.WireGuardPool.String(), "cluster", replicated.WireGuardPool.String())
	}
	if configured.WireGuardPortCount == replicated.WireGuardPortCount {
		return nil
	}
	if cfg.Explicit.WireGuardPortCount {
		return fmt.Errorf("wireguard_port_count or --wireguard-port-count is %d but the cluster uses %d; set it to %d or remove it", configured.WireGuardPortCount, replicated.WireGuardPortCount, replicated.WireGuardPortCount)
	}
	if cfg.WireGuardPort+replicated.WireGuardPortCount-1 > 65535 {
		return fmt.Errorf("--wireguard-port %d cannot hold the cluster's %d WireGuard ports below 65536", cfg.WireGuardPort, replicated.WireGuardPortCount)
	}
	cfg.WireGuardPortCount = replicated.WireGuardPortCount
	return nil
}
