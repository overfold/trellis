package main

import (
	"fmt"
	"io"
	"slices"
	"text/tabwriter"
	"time"

	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/internal/spec"
	"github.com/spf13/cobra"
)

func NewClusterCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cluster",
		Short: "Inspect and change cluster-wide settings",
		Long:  "Cluster settings are replicated with the rest of the control-plane state, so every leader applies the same values. The node that creates the cluster supplies their initial values; later node configuration does not change them.",
	}
	cmd.AddCommand(newClusterSettingsCmd(), newClusterSetJobLimitsCmd(), newClusterSetReconciliationCmd())
	return cmd
}

func newClusterSettingsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "settings",
		Short: "Show the replicated cluster settings",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			serverClient, err := apiClient("")
			if err != nil {
				return err
			}
			settings, err := serverClient.ClusterSettings(cmd.Context())
			if err != nil {
				return err
			}
			if config.Output == "json" {
				return writeJSON(cmd.OutOrStdout(), settings)
			}
			return writeClusterSettings(cmd.OutOrStdout(), settings)
		},
	}
}

var jobLimitFlags = []string{"max-replicas-per-task-group", "max-task-groups-per-job", "max-tasks-per-task-group", "max-desired-allocations", "max-desired-allocations-per-namespace", "default-task-cpu", "default-task-memory", "max-task-cpu", "max-task-memory"}

func newClusterSetJobLimitsCmd() *cobra.Command {
	var replicas, groups, tasks, allocations, namespaceAllocations, defaultCPU, maxCPU int
	var defaultMemory, maxMemory string
	cmd := &cobra.Command{
		Use:   "set-job-limits",
		Short: "Change the cluster's job limits",
		Long:  "Change the operator-only job admission limits and task resource defaults. Only the flags given change; the rest keep their replicated values. The leader refuses limits that would stop admitting a job that is currently desired.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			flags := cmd.Flags()
			if !slices.ContainsFunc(jobLimitFlags, flags.Changed) {
				return fmt.Errorf("set at least one job limit flag")
			}
			serverClient, err := administratorServerClient()
			if err != nil {
				return err
			}
			current, err := serverClient.ClusterSettings(cmd.Context())
			if err != nil {
				return err
			}
			limits := current.JobLimits
			for name, target := range map[string]*int{
				"max-replicas-per-task-group":           &limits.MaxReplicasPerTaskGroup,
				"max-task-groups-per-job":               &limits.MaxTaskGroupsPerJob,
				"max-tasks-per-task-group":              &limits.MaxTasksPerTaskGroup,
				"max-desired-allocations":               &limits.MaxDesiredAllocations,
				"max-desired-allocations-per-namespace": &limits.MaxDesiredAllocationsPerNamespace,
				"default-task-cpu":                      &limits.DefaultTaskCPU,
				"max-task-cpu":                          &limits.MaxTaskCPU,
			} {
				if flags.Changed(name) {
					value, _ := flags.GetInt(name)
					*target = value
				}
			}
			for name, target := range map[string]*int64{"default-task-memory": &limits.DefaultTaskMemory, "max-task-memory": &limits.MaxTaskMemory} {
				if flags.Changed(name) {
					raw, _ := flags.GetString(name)
					value, err := spec.ParseByteSize(raw)
					if err != nil {
						return fmt.Errorf("--%s: %w", name, err)
					}
					*target = int64(value)
				}
			}
			settings, err := serverClient.UpdateJobLimits(cmd.Context(), limits)
			if err != nil {
				return err
			}
			return writeClusterSettings(cmd.OutOrStdout(), settings)
		},
	}
	flags := cmd.Flags()
	flags.IntVar(&replicas, "max-replicas-per-task-group", 0, "Maximum replicas allowed in one task group")
	flags.IntVar(&groups, "max-task-groups-per-job", 0, "Maximum task groups allowed in one job")
	flags.IntVar(&tasks, "max-tasks-per-task-group", 0, "Maximum tasks allowed in one task group")
	flags.IntVar(&allocations, "max-desired-allocations", 0, "Maximum desired allocations allowed in one job")
	flags.IntVar(&namespaceAllocations, "max-desired-allocations-per-namespace", 0, "Maximum desired allocations allowed in one namespace")
	flags.IntVar(&defaultCPU, "default-task-cpu", 0, "Default task CPU request in millicores")
	flags.StringVar(&defaultMemory, "default-task-memory", "", "Default task memory request, such as 128MiB")
	flags.IntVar(&maxCPU, "max-task-cpu", 0, "Maximum task CPU request in millicores")
	flags.StringVar(&maxMemory, "max-task-memory", "", "Maximum task memory request, such as 1TiB")
	return cmd
}

var reconciliationFlags = []string{"allocation-loss-timeout", "replacement-backoff-base", "replacement-backoff-max", "replacement-stable-after", "terminal-allocation-retention"}

func newClusterSetReconciliationCmd() *cobra.Command {
	var lossTimeout, backoffBase, backoffMax, stableAfter time.Duration
	var retention int
	cmd := &cobra.Command{
		Use:   "set-reconciliation",
		Short: "Change how the cluster replaces lost and failed allocations",
		Long:  "Change the replicated reconciliation settings every leader applies: how long a silent node's allocations wait before becoming lost, the replacement backoff after repeated failures, and how many terminal allocation records each task group keeps. Only the flags given change; the rest keep their replicated values.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			flags := cmd.Flags()
			if !slices.ContainsFunc(reconciliationFlags, flags.Changed) {
				return fmt.Errorf("set at least one reconciliation flag")
			}
			serverClient, err := administratorServerClient()
			if err != nil {
				return err
			}
			current, err := serverClient.ClusterSettings(cmd.Context())
			if err != nil {
				return err
			}
			reconciliation := current.Reconciliation
			for name, target := range map[string]*time.Duration{
				"allocation-loss-timeout":  &reconciliation.AllocationLossTimeout,
				"replacement-backoff-base": &reconciliation.ReplacementBackoffBase,
				"replacement-backoff-max":  &reconciliation.ReplacementBackoffMax,
				"replacement-stable-after": &reconciliation.ReplacementStableAfter,
			} {
				if flags.Changed(name) {
					*target, _ = flags.GetDuration(name)
				}
			}
			if flags.Changed("terminal-allocation-retention") {
				reconciliation.TerminalAllocationRetention = retention
			}
			settings, err := serverClient.UpdateReconciliationSettings(cmd.Context(), reconciliation)
			if err != nil {
				return err
			}
			return writeClusterSettings(cmd.OutOrStdout(), settings)
		},
	}
	flags := cmd.Flags()
	flags.DurationVar(&lossTimeout, "allocation-loss-timeout", 0, "How long a node may miss heartbeats before its allocations become lost and are replaced, such as 2m")
	flags.DurationVar(&backoffBase, "replacement-backoff-base", 0, "Replacement delay after a task group's first consecutive failure")
	flags.DurationVar(&backoffMax, "replacement-backoff-max", 0, "Maximum replacement delay after repeated failures")
	flags.DurationVar(&stableAfter, "replacement-stable-after", 0, "How long a replacement must run without being unhealthy before the failure count resets")
	flags.IntVar(&retention, "terminal-allocation-retention", 0, "Stopped, failed, or lost allocation records kept per task group")
	return cmd
}

func writeClusterSettings(out io.Writer, settings *api.ClusterSettings) error {
	limits := settings.JobLimits
	reconciliation := settings.Reconciliation
	w := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	rows := [][2]string{
		{"Job limits", ""},
		{"  Max replicas per task group", fmt.Sprint(limits.MaxReplicasPerTaskGroup)},
		{"  Max task groups per job", fmt.Sprint(limits.MaxTaskGroupsPerJob)},
		{"  Max tasks per task group", fmt.Sprint(limits.MaxTasksPerTaskGroup)},
		{"  Max desired allocations", fmt.Sprint(limits.MaxDesiredAllocations)},
		{"  Max desired allocations per namespace", fmt.Sprint(limits.MaxDesiredAllocationsPerNamespace)},
		{"  Default task CPU", fmt.Sprintf("%dm", limits.DefaultTaskCPU)},
		{"  Default task memory", spec.ByteSize(limits.DefaultTaskMemory).String()},
		{"  Max task CPU", fmt.Sprintf("%dm", limits.MaxTaskCPU)},
		{"  Max task memory", spec.ByteSize(limits.MaxTaskMemory).String()},
		{"Reconciliation", ""},
		{"  Allocation loss timeout", reconciliation.AllocationLossTimeout.String()},
		{"  Replacement backoff base", reconciliation.ReplacementBackoffBase.String()},
		{"  Replacement backoff max", reconciliation.ReplacementBackoffMax.String()},
		{"  Replacement stable after", reconciliation.ReplacementStableAfter.String()},
		{"  Terminal allocation retention", fmt.Sprint(reconciliation.TerminalAllocationRetention)},
		{"Network", ""},
		{"  WireGuard pool", settings.Network.WireGuardPool},
		{"  WireGuard port count", fmt.Sprint(settings.Network.WireGuardPortCount)},
	}
	for _, row := range rows {
		if _, err := fmt.Fprintf(w, "%s\t%s\n", row[0], row[1]); err != nil {
			return err
		}
	}
	return w.Flush()
}
