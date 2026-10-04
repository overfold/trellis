package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"text/tabwriter"
	"time"

	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/client"
	"github.com/overfold/trellis/orchestrator/internal/spec"
	"github.com/spf13/cobra"
)

func NewJobsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "jobs",
		Short: "Manage desired jobs",
		Long:  "Apply desired jobs, inspect their status, read logs, and delete them in the selected namespace.",
	}
	cmd.AddCommand(NewJobsApplyCmd())
	cmd.AddCommand(NewJobsListCmd())
	cmd.AddCommand(NewJobsStatusCmd())
	cmd.AddCommand(NewJobsLogsCmd())
	cmd.AddCommand(NewJobsDeleteCmd())
	cmd.AddCommand(NewJobsResetBackoffCmd())
	return cmd
}

func NewJobsApplyCmd() *cobra.Command {
	var path string
	var check bool
	var dryRun bool
	var wait bool
	var timeout time.Duration
	var interval time.Duration
	cmd := &cobra.Command{
		Use:   "apply [SOURCE]",
		Args:  cobra.MaximumNArgs(1),
		Short: "Apply a YAML job manifest",
		Long:  "Apply a YAML job manifest from a local file or GitHub repository. A GitHub repository is expected to contain trellis.yml or trellis.yaml at its root. Use --check for local validation, --dry-run to preview semantic changes, or --wait to follow the resulting revision until desired capacity is healthy.",
		RunE: func(cmd *cobra.Command, args []string) error {
			if check && dryRun {
				return fmt.Errorf("--check and --dry-run cannot be used together")
			}
			if check && wait {
				return fmt.Errorf("--check and --wait cannot be used together")
			}
			if dryRun && wait {
				return fmt.Errorf("--dry-run and --wait cannot be used together")
			}
			source := path
			if len(args) == 1 {
				if cmd.Flags().Changed("file") {
					return fmt.Errorf("SOURCE and --file cannot be used together")
				}
				source = args[0]
			}
			job, err := readJobManifest(cmd.Context(), source)
			if err != nil {
				return err
			}
			if check {
				_, err = fmt.Fprintf(cmd.OutOrStdout(), "Valid manifest: %s/%s (%d task groups, %d desired allocations)\n", job.Namespace, job.Name, len(job.TaskGroups), desiredAllocations(job))
				return err
			}
			if err := ensureActiveNamespace(job); err != nil {
				return err
			}
			serverClient, err := apiClient(job.Namespace)
			if err != nil {
				return err
			}
			rawSpec, _ := json.Marshal(job)
			jobPlan, err := serverClient.PlanJob(cmd.Context(), rawSpec)
			if err != nil {
				return err
			}
			if dryRun {
				return printJobPlan(cmd.OutOrStdout(), jobPlan)
			}
			if jobPlan.Action == "none" {
				if _, err := fmt.Fprintf(cmd.OutOrStdout(), "Job %s/%s already matches the manifest (version %d, revision %d).\n", job.Namespace, job.Name, jobPlan.BaseVersion, jobPlan.BaseRevision); err != nil {
					return err
				}
				if wait {
					return waitForJob(cmd.Context(), cmd.OutOrStdout(), serverClient, job.Name, interval, timeout)
				}
				return nil
			}
			// Apply only if the job is still the incarnation and version this
			// plan was computed against; a concurrent apply, or a delete and
			// recreate, makes the server reject it.
			expectedVersion := jobPlan.BaseVersion
			applied, err := serverClient.ApplyJob(cmd.Context(), &api.JobRegistrationRequest{
				Spec:                rawSpec,
				ResolvedImages:      jobPlan.ResolvedImages,
				ExpectedVersion:     &expectedVersion,
				ExpectedIncarnation: jobPlan.BaseIncarnation,
			})
			if err != nil {
				var httpErr *client.HTTPError
				if errors.As(err, &httpErr) && httpErr.Status == http.StatusConflict {
					return fmt.Errorf("apply job %s/%s: the job changed after it was planned (%s); run apply again to review the new plan", job.Namespace, job.Name, httpErr.Message())
				}
				return fmt.Errorf("apply job: %w", err)
			}
			if jobPlan.Action == "update" {
				if _, err := fmt.Fprintf(cmd.OutOrStdout(), "Applied job %s/%s: version %d -> %d, %s.\n", job.Namespace, job.Name, jobPlan.BaseVersion, applied.Version, revisionTransition(jobPlan.BaseRevision, applied.Revision)); err != nil {
					return err
				}
			} else {
				if _, err := fmt.Fprintf(cmd.OutOrStdout(), "Created job %s/%s at version %d, revision %d.\n", job.Namespace, job.Name, applied.Version, applied.Revision); err != nil {
					return err
				}
			}
			if wait {
				return waitForJob(cmd.Context(), cmd.OutOrStdout(), serverClient, job.Name, interval, timeout)
			}
			return nil
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&path, "file", "trellis.yaml", "YAML job manifest path")
	flags.BoolVar(&check, "check", false, "Validate the manifest locally without contacting a cluster")
	flags.BoolVar(&dryRun, "dry-run", false, "Validate and show the plan without changing the cluster")
	flags.BoolVarP(&wait, "wait", "w", false, "Wait until desired job capacity is healthy")
	flags.DurationVar(&timeout, "timeout", 5*time.Minute, "Maximum time to wait (0 means no timeout)")
	flags.DurationVar(&interval, "interval", 2*time.Second, "Polling interval while waiting")
	return cmd
}

func revisionTransition(before, after int) string {
	if before == after {
		return fmt.Sprintf("revision %d unchanged", after)
	}
	return fmt.Sprintf("revision %d -> %d", before, after)
}

func NewJobsListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List jobs in the selected namespace",
		RunE: func(cmd *cobra.Command, _ []string) error {
			serverClient, err := namespaceClient(cmd.Context())
			if err != nil {
				return err
			}
			jobs, err := serverClient.ListJobs(cmd.Context())
			if err != nil {
				return err
			}
			if config.Output == "json" {
				return writeJSON(cmd.OutOrStdout(), jobs)
			}
			if len(jobs) == 0 {
				_, err = fmt.Fprintln(cmd.OutOrStdout(), "No jobs")
				return err
			}
			w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
			if _, err := fmt.Fprintln(w, "Name\tState\tDesired\tRunning\tHealthy\tRevision"); err != nil {
				return err
			}
			for _, job := range jobs {
				if _, err := fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%d\t%d\n", job.Name, jobState(&job), job.Desired, job.Running, job.Healthy, job.Revision); err != nil {
					return err
				}
			}
			return w.Flush()
		},
	}
}

func NewJobsStatusCmd() *cobra.Command {
	var watch bool
	var history bool
	var allocation string
	var timeout time.Duration
	var interval time.Duration
	cmd := &cobra.Command{
		Use:   "status NAME",
		Args:  cobra.ExactArgs(1),
		Short: "Inspect a job and its allocations",
		Long:  "Inspect a job and its allocations. Unhealthy or converging jobs include diagnostics automatically. Use --watch to follow convergence or --history to show allocation lifecycle history.",
		RunE: func(cmd *cobra.Command, args []string) error {
			if watch && history {
				return fmt.Errorf("--watch and --history cannot be used together")
			}
			if allocation != "" && !history {
				return fmt.Errorf("--allocation requires --history")
			}
			serverClient, err := namespaceClient(cmd.Context())
			if err != nil {
				return err
			}
			if watch {
				if config.Output == "json" {
					return fmt.Errorf("--watch does not support --output json")
				}
				return waitForJob(cmd.Context(), cmd.OutOrStdout(), serverClient, args[0], interval, timeout)
			}
			if history {
				events, err := loadJobEvents(cmd.Context(), serverClient, args[0], allocation)
				if err != nil {
					return err
				}
				if config.Output == "json" {
					return writeJSON(cmd.OutOrStdout(), events)
				}
				return printJobEvents(cmd.OutOrStdout(), events)
			}
			status, err := serverClient.GetJob(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			if config.Output == "json" {
				return writeJSON(cmd.OutOrStdout(), status)
			}
			return printJobStatus(cmd.OutOrStdout(), status)
		},
	}
	flags := cmd.Flags()
	flags.BoolVarP(&watch, "watch", "w", false, "Follow the job until desired capacity is healthy")
	flags.BoolVar(&history, "history", false, "Show allocation lifecycle history instead of current status")
	flags.StringVar(&allocation, "allocation", "", "With --history, limit events to an allocation ID or unique prefix")
	flags.DurationVar(&timeout, "timeout", 5*time.Minute, "Maximum time to watch (0 means no timeout)")
	flags.DurationVar(&interval, "interval", 2*time.Second, "Polling interval while watching")
	return cmd
}

func NewJobsLogsCmd() *cobra.Command {
	var follow bool
	var tail int
	var allocation string
	var group string
	var task string
	cmd := &cobra.Command{
		Use:   "logs JOB",
		Args:  cobra.ExactArgs(1),
		Short: "Show task logs for a job without requiring full allocation IDs",
		Long:  "Show logs for a job. Non-following output includes every matching task stream. Use --allocation, --group, and --task to narrow the selection; --follow requires exactly one task stream.",
		RunE: func(cmd *cobra.Command, args []string) error {
			serverClient, err := namespaceClient(cmd.Context())
			if err != nil {
				return err
			}
			return runJobLogs(cmd.Context(), cmd.OutOrStdout(), serverClient, args[0], allocation, group, task, follow, tail)
		},
	}
	flags := cmd.Flags()
	flags.BoolVarP(&follow, "follow", "f", false, "Follow new log output")
	flags.IntVar(&tail, "tail", 100, "Number of trailing lines (0 means all)")
	flags.StringVar(&allocation, "allocation", "", "Allocation ID or unique ID prefix")
	flags.StringVar(&group, "group", "", "Only allocations for this task group")
	flags.StringVar(&task, "task", "", "Only logs for this task name")
	return cmd
}

func NewJobsResetBackoffCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "reset-backoff NAME GROUP",
		Args:  cobra.ExactArgs(2),
		Short: "Reset a task group's replacement backoff",
		Long:  "Reset the replacement backoff of a task group so its failed allocations are replaced without waiting. Use it after fixing the cause of the failures; a group without counted failures is left unchanged.",
		RunE: func(cmd *cobra.Command, args []string) error {
			serverClient, err := namespaceClient(cmd.Context())
			if err != nil {
				return err
			}
			if err := serverClient.ResetReplacementBackoff(cmd.Context(), args[0], args[1]); err != nil {
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "Reset replacement backoff of task group %s in job %s.\n", args[1], args[0])
			return err
		},
	}
}

func NewJobsDeleteCmd() *cobra.Command {
	var wait bool
	var timeout time.Duration
	var interval time.Duration
	cmd := &cobra.Command{
		Use:   "delete NAME",
		Args:  cobra.ExactArgs(1),
		Short: "Delete a job and stop its allocations",
		RunE: func(cmd *cobra.Command, args []string) error {
			serverClient, err := namespaceClient(cmd.Context())
			if err != nil {
				return err
			}
			if err := serverClient.DeleteJob(cmd.Context(), args[0]); err != nil {
				return err
			}
			if _, err = fmt.Fprintf(cmd.OutOrStdout(), "Deleted job %s.\n", args[0]); err != nil {
				return err
			}
			if wait {
				return waitForJobDeletion(cmd.Context(), cmd.OutOrStdout(), serverClient, args[0], interval, timeout)
			}
			return nil
		},
	}
	cmd.Flags().BoolVarP(&wait, "wait", "w", false, "Wait until the job is no longer present")
	cmd.Flags().DurationVar(&timeout, "timeout", 2*time.Minute, "Maximum time to wait (0 means no timeout)")
	cmd.Flags().DurationVar(&interval, "interval", time.Second, "Polling interval while waiting")
	return cmd
}

func ensureActiveNamespace(job *spec.JobSpec) error {
	if config.Namespace == "" || config.Namespace == job.Namespace {
		return nil
	}
	source := "active namespace"
	if config.Context != "" {
		source = fmt.Sprintf("context %q", config.Context)
	}
	return fmt.Errorf("manifest namespace %q does not match %s namespace %q; change the manifest or select the intended namespace", job.Namespace, source, config.Namespace)
}

// namespaceClient returns a client for the explicitly selected namespace.
func namespaceClient(_ context.Context) (*client.Client, error) {
	if config.Namespace != "" {
		return apiClient(config.Namespace)
	}
	return nil, fmt.Errorf("--namespace is required for namespace-scoped commands")
}

// apiClient returns a client authenticated with the selected credential. The
// namespace may be empty for cluster-scoped operations.
func apiClient(namespace string) (*client.Client, error) {
	tlsCfg, err := buildCLITLSConfig()
	if err != nil {
		return nil, fmt.Errorf("build TLS config: %w", err)
	}
	return client.New(client.Config{Address: config.ServerAddr, Namespace: namespace, Token: config.ClusterToken, TLSConfig: tlsCfg})
}

func printJobStatus(w io.Writer, status *api.JobStatusResponse) error {
	if _, err := fmt.Fprintf(w, "Job: %s\nIncarnation: %s\nVersion: %d\nRevision: %d\nState: %s\nDesired: %d\nRunning: %d\nHealthy: %d\n", status.Name, status.Incarnation, status.Version, status.Revision, jobState(status), status.Desired, status.Running, status.Healthy); err != nil {
		return err
	}
	if len(status.Allocations) == 0 {
		if _, err := fmt.Fprintln(w, "Allocations: none"); err != nil {
			return err
		}
	} else {
		if _, err := fmt.Fprintln(w, "Allocations:"); err != nil {
			return err
		}
		tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		if _, err := fmt.Fprintln(tw, "Allocation\tTask group\tNode\tLifecycle\tHealth\tDiagnostic"); err != nil {
			return err
		}
		for _, a := range status.Allocations {
			if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n", a.ID, a.Group, allocationNode(a), a.Phase, a.Health, diagnosticSummary(a)); err != nil {
				return err
			}
		}
		if err := tw.Flush(); err != nil {
			return err
		}
	}
	if err := printReplacementBackoff(w, status.ReplacementBackoff); err != nil {
		return err
	}
	if jobReady(status) {
		return nil
	}
	return printJobProblems(w, status)
}

func printReplacementBackoff(w io.Writer, backoffs []api.ReplacementBackoffResponse) error {
	if len(backoffs) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w, "Replacement backoff:"); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "Task group\tFailures\tNext replacement\tLast failure\tDiagnostic"); err != nil {
		return err
	}
	for _, b := range backoffs {
		diagnostic := b.Reason
		if diagnostic == "" {
			diagnostic = "—"
		}
		lastFailure := "—"
		if b.LastAllocationID != "" {
			lastFailure = b.LastAllocationID
		}
		if _, err := fmt.Fprintf(tw, "%s\t%d\t%s\t%s\t%s\n", b.Group, b.Failures, b.NextReplacementAt.Format(time.RFC3339), lastFailure, diagnostic); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func printJobProblems(w io.Writer, status *api.JobStatusResponse) error {
	if _, err := fmt.Fprintln(w, "\nProblems:"); err != nil {
		return err
	}
	if len(status.Allocations) == 0 {
		if _, err := fmt.Fprintln(w, "No allocations have been created yet. Check schedulable node capacity, placement constraints, required host volumes, and node health."); err != nil {
			return err
		}
		return printJobNextSteps(w, status.Name)
	}
	problems := 0
	for _, a := range status.Allocations {
		if !allocationNeedsAttention(a, status.Revision) {
			continue
		}
		problems++
		if _, err := fmt.Fprintf(w, "- %s %s on %s: lifecycle=%s health=%s\n", a.ID, a.Group, allocationNode(a), a.Phase, a.Health); err != nil {
			return err
		}
		if a.Reason != "" {
			if _, err := fmt.Fprintf(w, "  reason: %s\n", a.Reason); err != nil {
				return err
			}
		}
		if a.Message != "" {
			if _, err := fmt.Fprintf(w, "  message: %s\n", a.Message); err != nil {
				return err
			}
		}
		if a.NextRetryAt != nil {
			if _, err := fmt.Fprintf(w, "  next retry: %s\n", a.NextRetryAt.Format(time.RFC3339)); err != nil {
				return err
			}
		}
		if a.Attempt > 0 {
			if _, err := fmt.Fprintf(w, "  attempt: %d\n", a.Attempt); err != nil {
				return err
			}
		}
	}
	if problems == 0 {
		if _, err := fmt.Fprintln(w, "The job is still converging; no allocation reports a specific failure yet."); err != nil {
			return err
		}
	}
	return printJobNextSteps(w, status.Name)
}

func printJobNextSteps(w io.Writer, name string) error {
	_, err := fmt.Fprintf(w, "\nInspect logs with: trellisctl jobs logs %s\nFollow convergence with: trellisctl jobs status %s --watch\nView lifecycle history with: trellisctl jobs status %s --history\n", name, name, name)
	return err
}

func allocationNeedsAttention(a api.AllocationResponse, currentRevision int) bool {
	if a.Draining && a.JobRevision < currentRevision && a.Health != "unhealthy" && a.Phase != "failed" && a.Phase != "lost" {
		return false
	}
	return a.Phase != "running" || a.Health != "healthy" || a.Reason != "" || a.Message != "" || a.NextRetryAt != nil
}

func diagnosticSummary(a api.AllocationResponse) string {
	if a.Reason != "" {
		return a.Reason
	}
	if a.Message != "" {
		return a.Message
	}
	if a.NextRetryAt != nil {
		return "retry scheduled"
	}
	return "—"
}

func allocationNode(a api.AllocationResponse) string {
	if a.NodeID.String() == "00000000-0000-0000-0000-000000000000" {
		return "—"
	}
	return shortID(a.NodeID.String())
}

func shortID(id string) string {
	if len(id) <= 8 {
		return id
	}
	return id[:8]
}

func jobReady(status *api.JobStatusResponse) bool {
	if status.Desired <= 0 {
		return false
	}
	if len(status.Allocations) == 0 {
		return status.Running >= status.Desired && status.Healthy >= status.Desired
	}
	currentRunning, currentHealthy := 0, 0
	for _, a := range status.Allocations {
		if a.JobRevision != status.Revision || a.Draining {
			continue
		}
		if a.Phase == api.PhaseRunning {
			currentRunning++
		}
		if a.Phase == api.PhaseRunning && a.Health == api.HealthHealthy {
			currentHealthy++
		}
	}
	return currentRunning >= status.Desired && currentHealthy >= status.Desired
}

func jobState(status *api.JobStatusResponse) string {
	if jobReady(status) {
		return "ready"
	}
	for _, a := range status.Allocations {
		if a.Draining && a.JobRevision < status.Revision {
			continue
		}
		if a.Health == "unhealthy" || a.Phase == "failed" || a.Phase == "lost" {
			return "degraded"
		}
	}
	return "converging"
}

func desiredAllocations(job *spec.JobSpec) int {
	total := 0
	for _, group := range job.TaskGroups {
		total += group.Count
	}
	return total
}
