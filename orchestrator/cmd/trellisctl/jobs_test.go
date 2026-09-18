package main

import (
	"strings"
	"testing"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/lifecycle"
	"github.com/clofour/trellis/internal/plan"
)

func TestJobsCommandSurface(t *testing.T) {
	previousConfig := config
	config = CLIConfig{}
	t.Cleanup(func() { config = previousConfig })

	root := newRootCmd()
	jobs, _, err := root.Find([]string{"jobs"})
	if err != nil {
		t.Fatalf("find jobs: %v", err)
	}
	want := map[string]bool{
		"apply":  true,
		"delete": true,
		"list":   true,
		"logs":   true,
		"status": true,
	}
	got := map[string]bool{}
	for _, command := range jobs.Commands() {
		if !command.Hidden {
			got[command.Name()] = true
		}
	}
	if len(got) != len(want) {
		t.Fatalf("jobs commands = %v, want %v", got, want)
	}
	for name := range want {
		if !got[name] {
			t.Fatalf("jobs command %q is missing: %v", name, got)
		}
	}

	apply, _, err := root.Find([]string{"jobs", "apply"})
	if err != nil {
		t.Fatalf("find jobs apply: %v", err)
	}
	for _, flag := range []string{"check", "dry-run", "wait"} {
		if apply.Flags().Lookup(flag) == nil {
			t.Fatalf("jobs apply is missing --%s", flag)
		}
	}
}

func TestJobsApplyRejectsSourceAndFile(t *testing.T) {
	cmd := NewJobsApplyCmd()
	cmd.SetArgs([]string{"github.com/overfold/example-app"})
	if err := cmd.Flags().Set("file", "trellis.yaml"); err != nil {
		t.Fatal(err)
	}
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "SOURCE and --file cannot be used together") {
		t.Fatalf("error = %v", err)
	}
}

func TestPrintJobPlanFormatsHumanDurations(t *testing.T) {
	previousOutput := config.Output
	config.Output = "table"
	defer func() { config.Output = previousOutput }()

	result := &plan.Result{
		Action:       "update",
		Namespace:    "default",
		Job:          "web",
		BaseRevision: 7,
		Changes: []plan.Change{{
			Operation: "change",
			Path:      "task_groups[frontend].restart.window",
			Before:    float64(60_000_000_000),
			After:     float64(120_000_000_000),
		}},
	}
	var out strings.Builder
	if err := printJobPlan(&out, result); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "1m0s -> 2m0s") {
		t.Fatalf("plan output did not humanize duration: %q", out.String())
	}
}

func TestPrintJobStatusIncludesDiagnostics(t *testing.T) {
	status := &api.JobStatusResponse{
		Name:     "web",
		Revision: 2,
		Desired:  1,
		Running:  1,
		Healthy:  0,
		Allocations: []api.AllocationResponse{{
			ID:          "abcdef12-rest",
			Group:       "web",
			JobRevision: 2,
			Phase:       lifecycle.PhaseRunning,
			Health:      lifecycle.HealthUnhealthy,
			Reason:      "health_check_failed",
			Message:     "connection refused",
		}},
	}
	var out strings.Builder
	if err := printJobStatus(&out, status); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	for _, want := range []string{
		"Problems:",
		"health_check_failed",
		"connection refused",
		"trellisctl jobs status web --watch",
		"trellisctl jobs status web --history",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("status output %q does not contain %q", text, want)
		}
	}
}

func TestResolveAllocationPrefix(t *testing.T) {
	allocations := []api.AllocationResponse{{ID: "abcdef12-one"}, {ID: "12345678-two"}}
	got, err := resolveAllocationPrefix(allocations, "abcdef")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "abcdef12-one" {
		t.Fatalf("resolved %q", got.ID)
	}
	_, err = resolveAllocationPrefix(append(allocations, api.AllocationResponse{ID: "abcdef99-three"}), "abcdef")
	if err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("expected ambiguous error, got %v", err)
	}
}

func TestJobStateSeparatesConvergingAndDegraded(t *testing.T) {
	converging := &api.JobStatusResponse{Desired: 2, Running: 1, Healthy: 1, Allocations: []api.AllocationResponse{{Phase: lifecycle.PhaseStarting, Health: lifecycle.HealthUnknown}}}
	if got := jobState(converging); got != "converging" {
		t.Fatalf("state = %q", got)
	}
	degraded := &api.JobStatusResponse{Desired: 2, Running: 2, Healthy: 1, Allocations: []api.AllocationResponse{{Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthUnhealthy}}}
	if got := jobState(degraded); got != "degraded" {
		t.Fatalf("state = %q", got)
	}
	ready := &api.JobStatusResponse{Desired: 2, Running: 2, Healthy: 2}
	if got := jobState(ready); got != "ready" {
		t.Fatalf("state = %q", got)
	}
	overlap := &api.JobStatusResponse{
		Revision: 2,
		Desired:  2,
		Running:  3,
		Healthy:  3,
		Allocations: []api.AllocationResponse{
			{JobRevision: 1, Draining: true, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy},
			{JobRevision: 2, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy},
			{JobRevision: 2, Phase: lifecycle.PhaseStarting, Health: lifecycle.HealthUnknown},
		},
	}
	if got := jobState(overlap); got != "converging" {
		t.Fatalf("rolling overlap state = %q, want converging", got)
	}
}
