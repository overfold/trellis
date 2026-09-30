package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/overfold/trellis/orchestrator/internal/api"
	"github.com/overfold/trellis/orchestrator/internal/client"
	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
)

func isHTTPStatus(err error, status int) bool {
	var httpErr *client.HTTPError
	return errors.As(err, &httpErr) && httpErr.Status == status
}

func waitForJob(parent context.Context, w io.Writer, serverClient *client.ServerClient, name string, interval, timeout time.Duration) error {
	if interval <= 0 {
		return fmt.Errorf("interval must be greater than zero")
	}

	ctx := parent
	cancel := func() {}
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(parent, timeout)
	}
	defer cancel()

	last := ""
	for {
		status, err := serverClient.GetJob(ctx, name)
		if err != nil {
			return err
		}
		line := fmt.Sprintf("revision %d: %d/%d running, %d/%d healthy (%s)", status.Revision, status.Running, status.Desired, status.Healthy, status.Desired, jobState(status))
		if line != last {
			if _, err := fmt.Fprintln(w, line); err != nil {
				return err
			}
			last = line
		}
		if jobReady(status) {
			_, err := fmt.Fprintf(w, "Ready: %d/%d allocations healthy.\n", status.Healthy, status.Desired)
			return err
		}

		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return fmt.Errorf("timed out waiting for job %s to become healthy; run 'trellisctl jobs status %s'", name, name)
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func waitForJobDeletion(parent context.Context, w io.Writer, serverClient *client.ServerClient, name string, interval, timeout time.Duration) error {
	if interval <= 0 {
		return fmt.Errorf("interval must be greater than zero")
	}

	ctx := parent
	cancel := func() {}
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(parent, timeout)
	}
	defer cancel()

	for {
		_, err := serverClient.GetJob(ctx, name)
		if isHTTPStatus(err, http.StatusNotFound) {
			_, err = fmt.Fprintln(w, "Job is no longer present.")
			return err
		}
		if err != nil {
			return err
		}

		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return fmt.Errorf("timed out waiting for job %s to disappear", name)
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

type jobAllocationEvent struct {
	Allocation string          `json:"allocation"`
	Group      string          `json:"group"`
	Phase      lifecycle.Phase `json:"phase"`
	Reason     string          `json:"reason,omitempty"`
	Message    string          `json:"message,omitempty"`
	At         time.Time       `json:"at"`
}

func loadJobEvents(ctx context.Context, serverClient *client.ServerClient, target, allocationRef string) ([]jobAllocationEvent, error) {
	status, err := serverClient.GetJob(ctx, target)
	if err != nil {
		return nil, err
	}
	matches := append([]api.AllocationResponse(nil), status.Allocations...)
	if allocationRef != "" {
		resolved, err := resolveAllocationPrefix(matches, allocationRef)
		if err != nil {
			return nil, err
		}
		matches = []api.AllocationResponse{resolved}
	}
	if len(matches) == 0 {
		return nil, fmt.Errorf("job %s has no allocations", target)
	}

	result := make([]jobAllocationEvent, 0)
	for _, allocation := range matches {
		events, err := serverClient.AllocationEvents(ctx, allocation.ID)
		if err != nil {
			return nil, fmt.Errorf("allocation %s: %w", shortID(allocation.ID), err)
		}
		for _, event := range *events {
			result = append(result, jobAllocationEvent{
				Allocation: allocation.ID,
				Group:      allocation.Group,
				Phase:      event.Phase,
				Reason:     event.Reason,
				Message:    event.Message,
				At:         event.At,
			})
		}
	}
	sort.SliceStable(result, func(i, j int) bool {
		if result[i].At.Equal(result[j].At) {
			return result[i].Allocation < result[j].Allocation
		}
		return result[i].At.Before(result[j].At)
	})
	return result, nil
}

func printJobEvents(w io.Writer, events []jobAllocationEvent) error {
	if len(events) == 0 {
		_, err := fmt.Fprintln(w, "No lifecycle events recorded.")
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "Time\tAllocation\tTask group\tLifecycle\tReason\tMessage"); err != nil {
		return err
	}
	for _, event := range events {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\t%s\n",
			event.At.Format(time.RFC3339),
			shortID(event.Allocation),
			event.Group,
			event.Phase,
			singleLine(event.Reason),
			singleLine(event.Message),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func singleLine(value string) string {
	value = strings.ReplaceAll(value, "\t", " ")
	value = strings.ReplaceAll(value, "\r", " ")
	value = strings.ReplaceAll(value, "\n", " ")
	if value == "" {
		return "—"
	}
	return value
}

type jobLogStream struct {
	allocation api.AllocationResponse
	task       string
}

func runJobLogs(ctx context.Context, w io.Writer, serverClient *client.ServerClient, target, allocationRef, group, task string, follow bool, tail int) error {
	streams, err := resolveLogStreams(ctx, serverClient, target, allocationRef, group, task)
	if err != nil {
		return err
	}
	if follow && len(streams) != 1 {
		return fmt.Errorf("--follow requires exactly one task stream; narrow with --allocation, --group, or --task (matches: %s)", logStreamRefs(streams))
	}

	for i, stream := range streams {
		if len(streams) > 1 {
			if _, err := fmt.Fprintf(w, "==> %s %s/%s <==\n", shortID(stream.allocation.ID), stream.allocation.Group, displayTask(stream.task)); err != nil {
				return err
			}
		}
		logs, err := serverClient.AllocationTaskLogs(ctx, stream.allocation.ID, stream.task, follow, tail)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(w, logs)
		closeErr := logs.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		if len(streams) > 1 && i < len(streams)-1 {
			if _, err := fmt.Fprintln(w); err != nil {
				return err
			}
		}
	}
	return nil
}

func resolveLogStreams(ctx context.Context, serverClient *client.ServerClient, target, allocationRef, group, task string) ([]jobLogStream, error) {
	status, err := serverClient.GetJob(ctx, target)
	if err != nil {
		return nil, err
	}
	matches := append([]api.AllocationResponse(nil), status.Allocations...)
	matches = filterAllocations(matches, group)
	matches = preferActiveAllocations(matches)
	if allocationRef != "" {
		resolved, err := resolveAllocationPrefix(matches, allocationRef)
		if err != nil {
			return nil, err
		}
		matches = []api.AllocationResponse{resolved}
	}
	if len(matches) == 0 {
		return nil, fmt.Errorf("job %s has no allocations matching the requested filters", target)
	}

	groupTasks := make(map[string][]string)
	if status.Spec != nil {
		for _, candidateGroup := range status.Spec.TaskGroups {
			for _, candidateTask := range candidateGroup.Tasks {
				groupTasks[candidateGroup.Name] = append(groupTasks[candidateGroup.Name], candidateTask.Name)
			}
		}
	}

	streams := make([]jobLogStream, 0)
	for _, allocation := range matches {
		tasks := groupTasks[allocation.Group]
		if task != "" {
			if containsTask(tasks, task) {
				streams = append(streams, jobLogStream{allocation: allocation, task: task})
			}
			continue
		}
		if len(tasks) == 0 {
			streams = append(streams, jobLogStream{allocation: allocation})
			continue
		}
		for _, taskName := range tasks {
			streams = append(streams, jobLogStream{allocation: allocation, task: taskName})
		}
	}
	if len(streams) == 0 {
		if task != "" {
			return nil, fmt.Errorf("job %s has no task %q matching the requested allocation/group filters", target, task)
		}
		return nil, fmt.Errorf("job %s has no log streams matching the requested filters", target)
	}
	return streams, nil
}

func containsTask(tasks []string, task string) bool {
	for _, candidate := range tasks {
		if candidate == task {
			return true
		}
	}
	return false
}

func displayTask(task string) string {
	if task == "" {
		return "task"
	}
	return task
}

func logStreamRefs(streams []jobLogStream) string {
	refs := make([]string, 0, len(streams))
	for _, stream := range streams {
		refs = append(refs, fmt.Sprintf("%s/%s/%s", shortID(stream.allocation.ID), stream.allocation.Group, displayTask(stream.task)))
	}
	sort.Strings(refs)
	return strings.Join(refs, ", ")
}

func preferActiveAllocations(allocations []api.AllocationResponse) []api.AllocationResponse {
	var active []api.AllocationResponse
	for _, a := range allocations {
		if a.Phase != lifecycle.PhaseStopped && a.Phase != lifecycle.PhaseFailed && a.Phase != lifecycle.PhaseLost {
			active = append(active, a)
		}
	}
	if len(active) > 0 {
		return active
	}
	return allocations
}

func filterAllocations(allocations []api.AllocationResponse, group string) []api.AllocationResponse {
	result := make([]api.AllocationResponse, 0, len(allocations))
	for _, allocation := range allocations {
		if group != "" && allocation.Group != group {
			continue
		}
		result = append(result, allocation)
	}
	return result
}

func resolveAllocationPrefix(allocations []api.AllocationResponse, ref string) (api.AllocationResponse, error) {
	var matches []api.AllocationResponse
	for _, allocation := range allocations {
		if allocation.ID == ref {
			return allocation, nil
		}
		if strings.HasPrefix(allocation.ID, ref) {
			matches = append(matches, allocation)
		}
	}
	if len(matches) == 1 {
		return matches[0], nil
	}
	if len(matches) == 0 {
		return api.AllocationResponse{}, fmt.Errorf("no allocation matches %q", ref)
	}
	return api.AllocationResponse{}, fmt.Errorf("allocation prefix %q is ambiguous (%s)", ref, allocationRefs(matches))
}

func allocationRefs(allocations []api.AllocationResponse) string {
	refs := make([]string, 0, len(allocations))
	for _, allocation := range allocations {
		refs = append(refs, shortID(allocation.ID))
	}
	sort.Strings(refs)
	return strings.Join(refs, ", ")
}
