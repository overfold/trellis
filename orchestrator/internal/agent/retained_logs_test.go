package agent

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
)

type retainedLogsRuntime struct {
	*listingRecoveryRuntime
	logs      map[string]string
	follow    bool
	removeErr error
}

func (r *retainedLogsRuntime) RemoveRetainingLogs(context.Context, string) error {
	r.removeCount++
	return nil
}

func (r *retainedLogsRuntime) RemoveRetainedLogs(id string) error {
	if r.removeErr != nil {
		return r.removeErr
	}
	delete(r.logs, id)
	return nil
}

func (r *retainedLogsRuntime) Logs(_ context.Context, id string, follow bool, _ int) (io.ReadCloser, error) {
	r.follow = follow
	return io.NopCloser(strings.NewReader(r.logs[id])), nil
}

func TestTerminalLogsSurviveCleanupAndRecovery(t *testing.T) {
	ctx := context.Background()
	rt := &retainedLogsRuntime{listingRecoveryRuntime: &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{}}, logs: map[string]string{"task": "nginx: missing pid directory\n"}}
	agent, local := newRecoveryTestAgent(t, rt)
	allocation := recoveryTestAllocation(0)
	allocation.Status, allocation.Health, allocation.RestartExhausted = "failed", "unhealthy", true
	agent.allocations[allocation.ID] = allocation
	if err := agent.persistAllocation(allocation); err != nil {
		t.Fatal(err)
	}
	request := &nodeapi.StopAllocationRequest{AllocationID: "allocation", Generation: 1, Epoch: 1, RetainLogs: true}
	for range 2 {
		if err := agent.StopGroup(ctx, request); err != nil {
			t.Fatal(err)
		}
	}
	if len(agent.allocations) != 0 || rt.removeCount != 1 {
		t.Fatalf("execution cleanup: allocations=%v removes=%d", agent.allocations, rt.removeCount)
	}
	statuses := agent.allocationStatuses()
	if len(statuses) != 1 || !statuses[0].RetainedLogs {
		t.Fatalf("retained observation: %+v", statuses)
	}

	restarted := newOperationTestAgent(t, rt)
	restarted.ConfigureDurability(local, "test")
	if err := restarted.recover(ctx); err != nil {
		t.Fatal(err)
	}
	logs, err := restarted.TaskLogs(ctx, "allocation", "task", true, 100)
	if err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(logs)
	_ = logs.Close()
	if err != nil || string(output) != "nginx: missing pid directory\n" || rt.follow {
		t.Fatalf("retained output=%q err=%v follow=%v", output, err, rt.follow)
	}
	if _, err := restarted.TaskLogs(ctx, "allocation", "other", false, 100); !errors.Is(err, ErrAllocationNotFound) {
		t.Fatalf("wrong task: %v", err)
	}

	// A stale leader cannot delete the retained output.
	request.RetainLogs, request.Epoch = false, 0
	if err := restarted.StopGroup(ctx, request); !errors.Is(err, ErrInvalidEpoch) {
		t.Fatalf("invalid epoch: %v", err)
	}
	request.Epoch = 2
	// Failed cleanup remains advertised and is retried.
	rt.removeErr = errors.New("disk unavailable")
	if err := restarted.StopGroup(ctx, request); !errors.Is(err, rt.removeErr) {
		t.Fatalf("cleanup error: %v", err)
	}
	if len(restarted.retainedLogs) != 1 {
		t.Fatal("failed cleanup lost retained metadata")
	}
	rt.removeErr = nil
	for range 2 {
		if err := restarted.StopGroup(ctx, request); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := restarted.TaskLogs(ctx, "allocation", "task", false, 100); !errors.Is(err, ErrAllocationNotFound) {
		t.Fatalf("pruned logs: %v", err)
	}
	if len(rt.logs) != 0 || len(restarted.allocationStatuses()) != 0 {
		t.Fatal("pruned logs still advertised or stored")
	}
}

func TestRetainedLogsFenceCleanupAndIsolateAllocations(t *testing.T) {
	rt := &retainedLogsRuntime{listingRecoveryRuntime: &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{}}, logs: map[string]string{"new-task": "new", "other-task": "other"}}
	a := newOperationTestAgent(t, rt)
	a.retainedLogs["new-task"] = &retainedTaskLog{AllocationID: "allocation", Generation: 2, TaskName: "app", ContainerID: "new-task"}
	a.retainedLogs["other-task"] = &retainedTaskLog{AllocationID: "other", Generation: 1, TaskName: "app", ContainerID: "other-task"}
	request := &nodeapi.StopAllocationRequest{AllocationID: "allocation", Generation: 1, Epoch: 5}
	if err := a.StopGroup(context.Background(), request); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("stale generation: %v", err)
	}
	if len(rt.logs) != 2 {
		t.Fatal("stale generation removed logs")
	}
	request.Generation, request.Epoch = 2, 4
	if err := a.StopGroup(context.Background(), request); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("stale epoch: %v", err)
	}
	request.Epoch = 5
	if err := a.StopGroup(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if len(rt.logs) != 1 || rt.logs["other-task"] != "other" || len(a.retainedLogs) != 1 {
		t.Fatal("cleanup affected an unrelated allocation")
	}
}

func TestRecoverLogsAfterCrashDuringContainerCleanup(t *testing.T) {
	rt := &retainedLogsRuntime{listingRecoveryRuntime: &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{}}, logs: map[string]string{"task": "startup failure"}}
	allocation := recoveryTestAllocation(0)
	allocation.Status = "stopping"
	a, _ := newRecoveryTestAgent(t, rt, allocation)
	// Cleanup persists the log index before removing the container. Simulate
	// a crash after removal but before deletion of the execution record.
	if err := a.persistRetainedLog(&retainedTaskLog{AllocationID: "allocation", Generation: 1, Namespace: "default", TaskName: "task", ContainerID: "task"}); err != nil {
		t.Fatal(err)
	}
	if err := a.recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(a.allocations) != 0 {
		t.Fatal("missing container retained execution resources")
	}
	stream, err := a.TaskLogs(context.Background(), "allocation", "task", true, 100)
	if err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(stream)
	_ = stream.Close()
	if err != nil || string(output) != "startup failure" || rt.follow {
		t.Fatalf("recovered logs=%q follow=%v err=%v", output, rt.follow, err)
	}
}
