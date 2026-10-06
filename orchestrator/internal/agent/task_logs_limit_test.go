package agent

import (
	"context"
	"errors"
	"io"
	"testing"
)

func TestFollowedLogStreamsAreBoundedPerAllocation(t *testing.T) {
	rt := &retainedLogsRuntime{listingRecoveryRuntime: &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{}}, logs: map[string]string{"task": "output"}}
	a := newOperationTestAgent(t, rt)
	a.allocations["task"] = recoveryTestAllocation(0)
	var streams []io.ReadCloser
	for range logFollowPerAllocationLimit {
		stream, err := a.TaskLogs(context.Background(), "allocation", "task", true, 100)
		if err != nil {
			t.Fatalf("admitted stream rejected: %v", err)
		}
		streams = append(streams, stream)
	}
	if _, err := a.TaskLogs(context.Background(), "allocation", "task", true, 100); !errors.Is(err, ErrLogStreamLimit) {
		t.Fatalf("over-limit follow error = %v, want ErrLogStreamLimit", err)
	}
	if stream, err := a.TaskLogs(context.Background(), "allocation", "task", false, 100); err != nil {
		t.Fatalf("non-follow log read was limited: %v", err)
	} else {
		_ = stream.Close()
	}
	_ = streams[0].Close()
	stream, err := a.TaskLogs(context.Background(), "allocation", "task", true, 100)
	if err != nil {
		t.Fatalf("closing a stream did not free its slot: %v", err)
	}
	_ = stream.Close()
	for _, stream := range streams[1:] {
		_ = stream.Close()
	}
}
