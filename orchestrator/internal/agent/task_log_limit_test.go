package agent

import (
	"context"
	"testing"
	"time"
)

type logLimitRuntime struct {
	*listingRecoveryRuntime
	limits chan int64
}

func (r *logLimitRuntime) EnforceLogLimit(limit int64) error {
	select {
	case r.limits <- limit:
	default:
	}
	return nil
}

func TestTaskLogLimitValidationAndDefault(t *testing.T) {
	agent, _ := newRecoveryTestAgent(t, &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{}})
	if got := agent.taskLogLimit(); got != DefaultTaskLogLimit {
		t.Fatalf("default task log limit = %d", got)
	}
	if err := agent.SetTaskLogLimit(MinTaskLogLimit - 1); err == nil {
		t.Fatal("task log limit below the minimum was accepted")
	}
	if err := agent.SetTaskLogLimit(16 << 20); err != nil {
		t.Fatal(err)
	}
	if got := agent.taskLogLimit(); got != 16<<20 {
		t.Fatalf("configured task log limit = %d", got)
	}
}

func TestLogLimitLoopEnforcesConfiguredLimit(t *testing.T) {
	rt := &logLimitRuntime{listingRecoveryRuntime: &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{}}, limits: make(chan int64, 1)}
	agent, _ := newRecoveryTestAgent(t, rt)
	if err := agent.SetTaskLogLimit(8 << 20); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		agent.runLogLimitLoop(ctx)
	}()
	select {
	case limit := <-rt.limits:
		if limit != 8<<20 {
			t.Fatalf("enforced limit = %d", limit)
		}
	case <-time.After(5 * logLimitInterval):
		t.Fatal("log limit was not enforced")
	}
	cancel()
	<-done
}
