package agent

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/clofour/trellis/internal/runtime"
	"github.com/clofour/trellis/internal/spec"
)

type reconcilerRuntime struct {
	status       runtime.ContainerStatus
	restartCount int
	restartErr   error
}

func (r *reconcilerRuntime) Pull(context.Context, string) error { return nil }
func (r *reconcilerRuntime) Create(context.Context, runtime.CreateOptions) (string, error) {
	return "", nil
}
func (r *reconcilerRuntime) Start(context.Context, string) error { return nil }
func (r *reconcilerRuntime) Restart(context.Context, string) error {
	r.restartCount++
	return r.restartErr
}
func (r *reconcilerRuntime) Stop(context.Context, string) error   { return nil }
func (r *reconcilerRuntime) Remove(context.Context, string) error { return nil }
func (r *reconcilerRuntime) Exec(context.Context, string, []string) (int, error) {
	return 0, nil
}
func (r *reconcilerRuntime) ExecOutput(context.Context, string, []string) ([]byte, []byte, int, error) {
	return nil, nil, 0, nil
}
func (r *reconcilerRuntime) StartTerminal(context.Context, string, []string, string, uint32, uint32) (runtime.TerminalSession, error) {
	return nil, nil
}
func (r *reconcilerRuntime) Metrics(context.Context, string) (*runtime.ContainerMetrics, error) {
	return &runtime.ContainerMetrics{}, nil
}
func (r *reconcilerRuntime) Inspect(context.Context, string) (*runtime.ContainerInfo, error) {
	return &runtime.ContainerInfo{Status: r.status}, nil
}
func (r *reconcilerRuntime) Logs(context.Context, string, bool, int) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

type statusRecorder struct {
	statuses []string
}

func (s *statusRecorder) OnReconciledStatus(_ string, status string) {
	s.statuses = append(s.statuses, status)
}

func TestAllocationReconcilerRestartsStoppedAllocation(t *testing.T) {
	rt := &reconcilerRuntime{status: runtime.StatusStopped}
	subscriber := &statusRecorder{}
	r := NewAllocationReconciler(rt, subscriber)
	r.Track("alloc-1", false, nil)

	if err := r.Reconcile(context.Background(), "alloc-1"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if rt.restartCount != 1 {
		t.Fatalf("restart count = %d, want 1", rt.restartCount)
	}
	if got := subscriber.statuses[len(subscriber.statuses)-1]; got != "healthy" {
		t.Fatalf("status = %q, want healthy", got)
	}
}

func TestAllocationReconcilerWaitsForHealthAfterRestart(t *testing.T) {
	rt := &reconcilerRuntime{status: runtime.StatusStopped}
	subscriber := &statusRecorder{}
	r := NewAllocationReconciler(rt, subscriber)
	r.Track("alloc-1", true, nil)

	if err := r.Reconcile(context.Background(), "alloc-1"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := subscriber.statuses[len(subscriber.statuses)-1]; got != "running" {
		t.Fatalf("status after restart = %q, want running", got)
	}

	if err := r.ObserveHealth("alloc-1", true); err != nil {
		t.Fatalf("observe health: %v", err)
	}
	if got := subscriber.statuses[len(subscriber.statuses)-1]; got != "healthy" {
		t.Fatalf("status after health observation = %q, want healthy", got)
	}
}

func TestAllocationReconcilerPublishesUnhealthyAfterRestartBudget(t *testing.T) {
	rt := &reconcilerRuntime{status: runtime.StatusStopped}
	subscriber := &statusRecorder{}
	r := NewAllocationReconciler(rt, subscriber)
	r.Track("alloc-1", false, nil)

	for i := 0; i < defaultMaxRestarts+1; i++ {
		if err := r.Reconcile(context.Background(), "alloc-1"); err != nil {
			t.Fatalf("reconcile %d: %v", i, err)
		}
	}
	if rt.restartCount != defaultMaxRestarts {
		t.Fatalf("restart count = %d, want %d", rt.restartCount, defaultMaxRestarts)
	}
	if got := subscriber.statuses[len(subscriber.statuses)-1]; got != "unhealthy" {
		t.Fatalf("status = %q, want unhealthy", got)
	}
}

func TestAllocationReconcilerDoesNothingWhenRunning(t *testing.T) {
	rt := &reconcilerRuntime{status: runtime.StatusRunning}
	r := NewAllocationReconciler(rt, nil)
	r.Track("alloc-1", false, nil)

	if err := r.Reconcile(context.Background(), "alloc-1"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if rt.restartCount != 0 {
		t.Fatalf("restart count = %d, want 0", rt.restartCount)
	}
}

func TestAllocationReconcilerUsesConfiguredRestartBudget(t *testing.T) {
	rt := &reconcilerRuntime{status: runtime.StatusStopped}
	subscriber := &statusRecorder{}
	r := NewAllocationReconciler(rt, subscriber)
	r.Track("alloc-1", false, &spec.RestartPolicySpec{MaxRestarts: 1, Window: time.Minute})

	for i := 0; i < 2; i++ {
		if err := r.Reconcile(context.Background(), "alloc-1"); err != nil {
			t.Fatalf("reconcile %d: %v", i, err)
		}
	}
	if rt.restartCount != 1 {
		t.Fatalf("restart count = %d, want 1", rt.restartCount)
	}
	if got := subscriber.statuses[len(subscriber.statuses)-1]; got != "unhealthy" {
		t.Fatalf("status = %q, want unhealthy", got)
	}
}

func TestAllocationReconcilerResetsBudgetAfterConfiguredWindow(t *testing.T) {
	rt := &reconcilerRuntime{status: runtime.StatusStopped}
	r := NewAllocationReconciler(rt, nil)
	window := time.Second
	r.Track("alloc-1", false, &spec.RestartPolicySpec{MaxRestarts: 1, Window: window})

	if err := r.Reconcile(context.Background(), "alloc-1"); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	r.mu.Lock()
	r.states["alloc-1"].window = time.Now().Add(-2 * window)
	r.mu.Unlock()
	if err := r.Reconcile(context.Background(), "alloc-1"); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if rt.restartCount != 2 {
		t.Fatalf("restart count = %d, want 2", rt.restartCount)
	}
}
