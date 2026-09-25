package agent

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/clofour/trellis/internal/runtime"
	"github.com/clofour/trellis/internal/spec"
)

const (
	reconcileInterval    = 3 * time.Second
	defaultMaxRestarts   = 3
	defaultRestartWindow = 10 * time.Minute
)

// AllocationReconciler is the single authority for local allocation lifecycle
// decisions. Runtime inspection and health checks provide observations; this
// reconciler decides how those observations affect allocation state.
type AllocationReconciler struct {
	log     *slog.Logger
	runtime runtime.ContainerRuntime

	mu     sync.Mutex
	states map[string]*allocationReconcileState

	Subscriber AllocationReconcileSubscriber
}

// AllocationReconcileSubscriber receives reconciliation state changes.
type AllocationReconcileSubscriber interface {
	OnReconciledStatus(allocID, status string)
}

type allocationReconcileState struct {
	operation     sync.Mutex
	stopping      bool
	healthManaged bool
	restarting    bool
	attempts      int
	window        time.Time
	maxRestarts   int
	restartWindow time.Duration
}

// NewAllocationReconciler creates an allocation reconciliation controller.
func NewAllocationReconciler(runtime runtime.ContainerRuntime, subscriber AllocationReconcileSubscriber) *AllocationReconciler {
	return &AllocationReconciler{
		log:        slog.Default(),
		runtime:    runtime,
		states:     make(map[string]*allocationReconcileState),
		Subscriber: subscriber,
	}
}

// Track begins reconciliation for an allocation.
func (r *AllocationReconciler) Track(allocID string, healthManaged bool, policy *spec.RestartPolicySpec) {
	r.TrackRecovered(allocID, healthManaged, policy, 0, time.Time{})
}

// TrackRecovered restores reconciliation state for an allocation.
func (r *AllocationReconciler) TrackRecovered(allocID string, healthManaged bool, policy *spec.RestartPolicySpec, attempts int, window time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()

	maxRestarts := defaultMaxRestarts
	restartWindow := defaultRestartWindow
	if policy != nil {
		maxRestarts = policy.MaxRestarts
		restartWindow = policy.Window
	}
	if window.IsZero() {
		window = time.Now()
	}
	r.states[allocID] = &allocationReconcileState{
		healthManaged: healthManaged,
		attempts:      attempts,
		window:        window,
		maxRestarts:   maxRestarts,
		restartWindow: restartWindow,
	}
}

// Untrack stops reconciliation for an allocation.
func (r *AllocationReconciler) Untrack(allocID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.states[allocID]; !ok {
		return nil
	}
	delete(r.states, allocID)
	return nil
}

// BeginStop waits for an active reconciliation pass and prevents further restarts.
func (r *AllocationReconciler) BeginStop(allocID string) {
	r.mu.Lock()
	state := r.states[allocID]
	r.mu.Unlock()
	if state == nil {
		return
	}
	state.operation.Lock()
	r.mu.Lock()
	state.stopping = true
	r.mu.Unlock()
	state.operation.Unlock()
}

// CancelStop restores reconciliation after a failed runtime stop.
func (r *AllocationReconciler) CancelStop(allocID string) {
	r.mu.Lock()
	if state := r.states[allocID]; state != nil {
		state.stopping = false
	}
	r.mu.Unlock()
}

// ObserveHealth records a health observation. The health manager owns how an
// observation is produced; the reconciler owns what that observation means for
// allocation lifecycle state.
func (r *AllocationReconciler) ObserveHealth(allocID string, healthy bool) error {
	r.mu.Lock()
	_, ok := r.states[allocID]
	r.mu.Unlock()
	if !ok {
		return fmt.Errorf("alloc %s not tracked", allocID)
	}

	status := "unhealthy"
	if healthy {
		status = "healthy"
	}
	r.publishStatus(allocID, status)
	return nil
}

// Run periodically resynchronizes tracked allocations against the container
// runtime. This polling loop is a safety net; other observation sources should
// feed the same reconciler rather than implementing their own lifecycle rules.
func (r *AllocationReconciler) Run(ctx context.Context) {
	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, allocID := range r.trackedAllocations() {
				if err := r.Reconcile(ctx, allocID); err != nil {
					r.log.Error("reconcile allocation", "alloc", allocID, "error", err)
				}
			}
		}
	}
}

func (r *AllocationReconciler) trackedAllocations() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	ids := make([]string, 0, len(r.states))
	for allocID := range r.states {
		ids = append(ids, allocID)
	}
	return ids
}

// Reconcile performs one local desired-vs-actual pass for an allocation.
func (r *AllocationReconciler) Reconcile(ctx context.Context, allocID string) error {
	r.mu.Lock()
	state := r.states[allocID]
	r.mu.Unlock()
	if state == nil {
		return fmt.Errorf("alloc %s not tracked", allocID)
	}
	state.operation.Lock()
	defer state.operation.Unlock()
	r.mu.Lock()
	active := r.states[allocID] == state && !state.stopping
	r.mu.Unlock()
	if !active {
		return nil
	}

	containerState, err := r.runtime.Inspect(ctx, allocID)
	if err != nil {
		return fmt.Errorf("inspect alloc %s: %w", allocID, err)
	}

	if containerState.Status != runtime.StatusStopped {
		return nil
	}
	return r.restart(ctx, allocID)
}

func (r *AllocationReconciler) restart(ctx context.Context, allocID string) error {
	r.mu.Lock()
	state, ok := r.states[allocID]
	if !ok {
		r.mu.Unlock()
		return fmt.Errorf("alloc %s not tracked", allocID)
	}
	if state.restarting {
		r.mu.Unlock()
		return nil
	}
	state.restarting = true

	now := time.Now()
	if now.Sub(state.window) > state.restartWindow {
		state.attempts = 0
		state.window = now
	}
	if state.attempts >= state.maxRestarts {
		state.restarting = false
		r.mu.Unlock()
		r.publishStatus(allocID, "unhealthy")
		return nil
	}
	state.attempts++
	attempts, window := state.attempts, state.window
	healthManaged := state.healthManaged
	r.mu.Unlock()
	if subscriber, ok := r.Subscriber.(interface{ OnRestartState(string, int, time.Time) }); ok {
		subscriber.OnRestartState(allocID, attempts, window)
	}

	if err := r.runtime.Restart(ctx, allocID); err != nil {
		r.mu.Lock()
		if current := r.states[allocID]; current != nil {
			current.restarting = false
		}
		r.mu.Unlock()
		return fmt.Errorf("restart alloc %s: %w", allocID, err)
	}

	r.mu.Lock()
	if current := r.states[allocID]; current != nil {
		current.restarting = false
	}
	r.mu.Unlock()

	if healthManaged {
		r.publishStatus(allocID, "running")
	} else {
		r.publishStatus(allocID, "healthy")
	}
	return nil
}

func (r *AllocationReconciler) publishStatus(allocID, status string) {
	if r.Subscriber != nil {
		r.Subscriber.OnReconciledStatus(allocID, status)
	}
}
