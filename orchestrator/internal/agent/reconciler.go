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
	// OnRestartState records restart accounting. When exhausted is true the
	// allocation has failed terminally; an error means the exhaustion was
	// not recorded and the reconciler will retry on its next pass.
	OnRestartState(allocID string, attempts int, window time.Time, exhausted bool) error
}

type allocationReconcileState struct {
	operation     sync.Mutex
	stopping      bool
	healthManaged bool
	restarting    bool
	exhausted     bool // the restart budget is terminally used up
	failed        bool // exhaustion has been recorded for a stopped container
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
	r.track(allocID, policy, &allocationReconcileState{healthManaged: healthManaged})
}

// TrackStopping observes an allocation while suppressing automatic restarts
// until ResumeRestarts. The persisted restart state is kept so a resumed
// allocation does not receive a fresh budget.
func (r *AllocationReconciler) TrackStopping(allocID string, healthManaged bool, policy *spec.RestartPolicySpec, attempts int, window time.Time, exhausted bool) {
	r.track(allocID, policy, &allocationReconcileState{healthManaged: healthManaged, attempts: attempts, window: window, exhausted: exhausted, stopping: true})
}

// TrackFailed observes an allocation whose exhausted restart budget and
// failed observation are already recorded. It is never restarted locally.
func (r *AllocationReconciler) TrackFailed(allocID string, healthManaged bool, policy *spec.RestartPolicySpec, attempts int, window time.Time) {
	r.track(allocID, policy, &allocationReconcileState{healthManaged: healthManaged, attempts: attempts, window: window, exhausted: true, failed: true})
}

// TrackRecovered restores reconciliation state for an allocation. An
// allocation whose restart budget was exhausted stays failed: it is never
// restarted locally again for the same allocation generation.
func (r *AllocationReconciler) TrackRecovered(allocID string, healthManaged bool, policy *spec.RestartPolicySpec, attempts int, window time.Time, exhausted bool) {
	r.track(allocID, policy, &allocationReconcileState{healthManaged: healthManaged, attempts: attempts, window: window, exhausted: exhausted})
}

func restartPolicyLimits(policy *spec.RestartPolicySpec) (int, time.Duration) {
	if policy == nil {
		return defaultMaxRestarts, defaultRestartWindow
	}
	return policy.MaxRestarts, policy.Window
}

func advanceRestartState(attempts int, window time.Time, maxRestarts int, restartWindow time.Duration, now time.Time) (int, time.Time, bool) {
	if window.IsZero() {
		window = now
	}
	if now.Sub(window) > restartWindow {
		attempts = 0
		window = now
	}
	if attempts >= maxRestarts {
		return attempts, window, false
	}
	return attempts + 1, window, true
}

func (r *AllocationReconciler) track(allocID string, policy *spec.RestartPolicySpec, state *allocationReconcileState) {
	r.mu.Lock()
	defer r.mu.Unlock()

	state.maxRestarts, state.restartWindow = restartPolicyLimits(policy)
	if state.window.IsZero() {
		state.window = time.Now()
	}
	r.states[allocID] = state
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

// SuppressRestarts waits for an active reconciliation pass and prevents further restarts.
func (r *AllocationReconciler) SuppressRestarts(allocID string) {
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

// ResumeRestarts restores reconciliation after a drain is cancelled.
func (r *AllocationReconciler) ResumeRestarts(allocID string, healthManaged bool, policy *spec.RestartPolicySpec, attempts int, window time.Time, exhausted bool) {
	r.mu.Lock()
	state := r.states[allocID]
	r.mu.Unlock()
	if state == nil {
		r.TrackRecovered(allocID, healthManaged, policy, attempts, window, exhausted)
		return
	}
	state.operation.Lock()
	r.mu.Lock()
	state.stopping = false
	r.mu.Unlock()
	state.operation.Unlock()
}

// BeginStop suppresses restarts before runtime cleanup starts.
func (r *AllocationReconciler) BeginStop(allocID string) {
	r.SuppressRestarts(allocID)
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
	active := r.states[allocID] == state && !state.stopping && !state.failed
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
	attempts, window, allowed := state.attempts, state.window, false
	if !state.exhausted {
		attempts, window, allowed = advanceRestartState(state.attempts, state.window, state.maxRestarts, state.restartWindow, now)
	}
	if !allowed {
		// Exhaustion is terminal for this allocation generation, including
		// after the window elapses. The subscriber records it together with
		// the failed observation; until that succeeds, later passes retry.
		state.restarting = false
		state.exhausted = true
		state.attempts, state.window = attempts, window
		r.mu.Unlock()
		if err := r.publishRestartState(allocID, attempts, window, true); err != nil {
			return fmt.Errorf("record exhausted restart budget for alloc %s: %w", allocID, err)
		}
		r.mu.Lock()
		state.failed = true
		r.mu.Unlock()
		return nil
	}
	state.attempts, state.window = attempts, window
	healthManaged := state.healthManaged
	r.mu.Unlock()
	// Restart accounting is best effort; the restart itself must proceed.
	if err := r.publishRestartState(allocID, attempts, window, false); err != nil {
		r.log.Error("record restart attempt", "alloc", allocID, "error", err)
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

func (r *AllocationReconciler) publishRestartState(allocID string, attempts int, window time.Time, exhausted bool) error {
	if r.Subscriber == nil {
		return nil
	}
	return r.Subscriber.OnRestartState(allocID, attempts, window, exhausted)
}

func (r *AllocationReconciler) publishStatus(allocID, status string) {
	if r.Subscriber != nil {
		r.Subscriber.OnReconciledStatus(allocID, status)
	}
}
