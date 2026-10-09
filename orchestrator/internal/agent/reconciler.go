package agent

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/containerd/errdefs"
	"github.com/overfold/trellis/orchestrator/internal/runtime"
	"github.com/overfold/trellis/orchestrator/internal/spec"
)

const (
	reconcileInterval = 3 * time.Second
	reconcileTimeout  = 10 * time.Second
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
	// OnRestartState records restart accounting before an allowed restart, or
	// the terminal failed observation when exhausted is true. An error leaves
	// the runtime untouched and the reconciler retries on its next pass.
	OnRestartState(allocID string, attempts int, window time.Time, exhausted bool) error
}

type allocationReconcileState struct {
	operation     chan struct{}
	cancel        context.CancelFunc // guarded by the reconciler mutex
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
	state.operation = make(chan struct{}, 1)
	if previous := r.states[allocID]; previous != nil {
		state.operation = previous.operation
		if previous.cancel != nil {
			previous.cancel()
		}
	}

	// Start requests carry the job's canonical restart policy and the
	// handler refuses requests without one, so policy is nil only for a
	// caller error; such an allocation is never restarted locally.
	if policy != nil {
		state.maxRestarts, state.restartWindow = policy.MaxRestarts, policy.Window
	}
	if state.window.IsZero() {
		state.window = time.Now()
	}
	r.states[allocID] = state
}

// Untrack stops reconciliation for an allocation.
func (r *AllocationReconciler) Untrack(allocID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	state, ok := r.states[allocID]
	if !ok {
		return nil
	}
	if state.cancel != nil {
		state.cancel()
	}
	delete(r.states, allocID)
	return nil
}

// SuppressRestarts cancels an active pass before waiting for runtime exclusion.
// A runtime that ignores cancellation remains fenced; callers can abandon the
// wait without allowing cleanup to race a late restart.
func (r *AllocationReconciler) SuppressRestarts(ctx context.Context, allocID string) error {
	ctx, cancel := context.WithTimeout(ctx, reconcileTimeout)
	defer cancel()
	r.mu.Lock()
	state := r.states[allocID]
	if state != nil {
		state.stopping = true
		if state.cancel != nil {
			state.cancel()
		}
	}
	r.mu.Unlock()
	if state == nil {
		return nil
	}
	select {
	case state.operation <- struct{}{}:
		<-state.operation
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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
	r.mu.Lock()
	state.stopping = false
	r.mu.Unlock()
}

// BeginStop suppresses restarts before runtime cleanup starts.
func (r *AllocationReconciler) BeginStop(ctx context.Context, allocID string) error {
	return r.SuppressRestarts(ctx, allocID)
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
				r.mu.Lock()
				state := r.states[allocID]
				r.mu.Unlock()
				if state == nil {
					continue
				}
				select {
				case state.operation <- struct{}{}:
					go func() {
						defer func() { <-state.operation }()
						if err := r.reconcile(ctx, allocID, state); err != nil {
							r.log.Error("reconcile allocation", "alloc", allocID, "error", err)
						}
					}()
				default: // At most one outstanding pass per allocation.
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
	ctx, cancel := context.WithTimeout(ctx, reconcileTimeout)
	defer cancel()
	r.mu.Lock()
	state := r.states[allocID]
	r.mu.Unlock()
	if state == nil {
		return fmt.Errorf("alloc %s not tracked", allocID)
	}
	select {
	case state.operation <- struct{}{}:
		defer func() { <-state.operation }()
	case <-ctx.Done():
		return ctx.Err()
	}
	return r.reconcile(ctx, allocID, state)
}

func (r *AllocationReconciler) reconcile(ctx context.Context, allocID string, state *allocationReconcileState) error {
	ctx, cancel := context.WithTimeout(ctx, reconcileTimeout)
	defer cancel()
	r.mu.Lock()
	active := r.states[allocID] == state && !state.stopping && !state.failed
	if active {
		state.cancel = cancel
	}
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		state.cancel = nil
		r.mu.Unlock()
	}()
	if !active {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	containerState, err := r.runtime.Inspect(ctx, allocID)
	if err != nil {
		if errdefs.IsNotFound(err) && ctx.Err() == nil {
			// Absence is a failed observation, not a transient inspection
			// error or permission to recreate the container locally.
			r.publishStatus(allocID, "failed")
			return nil
		}
		return fmt.Errorf("inspect alloc %s: %w", allocID, err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	// A crash between task creation and Start leaves Created behind. Only
	// committed allocations are tracked here, so recover it with the same
	// durable restart budget as an exited task, never an unbudgeted Start.
	if containerState.Status != runtime.StatusStopped && containerState.Status != runtime.StatusCreated {
		return nil
	}
	return r.restart(ctx, allocID, state)
}

func (r *AllocationReconciler) restart(ctx context.Context, allocID string, state *allocationReconcileState) error {
	r.mu.Lock()
	if r.states[allocID] != state || state.stopping {
		r.mu.Unlock()
		return nil
	}
	if state.restarting {
		r.mu.Unlock()
		return nil
	}
	if err := ctx.Err(); err != nil {
		r.mu.Unlock()
		return err
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
	previousAttempts, previousWindow := state.attempts, state.window
	state.attempts, state.window = attempts, window
	healthManaged := state.healthManaged
	r.mu.Unlock()
	// Consume the budget durably before touching the runtime. Otherwise an
	// agent crash after Restart can reload the old counter and exceed the
	// generation's restart policy.
	if err := r.publishRestartState(allocID, attempts, window, false); err != nil {
		r.mu.Lock()
		if current := r.states[allocID]; current == state {
			state.restarting = false
			state.attempts, state.window = previousAttempts, previousWindow
		}
		r.mu.Unlock()
		return fmt.Errorf("record restart attempt for alloc %s: %w", allocID, err)
	}
	if err := ctx.Err(); err != nil {
		r.mu.Lock()
		state.restarting = false
		r.mu.Unlock()
		return err
	}

	if err := r.runtime.Restart(ctx, allocID); err != nil {
		r.mu.Lock()
		if current := r.states[allocID]; current == state {
			current.restarting = false
		}
		r.mu.Unlock()
		return fmt.Errorf("restart alloc %s: %w", allocID, err)
	}

	r.mu.Lock()
	if current := r.states[allocID]; current == state {
		current.restarting = false
	}
	active := r.states[allocID] == state && !state.stopping
	r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if !active {
		return nil
	}

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
