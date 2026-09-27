package server

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/clofour/trellis/internal/lifecycle"
	"github.com/clofour/trellis/internal/spec"
	"github.com/clofour/trellis/internal/state"
)

type controlledBatchStore struct {
	memoryStore
	started chan struct{}
	release chan struct{}
	fail    bool
}

func (s *controlledBatchStore) Batch(ctx context.Context, mutations []state.Mutation) error {
	close(s.started)
	if s.release != nil {
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if s.fail {
		return errors.New("storage unavailable")
	}
	return s.memoryStore.Batch(ctx, mutations)
}

func newObsoletePendingReconcileServer(t *testing.T, store state.Store) (*Server, *Allocation) {
	t.Helper()
	controller := NewStateController(store, "test")
	s := NewServer(slog.Default(), nil, controller, store, "test", "")
	s.jobs[jobKey("default", "web")] = &Job{
		Spec: &spec.JobSpec{
			Namespace: "default",
			Name:      "web",
			TaskGroups: []spec.TaskGroupSpec{{
				Name:  "api",
				Count: 1,
				Tasks: []spec.TaskSpec{{Name: "server", Image: "app:v2"}},
			}},
		},
		Revision: 2,
	}
	allocation := &Allocation{
		ID:            "old",
		Namespace:     "default",
		JobName:       "web",
		TaskGroupName: "api",
		Tasks:         []spec.TaskSpec{{Name: "server", Image: "app:v1"}},
		Generation:    1,
		JobRevision:   1,
		Phase:         lifecycle.PhasePending,
		Health:        lifecycle.HealthUnknown,
	}
	s.allocations = []*Allocation{allocation}
	if err := controller.PutAllocation(context.Background(), allocation); err != nil {
		t.Fatalf("seed allocation: %v", err)
	}
	return s, allocation
}

func TestReconcileDoesNotHoldServerLockDuringAllocationPersistence(t *testing.T) {
	store := &controlledBatchStore{
		memoryStore: memoryStore{},
		started:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	s, allocation := newObsoletePendingReconcileServer(t, store)
	done := make(chan struct{})
	go func() {
		s.Reconcile(context.Background())
		close(done)
	}()

	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("reconciliation did not reach allocation persistence")
	}

	locked := make(chan struct{})
	go func() {
		s.mu.Lock()
		_ = len(s.allocations)
		s.mu.Unlock()
		close(locked)
	}()
	select {
	case <-locked:
	case <-time.After(time.Second):
		close(store.release)
		t.Fatal("server lock remained held while allocation persistence blocked")
	}
	if allocation.Phase != lifecycle.PhasePending {
		close(store.release)
		t.Fatalf("in-memory phase advanced before persistence: %s", allocation.Phase)
	}

	close(store.release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reconciliation did not complete after persistence resumed")
	}
	if allocation.Phase != lifecycle.PhaseStopped {
		t.Fatalf("in-memory phase after persistence = %s, want stopped", allocation.Phase)
	}
}

func TestReconcilePersistenceFailureDoesNotAdvanceMemory(t *testing.T) {
	store := &controlledBatchStore{
		memoryStore: memoryStore{},
		started:     make(chan struct{}),
		fail:        true,
	}
	s, allocation := newObsoletePendingReconcileServer(t, store)

	s.Reconcile(context.Background())

	if allocation.Phase != lifecycle.PhasePending {
		t.Fatalf("in-memory phase after failed persistence = %s, want pending", allocation.Phase)
	}
	persisted, err := s.state.ListAllocations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if persisted[allocation.ID] == nil || persisted[allocation.ID].Phase != lifecycle.PhasePending {
		t.Fatalf("persisted allocation after failed update = %#v, want pending", persisted[allocation.ID])
	}
}
