package server

import (
	"context"
	"log/slog"
	"runtime"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/internal/lifecycle"
)

func TestDeleteAndRecreateJobDoesNotAdoptPreviousIncarnation(t *testing.T) {
	store := memoryStore{}
	s := NewServer(slog.Default(), nil, NewStateController(store, "test"), store, "test", "")
	agent := newTestAgent()
	t.Cleanup(agent.server.Close)
	s.client = newTestAgentClient()
	s.now = func() time.Time { return time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC) }
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy}
	addTestNode(s, node, s.now())
	ctx := context.Background()

	if _, err := s.RegisterJob(ctx, "default", versionTestSpec("app:v1", 1), nil); err != nil {
		t.Fatal(err)
	}
	firstIncarnation := s.jobs[jobKey("default", "web")].Incarnation
	s.Reconcile(ctx)
	if len(s.allocations) != 1 || s.allocations[0].JobIncarnation != firstIncarnation {
		t.Fatalf("initial allocations = %#v, want first incarnation", s.allocations)
	}
	old := s.allocations[0]
	backoff := &ReplacementBackoff{
		Namespace: "default", JobName: "web", TaskGroupName: "api",
		JobIncarnation: firstIncarnation, JobRevision: 1, Failures: 3,
		NextReplacementAt: s.now().Add(time.Hour), DelayedReplacements: 1,
	}
	if err := s.state.PutReplacementBackoff(ctx, backoff); err != nil {
		t.Fatal(err)
	}
	s.replacementBackoffs = make(map[string]*ReplacementBackoff)
	s.replacementBackoffs[backoff.key()] = backoff

	if err := s.DeleteJob(ctx, "default", "web"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterJob(ctx, "default", versionTestSpec("app:v2", 1), nil); err != nil {
		t.Fatal(err)
	}
	secondIncarnation := s.jobs[jobKey("default", "web")].Incarnation
	if secondIncarnation == firstIncarnation {
		t.Fatal("recreated job reused its previous incarnation")
	}
	s.Reconcile(ctx)

	if old.Phase != lifecycle.PhaseStopped {
		t.Fatalf("old allocation phase = %s, want retained as stopped", old.Phase)
	}
	if len(s.allocations) != 2 || s.allocations[1].JobIncarnation != secondIncarnation || s.allocations[1].JobRevision != 1 {
		t.Fatalf("allocations after recreation = %#v, want a new revision-one allocation", s.allocations)
	}
	if current := s.replacementBackoffs[backoff.key()]; current == nil || current.JobIncarnation != secondIncarnation || current.Failures != 0 {
		t.Fatalf("replacement backoff after recreation = %#v, want reset for the new incarnation", current)
	}

	successor := NewServer(slog.Default(), nil, NewStateController(store, "test"), store, "test", "")
	if err := successor.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	if got := successor.jobs[jobKey("default", "web")]; got == nil || got.Incarnation != secondIncarnation {
		t.Fatalf("reloaded job = %#v, want incarnation %q", got, secondIncarnation)
	}
	foundCurrent := false
	for _, got := range successor.allocations {
		foundCurrent = foundCurrent || got.JobIncarnation == secondIncarnation
	}
	if !foundCurrent {
		t.Fatalf("reloaded allocations = %#v, want incarnation %q", successor.allocations, secondIncarnation)
	}
	if got := successor.replacementBackoffs[backoff.key()]; got == nil || got.JobIncarnation != secondIncarnation {
		t.Fatalf("reloaded backoff = %#v, want incarnation %q", got, secondIncarnation)
	}
}

func TestConcurrentDeleteAndRecreateFencesOldAllocations(t *testing.T) {
	store := memoryStore{}
	s := NewServer(slog.Default(), nil, NewStateController(store, "test"), store, "test", "")
	agent := newTestAgent()
	t.Cleanup(agent.server.Close)
	s.client = newTestAgentClient()
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy}
	addTestNode(s, node, s.now())
	ctx := context.Background()
	if _, err := s.RegisterJob(ctx, "default", versionTestSpec("app:v1", 1), nil); err != nil {
		t.Fatal(err)
	}
	s.Reconcile(ctx)
	old := s.allocations[0]
	oldIncarnation := old.JobIncarnation

	// DeleteJob publishes after releasing mutationMu and before reconciling.
	// Hold publication so recreation deterministically lands in that gap.
	s.events.mu.Lock()
	deleted := make(chan error, 1)
	go func() { deleted <- s.DeleteJob(ctx, "default", "web") }()
	for {
		if _, exists := s.GetJob("default", "web"); !exists {
			break
		}
		runtime.Gosched()
	}
	recreated := make(chan error, 1)
	go func() {
		_, err := s.RegisterJob(ctx, "default", versionTestSpec("app:v2", 1), nil)
		recreated <- err
	}()
	for {
		s.mu.RLock()
		job := s.jobs[jobKey("default", "web")]
		ready := job != nil && job.Revision == 1 && job.Incarnation != oldIncarnation
		s.mu.RUnlock()
		if ready {
			break
		}
		runtime.Gosched()
	}
	s.events.mu.Unlock()
	if err := <-deleted; err != nil {
		t.Fatal(err)
	}
	if err := <-recreated; err != nil {
		t.Fatal(err)
	}

	job := s.jobs[jobKey("default", "web")]
	if job.Incarnation == oldIncarnation {
		t.Fatal("concurrent recreation reused the deleted job incarnation")
	}
	if old.Phase != lifecycle.PhaseStopped {
		t.Fatalf("old allocation phase = %s, want stopped", old.Phase)
	}
	s.Reconcile(ctx)
	current := 0
	for _, allocation := range s.allocations {
		if activeAllocationPhase(allocation.Phase) && allocation.JobIncarnation == job.Incarnation {
			current++
		}
		if activeAllocationPhase(allocation.Phase) && allocation.JobIncarnation == oldIncarnation {
			t.Fatalf("old allocation remained active: %#v", allocation)
		}
	}
	if current != 1 {
		t.Fatalf("active allocations for recreated job = %d, want 1", current)
	}
}

func TestStaleIncarnationDoesNotCountAsUnavailableCapacity(t *testing.T) {
	healthy := planTestNode(1, NodeStatusHealthy)
	unhealthy := planTestNode(2, NodeStatusUnhealthy)
	job := planTestJob("web", 1, 1, "")
	job.Incarnation = "new"
	current := planTestAllocation("current", healthy, lifecycle.PhaseRunning, 1)
	current.JobIncarnation = job.Incarnation
	stale := planTestAllocation("stale", unhealthy, lifecycle.PhaseRunning, 1)
	stale.JobIncarnation = "old"

	plan, err := planReconciliation(planTestInput(map[string]*Job{jobKey("default", "web"): job}, []*Node{healthy, unhealthy}, current, stale))
	if err != nil {
		t.Fatal(err)
	}
	if got := summarizeActions(plan.Actions); len(got) != 1 || got[0] != (plannedAction{Type: ActionStop, Allocation: stale.ID}) {
		t.Fatalf("actions = %#v, want only the stale allocation stopped", got)
	}
	if len(plan.NewAllocations) != 0 {
		t.Fatalf("new allocations = %#v, want current capacity satisfied", plan.NewAllocations)
	}
}
