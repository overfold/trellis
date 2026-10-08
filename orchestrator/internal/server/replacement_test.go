package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/client"
	"github.com/overfold/trellis/orchestrator/internal/auth"
	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	"github.com/overfold/trellis/orchestrator/internal/spec"
	"github.com/overfold/trellis/orchestrator/internal/state"
)

func TestRestoreReloadDoesNotWithholdPlacementsForOldBackoffs(t *testing.T) {
	for _, incarnation := range []string{"same", "different"} {
		for _, revision := range []int{3, 4} {
			t.Run(fmt.Sprintf("incarnation=%s/revision=%d", incarnation, revision), func(t *testing.T) {
				store, err := state.NewBoltStore(filepath.Join(t.TempDir(), "state.db"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				controller := NewStateController(store, "test")
				job := planTestJob("web", 1, 3, "")
				job.Incarnation, job.Version = "same", 3
				value, err := json.Marshal(job)
				if err != nil {
					t.Fatal(err)
				}
				snapshot := &state.DesiredSnapshot{Jobs: map[string][]byte{url.QueryEscape(jobKey("default", "web")): value}}
				old := &ReplacementBackoff{Namespace: "default", JobName: "web", TaskGroupName: "app", JobIncarnation: incarnation, JobRevision: revision, Failures: 7, DelayedReplacements: 1, NextReplacementAt: planNow.Add(time.Hour)}
				if err := controller.PutReplacementBackoff(t.Context(), old); err != nil {
					t.Fatal(err)
				}
				// This demonstrates why a matching stale record is dangerous even
				// with no allocations, while a changed incarnation/revision resets.
				planned := planReplacementBackoff(DefaultReplacementPolicy(), old, "default", "web", "app", 3, nil, planNow, "same")
				wantWithheld := 0
				if incarnation == "same" && revision == 3 {
					wantWithheld = 1
				}
				if got := planned.withheld(1, planNow); got != wantWithheld {
					t.Fatalf("old backoff withheld %d, want %d", got, wantWithheld)
				}
				if err := store.RestoreDesired("test", snapshot); err == nil {
					t.Fatal("restore accepted stale scheduling state")
				}
				if err := store.Batch(t.Context(), []state.Mutation{{DeletePrefix: "trellis/test/replacement-backoffs/"}}); err != nil {
					t.Fatal(err)
				}
				if err := store.RestoreDesired("test", snapshot); err != nil {
					t.Fatal(err)
				}
				s := NewServer(slog.Default(), nil, controller, store, "test", "")
				if err := s.Reload(t.Context()); err != nil {
					t.Fatal(err)
				}
				input := planTestInput(s.jobs, []*Node{planTestNode(1, NodeStatusHealthy)})
				input.Backoffs = s.replacementBackoffs
				plan, err := planReconciliation(input)
				if err != nil {
					t.Fatal(err)
				}
				if len(plan.NewAllocations) != 1 || len(s.replacementBackoffs) != 0 {
					t.Fatalf("restored placements=%d backoffs=%#v, want one clean placement", len(plan.NewAllocations), s.replacementBackoffs)
				}
			})
		}
	}
}

func TestReplacementBackoffDelaySchedule(t *testing.T) {
	policy := DefaultReplacementPolicy()
	for failures, want := range []time.Duration{
		0,
		10 * time.Second,
		20 * time.Second,
		40 * time.Second,
		80 * time.Second,
		160 * time.Second,
		5 * time.Minute,
		5 * time.Minute,
	} {
		if got := replacementBackoffDelay(policy, failures); got != want {
			t.Errorf("delay after %d failures = %s, want %s", failures, got, want)
		}
	}
	if got := replacementBackoffDelay(policy, 1<<30); got != policy.BackoffMax {
		t.Fatalf("delay after many failures = %s, want cap %s", got, policy.BackoffMax)
	}
	uncapped := ReplacementPolicy{BackoffBase: time.Second}
	if got := replacementBackoffDelay(uncapped, 1000); got <= 0 {
		t.Fatalf("uncapped delay overflowed to %s", got)
	}
}

func failedAllocation(id string, revision int, at time.Time) *Allocation {
	return &Allocation{
		ID: id, Namespace: "default", JobName: "web", TaskGroupName: "api",
		Generation: 1, JobRevision: revision, Phase: lifecycle.PhaseFailed, Health: lifecycle.HealthUnhealthy,
		Diagnostic: lifecycle.Diagnostic{CreatedAt: at.Add(-time.Minute), TransitionedAt: at, Reason: "restart_budget_exhausted", Message: "task exited"},
	}
}

func marshalAllocations(t *testing.T, allocations []*Allocation) string {
	t.Helper()
	raw, err := json.Marshal(allocations)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func TestPlanReplacementBackoffCountsEachFailureOnce(t *testing.T) {
	policy := DefaultReplacementPolicy()
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	allocations := []*Allocation{
		failedAllocation("b", 1, now.Add(-2*time.Second)),
		failedAllocation("a", 1, now.Add(-time.Second)),
	}
	before := marshalAllocations(t, allocations)

	first := planReplacementBackoff(policy, nil, "default", "web", "api", 1, allocations, now)
	if first == nil || first.Failures != 2 || first.LastAllocationID != "a" || !first.LastFailureAt.Equal(now.Add(-time.Second)) {
		t.Fatalf("first plan = %#v, want two failures with newest a", first)
	}
	if !first.NextReplacementAt.Equal(now.Add(20*time.Second)) || first.Reason != "restart_budget_exhausted" {
		t.Fatalf("first plan next=%s reason=%q", first.NextReplacementAt, first.Reason)
	}
	if !first.active(now.Add(19*time.Second)) || first.active(now.Add(20*time.Second)) {
		t.Fatal("backoff window does not end exactly at next_replacement_at")
	}

	// Planning again must not count the same records again. Once the
	// backoff has elapsed, only the delayed replacements are released.
	second := planReplacementBackoff(policy, first, "default", "web", "api", 1, allocations, now.Add(time.Second))
	if !second.equal(first) {
		t.Fatalf("replanning changed record: %#v, want %#v", second, first)
	}
	later := planReplacementBackoff(policy, first, "default", "web", "api", 1, allocations, now.Add(time.Minute))
	if later.Failures != 2 || !later.NextReplacementAt.Equal(first.NextReplacementAt) || later.DelayedReplacements != 0 {
		t.Fatalf("replanning after the backoff = %#v, want the same failures with nothing delayed", later)
	}
	if after := marshalAllocations(t, allocations); after != before {
		t.Fatal("planning mutated its allocation inputs")
	}

	third := planReplacementBackoff(policy, first, "default", "web", "api", 1, append(allocations, failedAllocation("c", 1, now.Add(30*time.Second))), now.Add(31*time.Second))
	if third.Failures != 3 || third.LastAllocationID != "c" || !third.NextReplacementAt.Equal(now.Add(71*time.Second)) {
		t.Fatalf("third plan = %#v, want third failure delayed by 40s", third)
	}
	if first.Failures != 2 {
		t.Fatal("planning mutated the previous record")
	}
}

func TestPlanReplacementBackoffIgnoresUnrelatedFailures(t *testing.T) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	draining := failedAllocation("draining", 2, now)
	draining.Draining = true
	otherGroup := failedAllocation("other", 2, now)
	otherGroup.TaskGroupName = "worker"
	stopped := failedAllocation("stopped", 2, now)
	stopped.Phase = lifecycle.PhaseStopped
	lost := failedAllocation("lost", 2, now)
	lost.Phase = lifecycle.PhaseLost
	allocations := []*Allocation{failedAllocation("old-revision", 1, now), draining, otherGroup, stopped, lost}

	if got := planReplacementBackoff(DefaultReplacementPolicy(), nil, "default", "web", "api", 2, allocations, now); got != nil {
		t.Fatalf("unrelated terminal allocations created backoff %#v", got)
	}
}

func TestPlanReplacementBackoffResetsOnNewRevisionAndRemembersSeenFailures(t *testing.T) {
	policy := DefaultReplacementPolicy()
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	failure := failedAllocation("a", 1, now)
	previous := planReplacementBackoff(policy, nil, "default", "web", "api", 1, []*Allocation{failure}, now)

	next := planReplacementBackoff(policy, previous, "default", "web", "api", 2, []*Allocation{failure}, now.Add(time.Second))
	if next.Failures != 0 || next.JobRevision != 2 || next.active(now.Add(time.Second)) || fmt.Sprint(next.SeenAllocations) != "[a]" {
		t.Fatalf("new revision plan = %#v, want reset that still remembers a", next)
	}

	// A group that is no longer desired forgets its failures too, and a job
	// recreated from revision one does not count the old record again.
	removed := planReplacementBackoff(policy, previous, "default", "web", "api", 0, []*Allocation{failure}, now.Add(time.Second))
	if removed.Failures != 0 || removed.active(now.Add(time.Second)) {
		t.Fatalf("undesired group plan = %#v, want reset", removed)
	}
	recreated := planReplacementBackoff(policy, removed, "default", "web", "api", 1, []*Allocation{failure}, now.Add(2*time.Second))
	if recreated.Failures != 0 {
		t.Fatalf("recreated job counted a seen failure: %#v", recreated)
	}

	// Pruned records leave the seen set, so it stays bounded by retention.
	pruned := planReplacementBackoff(policy, recreated, "default", "web", "api", 1, nil, now.Add(3*time.Second))
	if len(pruned.SeenAllocations) != 0 {
		t.Fatalf("seen allocations after pruning = %v, want none", pruned.SeenAllocations)
	}
}

func TestPlanReplacementBackoffDoesNotDependOnFailureClock(t *testing.T) {
	policy := DefaultReplacementPolicy()
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	first := failedAllocation("a", 1, now)
	previous := planReplacementBackoff(policy, nil, "default", "web", "api", 1, []*Allocation{first}, now)

	// A later leader with a slower clock may record an older transition time,
	// and two failures may share one; each still counts exactly once.
	skewed := failedAllocation("b", 1, now.Add(-time.Minute))
	tied := failedAllocation("c", 1, now)
	next := planReplacementBackoff(policy, previous, "default", "web", "api", 1, []*Allocation{first, skewed, tied}, now.Add(time.Second))
	if next.Failures != 3 || !next.LastFailureAt.Equal(now) || next.LastAllocationID != "c" {
		t.Fatalf("plan with skewed and tied failures = %#v, want three failures", next)
	}
	if again := planReplacementBackoff(policy, next, "default", "web", "api", 1, []*Allocation{first, skewed, tied}, now.Add(2*time.Second)); !again.equal(next) {
		t.Fatalf("replanning recounted failures: %#v", again)
	}
}

func TestPlanReplacementBackoffResetsAfterStableReplacement(t *testing.T) {
	policy := DefaultReplacementPolicy()
	failedAt := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	previous := planReplacementBackoff(policy, nil, "default", "web", "api", 1, []*Allocation{failedAllocation("a", 1, failedAt)}, failedAt)
	previous = planReplacementBackoff(policy, previous, "default", "web", "api", 1, []*Allocation{failedAllocation("b", 1, failedAt.Add(time.Minute))}, failedAt.Add(time.Minute))
	if previous.Failures != 2 {
		t.Fatalf("setup failures = %d, want 2", previous.Failures)
	}
	runningSince := failedAt.Add(2 * time.Minute)
	replacement := func(mutate func(*Allocation)) *Allocation {
		allocation := &Allocation{ID: "c", Namespace: "default", JobName: "web", TaskGroupName: "api", Generation: 1, JobRevision: 1, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy, Diagnostic: lifecycle.Diagnostic{CreatedAt: runningSince.Add(-time.Second), TransitionedAt: runningSince}}
		if mutate != nil {
			mutate(allocation)
		}
		return allocation
	}
	stable := runningSince.Add(policy.StableAfter)

	for _, tc := range []struct {
		name       string
		allocation *Allocation
		now        time.Time
		reset      bool
	}{
		{name: "healthy long enough", allocation: replacement(nil), now: stable, reset: true},
		{name: "running with unknown health", allocation: replacement(func(a *Allocation) { a.Health = lifecycle.HealthUnknown }), now: stable, reset: true},
		{name: "not long enough", allocation: replacement(nil), now: stable.Add(-time.Nanosecond)},
		{name: "unhealthy", allocation: replacement(func(a *Allocation) { a.Health = lifecycle.HealthUnhealthy }), now: stable},
		{name: "starting", allocation: replacement(func(a *Allocation) { a.Phase = lifecycle.PhaseStarting }), now: stable},
		{name: "created before the latest failure", allocation: replacement(func(a *Allocation) { a.CreatedAt = failedAt }), now: stable},
		{name: "draining", allocation: replacement(func(a *Allocation) { a.Draining = true }), now: stable},
		{name: "older revision", allocation: replacement(func(a *Allocation) { a.JobRevision = 0 }), now: stable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			next := planReplacementBackoff(policy, previous, "default", "web", "api", 1, []*Allocation{tc.allocation}, tc.now)
			if reset := next.Failures == 0; reset != tc.reset {
				t.Fatalf("reset = %t (failures %d), want %t", reset, next.Failures, tc.reset)
			}
			if tc.reset && (!next.NextReplacementAt.IsZero() || !next.LastFailureAt.Equal(previous.LastFailureAt)) {
				t.Fatalf("reset record = %#v, want no delay and retained watermark", next)
			}
		})
	}

	// After a reset the next failure starts the schedule again.
	reset := planReplacementBackoff(policy, previous, "default", "web", "api", 1, []*Allocation{replacement(nil)}, stable)
	failedAgain := stable.Add(time.Minute)
	next := planReplacementBackoff(policy, reset, "default", "web", "api", 1, []*Allocation{failedAllocation("d", 1, failedAgain)}, failedAgain)
	if next.Failures != 1 || !next.NextReplacementAt.Equal(failedAgain.Add(policy.BackoffBase)) {
		t.Fatalf("failure after reset = %#v, want first-step delay", next)
	}
}

func TestPlanTerminalPruningKeepsNewestAndSkipsCurrentUpdates(t *testing.T) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	node := &Node{ID: uuid.New(), Status: NodeStatusHealthy, observedAllocations: []observedAllocation{{ID: "f01", Generation: 3}}, observedAt: now.Add(time.Hour)}
	unhealthy := &Node{ID: uuid.New(), Status: NodeStatusUnhealthy, observedAt: now.Add(time.Hour)}
	stale := &Node{ID: uuid.New(), Status: NodeStatusHealthy, observedAt: now.Add(-time.Hour)}

	var allocations []*Allocation
	for i := range 10 {
		allocation := failedAllocation(fmt.Sprintf("f%02d", i), 1, now.Add(time.Duration(i)*time.Second))
		allocation.Node = &Node{ID: node.ID}
		allocations = append(allocations, allocation)
	}
	allocations[0].Node = nil                      // never placed: always released
	allocations[1].Node = &Node{ID: node.ID}       // observed by its node: running or cleanup pending
	allocations[2].Node = &Node{ID: unhealthy.ID}  // node unavailable: unknown
	allocations[3].Node = &Node{ID: stale.ID}      // node has not reported since termination
	allocations[4].Node = &Node{ID: uuid.New()}    // node removed from the cluster: released
	running := failedAllocation("running", 1, now) // active records are never candidates
	running.Phase = lifecycle.PhaseRunning
	running.Node = &Node{ID: node.ID}
	other := failedAllocation("other", 1, now) // separate group, below retention
	other.TaskGroupName = "worker"
	skipped := allocations[5]
	input := append([]*Allocation{running, other}, allocations...)
	before := marshalAllocations(t, input)

	pruned := planTerminalPruning(4, input, map[*Allocation]bool{skipped: true})
	var ids []string
	for _, allocation := range pruned {
		ids = append(ids, allocation.ID)
	}
	// f06..f09 are the newest four. Every older record except f05, which is
	// being updated in this pass, is pruned even if its node is unavailable or
	// still reports it. A later report is fenced as an observed orphan.
	if fmt.Sprint(ids) != "[f04 f03 f02 f01 f00]" {
		t.Fatalf("pruned %v, want [f04 f03 f02 f01 f00]", ids)
	}
	if after := marshalAllocations(t, input); after != before {
		t.Fatal("pruning mutated its allocation inputs")
	}

	// Ties on transition time use the allocation ID, independent of input order.
	tied := []*Allocation{failedAllocation("x2", 1, now), failedAllocation("x1", 1, now), failedAllocation("x3", 1, now)}
	reversed := []*Allocation{tied[2], tied[0], tied[1]}
	for _, order := range [][]*Allocation{tied, reversed} {
		pruned := planTerminalPruning(1, order, nil)
		if len(pruned) != 2 || pruned[0].ID != "x2" || pruned[1].ID != "x1" {
			t.Fatalf("tie-broken pruning = %v, want [x2 x1]", pruned)
		}
	}
}

type backoffTestClock struct {
	now    time.Time
	server *Server
}

func newBackoffReconcileServer(t *testing.T, store state.Store) (*Server, *Node, *backoffTestClock) {
	t.Helper()
	agent := newTestAgent()
	t.Cleanup(agent.server.Close)
	clock := &backoffTestClock{now: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}
	s := NewServer(slog.Default(), nil, NewStateController(store, "test"), store, "test", "")
	clock.server = s
	s.client = newTestAgentClient()
	s.now = func() time.Time { return clock.now }
	node := &Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000001"), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy}
	addTestNode(s, node, clock.now)
	s.jobs[jobKey("default", "web")] = &Job{Spec: canonicalTestSpec(&spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "api", Count: 1, Tasks: []spec.TaskSpec{{Name: "server", Image: "app"}}}}}), Revision: 1}
	return s, node, clock
}

func (c *backoffTestClock) advance(node *Node, d time.Duration) {
	c.now = c.now.Add(d)
	setTestHeartbeat(c.server, node.ID, c.now)
}

func activeAllocations(s *Server) []*Allocation {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*Allocation
	for _, allocation := range s.allocations {
		if !isTerminalPhase(allocation.Phase) {
			result = append(result, allocation)
		}
	}
	return result
}

func failActive(t *testing.T, s *Server, now time.Time) *Allocation {
	t.Helper()
	active := activeAllocations(s)
	if len(active) != 1 {
		t.Fatalf("active allocations = %d, want 1", len(active))
	}
	allocation := active[0]
	allocation.mu.Lock()
	if err := allocation.Transition(lifecycle.PhaseFailed, now, "restart_budget_exhausted", "task exited"); err != nil {
		t.Fatal(err)
	}
	allocation.mu.Unlock()
	return allocation
}

func TestReconcileDelaysReplacementOfFailedAllocations(t *testing.T) {
	store := memoryStore{}
	s, node, clock := newBackoffReconcileServer(t, store)
	events, ok := s.events.subscribe("default")
	if !ok {
		t.Fatal("subscribe rejected")
	}
	ctx := context.Background()

	s.Reconcile(ctx)
	first := failActive(t, s, clock.now)
	s.Reconcile(ctx)

	if active := activeAllocations(s); len(active) != 0 {
		t.Fatalf("replacement placed during backoff: %d active", len(active))
	}
	status, _ := s.GetJob("default", "web")
	if len(status.ReplacementBackoff) != 1 {
		t.Fatalf("job status backoff = %#v, want one group", status.ReplacementBackoff)
	}
	backoff := status.ReplacementBackoff[0]
	if backoff.Group != "api" || backoff.Failures != 1 || backoff.LastAllocationID != first.ID || backoff.Reason != "restart_budget_exhausted" || !backoff.NextReplacementAt.Equal(clock.now.Add(10*time.Second)) {
		t.Fatalf("job status backoff = %#v", backoff)
	}
	persisted, err := s.state.ListReplacementBackoffs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if record := persisted[replacementBackoffKey("default", "web", "api")]; record == nil || !record.NextReplacementAt.Equal(backoff.NextReplacementAt) {
		t.Fatalf("persisted backoff = %#v", record)
	}
	select {
	case event := <-events:
		if event.Type != api.EventJobReplacementDelayed || event.Group != "api" || event.Failures != 1 || event.NextReplacementAt == nil || !event.NextReplacementAt.Equal(backoff.NextReplacementAt) || event.AllocationID != first.ID {
			t.Fatalf("event = %#v", event)
		}
	default:
		t.Fatal("no replacement-delayed event published")
	}

	clock.advance(node, 9*time.Second)
	s.Reconcile(ctx)
	if active := activeAllocations(s); len(active) != 0 {
		t.Fatal("replacement placed before backoff elapsed")
	}
	select {
	case event := <-events:
		t.Fatalf("unchanged backoff published %#v", event)
	default:
	}

	clock.advance(node, time.Second)
	s.Reconcile(ctx)
	if active := activeAllocations(s); len(active) != 1 || active[0].Phase != lifecycle.PhaseStarting {
		t.Fatalf("replacement after backoff = %d active allocations", len(active))
	}

	// The replacement fails too: the next delay doubles.
	clock.advance(node, 5*time.Second)
	failActive(t, s, clock.now)
	s.Reconcile(ctx)
	status, _ = s.GetJob("default", "web")
	if len(status.ReplacementBackoff) != 1 || status.ReplacementBackoff[0].Failures != 2 || !status.ReplacementBackoff[0].NextReplacementAt.Equal(clock.now.Add(20*time.Second)) {
		t.Fatalf("second backoff = %#v", status.ReplacementBackoff)
	}
	clock.advance(node, 19*time.Second)
	s.Reconcile(ctx)
	if active := activeAllocations(s); len(active) != 0 {
		t.Fatal("second replacement placed before doubled backoff elapsed")
	}
	clock.advance(node, time.Second)
	s.Reconcile(ctx)
	if active := activeAllocations(s); len(active) != 1 {
		t.Fatal("second replacement was not placed after backoff")
	}

	// A replacement that stays running resets the backoff. The agent reports
	// the accepted start running.
	replacement := activeAllocations(s)[0]
	replacement.mu.Lock()
	if err := replacement.Transition(lifecycle.PhaseRunning, clock.now, "", ""); err != nil {
		t.Fatal(err)
	}
	replacement.mu.Unlock()
	clock.advance(node, DefaultReplacementPolicy().StableAfter)
	s.Reconcile(ctx)
	status, _ = s.GetJob("default", "web")
	if len(status.ReplacementBackoff) != 0 {
		t.Fatalf("backoff after stable replacement = %#v, want reset", status.ReplacementBackoff)
	}
	failActive(t, s, clock.now)
	s.Reconcile(ctx)
	status, _ = s.GetJob("default", "web")
	if len(status.ReplacementBackoff) != 1 || status.ReplacementBackoff[0].Failures != 1 || !status.ReplacementBackoff[0].NextReplacementAt.Equal(clock.now.Add(10*time.Second)) {
		t.Fatalf("backoff after reset = %#v, want first step", status.ReplacementBackoff)
	}
}

func TestReconcileNewRevisionReplacesImmediately(t *testing.T) {
	s, _, clock := newBackoffReconcileServer(t, memoryStore{})
	ctx := context.Background()
	s.Reconcile(ctx)
	failActive(t, s, clock.now)
	s.Reconcile(ctx)
	if active := activeAllocations(s); len(active) != 0 {
		t.Fatal("setup: replacement was not delayed")
	}

	job := s.jobs[jobKey("default", "web")]
	job.Spec.TaskGroups[0].Tasks[0].Image = "app:v2"
	job.Revision = 2
	s.Reconcile(ctx)
	if active := activeAllocations(s); len(active) != 1 || active[0].JobRevision != 2 {
		t.Fatalf("new revision placement = %d active allocations", len(active))
	}
	status, _ := s.GetJob("default", "web")
	if len(status.ReplacementBackoff) != 0 {
		t.Fatalf("backoff after new revision = %#v", status.ReplacementBackoff)
	}
}

func TestReconcileRemovesBackoffOfDeletedJobWithItsRecords(t *testing.T) {
	store := memoryStore{}
	s, node, clock := newBackoffReconcileServer(t, store)
	ctx := context.Background()
	s.Reconcile(ctx)
	failed := failActive(t, s, clock.now)
	s.Reconcile(ctx)
	delete(s.jobs, jobKey("default", "web"))

	s.Reconcile(ctx)
	record := s.replacementBackoffs[replacementBackoffKey("default", "web", "api")]
	if record == nil || record.Failures != 0 {
		t.Fatalf("backoff while records remain = %#v, want retained without failures", record)
	}

	s.reconciliation = DefaultReconciliationSettings()
	s.reconciliation.TerminalAllocationRetention = 0
	node.observedAt = clock.now.Add(time.Second)
	clock.advance(node, 2*time.Second)
	s.Reconcile(ctx)
	if len(s.allocations) != 0 {
		t.Fatalf("allocations after pruning = %d, want 0", len(s.allocations))
	}
	if _, exists := store[s.state.allocationKey(failed.ID)]; exists {
		t.Fatal("pruned allocation remains persisted")
	}
	if len(s.replacementBackoffs) != 0 {
		t.Fatalf("backoffs = %#v, want deleted with the group's last record", s.replacementBackoffs)
	}
	persisted, err := s.state.ListReplacementBackoffs(ctx)
	if err != nil || len(persisted) != 0 {
		t.Fatalf("persisted backoffs = %#v err=%v, want none", persisted, err)
	}
}

func TestReconcilePrunesTerminalAllocationRecords(t *testing.T) {
	store := memoryStore{}
	s, node, clock := newBackoffReconcileServer(t, store)
	ctx := context.Background()
	var ids []string
	for i := range 8 {
		allocation := failedAllocation(fmt.Sprintf("f%d", i), 1, clock.now.Add(time.Duration(i-10)*time.Second))
		allocation.Phase = lifecycle.PhaseStopped
		allocation.Node = node
		s.allocations = append(s.allocations, allocation)
		if err := s.state.PutAllocation(ctx, allocation); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, allocation.ID)
	}
	// f0 still has a container on its node.
	node.observedAllocations = []observedAllocation{{ID: "f0", Generation: 1}}
	node.observedAt = clock.now

	s.Reconcile(ctx)

	var remaining []string
	for _, allocation := range s.allocations {
		remaining = append(remaining, allocation.ID)
	}
	persisted, err := s.state.ListAllocations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Default retention keeps the newest five terminal records, f3..f7.
	// f0 must also retain its occupancy evidence until cleanup is observed.
	for _, id := range ids {
		_, inMemory := func() (*Allocation, bool) {
			for _, allocation := range s.allocations {
				if allocation.ID == id {
					return allocation, true
				}
			}
			return nil, false
		}()
		_, stored := persisted[id]
		want := id == "f0" || id >= "f3"
		if inMemory != want || stored != want {
			t.Fatalf("allocation %s retained in memory=%t store=%t, want %t (remaining %v)", id, inMemory, stored, want, remaining)
		}
	}
	node.observedAllocations = nil
	node.observedAt = clock.now.Add(time.Second)
	clock.advance(node, 2*time.Second)
	s.Reconcile(ctx)
	if _, exists := store[s.state.allocationKey("f0")]; exists {
		t.Fatal("cleaned-up f0 remained beyond terminal retention")
	}
}

// TestReplacementStateIsDeterministicAcrossRaftReplayAndSnapshot drives
// reconciliation against a real Raft store and checks that a follower holds
// byte-identical state, including leader-chosen backoff timestamps and pruned
// records, after catching up by log replay and again after restarting from its
// own snapshot plus the entries that follow it. A new leader then reloads the
// same backoff.
func TestReplacementStateIsDeterministicAcrossRaftReplayAndSnapshot(t *testing.T) {
	if testing.Short() {
		t.Skip("starts a Raft cluster")
	}
	leaderStore := newReplacementRaftStore(t, t.TempDir(), "", true)
	s, node, clock := newBackoffReconcileServer(t, leaderStore)
	ctx := context.Background()
	seed := failedAllocation("seed", 0, clock.now.Add(-time.Hour))
	if err := s.state.PutAllocation(ctx, seed); err != nil {
		t.Fatal(err)
	}
	s.allocations = append(s.allocations, seed)

	// First failure and backoff, then a snapshot.
	s.Reconcile(ctx)
	failActive(t, s, clock.now)
	s.Reconcile(ctx)
	if err := leaderStore.Raft().Snapshot().Error(); err != nil {
		t.Fatal(err)
	}
	// Later entries are replayed from the log: a second failure, and pruning
	// once the node reports that it no longer holds the old containers.
	clock.advance(node, 10*time.Second)
	s.Reconcile(ctx)
	clock.advance(node, 5*time.Second)
	failActive(t, s, clock.now)
	s.reconciliation = DefaultReconciliationSettings()
	s.reconciliation.TerminalAllocationRetention = 1
	node.observedAt = clock.now.Add(time.Second)
	clock.advance(node, 2*time.Second)
	s.Reconcile(ctx)

	want := s.replacementBackoffs[replacementBackoffKey("default", "web", "api")]
	if want == nil || want.Failures != 2 || !want.NextReplacementAt.Equal(clock.now.Add(20*time.Second)) {
		t.Fatalf("leader backoff = %#v", want)
	}

	followerDir := t.TempDir()
	followerStore := newReplacementRaftStore(t, followerDir, "", false)
	followerAddr := followerStore.LocalAddr()
	if err := leaderStore.AddNonvoter(followerAddr, followerAddr); err != nil {
		t.Fatal(err)
	}
	followerState := waitReplicatedState(t, leaderStore, followerStore)
	if _, exists := followerState["trellis/test/allocations/seed"]; exists {
		t.Fatal("pruned allocation survived replay")
	}

	// Restart the follower from its own snapshot followed by later entries.
	clock.advance(node, 20*time.Second)
	s.Reconcile(ctx)
	if active := activeAllocations(s); len(active) != 1 {
		t.Fatal("replacement was not placed after the second backoff")
	}
	waitReplicatedState(t, leaderStore, followerStore)
	deadline := time.Now().Add(10 * time.Second)
	for err := followerStore.Raft().Snapshot().Error(); err != nil; err = followerStore.Raft().Snapshot().Error() {
		if time.Now().After(deadline) {
			t.Fatalf("follower snapshot: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	clock.advance(node, 5*time.Second)
	failActive(t, s, clock.now)
	s.Reconcile(ctx)
	waitReplicatedState(t, leaderStore, followerStore)
	if err := followerStore.Close(); err != nil {
		t.Fatal(err)
	}
	followerStore = newReplacementRaftStore(t, followerDir, followerAddr, false)
	waitReplicatedState(t, leaderStore, followerStore)

	want = s.replacementBackoffs[replacementBackoffKey("default", "web", "api")]
	if want == nil || want.Failures != 3 || !want.NextReplacementAt.Equal(clock.now.Add(40*time.Second)) {
		t.Fatalf("leader backoff after third failure = %#v", want)
	}
	successor := NewServer(slog.Default(), nil, NewStateController(followerStore, "test"), followerStore, "test", "")
	if err := successor.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	if got := successor.replacementBackoffs[want.key()]; !got.equal(want) {
		t.Fatalf("reloaded backoff = %#v, want %#v", got, want)
	}
}

func newReplacementRaftStore(t *testing.T, dir, bind string, bootstrap bool) *state.RaftStore {
	t.Helper()
	if bind == "" {
		bind = fmt.Sprintf("127.0.0.1:%d", freeTCPPort(t))
	}
	store, err := state.NewRaftStore(state.RaftConfig{DataDir: dir, BindAddr: bind, Advertise: bind, ServerID: bind, Bootstrap: bootstrap})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if bootstrap {
		deadline := time.Now().Add(10 * time.Second)
		for leader, _ := store.Raft().LeaderWithID(); leader == ""; leader, _ = store.Raft().LeaderWithID() {
			if time.Now().After(deadline) {
				t.Fatal("timed out waiting for Raft leadership")
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	return store
}

// waitReplicatedState waits until the follower's FSM matches the leader's
// cluster state byte for byte and returns it.
func waitReplicatedState(t *testing.T, leader, follower *state.RaftStore) map[string][]byte {
	t.Helper()
	ctx := context.Background()
	want, err := leader.List(ctx, "trellis/test/")
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		got, err := follower.List(ctx, "trellis/test/")
		if err != nil {
			t.Fatal(err)
		}
		if sameStateEntries(want, got) {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("follower state did not converge:\nleader   %v\nfollower %v", stateKeys(want), stateKeys(got))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func sameStateEntries(a, b map[string][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if other, ok := b[key]; !ok || string(other) != string(value) {
			return false
		}
	}
	return true
}

func stateKeys(entries map[string][]byte) []string {
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	return keys
}

func freeTCPPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	return listener.Addr().(*net.TCPAddr).Port
}

func TestPlanReplacementBackoffTracksDelayedReplacements(t *testing.T) {
	policy := DefaultReplacementPolicy()
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	first := planReplacementBackoff(policy, nil, "default", "web", "api", 1, []*Allocation{failedAllocation("a", 1, now)}, now)
	if first.DelayedReplacements != 1 || first.withheld(3, now) != 1 || first.withheld(0, now) != 0 {
		t.Fatalf("first failure = %#v, want one delayed replacement", first)
	}
	// A second failure while the backoff is active adds to the delayed
	// replacements.
	during := now.Add(5 * time.Second)
	second := planReplacementBackoff(policy, first, "default", "web", "api", 1, []*Allocation{failedAllocation("a", 1, now), failedAllocation("b", 1, during)}, during)
	if second.Failures != 2 || second.DelayedReplacements != 2 || second.withheld(1, during) != 1 {
		t.Fatalf("second failure = %#v, want two delayed replacements", second)
	}
	// Once the backoff elapses the pass places every replacement, so nothing
	// stays delayed; a failure counted in that pass is delayed alone.
	elapsed := second.NextReplacementAt
	after := planReplacementBackoff(policy, second, "default", "web", "api", 1, []*Allocation{failedAllocation("a", 1, now), failedAllocation("b", 1, during)}, elapsed)
	if after.DelayedReplacements != 0 || after.withheld(2, elapsed) != 0 {
		t.Fatalf("elapsed backoff = %#v, want no delayed replacements", after)
	}
	third := planReplacementBackoff(policy, second, "default", "web", "api", 1, []*Allocation{failedAllocation("a", 1, now), failedAllocation("b", 1, during), failedAllocation("c", 1, elapsed)}, elapsed)
	if third.Failures != 3 || third.DelayedReplacements != 1 {
		t.Fatalf("failure after elapsed backoff = %#v, want one delayed replacement", third)
	}
}

func allocationsInPhase(s *Server, phase lifecycle.Phase) []*Allocation {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*Allocation
	for _, allocation := range s.allocations {
		if allocation.Phase == phase {
			result = append(result, allocation)
		}
	}
	return result
}

func transitionAllocation(t *testing.T, allocation *Allocation, phase lifecycle.Phase, now time.Time, reason string) {
	t.Helper()
	allocation.mu.Lock()
	defer allocation.mu.Unlock()
	if err := allocation.Transition(phase, now, reason, reason); err != nil {
		t.Fatal(err)
	}
}

func TestReconcilePlacesLostAndScaledDeficitsDuringBackoff(t *testing.T) {
	s, node, clock := newBackoffReconcileServer(t, memoryStore{})
	ctx := context.Background()
	job := s.jobs[jobKey("default", "web")]
	job.Spec.TaskGroups[0].Count = 2
	s.Reconcile(ctx)
	running := activeAllocations(s)
	if len(running) != 2 {
		t.Fatalf("setup: active allocations = %d, want 2", len(running))
	}

	transitionAllocation(t, running[0], lifecycle.PhaseFailed, clock.now, "restart_budget_exhausted")
	s.Reconcile(ctx)
	if active := activeAllocations(s); len(active) != 1 {
		t.Fatalf("failed allocation replaced during backoff: %d active", len(active))
	}

	// The node loses the other allocation: its replacement is placed
	// immediately, while the failed allocation's replacement still waits.
	clock.advance(node, time.Second)
	transitionAllocation(t, running[1], lifecycle.PhaseLost, clock.now, "node_unavailable")
	s.Reconcile(ctx)
	active := activeAllocations(s)
	if len(active) != 1 || active[0] == running[1] {
		t.Fatalf("lost allocation replacement = %d active, want one new allocation", len(active))
	}
	backoff := s.replacementBackoffs[replacementBackoffKey("default", "web", "api")]
	if !backoff.active(clock.now) || backoff.Failures != 1 || backoff.DelayedReplacements != 1 {
		t.Fatalf("backoff after lost replacement = %#v, want one delayed replacement", backoff)
	}

	// A higher count is placed immediately as well.
	clock.advance(node, time.Second)
	job.Spec.TaskGroups[0].Count = 3
	s.Reconcile(ctx)
	if active := activeAllocations(s); len(active) != 2 {
		t.Fatalf("count increase during backoff = %d active, want 2", len(active))
	}
	if lost := allocationsInPhase(s, lifecycle.PhaseLost); len(lost) != 1 {
		t.Fatalf("lost allocations = %d, want 1", len(lost))
	}

	// The failed allocation is replaced once the backoff elapses.
	clock.advance(node, 7*time.Second)
	s.Reconcile(ctx)
	if active := activeAllocations(s); len(active) != 2 {
		t.Fatal("failed allocation replaced before backoff elapsed")
	}
	clock.advance(node, time.Second)
	s.Reconcile(ctx)
	if active := activeAllocations(s); len(active) != 3 {
		t.Fatalf("failed allocation replacement after backoff = %d active, want 3", len(active))
	}
	if backoff := s.replacementBackoffs[replacementBackoffKey("default", "web", "api")]; backoff.DelayedReplacements != 0 || backoff.Failures != 1 {
		t.Fatalf("backoff after replacement = %#v, want failures kept and nothing delayed", backoff)
	}
}

func TestReconcileDropsDelayedReplacementsWithoutMissingCapacity(t *testing.T) {
	s, node, clock := newBackoffReconcileServer(t, memoryStore{})
	ctx := context.Background()
	job := s.jobs[jobKey("default", "web")]
	job.Spec.TaskGroups[0].Count = 2
	s.Reconcile(ctx)
	running := activeAllocations(s)
	if len(running) != 2 {
		t.Fatalf("setup: active allocations = %d, want 2", len(running))
	}
	transitionAllocation(t, running[0], lifecycle.PhaseFailed, clock.now, "restart_budget_exhausted")
	s.Reconcile(ctx)

	// Scaling down leaves no failed capacity to replace; scaling back up
	// during the backoff is a count increase and is placed immediately.
	job.Spec.TaskGroups[0].Count = 1
	clock.advance(node, time.Second)
	s.Reconcile(ctx)
	if backoff := s.replacementBackoffs[replacementBackoffKey("default", "web", "api")]; backoff.DelayedReplacements != 0 || !backoff.active(clock.now) {
		t.Fatalf("backoff after scale down = %#v, want active without delayed replacements", backoff)
	}
	job.Spec.TaskGroups[0].Count = 2
	clock.advance(node, time.Second)
	s.Reconcile(ctx)
	if active := activeAllocations(s); len(active) != 2 {
		t.Fatalf("count increase during backoff = %d active, want 2", len(active))
	}
}

// authenticatedHandler serves the control-plane handler as a caller with the
// cluster scope and the given access level, standing in for the
// authentication middleware.
func authenticatedHandler(s *Server, access auth.AccessLevel) http.Handler {
	e := echo.New()
	NewHandler(s).Register(e)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), NamespaceContextKey, auth.EncodeScope(auth.AccessCluster, access))
		e.ServeHTTP(w, r.WithContext(ctx))
	})
}

// TestResetReplacementBackoffThroughClientAndRaft resets a backoff through
// the client and HTTP handler against a Raft store, and checks that the leader
// replaces the failed allocation at once, that a follower replays the reset
// byte for byte, and that a new leader reloads it.
func TestResetReplacementBackoffThroughClientAndRaft(t *testing.T) {
	if testing.Short() {
		t.Skip("starts a Raft cluster")
	}
	leaderStore := newReplacementRaftStore(t, t.TempDir(), "", true)
	s, _, clock := newBackoffReconcileServer(t, leaderStore)
	ctx := context.Background()
	s.Reconcile(ctx)
	failed := failActive(t, s, clock.now)
	s.Reconcile(ctx)
	key := replacementBackoffKey("default", "web", "api")
	if backoff := s.replacementBackoffs[key]; !backoff.active(clock.now) {
		t.Fatalf("setup: backoff = %#v, want active", backoff)
	}
	events, ok := s.events.subscribe("default")
	if !ok {
		t.Fatal("subscribe rejected")
	}

	httpServer := httptest.NewServer(authenticatedHandler(s, auth.AccessWrite))
	defer httpServer.Close()
	serverClient, err := client.New(client.Config{Address: httpServer.URL, Token: "token", Namespace: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if err := serverClient.ResetReplacementBackoff(ctx, "web", "missing"); err == nil || !strings.Contains(err.Error(), "404") {
		t.Fatalf("reset of unknown task group error = %v, want 404", err)
	}
	if err := serverClient.ResetReplacementBackoff(ctx, "web", "api"); err != nil {
		t.Fatal(err)
	}

	reset := s.replacementBackoffs[key]
	if reset == nil || reset.Failures != 0 || !reset.NextReplacementAt.IsZero() || reset.DelayedReplacements != 0 || !slices.Contains(reset.SeenAllocations, failed.ID) {
		t.Fatalf("backoff after reset = %#v, want cleared with the failure still seen", reset)
	}
	if status, _ := s.GetJob("default", "web"); len(status.ReplacementBackoff) != 0 {
		t.Fatalf("job status after reset = %#v, want no backoff", status.ReplacementBackoff)
	}
	if active := activeAllocations(s); len(active) != 1 {
		t.Fatalf("active allocations after reset = %d, want the replacement", len(active))
	}
	select {
	case event := <-events:
		if event.Type != api.EventJobReplacementBackoffReset || event.JobName != "web" || event.Group != "api" {
			t.Fatalf("event = %#v", event)
		}
	default:
		t.Fatal("no replacement-backoff-reset event published")
	}
	// Resetting again is a no-op.
	if err := serverClient.ResetReplacementBackoff(ctx, "web", "api"); err != nil {
		t.Fatal(err)
	}

	followerDir := t.TempDir()
	followerStore := newReplacementRaftStore(t, followerDir, "", false)
	followerAddr := followerStore.LocalAddr()
	if err := leaderStore.AddNonvoter(followerAddr, followerAddr); err != nil {
		t.Fatal(err)
	}
	waitReplicatedState(t, leaderStore, followerStore)
	successor := NewServer(slog.Default(), nil, NewStateController(followerStore, "test"), followerStore, "test", "")
	if err := successor.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	if got := successor.replacementBackoffs[key]; !got.equal(reset) {
		t.Fatalf("reloaded backoff = %#v, want %#v", got, reset)
	}
}

func TestResetReplacementBackoffRequiresWriteAccess(t *testing.T) {
	s, _, clock := newBackoffReconcileServer(t, memoryStore{})
	ctx := context.Background()
	s.Reconcile(ctx)
	failActive(t, s, clock.now)
	s.Reconcile(ctx)

	for _, tt := range []struct {
		name   string
		access auth.AccessLevel
		want   int
	}{
		{name: "read credential", access: auth.AccessRead, want: http.StatusForbidden},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/v1/namespaces/default/jobs/web/groups/api/replacement-backoff/reset", nil)
			authenticatedHandler(s, tt.access).ServeHTTP(rec, req)
			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d; body: %s", rec.Code, tt.want, rec.Body.String())
			}
		})
	}
	if backoff := s.replacementBackoffs[replacementBackoffKey("default", "web", "api")]; !backoff.active(clock.now) {
		t.Fatalf("backoff = %#v, want unchanged", backoff)
	}
}

func TestReconcilePrunesRecordsOfRemovedAndUnavailableNodes(t *testing.T) {
	store := memoryStore{}
	s, node, clock := newBackoffReconcileServer(t, store)
	ctx := context.Background()
	removed := &Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000002"), Status: NodeStatusHealthy}
	down := &Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000003"), Status: NodeStatusUnhealthy}
	for _, n := range []*Node{removed, down} {
		s.nodes[n.ID] = n
		if err := s.state.PutNode(ctx, n.ID.String(), nodeSummary(n)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.state.PutNode(ctx, node.ID.String(), nodeSummary(node)); err != nil {
		t.Fatal(err)
	}
	s.reconciliation = DefaultReconciliationSettings()
	s.reconciliation.TerminalAllocationRetention = 0
	for i, n := range []*Node{removed, down} {
		allocation := failedAllocation(fmt.Sprintf("t%d", i), 1, clock.now.Add(-time.Minute))
		allocation.Phase = lifecycle.PhaseStopped
		allocation.Node = n
		s.allocations = append(s.allocations, allocation)
		if err := s.state.PutAllocation(ctx, allocation); err != nil {
			t.Fatal(err)
		}
	}
	// The removed node's registration is gone; the down node is still
	// registered and may still hold its allocation's container.
	delete(s.nodes, removed.ID)
	delete(store, fmt.Sprintf("trellis/test/nodes/%s", removed.ID))

	// Every leader decides the same: one that kept the node pointer, and one
	// that reloaded the record after the removal and sees no node at all.
	successor := NewServer(slog.Default(), nil, NewStateController(store, "test"), store, "test", "")
	if err := successor.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	for name, leader := range map[string]*Server{"current": s, "reloaded": successor} {
		leader.mu.RLock()
		pruned := planTerminalPruning(0, leader.allocations, nil)
		leader.mu.RUnlock()
		if len(pruned) != 2 || pruned[0].ID != "t1" || pruned[1].ID != "t0" {
			t.Fatalf("%s leader pruned %v, want [t1 t0]", name, pruned)
		}
	}

	s.Reconcile(ctx)
	if _, exists := store[s.state.allocationKey("t0")]; exists {
		t.Fatal("record of removed node remains persisted")
	}
	if _, exists := store[s.state.allocationKey("t1")]; exists {
		t.Fatal("record of down node remains persisted")
	}
}
