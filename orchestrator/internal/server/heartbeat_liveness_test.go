package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/internal/api"
	"github.com/overfold/trellis/internal/lifecycle"
	"github.com/overfold/trellis/internal/spec"
)

// livenessClock is a test clock that heartbeating goroutines can read while
// the test advances it.
type livenessClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *livenessClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *livenessClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type livenessFixture struct {
	s     *Server
	store *auditStore
	clock *livenessClock
	agent *testAgent
	node  *Node
}

func newLivenessFixture(t *testing.T) *livenessFixture {
	t.Helper()
	store := &auditStore{memoryStore: memoryStore{}}
	clock := &livenessClock{now: time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)}
	agent := newTestAgent()
	t.Cleanup(agent.server.Close)
	s := NewServer(slog.Default(), nil, NewStateController(store, "test"), store, "test", "")
	s.client = newTestAgentClient()
	s.catalog = newNopCatalog()
	s.now = clock.Now
	s.leaderSince = clock.Now().Add(-time.Hour)
	node := &Node{ID: uuid.MustParse("00000000-0000-0000-0000-00000000000a"), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy}
	addTestNode(s, node, clock.Now())
	if err := s.state.PutNode(context.Background(), node.ID.String(), nodeSummary(node)); err != nil {
		t.Fatal(err)
	}
	return &livenessFixture{s: s, store: store, clock: clock, agent: agent, node: node}
}

// heartbeatPromptly delivers a heartbeat and fails the test unless it
// returns well within one heartbeat interval.
func (f *livenessFixture) heartbeatPromptly(t *testing.T, version string, actual []api.AllocationStatus) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		done <- f.s.Heartbeat(context.Background(), f.node.ID, actual, version, nil, nil, nodeResourceObservation{})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("heartbeat waited on a blocked reconciliation pass or Raft commit")
	}
}

func (f *livenessFixture) assertLiveInAPI(t *testing.T) {
	t.Helper()
	nodes := f.s.ListNodes()
	if len(nodes) != 1 || nodes[0].Status != NodeStatusHealthy || !nodes[0].LastHeartbeat.Equal(f.clock.Now()) {
		t.Fatalf("nodes = %+v, want the heartbeating node healthy with its latest heartbeat", nodes)
	}
}

func (f *livenessFixture) block() {
	f.store.mu.Lock()
	f.store.blockBatch = make(chan struct{})
	f.store.batchStarted = make(chan struct{})
	f.store.batchOnce = sync.Once{}
	f.store.mu.Unlock()
}

func (f *livenessFixture) blockStarted() <-chan struct{} {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	return f.store.batchStarted
}

func (f *livenessFixture) unblock() {
	f.store.mu.Lock()
	block := f.store.blockBatch
	f.store.blockBatch = nil
	f.store.mu.Unlock()
	close(block)
}

func waitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// A reconciliation pass stuck in its Raft commit for longer than the liveness
// window holds mutationMu the whole time. Heartbeats must neither wait for it
// nor let the node look silent, so the next pass still plans against a
// healthy node and loses nothing.
func TestBlockedReconciliationDoesNotMakeHeartbeatingNodeUnhealthy(t *testing.T) {
	f := newLivenessFixture(t)
	running := &Allocation{ID: "default-web-app-running", Namespace: "default", JobName: "web", TaskGroupName: "app", Tasks: []spec.TaskSpec{{Name: "server", Image: "app"}},
		Node: f.node, Generation: 1, JobRevision: 1, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy,
		Diagnostic: lifecycle.Diagnostic{CreatedAt: f.clock.Now(), TransitionedAt: f.clock.Now()}}
	f.s.allocations = []*Allocation{running}
	f.s.reconciliation.AllocationLossTimeout = livenessWindow
	job := canonicalTestSpec(&spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "app", Count: 2, Tasks: []spec.TaskSpec{{Name: "server", Image: "app"}}}}})
	f.s.jobs[jobKey("default", "web")] = &Job{Spec: job, Revision: 1}
	report := []api.AllocationStatus{{ID: running.ID, Generation: 1, Task: "server", Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy}}

	f.block()
	passDone := make(chan struct{})
	go func() {
		f.s.Reconcile(context.Background())
		close(passDone)
	}()
	waitSignal(t, f.blockStarted(), "the reconciliation commit")

	for elapsed := time.Duration(0); elapsed <= 2*livenessWindow; elapsed += heartbeatInterval {
		f.clock.advance(heartbeatInterval)
		f.heartbeatPromptly(t, "test", report)
		f.assertLiveInAPI(t)
	}
	f.unblock()
	waitSignal(t, passDone, "the blocked reconciliation pass")

	// The observation applier now runs, then the next pass plans.
	f.s.applyObservations(context.Background())
	f.s.Reconcile(context.Background())
	if f.node.Status != NodeStatusHealthy {
		t.Fatalf("node status after the blocked pass = %s, want healthy", f.node.Status)
	}
	if running.Phase != lifecycle.PhaseRunning {
		t.Fatalf("allocation on the heartbeating node = %s, want running", running.Phase)
	}
}

// An observation commit stuck in Raft for longer than the liveness window
// must not delay heartbeats either. Reports that arrive meanwhile coalesce to
// the node's latest one, which is applied once the commit returns.
func TestBlockedObservationCommitDoesNotMakeHeartbeatingNodeUnhealthy(t *testing.T) {
	f := newLivenessFixture(t)
	starting := &Allocation{ID: "web-1", Namespace: "default", JobName: "web", TaskGroupName: "app", Tasks: []spec.TaskSpec{{Name: "server"}},
		Node: f.node, Generation: 1, JobRevision: 1, Phase: lifecycle.PhaseStarting, Health: lifecycle.HealthUnknown}
	f.s.allocations = []*Allocation{starting}
	f.s.rebuildAllocationNodeIndexLocked()
	report := []api.AllocationStatus{{ID: starting.ID, Generation: 1, Task: "server", Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy}}

	f.block()
	f.heartbeatPromptly(t, "v1", report)
	applied := make(chan struct{})
	go func() {
		f.s.applyObservations(context.Background())
		close(applied)
	}()
	waitSignal(t, f.blockStarted(), "the observation commit")

	var last string
	for i := 0; time.Duration(i)*heartbeatInterval <= 2*livenessWindow; i++ {
		f.clock.advance(heartbeatInterval)
		last = fmt.Sprintf("v%d", i+2)
		f.heartbeatPromptly(t, last, report)
		f.assertLiveInAPI(t)
		if pending := f.s.observations.len(); pending != 1 {
			t.Fatalf("pending reports = %d, want the node's reports coalesced to one", pending)
		}
	}
	f.unblock()
	waitSignal(t, applied, "the observation applier")

	if starting.Phase != lifecycle.PhaseRunning || f.node.Version != last {
		t.Fatalf("after the commit: allocation %s, node version %q; want running and the latest report %q", starting.Phase, f.node.Version, last)
	}
	f.s.Reconcile(context.Background())
	if f.node.Status != NodeStatusHealthy {
		t.Fatalf("node status after the blocked commit = %s, want healthy", f.node.Status)
	}
}

func TestHeartbeatRejectsUnregisteredNodeWithoutQueueing(t *testing.T) {
	s := NewServer(slog.Default(), nil, NewStateController(memoryStore{}, "test"), memoryStore{}, "test", "")
	err := s.Heartbeat(context.Background(), uuid.New(), nil, "test", nil, nil, nodeResourceObservation{})
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("heartbeat error = %v, want ErrNodeNotFound", err)
	}
	if pending := s.observations.len(); pending != 0 {
		t.Fatalf("pending reports = %d, want none from an unregistered node", pending)
	}
}

func TestInvalidHeartbeatDoesNotStampLiveness(t *testing.T) {
	s := NewServer(slog.Default(), nil, NewStateController(memoryStore{}, "test"), memoryStore{}, "test", "")
	node := &Node{ID: uuid.New()}
	addTestNode(s, node, time.Time{})
	invalid := []api.AllocationStatus{{ID: "web-1", Generation: 1, Task: "app", Phase: "exploded", Health: lifecycle.HealthHealthy}}
	if err := s.Heartbeat(context.Background(), node.ID, invalid, "test", nil, nil, nodeResourceObservation{}); err == nil {
		t.Fatal("invalid heartbeat accepted")
	}
	if heartbeat := s.liveness.lastHeartbeat(node.ID); !heartbeat.IsZero() {
		t.Fatalf("rejected heartbeat stamped liveness at %s", heartbeat)
	}
}

func TestObservationQueueCoalescesPerNodeAndOrdersByNode(t *testing.T) {
	var queue observationQueue
	a, b := uuid.MustParse("00000000-0000-0000-0000-000000000001"), uuid.MustParse("00000000-0000-0000-0000-000000000002")
	for i := 0; i < 100; i++ {
		for _, id := range []uuid.UUID{b, a} {
			superseded := queue.submit(&nodeObservation{node: id, version: fmt.Sprint(i)})
			if superseded != (i > 0) {
				t.Fatalf("report %d of %s superseded = %t", i, id, superseded)
			}
		}
	}
	if pending := queue.len(); pending != 2 {
		t.Fatalf("pending reports = %d, want one per node", pending)
	}
	batch, _ := queue.take()
	if len(batch) != 2 || batch[0].node != a || batch[1].node != b || batch[0].version != "99" || batch[1].version != "99" {
		t.Fatalf("batch = %+v, want each node's latest report in node order", batch)
	}
	if again, _ := queue.take(); len(again) != 0 {
		t.Fatalf("second take = %+v, want nothing pending", again)
	}
}

func TestObservationCommitsAreOrderedAndBounded(t *testing.T) {
	store := &auditStore{memoryStore: memoryStore{}}
	s := NewServer(slog.Default(), nil, NewStateController(store, "test"), store, "test", "")
	s.catalog = newNopCatalog()
	const perNode = 400
	nodes := make([]*Node, 3)
	for i := range nodes {
		nodes[i] = &Node{ID: uuid.UUID{byte(3 - i)}, Version: "old"}
		addTestNode(s, nodes[i], time.Time{})
		for j := 0; j < perNode; j++ {
			s.allocations = append(s.allocations, &Allocation{ID: fmt.Sprintf("n%d-%03d", i, perNode-j), Node: nodes[i], Tasks: []spec.TaskSpec{{Name: "app"}},
				Generation: 1, Phase: lifecycle.PhaseStarting, Health: lifecycle.HealthUnknown})
		}
	}
	s.rebuildAllocationNodeIndexLocked()
	for _, node := range nodes {
		var report []api.AllocationStatus
		for _, allocation := range s.allocationsByNode[node.ID] {
			report = append(report, api.AllocationStatus{ID: allocation.ID, Generation: 1, Task: "app", Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy})
		}
		if err := s.Heartbeat(context.Background(), node.ID, report, "new", nil, nil, nodeResourceObservation{}); err != nil {
			t.Fatal(err)
		}
	}
	s.applyObservations(context.Background())

	store.mu.Lock()
	defer store.mu.Unlock()
	// Each node writes its summary and its allocations. Two nodes fit one
	// commit; a node's records are never split.
	if want := []int{2 * (perNode + 1), perNode + 1}; fmt.Sprint(store.batchSizes) != fmt.Sprint(want) {
		t.Fatalf("commit sizes = %v, want %v", store.batchSizes, want)
	}
	var keys []string
	for _, batch := range store.batchKeys {
		keys = append(keys, batch...)
	}
	var want []string
	for _, i := range []int{2, 1, 0} {
		want = append(want, fmt.Sprintf("trellis/test/nodes/%s", nodes[i].ID))
	}
	for _, key := range keys {
		if strings.Contains(key, "/nodes/") {
			if len(want) == 0 || key != want[0] {
				t.Fatalf("node records written as %v, want node ID order", keys)
			}
			want = want[1:]
		}
	}
	// Within a commit, node summaries lead and allocations follow in node,
	// then allocation ID, order.
	first := store.batchKeys[0]
	if !strings.HasSuffix(first[2], "/allocations/n2-001") || !strings.HasSuffix(first[len(first)-1], fmt.Sprintf("/allocations/n1-%03d", perNode)) {
		t.Fatalf("first commit allocation order = %s ... %s", first[2], first[len(first)-1])
	}
}

// A new leadership term forgets the previous term's heartbeats and discards
// reports queued before it, so the new leader acts only on what nodes report
// to it. Silence, and so allocation loss, is measured from the term start.
func TestNewLeadershipTermResetsLivenessAndObservations(t *testing.T) {
	ctx := context.Background()
	store := memoryStore{}
	_, encoded := encodedAdministratorPublicKey(t)
	s := newSettingsTestServer(t, store)
	if err := s.Init(ctx, ClusterBootstrap{AdministratorPublicKey: encoded, Settings: DefaultClusterSettings()}); err != nil {
		t.Fatal(err)
	}
	node := uuid.New()
	if err := s.RegisterNode(ctx, &NodeRegistration{ID: node, Host: "node", Port: 8127}); err != nil {
		t.Fatal(err)
	}
	if err := s.Heartbeat(ctx, node, nil, "queued", nil, nil, nodeResourceObservation{}); err != nil {
		t.Fatal(err)
	}
	batch, term := s.observations.take()
	if err := s.Heartbeat(ctx, node, nil, "queued", nil, nil, nodeResourceObservation{}); err != nil {
		t.Fatal(err)
	}

	if err := s.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	if s.liveness.lastHeartbeat(node).IsZero() {
		t.Fatal("reloading replicated state forgot the node's heartbeat within the term")
	}
	if err := s.AcquireLeadership(ctx); err != nil {
		t.Fatal(err)
	}
	if heartbeat := s.liveness.lastHeartbeat(node); !heartbeat.IsZero() {
		t.Fatalf("new term kept the previous term's heartbeat %s", heartbeat)
	}
	if pending := s.observations.len(); pending != 0 {
		t.Fatalf("new term kept %d queued reports", pending)
	}
	s.applyObservationBatch(ctx, batch, term)
	if version := s.ListNodes()[0].Version; version == "queued" {
		t.Fatal("report taken in the previous term was applied")
	}
	if nodes := s.ListNodes(); nodes[0].Status != NodeStatusUnhealthy || !nodes[0].LastHeartbeat.IsZero() {
		t.Fatalf("node before its first heartbeat of the term = %+v, want unhealthy without a heartbeat", nodes[0])
	}
	if err := s.Heartbeat(ctx, node, nil, "fresh", nil, nil, nodeResourceObservation{}); err != nil {
		t.Fatal(err)
	}
	if nodes := s.ListNodes(); nodes[0].Status != NodeStatusHealthy {
		t.Fatalf("node after its first heartbeat of the term = %+v, want healthy", nodes[0])
	}
}
