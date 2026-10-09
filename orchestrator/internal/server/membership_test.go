package server

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/internal/state"
)

// fakeMembership is an in-memory Raft configuration with the same rejoin
// semantics as RaftStore.
type fakeMembership struct {
	mu      sync.Mutex
	members []state.RaftMember
	applied uint64
	err     error
	ops     []string
}

func newFakeMembership(members ...state.RaftMember) *fakeMembership {
	return &fakeMembership{members: members, applied: 1000}
}

func fakeMember(id uuid.UUID, voter bool) state.RaftMember {
	return state.RaftMember{ID: id.String(), Address: id.String()[:8] + ":8129", Voter: voter}
}

func (f *fakeMembership) Membership() ([]state.RaftMember, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	members := slices.Clone(f.members)
	for i := range members {
		members[i].ConfigurationIndex = uint64(len(f.ops) + 1)
	}
	return members, nil
}

func (f *fakeMembership) ChangeMembership(ctx context.Context, index uint64, action raft.ConfigurationChangeCommand, id, address string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	current := uint64(len(f.ops) + 1)
	f.mu.Unlock()
	if index != current {
		return errors.New("configuration changed")
	}
	switch action {
	case raft.AddNonvoter:
		return f.AddNonvoter(id, address)
	case raft.AddVoter:
		return f.PromoteVoter(id, address)
	case raft.DemoteVoter:
		return f.DemoteVoter(id)
	case raft.RemoveServer:
		return f.RemoveServer(id)
	default:
		return errors.New("unknown membership action")
	}
}

func (f *fakeMembership) change(op string, apply func()) error {
	if f.err != nil {
		return f.err
	}
	apply()
	f.ops = append(f.ops, op)
	return nil
}

func (f *fakeMembership) find(id string) int {
	return slices.IndexFunc(f.members, func(member state.RaftMember) bool { return member.ID == id })
}

func (f *fakeMembership) AddNonvoter(id, address string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.change("add-nonvoter "+id+" "+address, func() {
		if i := f.find(id); i >= 0 {
			f.members[i].Address = address
			return
		}
		f.members = append(f.members, state.RaftMember{ID: id, Address: address})
	})
}

func (f *fakeMembership) PromoteVoter(id, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.change("promote "+id, func() { f.members[f.find(id)].Voter = true })
}

func (f *fakeMembership) DemoteVoter(id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.change("demote "+id, func() { f.members[f.find(id)].Voter = false })
}

func (f *fakeMembership) RemoveServer(id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.change("remove "+id, func() { f.members = slices.Delete(f.members, f.find(id), f.find(id)+1) })
}

func (*fakeMembership) LeadershipTransfer() error { return nil }

func (f *fakeMembership) AppliedIndex() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.applied
}

func (f *fakeMembership) operations() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.ops)
}

func (f *fakeMembership) voters() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var voters []string
	for _, member := range f.members {
		if member.Voter {
			voters = append(voters, member.ID)
		}
	}
	slices.Sort(voters)
	return voters
}

// membershipTestServer returns a leader whose clock is fixed well after its
// election, with every listed node healthy, heartbeating, and caught up.
func membershipTestServer(joiner *fakeMembership, leader uuid.UUID, healthy ...uuid.UUID) *Server {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	s := &Server{joiner: joiner, nodeID: leader, now: func() time.Time { return now }, leaderSince: now.Add(-time.Hour), nodes: map[uuid.UUID]*Node{}, state: NewStateController(memoryStore{}, "test")}
	members, _ := joiner.Membership()
	for _, member := range members {
		if err := s.state.put(context.Background(), s.state.nodeRoleKey(member.ID), api.NodeRoleControlPlane); err != nil {
			panic(err)
		}
	}
	for _, id := range healthy {
		addTestNode(s, &Node{ID: id, Status: NodeStatusHealthy}, now)
		s.RecordRaftProgress(context.Background(), id, joiner.AppliedIndex())
	}
	return s
}

func sortedIDs(ids ...uuid.UUID) []string {
	result := make([]string, 0, len(ids))
	for _, id := range ids {
		result = append(result, id.String())
	}
	slices.Sort(result)
	return result
}

func TestVoterTarget(t *testing.T) {
	for members, want := range map[int]int{0: 1, 1: 1, 2: 1, 3: 3, 4: 3, 5: 5, 6: 5, 7: 5, 100: 5} {
		if got := voterTarget(members); got != want {
			t.Errorf("voterTarget(%d) = %d, want %d", members, got, want)
		}
	}
}

func TestPlanMembership(t *testing.T) {
	leader := memberState{ID: "a", Voter: true, Leader: true, Eligible: true, Live: true}
	voter := func(id string) memberState { return memberState{ID: id, Voter: true, Eligible: true, Live: true} }
	nonvoter := func(id string) memberState { return memberState{ID: id, Eligible: true, Live: true} }
	lagging := func(id string) memberState { return memberState{ID: id, Live: true} }
	gone := func(member memberState) memberState {
		member.Eligible, member.Live, member.Gone = false, false, true
		return member
	}
	tests := []struct {
		name    string
		members []memberState
		want    *membershipChange
	}{
		{name: "single node", members: []memberState{leader}},
		{name: "second node stays a non-voter", members: []memberState{leader, nonvoter("b")}},
		{name: "third node promotes lowest ID first", members: []memberState{leader, nonvoter("c"), nonvoter("b")}, want: &membershipChange{Action: promoteMember, ID: "b"}},
		{name: "promotion completes an odd set", members: []memberState{leader, voter("b"), nonvoter("c")}, want: &membershipChange{Action: promoteMember, ID: "c"}},
		{name: "lagging node is not promoted", members: []memberState{leader, nonvoter("b"), lagging("c")}},
		{name: "no promotion to an even set", members: []memberState{leader, voter("b"), voter("c"), nonvoter("d"), lagging("e")}},
		{name: "fifth node completes five voters", members: []memberState{leader, voter("b"), voter("c"), nonvoter("d"), nonvoter("e")}, want: &membershipChange{Action: promoteMember, ID: "d"}},
		{name: "sixth node stays a non-voter", members: []memberState{leader, voter("b"), voter("c"), voter("d"), voter("e"), nonvoter("f")}},
		{name: "surplus voter is demoted, never the leader", members: []memberState{leader, voter("b"), voter("c"), voter("d")}, want: &membershipChange{Action: demoteMember, ID: "b"}},
		{name: "surplus demotion prefers a lagging voter", members: []memberState{leader, voter("b"), voter("c"), {ID: "d", Voter: true, Live: true}}, want: &membershipChange{Action: demoteMember, ID: "d"}},
		{name: "gone voter is replaced by promotion first", members: []memberState{leader, voter("b"), gone(voter("c")), nonvoter("d")}, want: &membershipChange{Action: promoteMember, ID: "d"}},
		{name: "then the gone voter is demoted", members: []memberState{leader, voter("b"), gone(voter("c")), voter("d")}, want: &membershipChange{Action: demoteMember, ID: "c"}},
		{name: "gone voter without a replacement leaves an odd set", members: []memberState{leader, voter("b"), gone(voter("c")), lagging("d")}, want: &membershipChange{Action: demoteMember, ID: "c"}},
		{name: "gone leader record is never replaced", members: []memberState{gone(leader), voter("b"), voter("c"), nonvoter("d")}},
		{name: "unhealthy but not gone voter is kept", members: []memberState{leader, voter("b"), {ID: "c", Voter: true}, nonvoter("d")}},
		{name: "gone voter below target is still swapped", members: []memberState{leader, voter("b"), gone(voter("c")), nonvoter("d"), lagging("e")}, want: &membershipChange{Action: promoteMember, ID: "d"}},
		{name: "then demoted to an odd set below target", members: []memberState{leader, voter("b"), gone(voter("c")), voter("d"), lagging("e")}, want: &membershipChange{Action: demoteMember, ID: "c"}},
		{name: "reachable voter keeps its vote without a replacement", members: []memberState{leader, voter("b"), lagging("c")}},
		{name: "even set with a promotion is completed", members: []memberState{leader, voter("b"), nonvoter("c")}, want: &membershipChange{Action: promoteMember, ID: "c"}},
		{name: "unreachable voter is demoted before a draining one", members: []memberState{leader, {ID: "b", Voter: true, Live: true}, {ID: "c", Voter: true}, voter("d")}, want: &membershipChange{Action: demoteMember, ID: "c"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := slices.Clone(tt.members)
			got, ok := planMembership(input)
			if !slices.Equal(input, tt.members) {
				t.Fatal("planMembership modified its input")
			}
			if tt.want == nil {
				if ok {
					t.Fatalf("planMembership = %+v, want no change", got)
				}
				return
			}
			if !ok || got.Action != tt.want.Action || got.ID != tt.want.ID {
				t.Fatalf("planMembership = %+v, %v; want %+v", got, ok, *tt.want)
			}
		})
	}
}

func TestJoinMemberAddsNonvoter(t *testing.T) {
	leader, joining := uuid.New(), uuid.New()
	joiner := newFakeMembership(fakeMember(leader, true))
	s := membershipTestServer(joiner, leader)
	if err := s.state.put(context.Background(), s.state.nodeRoleKey(joining.String()), api.NodeRoleControlPlane); err != nil {
		t.Fatal(err)
	}
	members, err := s.JoinMember(context.Background(), joining, "joining:8129")
	if err != nil {
		t.Fatal(err)
	}
	if want := sortedIDs(leader, joining); !slices.Equal(members, want) {
		t.Fatalf("admitted members = %v, want %v", members, want)
	}
	configuration, _ := joiner.Membership()
	if len(configuration) != 2 || configuration[1].ID != joining.String() || configuration[1].Voter {
		t.Fatalf("membership = %+v, want the joining node as a non-voter", configuration)
	}
}

func TestWorkerCannotJoinRaft(t *testing.T) {
	leader, worker := uuid.New(), uuid.New()
	joiner := newFakeMembership(fakeMember(leader, true))
	s := membershipTestServer(joiner, leader)
	if err := s.state.put(context.Background(), s.state.nodeRoleKey(worker.String()), api.NodeRoleWorker); err != nil {
		t.Fatal(err)
	}
	if _, err := s.JoinMember(context.Background(), worker, "worker:8129"); err == nil {
		t.Fatal("worker joined Raft")
	}
	if got := joiner.operations(); len(got) != 0 {
		t.Fatalf("rejected worker changed membership: %v", got)
	}
}

func TestReconcileMembershipPromotesHealthyCaughtUpNodes(t *testing.T) {
	leader, b, c, d := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	joiner := newFakeMembership(fakeMember(leader, true), fakeMember(b, false), fakeMember(c, false), fakeMember(d, false))
	s := membershipTestServer(joiner, leader, b, c)
	// d heartbeats but trails the leader's log too far to vote.
	addTestNode(s, &Node{ID: d, Status: NodeStatusHealthy}, s.now())
	s.RecordRaftProgress(t.Context(), d, joiner.AppliedIndex()-raftCatchUpLag-1)

	if err := s.ReconcileMembership(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, want := joiner.voters(), sortedIDs(leader, b, c); !slices.Equal(got, want) {
		t.Fatalf("voters = %v, want %v", got, want)
	}

	// Once caught up, d stays a non-voter: four voters would not tolerate
	// more failures than three.
	s.RecordRaftProgress(t.Context(), d, joiner.AppliedIndex())
	if err := s.ReconcileMembership(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := len(joiner.voters()); got != 3 {
		t.Fatalf("voters = %d, want 3", got)
	}
}

func TestReconcileMembershipIgnoresStaleProgressAndDrainingNodes(t *testing.T) {
	leader, b, c := uuid.New(), uuid.New(), uuid.New()
	joiner := newFakeMembership(fakeMember(leader, true), fakeMember(b, false), fakeMember(c, false))
	s := membershipTestServer(joiner, leader, b, c)
	s.nodes[b].Status = NodeStatusDraining
	s.liveness.recordRaftProgress(c, raftProgress{applied: joiner.applied, leaderApplied: joiner.applied, at: s.now().Add(-recentHeartbeat - time.Second)})
	if err := s.ReconcileMembership(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := joiner.operations(); len(got) != 0 {
		t.Fatalf("operations = %v, want no promotion", got)
	}
}

func TestReconcileMembershipReplacesGoneVoter(t *testing.T) {
	leader, b, c, d := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	joiner := newFakeMembership(fakeMember(leader, true), fakeMember(b, true), fakeMember(c, true), fakeMember(d, false))
	s := membershipTestServer(joiner, leader, b, d)
	addTestNode(s, &Node{ID: c, Status: NodeStatusUnhealthy}, s.now().Add(-voterLossTimeout+time.Second))
	if err := s.ReconcileMembership(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := joiner.operations(); len(got) != 0 {
		t.Fatalf("operations = %v before the voter loss timeout", got)
	}

	setTestHeartbeat(s, c, s.now().Add(-voterLossTimeout))
	if err := s.ReconcileMembership(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, want := joiner.operations(), []string{"promote " + d.String(), "demote " + c.String()}; !slices.Equal(got, want) {
		t.Fatalf("operations = %v, want %v", got, want)
	}
	if got, want := joiner.voters(), sortedIDs(leader, b, d); !slices.Equal(got, want) {
		t.Fatalf("voters = %v, want %v", got, want)
	}
}

func TestReconcileMembershipWaitsForNewLeaderToObserveNodes(t *testing.T) {
	leader, b, c, d := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	joiner := newFakeMembership(fakeMember(leader, true), fakeMember(b, true), fakeMember(c, true), fakeMember(d, false))
	s := membershipTestServer(joiner, leader, b, d)
	// c's last heartbeat is old, but this leader was elected only recently.
	addTestNode(s, &Node{ID: c, Status: NodeStatusUnhealthy}, s.now().Add(-time.Hour))
	s.leaderSince = s.now().Add(-time.Minute)
	if err := s.ReconcileMembership(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := joiner.operations(); len(got) != 0 {
		t.Fatalf("operations = %v, want none until the node has been silent under this leader", got)
	}
}

func TestRemoveMemberPromotesReplacementBeforeRemovingVoter(t *testing.T) {
	leader, b, c, d := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	joiner := newFakeMembership(fakeMember(leader, true), fakeMember(b, true), fakeMember(c, true), fakeMember(d, false))
	s := membershipTestServer(joiner, leader, b, c, d)
	if err := s.RemoveMember(context.Background(), b.String()); err != nil {
		t.Fatal(err)
	}
	if got, want := joiner.operations(), []string{"promote " + d.String(), "remove " + b.String()}; !slices.Equal(got, want) {
		t.Fatalf("operations = %v, want %v", got, want)
	}
	if got, want := joiner.voters(), sortedIDs(leader, c, d); !slices.Equal(got, want) {
		t.Fatalf("voters = %v, want %v", got, want)
	}
}

func TestRemoveMemberPromotionFailurePreservesRevocationAndRetries(t *testing.T) {
	leader, b, c, d := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	joiner := newFakeMembership(fakeMember(leader, true), fakeMember(b, true), fakeMember(c, true), fakeMember(d, false))
	s := membershipTestServer(joiner, leader, b, d) // c is already unavailable.
	failure := errors.New("leadership lost while committing log")
	joiner.err = failure
	err := s.RemoveMember(t.Context(), b.String())
	if !errors.Is(err, failure) || !strings.Contains(err.Error(), "tombstoned but removal is incomplete") {
		t.Fatalf("promotion failure = %v", err)
	}
	if removed, err := s.state.NodeRemoved(t.Context(), b.String()); err != nil || !removed {
		t.Fatalf("tombstone after failed promotion = %v, %v", removed, err)
	}
	if _, err := s.JoinMember(t.Context(), b, "b:8129"); !errors.Is(err, ErrNodeRemoved) {
		t.Fatalf("revoked member rejoined: %v", err)
	}
	if got := joiner.operations(); len(got) != 0 {
		t.Fatalf("failed promotion continued to removal: %v", got)
	}
	joiner.err = nil
	if err := s.RemoveMember(t.Context(), b.String()); err != nil {
		t.Fatal(err)
	}
	if err := s.RemoveMember(t.Context(), b.String()); err != nil {
		t.Fatal(err)
	}
	if got, want := joiner.operations(), []string{"promote " + d.String(), "remove " + b.String()}; !slices.Equal(got, want) {
		t.Fatalf("retry operations = %v, want %v", got, want)
	}
	if got, want := joiner.voters(), sortedIDs(leader, c, d); !slices.Equal(got, want) {
		t.Fatalf("final voters = %v, want %v", got, want)
	}
}

func TestRemoveMemberRebalancesToOddVoterSet(t *testing.T) {
	leader, b, c := uuid.New(), uuid.New(), uuid.New()
	joiner := newFakeMembership(fakeMember(leader, true), fakeMember(b, true), fakeMember(c, true))
	s := membershipTestServer(joiner, leader, b, c)
	if err := s.RemoveMember(context.Background(), c.String()); err != nil {
		t.Fatal(err)
	}
	if err := s.ReconcileMembership(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Two voters tolerate no failure, and losing either would stop the
	// control plane; one voter plus a non-voter keeps the survivor harmless.
	if got, want := joiner.operations(), []string{"remove " + c.String(), "demote " + b.String()}; !slices.Equal(got, want) {
		t.Fatalf("operations = %v, want %v", got, want)
	}
}

func TestRemoveMemberAllowsRemovingGoneVoter(t *testing.T) {
	leader, b, c := uuid.New(), uuid.New(), uuid.New()
	joiner := newFakeMembership(fakeMember(leader, true), fakeMember(b, true), fakeMember(c, true))
	s := membershipTestServer(joiner, leader, b)
	addTestNode(s, &Node{ID: c, Status: NodeStatusUnhealthy}, s.now().Add(-time.Hour))
	if err := s.RemoveMember(context.Background(), c.String()); err != nil {
		t.Fatal(err)
	}
	if got := joiner.operations(); len(got) == 0 || got[0] != "remove "+c.String() {
		t.Fatalf("operations = %v, want the gone voter removed", got)
	}
}

func TestRemoveMemberRefusesQuorumLoss(t *testing.T) {
	leader, b, c, d, e := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	joiner := newFakeMembership(fakeMember(leader, true), fakeMember(b, true), fakeMember(c, true), fakeMember(d, true), fakeMember(e, true))
	s := membershipTestServer(joiner, leader, b)
	for _, id := range []uuid.UUID{c, d} {
		addTestNode(s, &Node{ID: id, Status: NodeStatusUnhealthy}, s.now().Add(-time.Minute))
	}
	addTestNode(s, &Node{ID: e, Status: NodeStatusHealthy}, s.now())
	// Removing live e would leave leader and b as the only live voters of
	// four, below the three needed to commit.
	err := s.RemoveMember(context.Background(), e.String())
	if !errors.Is(err, ErrMembershipUnsafe) {
		t.Fatalf("RemoveMember error = %v, want ErrMembershipUnsafe", err)
	}
	if got := joiner.operations(); len(got) != 0 {
		t.Fatalf("unsafe removal changed membership: %v", got)
	}
	// A removal refused for quorum must not leave a promotion behind.
	f := uuid.New()
	joiner.members = append(joiner.members, fakeMember(f, false))
	addTestNode(s, &Node{ID: f, Status: NodeStatusHealthy}, s.now())
	s.RecordRaftProgress(t.Context(), f, joiner.AppliedIndex())
	setTestHeartbeat(s, e, s.now().Add(-time.Minute))
	setTestHeartbeat(s, b, s.now().Add(-time.Minute))
	if err := s.RemoveMember(context.Background(), leader.String()); !errors.Is(err, ErrMembershipUnsafe) {
		t.Fatalf("RemoveMember error = %v, want ErrMembershipUnsafe", err)
	}
	if got := joiner.operations(); len(got) != 0 {
		t.Fatalf("refused removal changed membership: %v", got)
	}
	setTestHeartbeat(s, e, s.now())
	setTestHeartbeat(s, b, s.now())
	// Removing an unreachable voter keeps three live voters of four.
	if err := s.RemoveMember(context.Background(), c.String()); err != nil {
		t.Fatal(err)
	}
}

func TestRemoveMemberIsIdempotent(t *testing.T) {
	leader := uuid.New()
	joiner := newFakeMembership(fakeMember(leader, true))
	s := membershipTestServer(joiner, leader)
	if err := s.RemoveMember(context.Background(), uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	if got := joiner.operations(); len(got) != 0 {
		t.Fatalf("operations = %v, want none", got)
	}
}

func TestReconcileMembershipReplacesVoterThatNeverRegistered(t *testing.T) {
	leader, b, c, d := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	joiner := newFakeMembership(fakeMember(leader, true), fakeMember(b, true), fakeMember(c, true), fakeMember(d, false))
	s := membershipTestServer(joiner, leader, b, d)
	// c votes but has not registered with this leader since its election an
	// hour ago.
	if err := s.ReconcileMembership(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, want := joiner.voters(), sortedIDs(leader, b, d); !slices.Equal(got, want) {
		t.Fatalf("voters = %v, want %v", got, want)
	}
}

func TestMembershipRejectsOriginatingTermCancellation(t *testing.T) {
	leader, removed := uuid.New(), uuid.New()
	joiner := newFakeMembership(fakeMember(leader, true), fakeMember(removed, false))
	s := membershipTestServer(joiner, leader, removed)
	term, cancel := context.WithCancel(t.Context())
	s.term = term
	origin, release := s.bindTerm(t.Context())
	defer release()
	cancel()
	ctx := context.WithoutCancel(origin)
	if _, err := s.JoinMember(ctx, removed, "stale:8129"); !errors.Is(err, context.Canceled) {
		t.Fatalf("old join = %v", err)
	}
	if err := s.ReconcileMembership(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("old reconciliation = %v", err)
	}
	if err := s.RemoveMember(ctx, removed.String()); !errors.Is(err, context.Canceled) {
		t.Fatalf("old removal = %v", err)
	}
	if tombstoned, err := s.state.NodeRemoved(t.Context(), removed.String()); err != nil || tombstoned || len(joiner.operations()) != 0 {
		t.Fatalf("old membership work mutated state: tombstoned=%v error=%v operations=%v", tombstoned, err, joiner.operations())
	}
}
