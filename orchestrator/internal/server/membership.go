package server

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/internal/state"
)

const (
	// maxVoters bounds the Raft voter set. Five voters tolerate two failures;
	// more would only add write fan-out and quorum size without a practical
	// availability gain, so larger clusters add further nodes as non-voters.
	maxVoters = 5
	// raftCatchUpLag is how many log entries a node may trail the leader and
	// still count as caught up for promotion.
	raftCatchUpLag = 256
	// recentHeartbeat bounds how old a node's last heartbeat, and the Raft
	// progress it reported, may be for the node to count as reachable.
	recentHeartbeat = livenessWindow
	// voterLossTimeout is how long a voter's node must be silent before its
	// vote is handed to a healthy non-voter. The replaced node stays a member
	// and can vote again later.
	voterLossTimeout = 5 * time.Minute
	// membershipInterval is how often the leader re-evaluates the voter set.
	// A removal also wakes it.
	membershipInterval = reconcileInterval
	// maxMembershipSteps bounds the configuration changes of one pass.
	maxMembershipSteps = 2 * maxVoters
)

// ErrMembershipUnsafe indicates that a membership change would leave the
// remaining voters unable to form a quorum.
var ErrMembershipUnsafe = errors.New("membership change would lose control-plane quorum")

type raftProgress struct {
	applied       uint64
	leaderApplied uint64
	at            time.Time
}

// RecordRaftProgress records the Raft applied index a node reported in its
// heartbeat, together with the leader's own applied index at receipt.
func (s *Server) RecordRaftProgress(id uuid.UUID, applied uint64) {
	if s.joiner == nil || applied == 0 {
		return
	}
	s.liveness.recordRaftProgress(id, raftProgress{applied: applied, leaderApplied: s.joiner.AppliedIndex(), at: s.now()})
}

// voterTarget returns the desired number of voters for a membership size: the
// largest odd number not above the member count or maxVoters. An even voter
// count tolerates no more failures than one voter fewer.
func voterTarget(members int) int {
	return oddFloor(min(members, maxVoters))
}

// oddFloor returns the largest odd number not above n, and at least one.
func oddFloor(n int) int {
	if n%2 == 0 {
		n--
	}
	return max(n, 1)
}

// memberState is the leader's view of one Raft member.
type memberState struct {
	ID      string
	Address string
	Voter   bool
	// Leader marks the local leader, which is never demoted.
	Leader bool
	// Eligible members are healthy and caught up, so they may be promoted.
	Eligible bool
	// Live members are known to be reachable right now.
	Live bool
	// Gone members have been silent for voterLossTimeout.
	Gone bool
}

type membershipAction string

const (
	promoteMember membershipAction = "promote"
	demoteMember  membershipAction = "demote"
)

type membershipChange struct {
	Action  membershipAction
	ID      string
	Address string
}

// planMembership returns the next single change that moves the voter set
// toward its desired size, or false when none is needed. It is a pure function
// of its input, which it does not modify, and breaks every tie by member ID.
//
// Promotions only choose eligible non-voters and only move toward the desired
// count: the largest odd number not above voterTarget or the members that
// could vote now (voters that are not gone plus eligible non-voters). A gone
// voter is replaced by promoting an eligible non-voter first; the resulting
// surplus then demotes the gone voter, so the number of reachable voters never
// shrinks during the swap. Voters that are not gone are demoted only when they
// exceed voterTarget, preferring unreachable, then ineligible ones, and never
// the leader: a reachable voter keeps its vote, and its copy of every commit,
// while a replacement is missing.
func planMembership(members []memberState) (membershipChange, bool) {
	sorted := slices.Clone(members)
	slices.SortFunc(sorted, func(a, b memberState) int { return strings.Compare(a.ID, b.ID) })
	target := voterTarget(len(sorted))
	voters, presentVoters := 0, 0
	var eligible, gone []memberState
	for _, member := range sorted {
		switch {
		case member.Voter && member.Gone && !member.Leader:
			voters++
			gone = append(gone, member)
		case member.Voter:
			voters++
			presentVoters++
		case member.Eligible:
			eligible = append(eligible, member)
		}
	}
	desired := oddFloor(min(target, presentVoters+len(eligible)))
	if len(eligible) > 0 && (voters < desired || (len(gone) > 0 && voters <= target)) {
		return membershipChange{Action: promoteMember, ID: eligible[0].ID, Address: eligible[0].Address}, true
	}
	if voters > desired && len(gone) > 0 {
		return membershipChange{Action: demoteMember, ID: gone[0].ID, Address: gone[0].Address}, true
	}
	if voters > target {
		if demote, ok := pickDemotion(sorted); ok {
			return membershipChange{Action: demoteMember, ID: demote.ID, Address: demote.Address}, true
		}
	}
	return membershipChange{}, false
}

func pickDemotion(sorted []memberState) (memberState, bool) {
	rank := func(member memberState) int {
		switch {
		case member.Gone:
			return 0
		case !member.Live:
			return 1
		case !member.Eligible:
			return 2
		default:
			return 3
		}
	}
	var pick memberState
	found := false
	for _, member := range sorted {
		if !member.Voter || member.Leader {
			continue
		}
		if !found || rank(member) < rank(pick) {
			pick, found = member, true
		}
	}
	return pick, found
}

// memberStates combines a Raft configuration with the leader's node
// observations.
func (s *Server) memberStates(members []state.RaftMember) []memberState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := s.now()
	local := s.nodeID.String()
	result := make([]memberState, 0, len(members))
	for _, member := range members {
		current := memberState{ID: member.ID, Address: member.Address, Voter: member.Voter, Leader: member.ID == local}
		if current.Leader {
			current.Eligible, current.Live = true, true
			result = append(result, current)
			continue
		}
		id, err := uuid.Parse(member.ID)
		node := s.nodes[id]
		if err != nil || node == nil {
			// A member that has never registered with this leader is silent
			// since the leader's election.
			current.Gone = now.Sub(s.leaderSince) >= voterLossTimeout
			result = append(result, current)
			continue
		}
		progress, reported := s.liveness.raftProgress(id)
		heartbeat := s.liveness.lastHeartbeat(id)
		caughtUp := reported && now.Sub(progress.at) <= recentHeartbeat && progress.leaderApplied <= progress.applied+raftCatchUpLag
		current.Live = heartbeatLive(heartbeat, now)
		current.Eligible = livenessStatus(node.Status, heartbeat, now) == NodeStatusHealthy && caughtUp
		// Silence is measured from no earlier than this leader's election.
		current.Gone = now.Sub(silentSince(heartbeat, s.leaderSince)) >= voterLossTimeout
		result = append(result, current)
	}
	return result
}

// Every Raft configuration change in the cluster is made here, by the leader,
// while holding membershipMu: a change is planned from the configuration read
// under the same hold, so no other change can interleave. hashicorp/raft
// allows one uncommitted configuration change at a time and fails in-flight
// changes on leadership loss, and a new leader re-reads the configuration.

// JoinMember admits an authenticated node to Raft as a non-voter and returns
// the member IDs after admission. The leader promotes it once it is healthy
// and caught up if the voter set needs it. A removed identity is refused
// under membershipMu, which RemoveMember holds while recording removals, so a
// join cannot race a removal back into the configuration.
func (s *Server) JoinMember(ctx context.Context, id uuid.UUID, raftAddress string) ([]string, error) {
	if s.joiner == nil {
		return nil, fmt.Errorf("cluster join not available")
	}
	s.membershipMu.Lock()
	defer s.membershipMu.Unlock()
	if removed, err := s.state.NodeRemoved(ctx, id.String()); err != nil {
		return nil, err
	} else if removed {
		return nil, fmt.Errorf("node %s: %w", id, ErrNodeRemoved)
	}
	if err := s.joiner.AddNonvoter(id.String(), raftAddress); err != nil {
		return nil, err
	}
	members, err := s.joiner.Membership()
	if err != nil {
		return nil, fmt.Errorf("read Raft membership: %w", err)
	}
	ids := make([]string, 0, len(members))
	for _, member := range members {
		ids = append(ids, member.ID)
	}
	slices.Sort(ids)
	return ids, nil
}

// ReconcileMembership moves the voter set toward its desired size one change
// at a time until no change is needed. Each step re-reads the configuration
// under membershipMu and applies its change before releasing it, so every
// change is planned from the configuration it applies to; joins and removals
// can interleave between steps.
func (s *Server) ReconcileMembership(ctx context.Context) error {
	if s.joiner == nil {
		return nil
	}
	for range maxMembershipSteps {
		if err := ctx.Err(); err != nil {
			return err
		}
		changed, err := s.reconcileMembershipStep()
		if err != nil || !changed {
			return err
		}
	}
	return nil
}

func (s *Server) reconcileMembershipStep() (bool, error) {
	s.membershipMu.Lock()
	defer s.membershipMu.Unlock()
	members, err := s.joiner.Membership()
	if err != nil {
		return false, fmt.Errorf("read Raft membership: %w", err)
	}
	change, ok := planMembership(s.memberStates(members))
	if !ok {
		return false, nil
	}
	return true, s.applyMembershipChange(change)
}

func (s *Server) applyMembershipChange(change membershipChange) error {
	var err error
	switch change.Action {
	case promoteMember:
		err = s.joiner.PromoteVoter(change.ID, change.Address)
	case demoteMember:
		err = s.joiner.DemoteVoter(change.ID)
	default:
		err = fmt.Errorf("unknown membership action %q", change.Action)
	}
	if err != nil {
		return fmt.Errorf("%s Raft member %s: %w", change.Action, change.ID, err)
	}
	if s.log != nil {
		s.log.Info("changed control-plane voter set", "action", string(change.Action), "node_id", change.ID)
	}
	return nil
}

// RemoveMember permanently removes a node. It first records a durable
// tombstone for the node UUID, which revokes the node's certificate on every
// node-authenticated path (control-plane API, agent API, enrollment, Raft
// join, and inbound Raft streams), and then removes it from Raft. Before a
// voter is removed, a healthy caught-up non-voter is promoted in its place so
// the number of live voters does not shrink. The removal is refused, before
// any change, for the current leader, and if the voters that would remain
// could not form a quorum from the members known to be live. Removing an absent member still records the
// tombstone and succeeds, so retries are safe. Any resulting surplus voter is
// demoted by the membership loop, which the removal wakes.
func (s *Server) RemoveMember(ctx context.Context, id string) error {
	nodeID, err := uuid.Parse(id)
	if err != nil || nodeID == uuid.Nil || nodeID.String() != id {
		return ErrInvalidNodeID
	}
	if s.joiner == nil {
		return fmt.Errorf("cluster membership changes not available")
	}
	s.membershipMu.Lock()
	defer s.membershipMu.Unlock()
	current, err := s.joiner.Membership()
	if err != nil {
		return fmt.Errorf("read Raft membership: %w", err)
	}
	members := s.memberStates(current)
	index := slices.IndexFunc(members, func(member memberState) bool { return member.ID == id })
	if index >= 0 && members[index].Leader {
		// The tombstone would make followers reject this leader's new Raft
		// streams before its own removal entry could commit.
		return fmt.Errorf("%w: %s is the current leader; transfer leadership first", ErrMembershipUnsafe, id)
	}
	var replacement membershipChange
	promote := false
	if index >= 0 && members[index].Voter {
		replacement, promote = planReplacement(members, id)
		if promote {
			// Check the configuration the removal would leave before changing
			// anything, counting the eligible, and so live, replacement.
			members[slices.IndexFunc(members, func(member memberState) bool { return member.ID == replacement.ID })].Voter = true
		}
		if err := checkRemovalQuorum(members, id); err != nil {
			return err
		}
	}
	// Revoke before changing Raft: if a later step fails, the node is already
	// unable to authenticate or rejoin, and a retry completes the removal.
	if err := s.state.PutNodeTombstone(ctx, id, NodeTombstone{RemovedAt: s.now().UTC()}); err != nil {
		return fmt.Errorf("record removal of node %s: %w", id, err)
	}
	if index < 0 {
		return nil
	}
	if members[index].Voter {
		if promote {
			if err := s.applyMembershipChange(replacement); err != nil {
				return err
			}
		}
	}
	if err := s.joiner.RemoveServer(id); err != nil {
		return fmt.Errorf("remove Raft member %s: %w", id, err)
	}
	s.liveness.forgetRaftProgress(nodeID)
	s.wakeMembership()
	return nil
}

func (s *Server) wakeMembership() {
	if s.membershipWake == nil {
		return
	}
	select {
	case s.membershipWake <- struct{}{}:
	default:
	}
}

// planReplacement chooses the eligible non-voter, by ID, to promote before
// removing the voter id.
func planReplacement(members []memberState, id string) (membershipChange, bool) {
	var pick *memberState
	for i := range members {
		member := &members[i]
		if member.ID == id || member.Voter || !member.Eligible {
			continue
		}
		if pick == nil || member.ID < pick.ID {
			pick = member
		}
	}
	if pick == nil {
		return membershipChange{}, false
	}
	return membershipChange{Action: promoteMember, ID: pick.ID, Address: pick.Address}, true
}

// checkRemovalQuorum refuses to remove voter id when the live voters that
// would remain are fewer than a majority of the remaining voters. Such a
// removal would leave the leader unable to commit, including the removal.
func checkRemovalQuorum(members []memberState, id string) error {
	remaining, live := 0, 0
	for _, member := range members {
		if !member.Voter || member.ID == id {
			continue
		}
		remaining++
		if member.Live {
			live++
		}
	}
	if remaining == 0 {
		return fmt.Errorf("%w: %s is the only voter", ErrMembershipUnsafe, id)
	}
	if live < remaining/2+1 {
		return fmt.Errorf("%w: only %d of the %d remaining voters are reachable; restore or remove unreachable voters first", ErrMembershipUnsafe, live, remaining)
	}
	return nil
}

// MemberVoters returns the Raft membership as voter flags keyed by node ID.
func (s *Server) MemberVoters() (map[string]bool, error) {
	if s.joiner == nil {
		return nil, nil
	}
	members, err := s.joiner.Membership()
	if err != nil {
		return nil, err
	}
	voters := make(map[string]bool, len(members))
	for _, member := range members {
		voters[member.ID] = member.Voter
	}
	return voters, nil
}

func (s *Server) runMembershipLoop(ctx context.Context) {
	// A wake left over from an earlier leadership term must not skip this
	// term's wait below.
	select {
	case <-s.membershipWake:
	default:
	}
	ticker := time.NewTicker(membershipInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-s.membershipWake:
		}
		// A new leader first lets nodes report health and progress to it.
		s.mu.RLock()
		settled := s.now().Sub(s.leaderSince) >= leaderRecoveryGrace
		s.mu.RUnlock()
		if !settled {
			continue
		}
		if err := s.ReconcileMembership(ctx); err != nil && ctx.Err() == nil && s.log != nil {
			s.log.Warn("reconcile control-plane voters", "error", err)
		}
	}
}
