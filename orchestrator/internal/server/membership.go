package server

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/internal/state"
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
	recentHeartbeat = 3 * heartbeatInterval
	// voterLossTimeout is how long a voter's node must be silent before its
	// vote is handed to a healthy non-voter. The replaced node stays a member
	// and can vote again later.
	voterLossTimeout = 5 * time.Minute
	// membershipInterval is how often the leader re-evaluates the voter set.
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
	progress := raftProgress{applied: applied, leaderApplied: s.joiner.AppliedIndex(), at: s.now()}
	s.mu.Lock()
	if s.raftProgress == nil {
		s.raftProgress = make(map[uuid.UUID]raftProgress)
	}
	s.raftProgress[id] = progress
	s.mu.Unlock()
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
// The desired size is the largest odd number not above voterTarget or the
// number of members that could vote now: voters that are not gone plus
// eligible non-voters. Promotions only choose eligible non-voters. Surplus
// voters are demoted, never the leader, preferring gone members, then
// unreachable ones, then ineligible ones, so an even voter set is always
// brought back to odd. A gone voter is replaced by promoting an eligible
// non-voter first; the resulting surplus then demotes the gone voter, so the
// number of reachable voters never shrinks during the swap.
func planMembership(members []memberState) (membershipChange, bool) {
	sorted := slices.Clone(members)
	slices.SortFunc(sorted, func(a, b memberState) int { return strings.Compare(a.ID, b.ID) })
	voters, presentVoters, goneVoters := 0, 0, 0
	var eligible []memberState
	for _, member := range sorted {
		switch {
		case member.Voter:
			voters++
			if member.Gone && !member.Leader {
				goneVoters++
			} else {
				presentVoters++
			}
		case member.Eligible:
			eligible = append(eligible, member)
		}
	}
	desired := oddFloor(min(voterTarget(len(sorted)), presentVoters+len(eligible)))
	if len(eligible) > 0 && (voters < desired || (goneVoters > 0 && voters <= desired)) {
		return membershipChange{Action: promoteMember, ID: eligible[0].ID, Address: eligible[0].Address}, true
	}
	if voters > desired {
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
			result = append(result, current)
			continue
		}
		progress, reported := s.raftProgress[id]
		caughtUp := reported && now.Sub(progress.at) <= recentHeartbeat && progress.leaderApplied <= progress.applied+raftCatchUpLag
		current.Eligible = node.Status == NodeStatusHealthy && caughtUp
		current.Live = now.Sub(node.LastHeartbeat) <= recentHeartbeat
		// A new leader may hold old heartbeat times until nodes report to it,
		// so silence is measured from no earlier than its election.
		silentSince := node.LastHeartbeat
		if s.leaderSince.After(silentSince) {
			silentSince = s.leaderSince
		}
		current.Gone = now.Sub(silentSince) >= voterLossTimeout
		result = append(result, current)
	}
	return result
}

// JoinMember admits an authenticated node to Raft as a non-voter. The leader
// promotes it once it is healthy and caught up if the voter set needs it.
func (s *Server) JoinMember(id uuid.UUID, raftAddress string) error {
	if s.joiner == nil {
		return fmt.Errorf("cluster join not available")
	}
	s.membershipMu.Lock()
	defer s.membershipMu.Unlock()
	return s.joiner.AddNonvoter(id.String(), raftAddress)
}

// ReconcileMembership moves the voter set toward its target one change at a
// time until no change is needed.
func (s *Server) ReconcileMembership(ctx context.Context) error {
	if s.joiner == nil {
		return nil
	}
	s.membershipMu.Lock()
	defer s.membershipMu.Unlock()
	return s.reconcileMembershipLocked(ctx)
}

func (s *Server) reconcileMembershipLocked(ctx context.Context) error {
	for range maxMembershipSteps {
		if err := ctx.Err(); err != nil {
			return err
		}
		membership, err := s.joiner.Membership()
		if err != nil {
			return fmt.Errorf("read Raft membership: %w", err)
		}
		change, ok := planMembership(s.memberStates(membership.Members))
		if !ok {
			return nil
		}
		if err := s.applyMembershipChange(change, membership.Index); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) applyMembershipChange(change membershipChange, configIndex uint64) error {
	var err error
	switch change.Action {
	case promoteMember:
		err = s.joiner.PromoteVoter(change.ID, change.Address, configIndex)
	case demoteMember:
		err = s.joiner.DemoteVoter(change.ID, configIndex)
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

// RemoveMember permanently removes a Raft member. Before a voter is removed, a
// healthy caught-up non-voter is promoted in its place so the number of live
// voters does not shrink. The removal is refused, before any change, if the
// voters that would remain could not form a quorum from the members known to
// be live. Removing an absent member succeeds, so retries are safe. Any
// resulting surplus voter is demoted by the membership loop, which the removal
// wakes.
func (s *Server) RemoveMember(_ context.Context, id string) error {
	if s.joiner == nil {
		return fmt.Errorf("cluster membership changes not available")
	}
	s.membershipMu.Lock()
	defer s.membershipMu.Unlock()
	membership, err := s.joiner.Membership()
	if err != nil {
		return fmt.Errorf("read Raft membership: %w", err)
	}
	members := s.memberStates(membership.Members)
	index := slices.IndexFunc(members, func(member memberState) bool { return member.ID == id })
	if index < 0 {
		return nil
	}
	if members[index].Voter {
		replacement, promote := planReplacement(members, id)
		if promote {
			// Check the configuration the removal would leave before changing
			// anything, counting the eligible, and so live, replacement.
			members[slices.IndexFunc(members, func(member memberState) bool { return member.ID == replacement.ID })].Voter = true
		}
		if err := checkRemovalQuorum(members, id); err != nil {
			return err
		}
		if promote {
			if err := s.applyMembershipChange(replacement, membership.Index); err != nil {
				return err
			}
			if membership, err = s.joiner.Membership(); err != nil {
				return fmt.Errorf("read Raft membership: %w", err)
			}
		}
	}
	if err := s.joiner.RemoveServer(id, membership.Index); err != nil {
		return fmt.Errorf("remove Raft member %s: %w", id, err)
	}
	if nodeID, err := uuid.Parse(id); err == nil {
		s.mu.Lock()
		delete(s.raftProgress, nodeID)
		s.mu.Unlock()
	}
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
	membership, err := s.joiner.Membership()
	if err != nil {
		return nil, err
	}
	voters := make(map[string]bool, len(membership.Members))
	for _, member := range membership.Members {
		voters[member.ID] = member.Voter
	}
	return voters, nil
}

func (s *Server) runMembershipLoop(ctx context.Context) {
	ticker := time.NewTicker(membershipInterval)
	defer ticker.Stop()
	for {
		woken := false
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-s.membershipWake:
			woken = true
		}
		// A new leader first lets nodes report health and progress to it.
		// An operator's removal is acted on immediately.
		s.mu.RLock()
		settled := s.now().Sub(s.leaderSince) >= leaderRecoveryGrace
		s.mu.RUnlock()
		if !settled && !woken {
			continue
		}
		if err := s.ReconcileMembership(ctx); err != nil && ctx.Err() == nil && s.log != nil {
			s.log.Warn("reconcile control-plane voters", "error", err)
		}
	}
}
