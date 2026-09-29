package server

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/overfold/trellis/internal/api"
	"github.com/overfold/trellis/internal/lifecycle"
	"github.com/overfold/trellis/internal/spec"
)

// ReplacementPolicy bounds how quickly count reconciliation replaces failed
// allocations and how many terminal allocation records it retains. These are
// server defaults rather than manifest fields: they protect the control plane
// from crash-looping workloads without changing job semantics.
type ReplacementPolicy struct {
	// BackoffBase is the delay after the first consecutive failure.
	BackoffBase time.Duration
	// BackoffMax caps the exponential delay.
	BackoffMax time.Duration
	// StableAfter is how long a replacement created after the latest failure
	// must stay running, and not unhealthy, before the failure count resets.
	StableAfter time.Duration
	// RetainTerminal is the number of stopped, failed, or lost allocation
	// records kept per job task group.
	RetainTerminal int
}

// DefaultReplacementPolicy returns the server's replacement defaults.
func DefaultReplacementPolicy() ReplacementPolicy {
	return ReplacementPolicy{
		BackoffBase:    10 * time.Second,
		BackoffMax:     5 * time.Minute,
		StableAfter:    10 * time.Minute,
		RetainTerminal: 5,
	}
}

// ReplacementBackoff is the persisted replacement backoff state of one job
// task group. Every timestamp is chosen by the leader and carried in the
// replicated record, so applying it never depends on a follower's clock.
type ReplacementBackoff struct {
	Namespace     string `json:"namespace"`
	JobName       string `json:"job"`
	TaskGroupName string `json:"group"`
	// JobRevision is the revision whose failures are counted. A new revision
	// is new desired state and starts from zero failures.
	JobRevision int `json:"job_revision"`
	// Failures counts failed allocations since the last reset.
	Failures int `json:"failures"`
	// LastFailureAt is the transition time of the newest counted failure.
	LastFailureAt time.Time `json:"last_failure_at"`
	// SeenAllocations lists, sorted, the retained failed allocations of the
	// group that were already considered. It survives resets so a failed
	// record is never counted twice, and it shrinks as records are pruned.
	SeenAllocations []string `json:"seen_allocations,omitempty"`
	// LastAllocationID, Reason, and Message describe the newest failure.
	LastAllocationID string `json:"last_allocation_id,omitempty"`
	Reason           string `json:"reason,omitempty"`
	Message          string `json:"message,omitempty"`
	// NextReplacementAt is the earliest time replacements of failed
	// allocations may be placed for the group while Failures is positive.
	NextReplacementAt time.Time `json:"next_replacement_at"`
	// DelayedReplacements is the number of counted failed allocations whose
	// replacement waits for NextReplacementAt. Only these placements are
	// delayed: a deficit from lost allocations or a higher count is placed
	// immediately. It is never larger than the group's missing capacity.
	DelayedReplacements int `json:"delayed_replacements,omitempty"`
}

func replacementBackoffKey(namespace, job, group string) string {
	return namespace + "\x00" + job + "\x00" + group
}

func (b *ReplacementBackoff) key() string {
	return replacementBackoffKey(b.Namespace, b.JobName, b.TaskGroupName)
}

// active reports whether the backoff currently withholds replacements of
// failed allocations.
func (b *ReplacementBackoff) active(now time.Time) bool {
	return b != nil && b.Failures > 0 && now.Before(b.NextReplacementAt)
}

// withheld returns how many of deficit placements replace counted failed
// allocations and must wait for the backoff. The rest of the deficit, from
// lost allocations or a higher count, is placed immediately.
func (b *ReplacementBackoff) withheld(deficit int, now time.Time) int {
	if deficit <= 0 || !b.active(now) {
		return 0
	}
	return min(deficit, b.DelayedReplacements)
}

// reset returns a copy of the record without failures, so replacements are
// no longer delayed. The failed allocations already seen stay seen, so they
// are never counted again.
func (b *ReplacementBackoff) reset() *ReplacementBackoff {
	next := *b
	next.SeenAllocations = slices.Clone(b.SeenAllocations)
	next.Failures = 0
	next.NextReplacementAt = time.Time{}
	next.DelayedReplacements = 0
	return &next
}

func (b *ReplacementBackoff) equal(other *ReplacementBackoff) bool {
	if b == nil || other == nil {
		return b == other
	}
	return b.Namespace == other.Namespace && b.JobName == other.JobName && b.TaskGroupName == other.TaskGroupName &&
		b.JobRevision == other.JobRevision && b.Failures == other.Failures && b.LastFailureAt.Equal(other.LastFailureAt) &&
		b.LastAllocationID == other.LastAllocationID && b.Reason == other.Reason && b.Message == other.Message &&
		b.NextReplacementAt.Equal(other.NextReplacementAt) && b.DelayedReplacements == other.DelayedReplacements && slices.Equal(b.SeenAllocations, other.SeenAllocations)
}

// replacementBackoffDelay returns min(base*2^(failures-1), max). Zero failures
// means no delay.
func replacementBackoffDelay(policy ReplacementPolicy, failures int) time.Duration {
	if failures < 1 || policy.BackoffBase <= 0 {
		return 0
	}
	delay := policy.BackoffBase
	for i := 1; i < failures; i++ {
		if policy.BackoffMax > 0 && delay >= policy.BackoffMax {
			break
		}
		if delay > time.Duration(1<<62) {
			break
		}
		delay *= 2
	}
	if policy.BackoffMax > 0 && delay > policy.BackoffMax {
		delay = policy.BackoffMax
	}
	return delay
}

func isTerminalPhase(phase lifecycle.Phase) bool {
	return phase == lifecycle.PhaseStopped || phase == lifecycle.PhaseFailed || phase == lifecycle.PhaseLost
}

// planReplacementBackoff derives the next backoff record of one task group
// from its previous record and the allocations known to the leader. It never
// mutates its inputs. A failed allocation counts once, when it is first seen,
// and only if it belongs to the current revision and was not draining. The
// result is nil only when there is neither a previous record nor a failure to
// count. revision is zero when the group is no longer desired.
//
// DelayedReplacements accumulates the failures counted while the backoff is
// active. Once it elapses, the pass places the whole deficit, so the count
// restarts from the failures counted in that pass.
func planReplacementBackoff(policy ReplacementPolicy, previous *ReplacementBackoff, namespace, job, group string, revision int, allocations []*Allocation, now time.Time) *ReplacementBackoff {
	var next *ReplacementBackoff
	seen := make(map[string]bool)
	if previous != nil {
		copied := *previous
		next = &copied
		for _, id := range previous.SeenAllocations {
			seen[id] = true
		}
		if !previous.active(now) {
			next.DelayedReplacements = 0
		}
		if next.JobRevision != revision {
			next.JobRevision = revision
			next.Failures = 0
			next.NextReplacementAt = time.Time{}
			next.DelayedReplacements = 0
		}
	}

	var retained []string
	var failures []*Allocation
	for _, allocation := range allocations {
		if allocation.Namespace != namespace || allocation.JobName != job || allocation.TaskGroupName != group || allocation.Phase != lifecycle.PhaseFailed {
			continue
		}
		retained = append(retained, allocation.ID)
		if !seen[allocation.ID] && revision != 0 && !allocation.Draining && allocation.JobRevision == revision {
			failures = append(failures, allocation)
		}
	}
	if next == nil && len(failures) == 0 {
		return nil
	}
	if next == nil {
		next = &ReplacementBackoff{Namespace: namespace, JobName: job, TaskGroupName: group, JobRevision: revision}
	}
	sort.Strings(retained)
	next.SeenAllocations = slices.Compact(retained)
	if len(failures) > 0 {
		sort.Slice(failures, func(i, j int) bool {
			if !failures[i].TransitionedAt.Equal(failures[j].TransitionedAt) {
				return failures[i].TransitionedAt.Before(failures[j].TransitionedAt)
			}
			return failures[i].ID < failures[j].ID
		})
		newest := failures[len(failures)-1]
		next.Failures += len(failures)
		next.DelayedReplacements += len(failures)
		if newest.TransitionedAt.After(next.LastFailureAt) {
			next.LastFailureAt = newest.TransitionedAt
		}
		next.LastAllocationID = newest.ID
		next.Reason, next.Message = newest.Reason, newest.Message
		next.NextReplacementAt = now.Add(replacementBackoffDelay(policy, next.Failures))
	}

	if next.Failures > 0 {
		for _, allocation := range allocations {
			if allocation.Namespace != namespace || allocation.JobName != job || allocation.TaskGroupName != group {
				continue
			}
			if allocation.Phase != lifecycle.PhaseRunning || allocation.Health == lifecycle.HealthUnhealthy || allocation.Draining || allocation.JobRevision != revision {
				continue
			}
			if !allocation.CreatedAt.After(next.LastFailureAt) || now.Sub(allocation.TransitionedAt) < policy.StableAfter {
				continue
			}
			next.Failures = 0
			next.NextReplacementAt = time.Time{}
			next.DelayedReplacements = 0
			break
		}
	}
	return next
}

// planTerminalPruning selects terminal allocation records to delete so that at
// most retain stopped, failed, or lost records remain per job task group. The
// newest records by transition time are retained, with the allocation ID as a
// deterministic tie-breaker. A pruned allocation that later reappears in a
// node heartbeat is no longer desired and reconciliation stops it as an
// observed orphan, preserving fencing without retaining unbounded history.
// skip excludes allocations that must not be deleted in this pass. The inputs
// are not mutated.
func planTerminalPruning(retain int, allocations []*Allocation, skip map[*Allocation]bool) []*Allocation {
	if retain < 0 {
		return nil
	}
	type groupKey struct{ namespace, job, group string }
	groups := make(map[groupKey][]*Allocation)
	var keys []groupKey
	for _, allocation := range allocations {
		if !isTerminalPhase(allocation.Phase) {
			continue
		}
		key := groupKey{allocation.Namespace, allocation.JobName, allocation.TaskGroupName}
		if _, exists := groups[key]; !exists {
			keys = append(keys, key)
		}
		groups[key] = append(groups[key], allocation)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].namespace != keys[j].namespace {
			return keys[i].namespace < keys[j].namespace
		}
		if keys[i].job != keys[j].job {
			return keys[i].job < keys[j].job
		}
		return keys[i].group < keys[j].group
	})

	var pruned []*Allocation
	for _, key := range keys {
		terminal := append([]*Allocation(nil), groups[key]...)
		if len(terminal) <= retain {
			continue
		}
		sort.Slice(terminal, func(i, j int) bool {
			if !terminal[i].TransitionedAt.Equal(terminal[j].TransitionedAt) {
				return terminal[i].TransitionedAt.After(terminal[j].TransitionedAt)
			}
			return terminal[i].ID > terminal[j].ID
		})
		for _, allocation := range terminal[retain:] {
			if skip[allocation] {
				continue
			}
			pruned = append(pruned, allocation)
		}
	}
	return pruned
}

// ErrTaskGroupNotFound reports that a job or one of its task groups does not
// exist.
var ErrTaskGroupNotFound = errors.New("task group not found")

// ResetReplacementBackoff clears the replacement backoff of a job task group
// so the next reconciliation pass replaces its failed allocations without
// waiting. Failed allocations already counted stay seen and are never counted
// again. Resetting a group without counted failures is a no-op. The cleared
// record is committed through the state store, like every other backoff
// change, before the leader uses it.
func (s *Server) ResetReplacementBackoff(ctx context.Context, namespace, job, group string) error {
	s.mutationMu.Lock()
	s.mu.RLock()
	current := s.jobs[jobKey(namespace, job)]
	if current == nil || !slices.ContainsFunc(current.Spec.TaskGroups, func(g spec.TaskGroupSpec) bool { return g.Name == group }) {
		s.mu.RUnlock()
		s.mutationMu.Unlock()
		return fmt.Errorf("task group %s of job %s: %w", group, job, ErrTaskGroupNotFound)
	}
	key := replacementBackoffKey(current.Spec.Namespace, current.Spec.Name, group)
	previous := s.replacementBackoffs[key]
	s.mu.RUnlock()
	if previous == nil || previous.Failures == 0 {
		s.mutationMu.Unlock()
		return nil
	}
	next := previous.reset()
	if err := s.state.PutReplacementBackoff(ctx, next); err != nil {
		s.mutationMu.Unlock()
		return fmt.Errorf("persist replacement backoff reset: %w", err)
	}
	s.mu.Lock()
	backoffs := make(map[string]*ReplacementBackoff, len(s.replacementBackoffs))
	for k, backoff := range s.replacementBackoffs {
		backoffs[k] = backoff
	}
	backoffs[key] = next
	s.replacementBackoffs = backoffs
	s.mu.Unlock()
	s.mutationMu.Unlock()
	s.log.Info("reset task group replacement backoff", "namespace", next.Namespace, "job", next.JobName, "group", next.TaskGroupName, "failures", previous.Failures)
	s.events.publish(api.ClusterEvent{
		Type:      api.EventJobReplacementBackoffReset,
		Namespace: next.Namespace,
		JobName:   next.JobName,
		Group:     next.TaskGroupName,
		Revision:  next.JobRevision,
		At:        s.now().UTC(),
	})
	s.Reconcile(ctx)
	return nil
}

// replacementBackoffResponsesLocked returns the task groups of a job whose
// replacements are being delayed, sorted by group. The caller holds s.mu.
func (s *Server) replacementBackoffResponsesLocked(namespace, job string) []api.ReplacementBackoffResponse {
	var result []api.ReplacementBackoffResponse
	for _, backoff := range s.replacementBackoffs {
		if backoff.Namespace != namespace || backoff.JobName != job || backoff.Failures == 0 {
			continue
		}
		result = append(result, replacementBackoffResponse(backoff))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Group < result[j].Group })
	return result
}

func replacementBackoffResponse(backoff *ReplacementBackoff) api.ReplacementBackoffResponse {
	return api.ReplacementBackoffResponse{
		Group:             backoff.TaskGroupName,
		JobRevision:       backoff.JobRevision,
		Failures:          backoff.Failures,
		LastFailureAt:     backoff.LastFailureAt,
		LastAllocationID:  backoff.LastAllocationID,
		Reason:            backoff.Reason,
		Message:           backoff.Message,
		NextReplacementAt: backoff.NextReplacementAt,
	}
}
