package server

import (
	"sync"
	"time"

	"github.com/google/uuid"
)

// livenessWindow is how long the leader keeps treating a node as live after
// its latest heartbeat.
const livenessWindow = 3 * heartbeatInterval

// nodeLiveness is the leader's record of node heartbeats. It is a renewable
// observation held only in the leader's memory and never replicated: a
// heartbeat stamps it on receipt, before any other work, and only its own
// mutex, a leaf lock, guards it. Reconciliation and Raft commits therefore
// never delay a stamp.
//
// The record starts empty in every leadership term. A new leader learns
// liveness from heartbeats only, so a node counts as live once it has
// heartbeated to this leader within livenessWindow. Silence, which decides
// allocation and voter loss, is measured from no earlier than the start of the
// term (see silentSince), which gives every node a grace period after a
// leadership change.
type nodeLiveness struct {
	mu sync.Mutex
	// nodes holds one record per registered node. A heartbeat from a node
	// without a record is rejected so the agent re-registers.
	nodes map[uuid.UUID]*livenessRecord
}

type livenessRecord struct {
	// heartbeat is when this leader last received a heartbeat or registration
	// from the node; zero until the node reports in the current term.
	heartbeat time.Time
	// raft is the node's latest reported Raft progress.
	raft    raftProgress
	hasRaft bool
	// registration counts the node's registrations with this server. A
	// heartbeat report carries the count it was stamped under, so a report
	// from before a re-registration is never applied after it.
	registration uint64
}

// track makes the registered node set exactly the given nodes, as reloaded
// from replicated state. Nodes already tracked keep their records, so a
// restore within a term does not forget recent heartbeats.
func (l *nodeLiveness) track(nodes []uuid.UUID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	records := make(map[uuid.UUID]*livenessRecord, len(nodes))
	for _, id := range nodes {
		record := l.nodes[id]
		if record == nil {
			record = &livenessRecord{}
		}
		records[id] = record
	}
	l.nodes = records
}

// startTerm forgets every heartbeat and Raft progress report received in an
// earlier leadership term, keeping the registered node set.
func (l *nodeLiveness) startTerm() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for id, record := range l.nodes {
		l.nodes[id] = &livenessRecord{registration: record.registration}
	}
}

// register tracks a newly registered node. Registration is evidence of life,
// so it also stamps the node.
func (l *nodeLiveness) register(id uuid.UUID, at time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.nodes == nil {
		l.nodes = make(map[uuid.UUID]*livenessRecord)
	}
	record := l.nodes[id]
	if record == nil {
		record = &livenessRecord{}
		l.nodes[id] = record
	}
	record.registration++
	if at.After(record.heartbeat) {
		record.heartbeat = at
	}
}

// stamp records a heartbeat received at the given time. It reports whether
// the node is registered and the registration the heartbeat belongs to.
func (l *nodeLiveness) stamp(id uuid.UUID, at time.Time) (uint64, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	record := l.nodes[id]
	if record == nil {
		return 0, false
	}
	if at.After(record.heartbeat) {
		record.heartbeat = at
	}
	return record.registration, true
}

// currentRegistration returns a node's registration count and whether the
// node is registered.
func (l *nodeLiveness) currentRegistration(id uuid.UUID) (uint64, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if record := l.nodes[id]; record != nil {
		return record.registration, true
	}
	return 0, false
}

// recordRaftProgress keeps a registered node's latest Raft progress report.
func (l *nodeLiveness) recordRaftProgress(id uuid.UUID, progress raftProgress) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if record := l.nodes[id]; record != nil {
		record.raft, record.hasRaft = progress, true
	}
}

// forgetRaftProgress drops a node's Raft progress, as when it leaves Raft.
func (l *nodeLiveness) forgetRaftProgress(id uuid.UUID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if record := l.nodes[id]; record != nil {
		record.raft, record.hasRaft = raftProgress{}, false
	}
}

// heartbeats copies the latest heartbeat time of every node that has
// reported in this term.
func (l *nodeLiveness) heartbeats() map[uuid.UUID]time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	result := make(map[uuid.UUID]time.Time, len(l.nodes))
	for id, record := range l.nodes {
		if !record.heartbeat.IsZero() {
			result[id] = record.heartbeat
		}
	}
	return result
}

// lastHeartbeat returns a node's latest heartbeat time in this term.
func (l *nodeLiveness) lastHeartbeat(id uuid.UUID) time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	if record := l.nodes[id]; record != nil {
		return record.heartbeat
	}
	return time.Time{}
}

// raftProgress returns a node's latest Raft progress report in this term.
func (l *nodeLiveness) raftProgress(id uuid.UUID) (raftProgress, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if record := l.nodes[id]; record != nil && record.hasRaft {
		return record.raft, true
	}
	return raftProgress{}, false
}

// heartbeatLive reports whether a heartbeat received at the given time still
// shows the node live.
func heartbeatLive(heartbeat, now time.Time) bool {
	return !heartbeat.IsZero() && now.Sub(heartbeat) <= livenessWindow
}

// silentSince returns when the leader last had evidence that a node was
// alive: its latest heartbeat in this leadership term, or the start of the
// term when the node has not heartbeated to this leader yet. Heartbeat times
// are leader observations and are not replicated, so a new leader measures
// allocation and voter loss from the start of its own term.
func silentSince(heartbeat, termStart time.Time) time.Time {
	if heartbeat.Before(termStart) {
		return termStart
	}
	return heartbeat
}

// livenessStatus derives a node's status from its durable drain intent and
// its liveness: a draining node stays draining, and any other node is healthy
// only while its heartbeats show it live.
func livenessStatus(status NodeStatus, heartbeat, now time.Time) NodeStatus {
	if status == NodeStatusDraining {
		return NodeStatusDraining
	}
	if heartbeatLive(heartbeat, now) {
		return NodeStatusHealthy
	}
	return NodeStatusUnhealthy
}
