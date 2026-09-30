package server

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/internal/api"
)

// addTestNode places node in s and records a heartbeat from it received at
// the given time. A zero time registers the node without a heartbeat in the
// current term.
func addTestNode(s *Server, node *Node, heartbeat time.Time) {
	if s.nodes == nil {
		s.nodes = make(map[uuid.UUID]*Node)
	}
	s.nodes[node.ID] = node
	setTestHeartbeat(s, node.ID, heartbeat)
}

// setTestHeartbeat sets the latest heartbeat the leader received from a
// node, registering the node's liveness record if needed.
func setTestHeartbeat(s *Server, id uuid.UUID, heartbeat time.Time) {
	s.liveness.mu.Lock()
	defer s.liveness.mu.Unlock()
	if s.liveness.nodes == nil {
		s.liveness.nodes = make(map[uuid.UUID]*livenessRecord)
	}
	record := s.liveness.nodes[id]
	if record == nil {
		record = &livenessRecord{}
		s.liveness.nodes[id] = record
	}
	record.heartbeat = heartbeat
}

// heartbeatAndApply delivers a heartbeat and applies the report it queued,
// as the observation applier would.
func heartbeatAndApply(t testing.TB, s *Server, nodeID uuid.UUID, actual []api.AllocationStatus, version string, resources nodeResourceObservation) error {
	t.Helper()
	if err := s.Heartbeat(context.Background(), nodeID, actual, version, nil, nil, resources); err != nil {
		return err
	}
	s.applyObservations(context.Background())
	return nil
}
