package election

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"
)

var _ Elector = (*RaftElector)(nil)

// RaftElector reports leadership changes from a Raft instance.
type RaftElector struct {
	raft    *raft.Raft
	self    Leader
	resolve func(context.Context, string) (string, error)
}

// NewRaftElector creates an elector backed by Raft.
func NewRaftElector(r *raft.Raft, self Leader, resolve func(context.Context, string) (string, error)) *RaftElector {
	return &RaftElector{raft: r, self: self, resolve: resolve}
}

// Run forwards Raft leadership changes until the context is canceled.
func (e *RaftElector) Run(ctx context.Context, events chan<- Event) error {
	ch := e.raft.LeaderCh()
	for {
		select {
		case isLeader := <-ch:
			events <- Event{Leader: e.self, Elected: isLeader}
		case <-ctx.Done():
			return nil
		}
	}
}

// Current returns the current Raft leader, if known.
func (e *RaftElector) Current(ctx context.Context) (*Leader, error) {
	nodeID, err := e.CurrentID()
	if err != nil || nodeID == uuid.Nil {
		return nil, err
	}
	if nodeID == e.self.NodeID {
		leader := e.self
		return &leader, nil
	}
	if e.resolve == nil {
		return nil, fmt.Errorf("no control-plane address resolver configured")
	}
	address, err := e.resolve(ctx, nodeID.String())
	if err != nil {
		return nil, err
	}
	return &Leader{NodeID: nodeID, Address: address}, nil
}

// CurrentID returns the leader identity directly from local Raft state.
func (e *RaftElector) CurrentID() (uuid.UUID, error) {
	_, id := e.raft.LeaderWithID()
	if id == "" {
		return uuid.Nil, nil
	}
	nodeID, err := uuid.Parse(string(id))
	if err != nil {
		return uuid.Nil, fmt.Errorf("invalid Raft leader node ID %q: %w", id, err)
	}
	return nodeID, nil
}
