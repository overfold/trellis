package election

import (
	"context"
	"strings"

	"github.com/hashicorp/raft"
)

var _ Elector = (*RaftElector)(nil)

// RaftElector reports leadership changes from a Raft instance.
type RaftElector struct {
	raft *raft.Raft
	self Leader
}

// NewRaftElector creates an elector backed by Raft.
func NewRaftElector(r *raft.Raft, self Leader) *RaftElector {
	return &RaftElector{raft: r, self: self}
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
func (e *RaftElector) Current(_ context.Context) (*Leader, error) {
	_, id := e.raft.LeaderWithID()
	if id == "" {
		return nil, nil
	}
	_, address, ok := strings.Cut(string(id), "@")
	if !ok || address == "" {
		return nil, nil
	}
	return &Leader{Address: address}, nil
}
