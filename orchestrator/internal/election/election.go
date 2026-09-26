// Package election provides leader election abstractions.
package election

import (
	"context"

	"github.com/google/uuid"
)

// Leader identifies the elected cluster leader.
type Leader struct {
	NodeID  uuid.UUID `json:"node_id"`
	Address string    `json:"address"`
}

// Event reports a change in leadership status.
type Event struct {
	Leader  Leader
	Elected bool
}

// Elector observes and reports cluster leadership.
type Elector interface {
	Run(ctx context.Context, events chan<- Event) error
	Current(ctx context.Context) (*Leader, error)
	CurrentID() (uuid.UUID, error)
}

// SingleNodeElector always elects its sole node.
type SingleNodeElector struct {
	leader Leader
}

// NewSingleNodeElector creates an elector for a single-node cluster.
func NewSingleNodeElector(leader Leader) *SingleNodeElector {
	return &SingleNodeElector{leader: leader}
}

// Run reports election and waits until the context is canceled.
func (e *SingleNodeElector) Run(ctx context.Context, events chan<- Event) error {
	select {
	case events <- Event{Leader: e.leader, Elected: true}:
	case <-ctx.Done():
		return nil
	}
	<-ctx.Done()
	return nil
}

// Current returns the single node as leader.
func (e *SingleNodeElector) Current(_ context.Context) (*Leader, error) {
	return &e.leader, nil
}

// CurrentID returns the locally known leader identity.
func (e *SingleNodeElector) CurrentID() (uuid.UUID, error) {
	return e.leader.NodeID, nil
}
