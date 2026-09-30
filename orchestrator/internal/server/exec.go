package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/overfold/trellis/orchestrator/internal/api"
	"github.com/overfold/trellis/orchestrator/internal/execstream"
)

const (
	// execRelayLimit bounds the exec streams one leader relays at a time.
	// Each holds two connections and two bounded frame buffers.
	execRelayLimit = 256
	// execRelayWriteTimeout bounds how long one relayed frame may wait for
	// the client to accept it, matching the agent's idle timeout.
	execRelayWriteTimeout = 30 * time.Minute
	// execRelayCloseTimeout bounds delivery of the final error frame.
	execRelayCloseTimeout = 5 * time.Second
)

var (
	errExecRelayLimit = errors.New("exec stream limit reached")
	errExecNoTerm     = errors.New("control-plane leader is not active")
)

// execRelays tracks the exec streams relayed during the current leadership
// term. The zero value refuses streams until a term starts.
type execRelays struct {
	mu     sync.Mutex
	term   context.Context
	active int
}

// startTerm makes ctx the term whose end closes every relayed stream.
func (r *execRelays) startTerm(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.term = ctx
}

// admit claims a relay slot in the current term.
func (r *execRelays) admit() (context.Context, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.term == nil || r.term.Err() != nil {
		return nil, errExecNoTerm
	}
	if r.active >= execRelayLimit {
		return nil, fmt.Errorf("%w: leader relays %d exec streams (maximum %d)", errExecRelayLimit, r.active, execRelayLimit)
	}
	r.active++
	return r.term, nil
}

func (r *execRelays) release() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.active--
}

// ExecStream is an open leader-to-agent exec stream awaiting its client.
type ExecStream struct {
	server  *Server
	term    context.Context
	agent   io.ReadWriteCloser
	release func()
}

// OpenExec resolves an allocation task in namespace and opens an exec stream
// to its node agent, fenced to the current leadership epoch. The stream
// stays open until ctx ends or it is closed.
func (s *Server) OpenExec(ctx context.Context, namespace, id string, request api.ExecRequest) (*ExecStream, error) {
	nodeID, address, tasks, err := s.allocationAgentAddress(namespace, id)
	if err != nil {
		return nil, err
	}
	request.Task, err = resolveExecTask(id, request.Task, tasks)
	if err != nil {
		return nil, err
	}
	s.mu.RLock()
	epoch := s.controlEpoch
	s.mu.RUnlock()
	if epoch == 0 {
		return nil, errExecNoTerm
	}
	term, err := s.exec.admit()
	if err != nil {
		return nil, err
	}
	agent, err := s.client.Exec(ctx, nodeID, address, id, api.AgentExecRequest{ExecRequest: request, Epoch: epoch})
	if err != nil {
		s.exec.release()
		return nil, err
	}
	return &ExecStream{server: s, term: term, agent: agent, release: sync.OnceFunc(s.exec.release)}, nil
}

// Close closes the agent side of the stream, which ends its process, and
// releases the relay slot.
func (e *ExecStream) Close() {
	_ = e.agent.Close()
	e.release()
}

// relayResult says why a relay ended.
type relayResult struct {
	// message, when set, is reported to the client in an error frame.
	message string
}

// Relay copies frames between client and agent until the process exits,
// either side disconnects, or the leadership term ends. Each direction
// holds one frame at a time, so a slow reader stalls its writer instead of
// growing a buffer. The client connection is closed on return.
func (e *ExecStream) Relay(client net.Conn) {
	defer func() { _ = client.Close() }()
	clientWriter := execstream.NewWriter(client, execRelayWriteTimeout)
	agentWriter := execstream.NewWriter(e.agent, 0)
	results := make(chan relayResult, 2)
	var wg sync.WaitGroup
	wg.Go(func() {
		reader := execstream.NewReader(e.agent)
		for {
			frame, err := reader.Next()
			if err != nil {
				results <- relayResult{message: "exec stream to the node agent was lost"}
				return
			}
			if !execstream.ServerFrame(frame.Type) {
				results <- relayResult{message: "node agent sent an invalid exec stream frame"}
				return
			}
			if err := clientWriter.WriteFrame(frame.Type, frame.Payload); err != nil {
				results <- relayResult{}
				return
			}
			if frame.Type == execstream.FrameExit || frame.Type == execstream.FrameError {
				results <- relayResult{}
				return
			}
		}
	})
	wg.Go(func() {
		reader := execstream.NewReader(client)
		for {
			frame, err := reader.Next()
			if err != nil {
				if errors.Is(err, execstream.ErrInvalidFrame) {
					results <- relayResult{message: err.Error()}
				} else {
					results <- relayResult{}
				}
				return
			}
			if !execstream.ClientFrame(frame.Type) {
				results <- relayResult{message: fmt.Sprintf("exec stream sent unexpected frame type %d", frame.Type)}
				return
			}
			if err := agentWriter.WriteFrame(frame.Type, frame.Payload); err != nil {
				results <- relayResult{message: "exec stream to the node agent was lost"}
				return
			}
		}
	})

	var result relayResult
	select {
	case result = <-results:
	case <-e.term.Done():
		result = relayResult{message: "exec session ended because control-plane leadership changed"}
	}
	// Closing the agent side ends the process if it still runs.
	_ = e.agent.Close()
	if result.message != "" {
		finished := make(chan struct{})
		go func() {
			defer close(finished)
			_ = clientWriter.WriteJSON(execstream.FrameError, api.ExecStreamError{Message: result.message})
		}()
		select {
		case <-finished:
		case <-time.After(execRelayCloseTimeout):
		}
	}
	_ = client.Close()
	wg.Wait()
}
