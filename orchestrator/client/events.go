package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/overfold/trellis/orchestrator/api"
)

// maxEventBytes bounds one server-sent event.
const maxEventBytes = 1 << 20

// Events streams cluster events for the client's namespace as they happen.
// The stream starts at the time of the call; it does not replay history.
func (c *Client) Events(ctx context.Context) (*EventStream, error) {
	path, err := c.namespacedPath("/events")
	if err != nil {
		return nil, fmt.Errorf("stream events: %w", err)
	}
	return c.events(ctx, path)
}

// ClusterEvents streams cluster events for every namespace. It requires
// cluster scope.
func (c *Client) ClusterEvents(ctx context.Context) (*EventStream, error) {
	return c.events(ctx, c.clusterPath("/v1/events"))
}

func (c *Client) events(ctx context.Context, target string) (*EventStream, error) {
	body, err := c.stream(ctx, target)
	if err != nil {
		return nil, fmt.Errorf("stream events: %w", err)
	}
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 4096), maxEventBytes)
	return &EventStream{body: body, scanner: scanner}, nil
}

// EventStream is an open stream of cluster events. It is not safe for
// concurrent use, except that Close may be called while Next blocks.
type EventStream struct {
	body    io.ReadCloser
	scanner *bufio.Scanner
}

// Next blocks until the next event arrives. It returns io.EOF when the
// server ends the stream.
func (s *EventStream) Next() (api.ClusterEvent, error) {
	var data []byte
	for s.scanner.Scan() {
		line := s.scanner.Bytes()
		if len(line) == 0 {
			if len(data) == 0 {
				continue
			}
			var event api.ClusterEvent
			if err := json.Unmarshal(data, &event); err != nil {
				return api.ClusterEvent{}, fmt.Errorf("decode event: %w", err)
			}
			return event, nil
		}
		if payload, ok := bytes.CutPrefix(line, []byte("data:")); ok {
			payload = bytes.TrimPrefix(payload, []byte(" "))
			if len(data)+len(payload)+1 > maxEventBytes {
				return api.ClusterEvent{}, fmt.Errorf("read events: event exceeds %d bytes", maxEventBytes)
			}
			if len(data) > 0 {
				data = append(data, '\n')
			}
			data = append(data, payload...)
		}
	}
	if err := s.scanner.Err(); err != nil {
		return api.ClusterEvent{}, fmt.Errorf("read events: %w", err)
	}
	if len(data) > 0 {
		return api.ClusterEvent{}, errors.New("read events: stream ended inside an event")
	}
	return api.ClusterEvent{}, io.EOF
}

// Close ends the stream.
func (s *EventStream) Close() error { return s.body.Close() }
