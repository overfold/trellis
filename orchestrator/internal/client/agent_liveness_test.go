package client

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/internal/api"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type closeTrackingTransport struct {
	closes atomic.Int32
}

func (t *closeTrackingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("unexpected request")
}

func (t *closeTrackingTransport) CloseIdleConnections() {
	t.closes.Add(1)
}

func TestAgentOperationDeadlineCoversStalledResponse(t *testing.T) {
	tests := []struct {
		name    string
		handler func(<-chan struct{}) http.HandlerFunc
	}{
		{
			name: "headers",
			handler: func(release <-chan struct{}) http.HandlerFunc {
				return func(_ http.ResponseWriter, request *http.Request) {
					select {
					case <-request.Context().Done():
					case <-release:
					}
				}
			},
		},
		{
			name: "body",
			handler: func(release <-chan struct{}) http.HandlerFunc {
				return func(response http.ResponseWriter, request *http.Request) {
					response.Header().Set("Content-Type", "application/json")
					_, _ = response.Write([]byte(`{"code":"ok"`))
					response.(http.Flusher).Flush()
					select {
					case <-request.Context().Done():
					case <-release:
					}
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			release := make(chan struct{})
			server := httptest.NewServer(test.handler(release))
			t.Cleanup(server.Close)
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			err := NewAgentClient("", nil).RunAllocation(ctx, uuid.New(), server.URL, &api.AllocationRequest{})
			close(release)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("RunAllocation error = %v, want context deadline exceeded", err)
			}
		})
	}
}

func TestAgentOperationAddsDeadlineWithoutBoundingLogStream(t *testing.T) {
	nodeID := uuid.New()
	operationSawDeadline := false
	operationTransport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		deadline, ok := request.Context().Deadline()
		operationSawDeadline = ok && time.Until(deadline) > agentOperationTimeout-time.Second && time.Until(deadline) <= agentOperationTimeout
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"code":"ok"}`)),
		}, nil
	})
	streamSawDeadline := true
	streamTransport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		_, streamSawDeadline = request.Context().Deadline()
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("line\n")),
		}, nil
	})
	agent := NewAgentClient("", nil)
	agent.clients[nodeID] = &client{client: &http.Client{Transport: operationTransport}}
	if err := agent.RunAllocation(context.Background(), nodeID, "agent.invalid", &api.AllocationRequest{}); err != nil {
		t.Fatalf("RunAllocation: %v", err)
	}
	if !operationSawDeadline {
		t.Fatalf("operation request did not receive an approximately %s deadline", agentOperationTimeout)
	}
	agent.clients[nodeID] = &client{client: &http.Client{Transport: streamTransport}}
	body, err := agent.TaskLogs(context.Background(), nodeID, "agent.invalid", "alloc", "task", true, 0)
	if err != nil {
		t.Fatalf("TaskLogs: %v", err)
	}
	defer func() { _ = body.Close() }()
	if streamSawDeadline {
		t.Fatal("streaming log request received an implicit operation deadline")
	}
}

func TestAgentClientRetainNodesEvictsBothTransportCaches(t *testing.T) {
	keep, remove := uuid.New(), uuid.New()
	agent := NewAgentClient("", nil)
	regularKeep, regularRemove := &closeTrackingTransport{}, &closeTrackingTransport{}
	planKeep, planRemove := &closeTrackingTransport{}, &closeTrackingTransport{}
	agent.clients[keep] = &client{client: &http.Client{Transport: regularKeep}}
	agent.clients[remove] = &client{client: &http.Client{Transport: regularRemove}}
	agent.networkPlanClients[keep] = &client{client: &http.Client{Transport: planKeep}}
	agent.networkPlanClients[remove] = &client{client: &http.Client{Transport: planRemove}}

	agent.RetainNodes(map[uuid.UUID]struct{}{keep: {}})

	if len(agent.clients) != 1 || agent.clients[keep] == nil || len(agent.networkPlanClients) != 1 || agent.networkPlanClients[keep] == nil {
		t.Fatalf("retained caches = regular %v, network plans %v", agent.clients, agent.networkPlanClients)
	}
	if regularRemove.closes.Load() != 1 || planRemove.closes.Load() != 1 {
		t.Fatalf("removed transport closes = regular %d, network plans %d; want 1 each", regularRemove.closes.Load(), planRemove.closes.Load())
	}
	if regularKeep.closes.Load() != 0 || planKeep.closes.Load() != 0 {
		t.Fatal("retained node transports were closed")
	}
}
