package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/internal/client"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
)

func TestReconcileActionsDoNotQueueBehindStalledAgentBody(t *testing.T) {
	stalledStarted := make(chan struct{})
	stalled := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"code":"ok"`))
		response.(http.Flusher).Flush()
		close(stalledStarted)
		<-request.Context().Done()
	}))
	t.Cleanup(stalled.Close)

	fastCalled := make(chan struct{})
	fast := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		close(fastCalled)
		response.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(response).Encode(nodeapi.OperationResponse{Code: "ok"})
	}))
	t.Cleanup(fast.Close)

	nodeAt := func(server *httptest.Server) *Node {
		host, portValue, err := net.SplitHostPort(server.Listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		port, err := strconv.Atoi(portValue)
		if err != nil {
			t.Fatal(err)
		}
		return &Node{ID: uuid.New(), Host: "http://" + host, Port: port, Status: NodeStatusHealthy}
	}
	stalledNode, fastNode := nodeAt(stalled), nodeAt(fast)
	s := NewServer(slog.Default(), nil, NewStateController(memoryStore{}, "test"), memoryStore{}, "test", "")
	s.client = client.NewAgentClient("", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() {
		<-s.dispatchReconcileActions(ctx, []Action{
			{Type: ActionStopObserved, Node: stalledNode, ID: "stalled", Generation: 1},
			{Type: ActionStopObserved, Node: fastNode, ID: "fast", Generation: 1},
		}, false)
		close(done)
	}()

	select {
	case <-stalledStarted:
	case <-time.After(time.Second):
		t.Fatal("stalled agent request did not start")
	}
	select {
	case <-fastCalled:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("healthy agent action queued behind stalled response body")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("action batch did not stop at its context deadline")
	}
}

func TestReconcileActionsPreserveOrderWithinNode(t *testing.T) {
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondStarted := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/v1/allocations/first":
			close(firstStarted)
			select {
			case <-releaseFirst:
			case <-request.Context().Done():
			}
		case "/v1/allocations/second":
			close(secondStarted)
		}
		response.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(response).Encode(nodeapi.OperationResponse{Code: "ok"})
	}))
	t.Cleanup(server.Close)
	host, portValue, err := net.SplitHostPort(server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portValue)
	if err != nil {
		t.Fatal(err)
	}
	node := &Node{ID: uuid.New(), Host: "http://" + host, Port: port, Status: NodeStatusHealthy}
	s := NewServer(slog.Default(), nil, NewStateController(memoryStore{}, "test"), memoryStore{}, "test", "")
	s.client = client.NewAgentClient("", nil)
	done := make(chan struct{})
	go func() {
		<-s.dispatchReconcileActions(context.Background(), []Action{
			{Type: ActionStopObserved, Node: node, ID: "first", Generation: 1},
			{Type: ActionStopObserved, Node: node, ID: "second", Generation: 1},
		}, false)
		close(done)
	}()
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first action did not start")
	}
	select {
	case <-secondStarted:
		t.Fatal("second action started before the first completed")
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseFirst)
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("second action did not start after the first completed")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("ordered node action batch did not complete")
	}
}

func TestReconcilePassesDoNotWaitForBusyNode(t *testing.T) {
	stalledCalls := make(chan string, 4)
	releaseStalled := make(chan struct{})
	stalled := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		stalledCalls <- request.URL.Path
		select {
		case <-releaseStalled:
		case <-request.Context().Done():
		}
		response.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(response).Encode(nodeapi.OperationResponse{Code: "ok"})
	}))
	t.Cleanup(stalled.Close)
	fastCalls := make(chan string, 4)
	fast := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		fastCalls <- request.URL.Path
		response.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(response).Encode(nodeapi.OperationResponse{Code: "ok"})
	}))
	t.Cleanup(fast.Close)
	nodeAt := func(server *httptest.Server) *Node {
		host, portValue, err := net.SplitHostPort(server.Listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		port, err := strconv.Atoi(portValue)
		if err != nil {
			t.Fatal(err)
		}
		return &Node{ID: uuid.New(), Host: "http://" + host, Port: port, Status: NodeStatusHealthy}
	}
	stalledNode, fastNode := nodeAt(stalled), nodeAt(fast)
	s := NewServer(slog.Default(), nil, NewStateController(memoryStore{}, "test"), memoryStore{}, "test", "")
	s.client = client.NewAgentClient("", nil)

	first := s.dispatchReconcileActions(context.Background(), []Action{{Type: ActionStopObserved, Node: stalledNode, ID: "first", Generation: 1}}, false)
	select {
	case <-stalledCalls:
	case <-time.After(time.Second):
		t.Fatal("stalled action did not start")
	}
	// A later pass runs other nodes' actions at once and defers the busy
	// node's, which the pass after it plans again.
	second := s.dispatchReconcileActions(context.Background(), []Action{
		{Type: ActionStopObserved, Node: stalledNode, ID: "second", Generation: 1},
		{Type: ActionStopObserved, Node: fastNode, ID: "fast", Generation: 1},
	}, false)
	select {
	case <-second:
	case <-time.After(time.Second):
		t.Fatal("pass waited for a node busy with an earlier pass")
	}
	select {
	case path := <-fastCalls:
		if path != "/v1/allocations/fast" {
			t.Fatalf("fast node call = %s", path)
		}
	default:
		t.Fatal("fast node action did not run")
	}
	select {
	case path := <-stalledCalls:
		t.Fatalf("busy node received a second concurrent action %s", path)
	default:
	}
	close(releaseStalled)
	select {
	case <-first:
	case <-time.After(time.Second):
		t.Fatal("stalled pass did not finish after release")
	}
	third := s.dispatchReconcileActions(context.Background(), []Action{{Type: ActionStopObserved, Node: stalledNode, ID: "second", Generation: 1}}, false)
	<-third
	if path := <-stalledCalls; path != "/v1/allocations/second" {
		t.Fatalf("released node call = %s, want deferred action", path)
	}
}

func TestQueueingPassWaitsForBusyNode(t *testing.T) {
	calls := make(chan string, 4)
	release := make(chan struct{})
	agent := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		calls <- request.URL.Path
		if request.URL.Path == "/v1/allocations/first" {
			select {
			case <-release:
			case <-request.Context().Done():
			}
		}
		response.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(response).Encode(nodeapi.OperationResponse{Code: "ok"})
	}))
	t.Cleanup(agent.Close)
	host, portValue, err := net.SplitHostPort(agent.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portValue)
	if err != nil {
		t.Fatal(err)
	}
	node := &Node{ID: uuid.New(), Host: "http://" + host, Port: port, Status: NodeStatusHealthy}
	s := NewServer(slog.Default(), nil, NewStateController(memoryStore{}, "test"), memoryStore{}, "test", "")
	s.client = client.NewAgentClient("", nil)

	periodic := s.dispatchReconcileActions(context.Background(), []Action{{Type: ActionStopObserved, Node: node, ID: "first", Generation: 1}}, false)
	if path := <-calls; path != "/v1/allocations/first" {
		t.Fatalf("first call = %s", path)
	}
	// A pass an API mutation waits for delivers its action once the node
	// is free rather than dropping it.
	queued := s.dispatchReconcileActions(context.Background(), []Action{{Type: ActionStopObserved, Node: node, ID: "second", Generation: 1}}, true)
	select {
	case path := <-calls:
		t.Fatalf("queued action %s ran while the node was busy", path)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	<-periodic
	select {
	case <-queued:
	case <-time.After(time.Second):
		t.Fatal("queued pass did not finish")
	}
	if path := <-calls; path != "/v1/allocations/second" {
		t.Fatalf("queued call = %s, want second", path)
	}
}
