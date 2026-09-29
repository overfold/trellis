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
	"github.com/overfold/trellis/internal/api"
	"github.com/overfold/trellis/internal/client"
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
		_ = json.NewEncoder(response).Encode(api.OperationResponse{Code: "ok"})
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
		s.executeReconcileActions(ctx, []Action{
			{Type: ActionStopObserved, Node: stalledNode, ID: "stalled", Generation: 1},
			{Type: ActionStopObserved, Node: fastNode, ID: "fast", Generation: 1},
		})
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
		_ = json.NewEncoder(response).Encode(api.OperationResponse{Code: "ok"})
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
		s.executeReconcileActions(context.Background(), []Action{
			{Type: ActionStopObserved, Node: node, ID: "first", Generation: 1},
			{Type: ActionStopObserved, Node: node, ID: "second", Generation: 1},
		})
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
