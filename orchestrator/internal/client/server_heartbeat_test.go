package client

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
)

func TestSendHeartbeatCarriesFailureReason(t *testing.T) {
	var raw map[string]json.RawMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
			t.Error(err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"leader_id":"00000000-0000-0000-0000-000000000000","addresses":[]}`))
	}))
	defer server.Close()

	nodeID := uuid.New()
	client := NewServerClient("token", server.URL, nil)
	err := client.SendHeartbeat(context.Background(), nodeID, &Heartbeat{NodeID: nodeID, Allocations: []nodeapi.AllocationStatus{
		{ID: "failed", Generation: 1, Phase: lifecycle.PhaseFailed, Health: lifecycle.HealthUnhealthy, Reason: nodeapi.OperationRestartExhausted},
		{ID: "running", Generation: 1, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy},
	}})
	if err != nil {
		t.Fatal(err)
	}
	var allocations []map[string]any
	if err := json.Unmarshal(raw["allocations"], &allocations); err != nil {
		t.Fatal(err)
	}
	if len(allocations) != 2 || allocations[0]["reason"] != "restart_budget_exhausted" {
		t.Fatalf("allocations = %v, want the failure reason on the wire", allocations)
	}
	if _, ok := allocations[1]["reason"]; ok {
		t.Fatalf("running allocation carries a reason: %v", allocations[1])
	}
}

type stalledHeartbeatBody struct {
	ctx    context.Context
	closed atomic.Bool
}

func (b *stalledHeartbeatBody) Read([]byte) (int, error) {
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}

func (b *stalledHeartbeatBody) Close() error {
	b.closed.Store(true)
	return nil
}

func TestHeartbeatBodyDeadlineRetryAndShutdown(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(map[bool]string{false: "timeout", true: "shutdown"}[shutdown], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client := NewServerClient("", "http://control-plane", nil)
				entered := make(chan *stalledHeartbeatBody, 1)
				var calls atomic.Int32
				client.client.HTTP.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
					body := io.NopCloser(strings.NewReader(`{"addresses":[]}`))
					if calls.Add(1) == 1 {
						stalled := &stalledHeartbeatBody{ctx: req.Context()}
						body = stalled
						entered <- stalled
					}
					return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: body}, nil
				})
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- client.SendHeartbeat(ctx, uuid.Nil, &Heartbeat{}) }()
				body := <-entered
				if shutdown {
					cancel()
				}
				err := <-done
				want := context.DeadlineExceeded
				if shutdown {
					want = context.Canceled
				}
				if !errors.Is(err, want) || !body.closed.Load() || body.ctx.Err() == nil {
					t.Fatalf("heartbeat error=%v closed=%v context=%v", err, body.closed.Load(), body.ctx.Err())
				}
				if err := client.SendHeartbeat(context.Background(), uuid.Nil, &Heartbeat{}); err != nil {
					t.Fatalf("fresh heartbeat retry: %v", err)
				}
				if calls.Load() != 2 {
					t.Fatalf("requests = %d", calls.Load())
				}
			})
		})
	}
}

func TestHeartbeatDeadlineClosesStalledHTTPConnection(t *testing.T) {
	cancelled := make(chan struct{})
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Content-Length", "100")
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			close(cancelled)
			return
		}
		_, _ = w.Write([]byte(`{"addresses":[]}`))
	}))
	defer server.Close()
	client := NewServerClient("", server.URL, nil)
	if client.client.HTTP.Timeout != 10*time.Second {
		t.Fatal("production request budget changed")
	}
	client.client.HTTP.Timeout = 100 * time.Millisecond
	if err := client.SendHeartbeat(context.Background(), uuid.Nil, &Heartbeat{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stalled body error = %v", err)
	}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("timed-out request left the connection open")
	}
	if err := client.SendHeartbeat(context.Background(), uuid.Nil, &Heartbeat{}); err != nil {
		t.Fatal(err)
	}
}
