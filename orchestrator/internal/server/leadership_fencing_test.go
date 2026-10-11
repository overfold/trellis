package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/internal/auth"
	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
)

func TestStaleControlDecisionCannotOverrideDurableUndrain(t *testing.T) {
	s, node, allocation := executionFixture(t)
	allocation.Phase = lifecycle.PhaseRunning
	allocation.Draining, allocation.DrainReason, allocation.DrainSequence = true, "node", 1
	stale := Action{Type: ActionStop, Allocation: allocation, controlPlanned: true, drainSequence: 1}
	if err := s.UndrainNode(t.Context(), node.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.Execute(t.Context(), &stale); err != nil {
		t.Fatal(err)
	}
	doc, err := s.ExecutionIntent(t.Context(), node.ID, 0)
	if err != nil || doc.Allocations[0].Mode != nodeapi.ExecutionRun || doc.Allocations[0].Request.Draining || doc.Allocations[0].Request.DrainSequence != 2 {
		t.Fatalf("stale decision replaced undrain: %+v %v", doc, err)
	}
}

func TestOldTermOutcomeCannotRewriteReloadedAllocation(t *testing.T) {
	s, agent := newTestServerWithAgent()
	t.Cleanup(agent.server.Close)
	term, cancel := context.WithCancel(t.Context())
	s.term, s.controlEpoch = term, 1
	ctx, release := s.bindTerm(t.Context())
	defer release()
	old := &Allocation{ID: "a", Generation: 1, Phase: lifecycle.PhaseStarting}
	s.allocations = []*Allocation{old}
	cancel()
	fresh := &Allocation{ID: "a", Generation: 1, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy, DrainSequence: 2}
	s.allocations = []*Allocation{fresh}
	s.controlEpoch = 2
	s.term = t.Context()
	if err := s.state.PutAllocation(t.Context(), fresh); err != nil {
		t.Fatal(err)
	}
	err := s.persistAllocationUpdate(context.WithoutCancel(ctx), old, func(next *Allocation) error {
		return next.Transition(lifecycle.PhaseFailed, s.now(), "old_start_failure", "")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("old callback error = %v", err)
	}
	if err := s.Execute(ctx, &Action{Type: ActionStop, Allocation: old}); !errors.Is(err, context.Canceled) {
		t.Fatalf("old API action error = %v", err)
	}
	stored, err := s.state.ListAllocations(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if stored["a"].Phase != lifecycle.PhaseRunning || stored["a"].Health != lifecycle.HealthHealthy || stored["a"].DrainSequence != 2 || len(agent.recordedCalls()) != 0 {
		t.Fatalf("old term changed fresh state: %+v", stored["a"])
	}
	// Identity fencing also rejects a detached object within the same term.
	if err := s.persistAllocationUpdate(t.Context(), old, func(*Allocation) error { t.Fatal("detached callback ran"); return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("detached callback error = %v", err)
	}
}

func TestStaleStartCannotReviveLostAllocation(t *testing.T) {
	s, node, allocation := executionFixture(t)
	allocation.Phase = lifecycle.PhaseLost
	if err := s.Execute(t.Context(), &Action{Type: ActionStart, Allocation: allocation}); err != nil {
		t.Fatal(err)
	}
	doc, err := s.ExecutionIntent(t.Context(), node.ID, 0)
	if err != nil || doc.Allocations[0].Mode != nodeapi.ExecutionRetain || allocation.Phase != lifecycle.PhaseLost {
		t.Fatalf("lost allocation relaunched: %+v %v", doc, err)
	}
}

func TestTermShutdownJoinsAdmittedWork(t *testing.T) {
	store := memoryStore{}
	s := NewServer(slog.Default(), nil, NewStateController(store, "test"), store, "test", "")
	term, cancel := context.WithCancel(t.Context())
	done := s.Run(term)
	ctx, release := s.bindTerm(t.Context())
	cancel()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("API context survived term loss")
	}
	select {
	case <-done:
		t.Fatal("term finished before admitted API work")
	case <-time.After(20 * time.Millisecond):
	}
	release()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("term did not join")
	}
}

func TestHeartbeatReceiptCannotCrossTermReset(t *testing.T) {
	s, agent := newTestServerWithAgent()
	t.Cleanup(agent.server.Close)
	node := &Node{ID: uuid.New()}
	addTestNode(s, node, time.Time{})
	s.controlEpoch = 1
	entered, proceed := make(chan struct{}), make(chan struct{})
	var once sync.Once
	s.now = func() time.Time { once.Do(func() { close(entered); <-proceed }); return time.Unix(10, 0) }
	result := make(chan error, 1)
	go func() { result <- s.Heartbeat(t.Context(), node.ID, nil, "old", nil, nil, nodeResourceObservation{}) }()
	<-entered
	s.termMu.Lock()
	s.controlEpoch = 2
	s.liveness.startTerm()
	s.observations.reset()
	s.termMu.Unlock()
	close(proceed)
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("old heartbeat error = %v", err)
	}
	if s.observations.len() != 0 || !s.liveness.lastHeartbeat(node.ID).IsZero() {
		t.Fatal("old heartbeat was relabeled into the new term")
	}
}

func TestObservationQueueKeepsNewestReceipt(t *testing.T) {
	var queue observationQueue
	id := uuid.New()
	fresh := &nodeObservation{node: id, at: time.Unix(20, 0), version: "new"}
	queue.submit(fresh)
	queue.submit(&nodeObservation{node: id, at: time.Unix(10, 0), version: "old"})
	batch, _ := queue.take()
	if len(batch) != 1 || batch[0] != fresh {
		t.Fatalf("pending reports = %+v", batch)
	}
}

func TestHeartbeatTermFencingDoesNotWaitForPlanningLock(t *testing.T) {
	s, agent := newTestServerWithAgent()
	t.Cleanup(agent.server.Close)
	node := &Node{ID: uuid.New()}
	addTestNode(s, node, time.Time{})
	s.mu.Lock()
	result := make(chan error, 1)
	go func() { result <- s.Heartbeat(t.Context(), node.ID, nil, "new", nil, nil, nodeResourceObservation{}) }()
	select {
	case err := <-result:
		s.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		s.mu.Unlock()
		<-result
		t.Fatal("term fencing made heartbeat wait for reconciliation planning")
	}
}

func TestEventStreamEndsWithLeadershipTerm(t *testing.T) {
	for _, proxied := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "follower proxy"}[proxied], func(t *testing.T) {
			s, agent := newTestServerWithAgent()
			t.Cleanup(agent.server.Close)
			s.events = newEventBus()
			term, cancel := context.WithCancel(t.Context())
			defer cancel()
			s.term = term
			leader := httptest.NewServer(authenticatedHandler(s, auth.AccessRead))
			t.Cleanup(leader.Close)
			address := leader.URL
			if proxied {
				target, err := url.Parse(address)
				if err != nil {
					t.Fatal(err)
				}
				follower := httptest.NewServer(httputil.NewSingleHostReverseProxy(target))
				t.Cleanup(follower.Close)
				address = follower.URL
			}
			request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, address+"/v1/events", nil)
			if err != nil {
				t.Fatal(err)
			}
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = response.Body.Close() }()
			if response.StatusCode != http.StatusOK {
				t.Fatalf("status = %d", response.StatusCode)
			}
			cancel()
			ended := make(chan error, 1)
			go func() { _, err := io.ReadAll(response.Body); ended <- err }()
			select {
			case err := <-ended:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("former leader kept SSE open")
			}
			waitForSubscriberCount(t, s.events, 0)
		})
	}
}

func TestOldTermRaftProgressCannotCrossReset(t *testing.T) {
	s, agent := newTestServerWithAgent()
	t.Cleanup(agent.server.Close)
	s.joiner = newFakeMembership()
	s.controlEpoch = 1
	node := &Node{ID: uuid.New()}
	addTestNode(s, node, time.Time{})
	ctx, release := s.bindTerm(t.Context())
	defer release()
	s.controlEpoch = 2
	s.liveness.startTerm()
	s.RecordRaftProgress(ctx, node.ID, 1000)
	if _, reported := s.liveness.raftProgress(node.ID); reported {
		t.Fatal("old progress was relabeled into the new term")
	}
	s.RecordRaftProgress(t.Context(), node.ID, 900)
	if progress, reported := s.liveness.raftProgress(node.ID); !reported || progress.applied != 900 {
		t.Fatalf("fresh progress = %+v, reported=%t", progress, reported)
	}
}

type signaledRequestBody struct {
	io.ReadCloser
	started chan struct{}
	once    sync.Once
}

func (b *signaledRequestBody) Read(p []byte) (int, error) {
	b.once.Do(func() { close(b.started) })
	return b.ReadCloser.Read(p)
}

func TestTermLossUnblocksReadingAPIRequestBody(t *testing.T) {
	s, agent := newTestServerWithAgent()
	t.Cleanup(agent.server.Close)
	term, cancel := context.WithCancel(t.Context())
	defer cancel()
	s.term = term
	started := make(chan struct{})
	handler := authenticatedHandler(s, auth.AccessWrite)
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = &signaledRequestBody{ReadCloser: r.Body, started: started}
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(leader.Close)
	reader, writer := io.Pipe()
	t.Cleanup(func() { _ = writer.Close() })
	ctx, cancelRequest := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancelRequest()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, leader.URL+"/v1/namespaces/default/jobs", reader)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.ContentLength = 100
	ended := make(chan error, 1)
	go func() {
		response, err := http.DefaultClient.Do(request)
		if err == nil {
			_ = response.Body.Close()
		}
		ended <- err
	}()
	if _, err := writer.Write([]byte("{")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("API did not start reading")
	}
	cancel()
	select {
	case <-ended:
	case <-time.After(time.Second):
		t.Fatal("API body read survived leadership loss")
	}
}
