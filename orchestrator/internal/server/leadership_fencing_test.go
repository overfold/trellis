package server

import (
	"context"
	"errors"
	"fmt"
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
	"github.com/overfold/trellis/orchestrator/internal/spec"
)

func TestQueuedControlActionsCannotOverrideDurableResume(t *testing.T) {
	for _, kind := range []ActionType{ActionDrain, ActionStop} {
		t.Run(string(kind), func(t *testing.T) {
			s, agent := newTestServerWithAgent()
			t.Cleanup(agent.server.Close)
			node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusDraining}
			addTestNode(s, node, s.now())
			s.jobs[jobKey("default", "web")] = &Job{Revision: 1, Spec: canonicalTestSpec(&spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "app", Count: 1, Tasks: []spec.TaskSpec{{Name: "app", Image: "app"}}}}})}
			allocation := &Allocation{ID: "a", Namespace: "default", JobName: "web", TaskGroupName: "app", JobRevision: 1, Generation: 1, Node: node, Phase: lifecycle.PhaseRunning, Draining: true, DrainReason: "node", DrainSequence: 1}
			s.allocations = []*Allocation{allocation}
			_, busy := s.claimActionNode(node.ID)
			if busy != nil {
				t.Fatal("unexpected busy node")
			}
			done := s.dispatchReconcileActions(t.Context(), []Action{{Type: kind, Allocation: allocation, controlPlanned: true, drainSequence: 1}}, true)
			resumes, err := s.resumeNodeAllocations(t.Context(), node.ID)
			if err != nil {
				t.Fatal(err)
			}
			s.deliverResumes(t.Context(), node.ID, resumes)
			s.releaseActionNode(node.ID)
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("queued action did not finish")
			}
			calls := agent.recordedCalls()
			if len(calls) != 1 || calls[0].path != "/v1/allocations/a/drain" || calls[0].method != http.MethodDelete {
				t.Fatalf("agent calls = %+v; want only the acknowledged resume", calls)
			}
			stored, err := s.state.ListAllocations(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if stored["a"].Phase != lifecycle.PhaseRunning || stored["a"].Draining || stored["a"].DrainSequence != 2 {
				t.Fatalf("resume overwritten: %+v", stored["a"])
			}
		})
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

func TestQueuedStartCannotReviveLostAllocation(t *testing.T) {
	for _, lost := range []bool{true, false} {
		t.Run(fmt.Sprint("lost=", lost), func(t *testing.T) {
			s, agent := newTestServerWithAgent()
			t.Cleanup(agent.server.Close)
			node := planTestNode(1, NodeStatusHealthy)
			node.Host, node.Port = agent.host, agent.port
			addTestNode(s, node, s.now())
			job := planTestJob("web", 1, 1, spec.UpdateRecreate)
			job.Spec.TaskGroups[0].Tasks[0].Networking = &spec.TaskNetworkingSpec{Mode: spec.TaskNetworkHost}
			s.jobs[jobKey("default", "web")] = job
			original := planTestAllocation("original", node, lifecycle.PhasePlaced, 1)
			s.allocations = []*Allocation{original}
			_, busy := s.claimActionNode(node.ID)
			if busy != nil {
				t.Fatal("unexpected busy node")
			}
			done := s.dispatchReconcileActions(t.Context(), []Action{{Type: ActionStart, Allocation: original, controlPlanned: true}}, true)
			phase := lifecycle.PhaseRunning
			if lost {
				phase = lifecycle.PhaseLost
				workerAgent := newTestAgent()
				t.Cleanup(workerAgent.server.Close)
				worker := planTestNode(2, NodeStatusHealthy)
				worker.Host, worker.Port = workerAgent.host, workerAgent.port
				addTestNode(s, worker, s.now())
				s.leaderSince = s.now().Add(-time.Hour)
				setTestHeartbeat(s, node.ID, s.now().Add(-2*DefaultAllocationLossTimeout))
				waitSignal(t, s.reconcile(t.Context(), false), "durable loss and replacement")
				if original.Phase != lifecycle.PhaseLost || len(workerAgent.recordedCalls()) != 1 {
					t.Fatal("loss pass did not replace original on independent node")
				}
				if err := heartbeatAndApply(t, s, node.ID, nil, "test", nodeResourceObservation{}); err != nil {
					t.Fatal(err)
				}
				s.mu.Lock()
				node.Status = NodeStatusHealthy
				s.mu.Unlock()
			} else {
				if err := s.persistAllocationUpdate(t.Context(), original, func(next *Allocation) error {
					if err := next.Transition(lifecycle.PhaseStarting, s.now(), "", ""); err != nil {
						return err
					}
					return next.Transition(phase, s.now(), "", "")
				}); err != nil {
					t.Fatal(err)
				}
			}
			s.releaseActionNode(node.ID)
			waitSignal(t, done, "queued start")
			stored, err := s.state.ListAllocations(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			wantCalls := 1
			if lost {
				wantCalls = 0
			}
			if len(agent.recordedCalls()) != wantCalls || stored[original.ID].Phase != phase {
				t.Fatalf("calls=%d phase=%s; want calls=%d phase=%s", len(agent.recordedCalls()), stored[original.ID].Phase, wantCalls, phase)
			}
		})
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

func TestExecuteFailureCallbackCannotCrossLeadershipChange(t *testing.T) {
	s, agent := newTestServerWithAgent()
	t.Cleanup(agent.server.Close)
	entered, respond := make(chan struct{}), make(chan struct{})
	agent.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-respond
		http.Error(w, "start failed", http.StatusServiceUnavailable)
	})
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy}
	s.nodes[node.ID] = node
	task := spec.TaskSpec{Name: "app", Image: "app"}
	s.jobs[jobKey("default", "web")] = &Job{Revision: 1, Spec: canonicalTestSpec(&spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "app", Tasks: []spec.TaskSpec{task}}}})}
	old := &Allocation{ID: "a", Namespace: "default", JobName: "web", TaskGroupName: "app", Generation: 1, JobRevision: 1, Tasks: []spec.TaskSpec{task}, Node: node, Phase: lifecycle.PhasePlaced}
	s.allocations = []*Allocation{old}
	term, cancel := context.WithCancel(t.Context())
	defer cancel()
	s.term, s.controlEpoch = term, 1
	result := make(chan error, 1)
	go func() { result <- s.Execute(t.Context(), &Action{Type: ActionStart, Allocation: old}) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("start was not sent")
	}
	// Hold mutation ordering so the actual failure callback can only run
	// after loss and replacement of its canonical allocation object.
	s.mutationMu.Lock()
	cancel()
	close(respond)
	s.termMu.Lock()
	s.mu.Lock()
	fresh := old.cloneRecord()
	fresh.Phase, fresh.Health, fresh.DrainSequence = lifecycle.PhaseRunning, lifecycle.HealthHealthy, 2
	s.allocations = []*Allocation{fresh}
	s.controlEpoch, s.term = 2, t.Context()
	s.mu.Unlock()
	s.termMu.Unlock()
	err := s.state.PutAllocation(t.Context(), fresh)
	s.mutationMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("old Execute error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("old Execute did not finish")
	}
	stored, err := s.state.ListAllocations(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if stored["a"].Phase != lifecycle.PhaseRunning || stored["a"].Health != lifecycle.HealthHealthy || stored["a"].DrainSequence != 2 || stored["a"].Attempt != 0 {
		t.Fatalf("failure callback overwrote fresh state: %+v", stored["a"])
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
