package server

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	"github.com/overfold/trellis/orchestrator/internal/spec"
	"github.com/prometheus/client_golang/prometheus"
)

func TestReconcileDoesNotMutateStoredJobSpec(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy}
	addTestNode(s, node, s.now())
	jobSpec := &spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "api", Count: 1, Tasks: []spec.TaskSpec{{Name: "server", Image: "app"}}}}}
	s.jobs[jobKey("default", "web")] = &Job{Spec: canonicalTestSpec(jobSpec), Revision: 1}
	before, err := json.Marshal(jobSpec)
	if err != nil {
		t.Fatal(err)
	}

	s.Reconcile(context.Background())

	if after, err := json.Marshal(jobSpec); err != nil || string(after) != string(before) {
		t.Fatalf("reconciliation changed the stored job spec:\nbefore %s\nafter  %s", before, after)
	}
	if len(s.allocations) != 1 || s.allocations[0].Tasks[0].Resources == nil {
		t.Fatalf("allocations = %d, want one placed with canonical resources", len(s.allocations))
	}
}

func abortedPasses(t *testing.T, registry *prometheus.Registry, reason string) float64 {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "trellis_reconcile_aborted_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "reason" && label.GetValue() == reason {
					return metric.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

func newAbortFixture(t *testing.T) (*Server, *bytes.Buffer, *prometheus.Registry, *Node, *Allocation) {
	t.Helper()
	s, agent := newTestServerWithAgent()
	t.Cleanup(agent.server.Close)
	logs := &bytes.Buffer{}
	s.log = slog.New(slog.NewTextHandler(logs, nil))
	registry := prometheus.NewRegistry()
	RegisterMetrics(s, registry)
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy}
	addTestNode(s, node, s.now())
	jobSpec := &spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "api", Count: 1, Tasks: []spec.TaskSpec{{Name: "server", Image: "app"}}}}}
	s.jobs[jobKey("default", "web")] = &Job{Spec: canonicalTestSpec(jobSpec), Revision: 1}
	pending := &Allocation{ID: "pending", Namespace: "default", JobName: "web", TaskGroupName: "api", Tasks: jobSpec.TaskGroups[0].Tasks, Generation: 1, JobRevision: 1, Phase: lifecycle.PhasePending, Health: lifecycle.HealthUnknown}
	s.allocations = []*Allocation{pending}
	return s, logs, registry, node, pending
}

func planAbortFixture(t *testing.T, s *Server) (*reconcilePlan, map[*Allocation]*Allocation) {
	t.Helper()
	input, originals := s.reconcilePlanInputLocked(s.now(), s.liveness.heartbeats(), nil, nil)
	plan, err := planReconciliation(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Updated) != 1 || plan.Updated[0].Phase != lifecycle.PhasePlaced {
		t.Fatalf("setup: plan updates = %d, want the pending allocation placed", len(plan.Updated))
	}
	return plan, originals
}

func TestReconcileAbortsWhenPlannedAllocationChanged(t *testing.T) {
	s, logs, registry, _, pending := newAbortFixture(t)
	s.mu.Lock()
	plan, originals := planAbortFixture(t, s)
	pending.mu.Lock()
	pending.Generation++
	pending.mu.Unlock()
	_, _, ok := s.lockReconcileTargetsLocked(plan, originals)
	s.mu.Unlock()

	if ok {
		t.Fatal("reconciliation committed a plan made from a stale allocation")
	}
	if !pending.mu.TryLock() {
		t.Fatal("aborted pass left the allocation locked")
	}
	pending.mu.Unlock()
	if got := abortedPasses(t, registry, "allocation_changed"); got != 1 {
		t.Fatalf("aborted passes = %v, want 1", got)
	}
	if !strings.Contains(logs.String(), "abort reconciliation") || !strings.Contains(logs.String(), "allocation=pending") {
		t.Fatalf("abort was not logged: %s", logs.String())
	}
}

func TestReconcileAbortsWhenPlannedNodeRemoved(t *testing.T) {
	s, logs, registry, node, _ := newAbortFixture(t)
	s.mu.Lock()
	plan, originals := planAbortFixture(t, s)
	delete(s.nodes, node.ID)
	_, _, ok := s.lockReconcileTargetsLocked(plan, originals)
	s.mu.Unlock()

	if ok {
		t.Fatal("reconciliation committed a placement on a removed node")
	}
	if got := abortedPasses(t, registry, "node_removed"); got != 1 {
		t.Fatalf("aborted passes = %v, want 1", got)
	}
	if !strings.Contains(logs.String(), "reason=node_removed") {
		t.Fatalf("abort was not logged: %s", logs.String())
	}
}

func TestStopAllocationByIDWaitsForNodeActionsAndReplaces(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy}
	addTestNode(s, node, s.now())
	jobSpec := &spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "api", Count: 1, Tasks: []spec.TaskSpec{{Name: "server", Image: "app"}}}}}
	s.jobs[jobKey("default", "web")] = &Job{Spec: canonicalTestSpec(jobSpec), Revision: 1}
	original := &Allocation{ID: "original", Namespace: "default", JobName: "web", TaskGroupName: "api", Tasks: jobSpec.TaskGroups[0].Tasks, Node: node, Generation: 1, JobRevision: 1, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy}
	s.allocations = []*Allocation{original}

	if _, busy := s.claimActionNode(node.ID); busy != nil {
		t.Fatal("setup: node already busy")
	}
	done := make(chan error, 1)
	go func() { done <- s.StopAllocationByID(context.Background(), "default", "original") }()
	select {
	case err := <-done:
		s.releaseActionNode(node.ID)
		t.Fatalf("stop completed while a pass's actions held the node: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if calls := agent.recordedCalls(); len(calls) != 0 {
		s.releaseActionNode(node.ID)
		t.Fatalf("agent called while a pass's actions held the node: %#v", calls)
	}
	s.releaseActionNode(node.ID)
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	if original.Phase != lifecycle.PhaseStopped {
		t.Fatalf("original phase = %s, want stopped", original.Phase)
	}
	if len(s.allocations) != 2 || s.allocations[1].Phase != lifecycle.PhaseStarting {
		t.Fatalf("allocations = %d, want the stop followed by a replacement", len(s.allocations))
	}
}

func TestRestartJobReconcilesImmediately(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy}
	addTestNode(s, node, s.now())
	jobSpec := &spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "api", Count: 1, Tasks: []spec.TaskSpec{{Name: "server", Image: "app"}}}}}
	s.jobs[jobKey("default", "web")] = &Job{Spec: canonicalTestSpec(jobSpec), Revision: 1}
	original := &Allocation{ID: "original", Namespace: "default", JobName: "web", TaskGroupName: "api", Tasks: jobSpec.TaskGroups[0].Tasks, Node: node, Generation: 1, JobRevision: 1, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy}
	s.allocations = []*Allocation{original}

	if err := s.RestartJob(context.Background(), "default", "web"); err != nil {
		t.Fatal(err)
	}

	if !original.Draining || original.DrainReason != "restart" || original.DrainSequence != 1 {
		t.Fatalf("restart intent = draining %t reason %q sequence %d", original.Draining, original.DrainReason, original.DrainSequence)
	}
	drained := false
	for _, call := range agent.recordedCalls() {
		if call.method == http.MethodPost && call.path == "/v1/allocations/original/drain" {
			drained = true
		}
	}
	if !drained || len(s.allocations) != 2 {
		t.Fatalf("restart did not reconcile: drained=%t allocations=%d", drained, len(s.allocations))
	}
	persisted, err := s.state.ListAllocations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !persisted["original"].Draining || persisted["original"].DrainReason != "restart" {
		t.Fatalf("persisted restart intent = %#v", persisted["original"])
	}
}
