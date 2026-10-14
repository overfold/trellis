package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
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
	before, _ := json.Marshal(jobSpec)

	s.Reconcile(context.Background())

	if after, _ := json.Marshal(jobSpec); string(after) != string(before) {
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

func TestRestartJobReconcilesImmediately(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy, CPUAllocatable: 1000, MemoryAllocatable: 1 << 30}
	addTestNode(s, node, s.now())
	jobSpec := &spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "api", Count: 1, Tasks: []spec.TaskSpec{{Name: "server", Image: "app", Resources: &spec.ResourcesSpec{CPU: 1000, Memory: 64 << 20}}}}}}
	s.jobs[jobKey("default", "web")] = &Job{Spec: canonicalTestSpec(jobSpec), Revision: 1}
	original := &Allocation{ID: "original", Namespace: "default", JobName: "web", TaskGroupName: "api", Tasks: jobSpec.TaskGroups[0].Tasks, Node: node, Generation: 7, JobRevision: 1, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy}
	s.allocations = []*Allocation{original}

	if err := s.RestartJob(context.Background(), "default", "web"); err != nil {
		t.Fatal(err)
	}

	if !original.Draining || original.DrainReason != "restart" || original.DrainSequence != 1 {
		t.Fatalf("restart intent = draining %t reason %q sequence %d", original.Draining, original.DrainReason, original.DrainSequence)
	}
	if original.Phase != lifecycle.PhaseStopping || original.CleanupComplete || original.Generation != 7 || len(s.allocations) != 1 {
		t.Fatal("unproved recreate restart cleanup did not preserve fenced old execution and block replacement")
	}
	entry := committedAllocationIntent(t, s, node.ID, original.ID, 7)
	if entry.Mode != nodeapi.ExecutionCleanup {
		t.Fatal("restart cleanup not published")
	}
	s.Reconcile(t.Context())
	if len(s.allocations) != 1 || original.CleanupComplete {
		t.Fatal("unproved cleanup admitted restart")
	}
	acknowledgeTestCleanup(t, s, original)
	s.Reconcile(context.Background())
	if len(s.allocations) != 2 || s.allocations[1].Phase != lifecycle.PhaseStarting || original.Generation != 7 {
		t.Fatal("restart did not place after successful stop")
	}
	persisted, err := s.state.ListAllocations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !persisted["original"].Draining || persisted["original"].DrainReason != "restart" {
		t.Fatalf("persisted restart intent = %#v", persisted["original"])
	}
}

// A diagnostic is emitted after scheduling but before commit. Pausing its
// evaluation exercises a real API mutation at that boundary, without a
// production scheduling hook.
type pausedPlanDiagnostic struct {
	slog.Handler
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (p *pausedPlanDiagnostic) Handle(ctx context.Context, record slog.Record) error {
	p.once.Do(func() {
		close(p.started)
		<-p.release
	})
	return p.Handler.Handle(ctx, record)
}

func TestReconcileReplansWhenNodeRegistrationChangesDuringPlanning(t *testing.T) {
	s, _, _, node, _ := newAbortFixture(t)
	invalid := planTestJob("invalid", 1, 1, spec.UpdateRecreate)
	invalid.Spec.TaskGroups[0].Count = 0
	s.jobs[jobKey("default", "invalid")] = invalid
	pause := &pausedPlanDiagnostic{Handler: slog.NewTextHandler(io.Discard, nil), started: make(chan struct{}), release: make(chan struct{})}
	s.log = slog.New(pause)
	var release sync.Once
	unblock := func() { release.Do(func() { close(pause.release) }) }
	t.Cleanup(unblock)
	done := make(chan struct{})
	go func() {
		s.Reconcile(t.Context())
		close(done)
	}()
	select {
	case <-pause.started:
	case <-time.After(2 * time.Second):
		t.Fatal("reconciliation did not reach its planning diagnostic")
	}
	registered := make(chan error, 1)
	go func() {
		registered <- s.RegisterNode(t.Context(), &NodeRegistration{ID: node.ID, Host: "127.0.0.1", Port: 8127, RunsWorkloads: false})
	}()
	select {
	case err := <-registered:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("node registration blocked on scheduling")
	}
	unblock()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("reconciliation did not replan")
	}
	allocations, err := s.state.ListAllocations(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(allocations) != 1 || allocations["pending"] == nil {
		t.Fatalf("replanned allocations = %+v, want the original pending allocation", allocations)
	}
	for _, allocation := range allocations {
		if allocation.Node != nil || allocation.Phase != lifecycle.PhasePending {
			t.Fatalf("stale placement committed after disabling workloads: %+v", allocation)
		}
	}
}
