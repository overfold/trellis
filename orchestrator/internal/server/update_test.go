package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/lifecycle"
	"github.com/clofour/trellis/internal/spec"
	"github.com/google/uuid"
)

func newTestServerWithAgent() (*Server, *testAgent) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	agent := newTestAgent()
	s := &Server{
		log:     slog.Default(),
		state:   newNopStateController(),
		client:  newTestAgentClient(),
		nodes:   make(map[uuid.UUID]*Node),
		jobs:    make(map[string]*Job),
		now:     func() time.Time { return now },
		catalog: newNopCatalog(),
	}
	return s, agent
}

func TestReconcileDoesNotCreateAllocationsForInvalidJob(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	limits := spec.DefaultLimits()
	limits.MaxReplicasPerTaskGroup = 1
	limits.MaxTaskGroupsPerJob = 1
	limits.MaxTasksPerTaskGroup = 1
	limits.MaxDesiredAllocations = 1
	if err := s.SetJobLimits(limits); err != nil {
		t.Fatal(err)
	}
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy, LastHeartbeat: s.now()}
	s.nodes[node.ID] = node
	s.jobs[jobKey("default", "oversized")] = &Job{Spec: &spec.JobSpec{Namespace: "default", Name: "oversized", TaskGroups: []spec.TaskGroupSpec{{Name: "api", Count: 2, Tasks: []spec.TaskSpec{{Name: "server", Image: "app"}}}}}, Revision: 1}

	s.Reconcile(context.Background())
	if len(s.allocations) != 0 {
		t.Fatalf("invalid job created allocations: %#v", s.allocations)
	}
}

func TestNamespaceDesiredAllocationLimitIncludesOtherJobs(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	limits := spec.DefaultLimits()
	limits.MaxDesiredAllocationsPerNamespace = 2
	if err := s.SetJobLimits(limits); err != nil {
		t.Fatal(err)
	}
	s.jobs[jobKey("default", "first")] = &Job{Spec: &spec.JobSpec{Namespace: "default", Name: "first", TaskGroups: []spec.TaskGroupSpec{{Name: "api", Count: 2, Tasks: []spec.TaskSpec{{Name: "app", Image: "app"}}}}}}
	candidate := &spec.JobSpec{Namespace: "default", Name: "second", TaskGroups: []spec.TaskGroupSpec{{Name: "worker", Count: 1, Tasks: []spec.TaskSpec{{Name: "worker", Image: "worker"}}}}}
	if err := s.CanonicalizeJob(candidate); err != nil {
		t.Fatal(err)
	}
	if err := s.ValidateNamespaceAllocationLimit("default", candidate); err == nil {
		t.Fatal("expected namespace desired-allocation limit rejection")
	}
}

func TestReconcileEnforcesNamespaceDesiredAllocationLimit(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	limits := spec.DefaultLimits()
	limits.MaxDesiredAllocationsPerNamespace = 2
	if err := s.SetJobLimits(limits); err != nil {
		t.Fatal(err)
	}
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy, LastHeartbeat: s.now()}
	s.nodes[node.ID] = node
	for _, name := range []string{"first", "second", "third"} {
		s.jobs[jobKey("default", name)] = &Job{Spec: &spec.JobSpec{Namespace: "default", Name: name, TaskGroups: []spec.TaskGroupSpec{{Name: "app", Count: 1, Tasks: []spec.TaskSpec{{Name: "app", Image: "app"}}}}}, Revision: 1}
	}
	s.allocations = append(s.allocations, &Allocation{ID: "third-existing", Namespace: "default", JobName: "third", TaskGroupName: "app", Tasks: s.jobs[jobKey("default", "third")].Spec.TaskGroups[0].Tasks, Node: node, Generation: 1, JobRevision: 1, Phase: lifecycle.PhasePlaced, Health: lifecycle.HealthUnknown, Diagnostic: lifecycle.Diagnostic{CreatedAt: s.now(), TransitionedAt: s.now()}})

	s.Reconcile(context.Background())
	if len(s.allocations) != 3 {
		t.Fatalf("allocations = %d, want two admitted plus one stopped", len(s.allocations))
	}
	for _, allocation := range s.allocations {
		if allocation.JobName == "third" {
			if allocation.Phase != lifecycle.PhaseStopped {
				t.Fatalf("excluded allocation phase = %s, want stopped", allocation.Phase)
			}
		}
	}
}

func TestReconcileRetiresSurplusAndRemovedPendingAllocations(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	s.jobs[jobKey("default", "web")] = &Job{Spec: &spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "api", Count: 1, Tasks: []spec.TaskSpec{{Name: "server", Image: "app"}}}}}, Revision: 2}
	for _, group := range []string{"api", "api", "api", "removed"} {
		s.allocations = append(s.allocations, &Allocation{ID: group + string(rune('a'+len(s.allocations))), Namespace: "default", JobName: "web", TaskGroupName: group, Generation: 1, JobRevision: 1, Phase: lifecycle.PhasePending})
	}

	s.Reconcile(context.Background())

	persisted, err := s.state.ListAllocations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	active := 0
	for _, allocation := range s.allocations {
		if allocation.Phase == lifecycle.PhasePending {
			active++
		} else if allocation.Phase != lifecycle.PhaseStopped || persisted[allocation.ID] == nil || persisted[allocation.ID].Phase != lifecycle.PhaseStopped {
			t.Errorf("allocation %s was not retired and persisted: phase=%s", allocation.ID, allocation.Phase)
		}
	}
	if active != 1 {
		t.Fatalf("pending allocations = %d, want 1", active)
	}
}

func TestReconcileRetiresSurplusDrainingPendingAllocation(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	jobSpec := &spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "api", Count: 2, Tasks: []spec.TaskSpec{{Name: "server", Image: "app"}}}}}
	s.jobs[jobKey("default", "web")] = &Job{Spec: jobSpec, Revision: 1}
	first := &Allocation{ID: "pending-first", Namespace: "default", JobName: "web", TaskGroupName: "api", Generation: 1, JobRevision: 1, Phase: lifecycle.PhasePending}
	surplus := &Allocation{ID: "pending-surplus", Namespace: "default", JobName: "web", TaskGroupName: "api", Generation: 1, JobRevision: 1, Phase: lifecycle.PhasePending}
	s.allocations = []*Allocation{first, surplus}
	if err := s.RestartJob(context.Background(), "default", "web"); err != nil {
		t.Fatal(err)
	}
	if !surplus.Draining || surplus.DrainReason != "restart" {
		t.Fatalf("restart did not mark pending allocation draining: draining=%t reason=%q", surplus.Draining, surplus.DrainReason)
	}

	jobSpec.TaskGroups[0].Count = 1
	s.Reconcile(context.Background())

	if first.Phase != lifecycle.PhasePending || surplus.Phase != lifecycle.PhaseStopped || surplus.Reason != "scale_down" {
		t.Fatalf("pending allocations after scale-down: first=%s surplus=%s reason=%q", first.Phase, surplus.Phase, surplus.Reason)
	}
	persisted, err := s.state.ListAllocations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if persisted[surplus.ID] == nil || persisted[surplus.ID].Phase != lifecycle.PhaseStopped {
		t.Fatalf("surplus pending allocation was not persisted as stopped: %#v", persisted[surplus.ID])
	}
}

func TestReconcileRefreshesPendingAllocationBeforePlacement(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	oldTasks := []spec.TaskSpec{{Name: "server", Image: "app:v1"}}
	newTasks := []spec.TaskSpec{{Name: "server", Image: "app:v2"}}
	s.jobs[jobKey("default", "web")] = &Job{Spec: &spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "api", Count: 1, Runtime: spec.RuntimeRunsc, Tasks: newTasks}}}, Revision: 2}
	allocation := &Allocation{ID: "pending", Namespace: "default", JobName: "web", TaskGroupName: "api", Tasks: oldTasks, Generation: 1, JobRevision: 1, Phase: lifecycle.PhasePending}
	s.allocations = []*Allocation{allocation}
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy, LastHeartbeat: s.now(), Capabilities: []spec.NodeCapability{spec.CapabilityRunsc}}
	s.nodes[node.ID] = node

	s.Reconcile(context.Background())

	if allocation.JobRevision != 2 || allocation.Tasks[0].Image != "app:v2" {
		t.Fatalf("placed allocation retained stale manifest: revision=%d tasks=%#v", allocation.JobRevision, allocation.Tasks)
	}
	calls := agent.recordedCalls()
	if len(calls) != 1 {
		t.Fatalf("agent calls = %d, want 1", len(calls))
	}
	var request api.AllocationRequest
	if err := json.Unmarshal(calls[0].body, &request); err != nil {
		t.Fatal(err)
	}
	if request.JobRevision != 2 || len(request.Tasks) != 1 || request.Tasks[0].Image != "app:v2" {
		t.Fatalf("agent received stale manifest: revision=%d tasks=%#v", request.JobRevision, request.Tasks)
	}
}

func TestReconcileRecreateStopsOldAllocations(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy, LastHeartbeat: s.now()}
	s.nodes[node.ID] = node
	s.leaderSince = s.now().Add(-time.Minute)

	jobSpec := &spec.JobSpec{
		Namespace: "default", Name: "web",
		TaskGroups: []spec.TaskGroupSpec{{
			Name: "api", Count: 2,
			Tasks: []spec.TaskSpec{{Name: "server", Image: "app:v1"}},
		}},
	}
	hashes := map[string]string{"api": spec.TaskGroupContentHash(&jobSpec.TaskGroups[0])}
	s.jobs[jobKey("default", "web")] = &Job{Spec: jobSpec, Revision: 1, ContentHashes: hashes}

	now := s.now()
	for i := 0; i < 2; i++ {
		a := &Allocation{
			ID:        "alloc-" + string(rune('a'+i)),
			Namespace: "default", JobName: "web", TaskGroupName: "api",
			Tasks: jobSpec.TaskGroups[0].Tasks, Node: node, Generation: 1,
			JobRevision: 1, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy,
			Diagnostic: lifecycle.Diagnostic{CreatedAt: now, TransitionedAt: now}}
		s.allocations = append(s.allocations, a)
	}

	// Bump revision with default (recreate) strategy.
	newSpec := &spec.JobSpec{
		Namespace: "default", Name: "web",
		TaskGroups: []spec.TaskGroupSpec{{
			Name: "api", Count: 2,
			Tasks: []spec.TaskSpec{{Name: "server", Image: "app:v2"}},
		}},
	}
	newHashes := map[string]string{"api": spec.TaskGroupContentHash(&newSpec.TaskGroups[0])}
	s.jobs[jobKey("default", "web")] = &Job{Spec: newSpec, Revision: 2, ContentHashes: newHashes}

	s.Reconcile(context.Background())

	for _, a := range s.allocations {
		a.mu.Lock()
		if a.JobRevision == 1 && a.Phase != lifecycle.PhaseStopped && a.Phase != lifecycle.PhaseStopping {
			t.Errorf("old allocation %s still in phase %s", a.ID, a.Phase)
		}
		a.mu.Unlock()
	}
}

func TestReconcileRollingDrainsOldAllocations(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy, LastHeartbeat: s.now()}
	s.nodes[node.ID] = node
	s.leaderSince = s.now().Add(-time.Minute)

	jobSpec := &spec.JobSpec{
		Namespace: "default", Name: "web",
		TaskGroups: []spec.TaskGroupSpec{{
			Name: "api", Count: 2,
			Update: &spec.UpdateSpec{Strategy: spec.UpdateRolling, MaxParallel: 1},
			Tasks:  []spec.TaskSpec{{Name: "server", Image: "app:v1"}},
		}},
	}
	hashes := map[string]string{"api": spec.TaskGroupContentHash(&jobSpec.TaskGroups[0])}
	s.jobs[jobKey("default", "web")] = &Job{Spec: jobSpec, Revision: 1, ContentHashes: hashes}

	now := s.now()
	for i := 0; i < 2; i++ {
		a := &Allocation{
			ID:        "alloc-" + string(rune('a'+i)),
			Namespace: "default", JobName: "web", TaskGroupName: "api",
			Tasks: jobSpec.TaskGroups[0].Tasks, Node: node, Generation: 1,
			JobRevision: 1, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy,
			Diagnostic: lifecycle.Diagnostic{CreatedAt: now, TransitionedAt: now}}
		s.allocations = append(s.allocations, a)
	}

	// Bump revision with rolling strategy.
	newSpec := &spec.JobSpec{
		Namespace: "default", Name: "web",
		TaskGroups: []spec.TaskGroupSpec{{
			Name: "api", Count: 2,
			Update: &spec.UpdateSpec{Strategy: spec.UpdateRolling, MaxParallel: 1},
			Tasks:  []spec.TaskSpec{{Name: "server", Image: "app:v2"}},
		}},
	}
	newHashes := map[string]string{"api": spec.TaskGroupContentHash(&newSpec.TaskGroups[0])}
	s.jobs[jobKey("default", "web")] = &Job{Spec: newSpec, Revision: 2, ContentHashes: newHashes}

	s.Reconcile(context.Background())

	var drainingCount, newCount int
	for _, a := range s.allocations {
		a.mu.Lock()
		if a.JobRevision == 1 {
			if !a.Draining {
				t.Errorf("old allocation %s not marked as draining", a.ID)
			}
			if a.Phase == lifecycle.PhaseStopped || a.Phase == lifecycle.PhaseStopping {
				t.Errorf("old allocation %s was stopped immediately under rolling strategy", a.ID)
			}
			drainingCount++
		}
		if a.JobRevision == 2 {
			newCount++
		}
		a.mu.Unlock()
	}
	if drainingCount != 2 {
		t.Errorf("expected 2 draining allocations, got %d", drainingCount)
	}
	if newCount != 1 {
		t.Errorf("expected 1 new allocation (max_parallel=1), got %d", newCount)
	}
	drainCalls := 0
	for _, call := range agent.recordedCalls() {
		if strings.HasSuffix(call.path, "/drain") {
			drainCalls++
		}
	}
	if drainCalls != 2 {
		t.Errorf("drain-control calls = %d, want 2", drainCalls)
	}
}

func TestReconcileRollingStopsDrainingAsNewBecomeHealthy(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy, LastHeartbeat: s.now()}
	s.nodes[node.ID] = node
	s.leaderSince = s.now().Add(-time.Minute)

	newSpec := &spec.JobSpec{
		Namespace: "default", Name: "web",
		TaskGroups: []spec.TaskGroupSpec{{
			Name: "api", Count: 2,
			Update: &spec.UpdateSpec{Strategy: spec.UpdateRolling, MaxParallel: 1},
			Tasks:  []spec.TaskSpec{{Name: "server", Image: "app:v2"}},
		}},
	}
	newHashes := map[string]string{"api": spec.TaskGroupContentHash(&newSpec.TaskGroups[0])}
	s.jobs[jobKey("default", "web")] = &Job{Spec: newSpec, Revision: 2, ContentHashes: newHashes}

	now := s.now()
	makeDraining := func(id string) *Allocation {
		a := &Allocation{
			ID:        id,
			Namespace: "default", JobName: "web", TaskGroupName: "api",
			Tasks: []spec.TaskSpec{{Name: "server", Image: "app:v1"}}, Node: node, Generation: 1,
			JobRevision: 1, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy, Draining: true,
			Diagnostic: lifecycle.Diagnostic{CreatedAt: now, TransitionedAt: now}}
		return a
	}
	drainingA := makeDraining("alloc-old-a")
	drainingB := makeDraining("alloc-old-b")
	healthy := &Allocation{
		ID:        "alloc-new",
		Namespace: "default", JobName: "web", TaskGroupName: "api",
		Tasks: newSpec.TaskGroups[0].Tasks, Node: node, Generation: 1,
		JobRevision: 2, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy,
		Diagnostic: lifecycle.Diagnostic{CreatedAt: now, TransitionedAt: now}}
	s.allocations = []*Allocation{drainingA, drainingB, healthy}

	s.Reconcile(context.Background())

	stopped := 0
	for _, draining := range []*Allocation{drainingA, drainingB} {
		draining.mu.Lock()
		phase := draining.Phase
		draining.mu.Unlock()
		if phase == lifecycle.PhaseStopped {
			stopped++
		}
	}
	if stopped != 1 {
		t.Errorf("expected exactly one draining allocation to stop for one unconsumed healthy replacement, got %d", stopped)
	}
}

func TestReconcileRollingDoesNotStopDrainingUntilNewHealthy(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy, LastHeartbeat: s.now()}
	s.nodes[node.ID] = node
	s.leaderSince = s.now().Add(-time.Minute)

	newSpec := &spec.JobSpec{
		Namespace: "default", Name: "web",
		TaskGroups: []spec.TaskGroupSpec{{
			Name: "api", Count: 1,
			Update: &spec.UpdateSpec{Strategy: spec.UpdateRolling, MaxParallel: 1},
			Tasks:  []spec.TaskSpec{{Name: "server", Image: "app:v2"}},
		}},
	}
	newHashes := map[string]string{"api": spec.TaskGroupContentHash(&newSpec.TaskGroups[0])}
	s.jobs[jobKey("default", "web")] = &Job{Spec: newSpec, Revision: 2, ContentHashes: newHashes}

	now := s.now()
	draining := &Allocation{
		ID:        "alloc-old",
		Namespace: "default", JobName: "web", TaskGroupName: "api",
		Tasks: []spec.TaskSpec{{Name: "server", Image: "app:v1"}}, Node: node, Generation: 1,
		JobRevision: 1, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy, Draining: true,
		Diagnostic: lifecycle.Diagnostic{CreatedAt: now, TransitionedAt: now}}
	// New allocation is still starting, not yet healthy.
	starting := &Allocation{
		ID:        "alloc-new",
		Namespace: "default", JobName: "web", TaskGroupName: "api",
		Tasks: newSpec.TaskGroups[0].Tasks, Node: node, Generation: 1,
		JobRevision: 2, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthUnknown,
		Diagnostic: lifecycle.Diagnostic{CreatedAt: now, TransitionedAt: now}}
	s.allocations = []*Allocation{draining, starting}

	s.Reconcile(context.Background())

	draining.mu.Lock()
	phase := draining.Phase
	draining.mu.Unlock()
	if phase == lifecycle.PhaseStopped || phase == lifecycle.PhaseStopping {
		t.Error("draining allocation was stopped before new allocation became healthy")
	}
}

func TestLabelOnlyRevisionSkipsDrain(t *testing.T) {
	s, _ := newTestServerWithAgent()

	jobSpec := &spec.JobSpec{
		Namespace: "default", Name: "web",
		TaskGroups: []spec.TaskGroupSpec{{
			Name: "api", Count: 1,
			Labels: map[string]string{"weight": "100"},
			Tasks:  []spec.TaskSpec{{Name: "server", Image: "app:v1"}},
		}},
	}
	if err := s.RegisterJob(context.Background(), "default", jobSpec); err != nil {
		t.Fatal(err)
	}
	job := s.jobs[jobKey("default", "web")]
	if job.Revision != 1 {
		t.Fatalf("expected revision 1, got %d", job.Revision)
	}

	// Change only labels.
	updatedSpec := &spec.JobSpec{
		Namespace: "default", Name: "web",
		TaskGroups: []spec.TaskGroupSpec{{
			Name: "api", Count: 1,
			Labels: map[string]string{"weight": "0"},
			Tasks:  []spec.TaskSpec{{Name: "server", Image: "app:v1"}},
		}},
	}
	if err := s.RegisterJob(context.Background(), "default", updatedSpec); err != nil {
		t.Fatal(err)
	}
	job = s.jobs[jobKey("default", "web")]
	if job.Revision != 1 {
		t.Errorf("expected revision to stay at 1 for label-only change, got %d", job.Revision)
	}
}

func TestTaskChangeBumpsRevision(t *testing.T) {
	s, _ := newTestServerWithAgent()

	jobSpec := &spec.JobSpec{
		Namespace: "default", Name: "web",
		TaskGroups: []spec.TaskGroupSpec{{
			Name: "api", Count: 1,
			Tasks: []spec.TaskSpec{{Name: "server", Image: "app:v1"}},
		}},
	}
	if err := s.RegisterJob(context.Background(), "default", jobSpec); err != nil {
		t.Fatal(err)
	}

	updated := &spec.JobSpec{
		Namespace: "default", Name: "web",
		TaskGroups: []spec.TaskGroupSpec{{
			Name: "api", Count: 1,
			Tasks: []spec.TaskSpec{{Name: "server", Image: "app:v2"}},
		}},
	}
	if err := s.RegisterJob(context.Background(), "default", updated); err != nil {
		t.Fatal(err)
	}
	job := s.jobs[jobKey("default", "web")]
	if job.Revision != 2 {
		t.Errorf("expected revision 2 for image change, got %d", job.Revision)
	}
}

func TestCountChangeIsLabelOnly(t *testing.T) {
	s, _ := newTestServerWithAgent()

	jobSpec := &spec.JobSpec{
		Namespace: "default", Name: "web",
		TaskGroups: []spec.TaskGroupSpec{{
			Name: "api", Count: 1,
			Tasks: []spec.TaskSpec{{Name: "server", Image: "app:v1"}},
		}},
	}
	if err := s.RegisterJob(context.Background(), "default", jobSpec); err != nil {
		t.Fatal(err)
	}

	// Change only count — should not bump revision.
	updated := &spec.JobSpec{
		Namespace: "default", Name: "web",
		TaskGroups: []spec.TaskGroupSpec{{
			Name: "api", Count: 3,
			Tasks: []spec.TaskSpec{{Name: "server", Image: "app:v1"}},
		}},
	}
	if err := s.RegisterJob(context.Background(), "default", updated); err != nil {
		t.Fatal(err)
	}
	job := s.jobs[jobKey("default", "web")]
	if job.Revision != 1 {
		t.Errorf("expected revision to stay at 1 for count-only change, got %d", job.Revision)
	}
}

func TestValidateUpdateStrategy(t *testing.T) {
	base := func(update *spec.UpdateSpec) *spec.JobSpec {
		return &spec.JobSpec{
			Namespace: "default", Name: "web",
			TaskGroups: []spec.TaskGroupSpec{{
				Name: "api", Count: 2, Update: update,
				Tasks: []spec.TaskSpec{{Name: "server", Image: "image"}},
			}},
		}
	}

	valid := []struct {
		name   string
		update *spec.UpdateSpec
	}{
		{"nil", nil},
		{"recreate", &spec.UpdateSpec{Strategy: spec.UpdateRecreate}},
		{"rolling", &spec.UpdateSpec{Strategy: spec.UpdateRolling}},
		{"rolling with max_parallel", &spec.UpdateSpec{Strategy: spec.UpdateRolling, MaxParallel: 3}},
		{"empty strategy", &spec.UpdateSpec{Strategy: ""}},
	}
	for _, tt := range valid {
		t.Run(tt.name, func(t *testing.T) {
			if err := spec.Validate(base(tt.update)); err != nil {
				t.Fatalf("valid update spec rejected: %v", err)
			}
		})
	}

	invalid := []struct {
		name   string
		update *spec.UpdateSpec
	}{
		{"invalid strategy", &spec.UpdateSpec{Strategy: "blue-green"}},
		{"negative max_parallel", &spec.UpdateSpec{Strategy: spec.UpdateRolling, MaxParallel: -1}},
	}

	for _, tt := range invalid {
		t.Run(tt.name, func(t *testing.T) {
			if err := spec.Validate(base(tt.update)); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestParseUpdateSpec(t *testing.T) {
	raw := []byte(`namespace: default
name: web
task_groups:
  - name: api
    count: 2
    update:
      strategy: rolling
      max_parallel: 3
    tasks:
      - name: server
        image: example/server:1
`)
	job, err := spec.ParseYAML(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if err := spec.Validate(job); err != nil {
		t.Fatalf("validate: %v", err)
	}
	update := job.TaskGroups[0].Update
	if update == nil {
		t.Fatal("expected update spec")
	}
	if update.Strategy != spec.UpdateRolling {
		t.Errorf("expected rolling strategy, got %q", update.Strategy)
	}
	if update.MaxParallel != 3 {
		t.Errorf("expected max_parallel=3, got %d", update.MaxParallel)
	}
}
