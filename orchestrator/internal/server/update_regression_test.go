package server

import (
	"context"
	"testing"
	"time"

	"github.com/clofour/trellis/internal/lifecycle"
	"github.com/clofour/trellis/internal/spec"
	"github.com/google/uuid"
)

func TestReconcileRollingDoesNotReuseHealthyReplacement(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()

	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy, LastHeartbeat: s.now()}
	s.nodes[node.ID] = node
	s.leaderSince = s.now().Add(-time.Minute)

	newSpec := &spec.JobSpec{
		Namespace: "default",
		Name:      "web",
		TaskGroups: []spec.TaskGroupSpec{{
			Name:   "api",
			Count:  3,
			Update: &spec.UpdateSpec{Strategy: spec.UpdateRolling, MaxParallel: 1},
			Tasks:  []spec.TaskSpec{{Name: "server", Image: "app:v2"}},
		}},
	}
	s.jobs[jobKey("default", "web")] = &Job{
		Spec:          newSpec,
		Revision:      2,
		ContentHashes: map[string]string{"api": spec.TaskGroupContentHash(&newSpec.TaskGroups[0])},
	}

	now := s.now()
	makeAllocation := func(id string, revision int, image string, health lifecycle.Health, draining bool) *Allocation {
		a := &Allocation{
			ID:            id,
			Namespace:     "default",
			JobName:       "web",
			TaskGroupName: "api",
			Tasks:         []spec.TaskSpec{{Name: "server", Image: image}},
			Node:          node,
			Generation:    1,
			JobRevision:   revision,
			Phase:         lifecycle.PhaseRunning,
			Health:        health,
			Draining:      draining,
			Diagnostic: lifecycle.Diagnostic{
				CreatedAt:      now,
				TransitionedAt: now,
			}}
		return a
	}

	oldA := makeAllocation("old-a", 1, "app:v1", lifecycle.HealthHealthy, true)
	oldB := makeAllocation("old-b", 1, "app:v1", lifecycle.HealthHealthy, true)
	newHealthy := makeAllocation("new-healthy", 2, "app:v2", lifecycle.HealthHealthy, false)
	newStarting := makeAllocation("new-starting", 2, "app:v2", lifecycle.HealthUnknown, false)
	s.allocations = []*Allocation{oldA, oldB, newHealthy, newStarting}

	s.Reconcile(context.Background())

	for _, old := range []*Allocation{oldA, oldB} {
		old.mu.Lock()
		phase := old.Phase
		old.mu.Unlock()
		if phase == lifecycle.PhaseStopping || phase == lifecycle.PhaseStopped {
			t.Fatalf("draining allocation %s stopped before the in-flight replacement became healthy", old.ID)
		}
	}
}

func TestDrainNodeStopsAllocationAfterReplacementHealthy(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	drainingNode := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy, LastHeartbeat: s.now()}
	replacementNode := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy, LastHeartbeat: s.now()}
	s.nodes[drainingNode.ID] = drainingNode
	s.nodes[replacementNode.ID] = replacementNode
	s.leaderSince = s.now().Add(-time.Minute)

	jobSpec := &spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "api", Count: 1, Tasks: []spec.TaskSpec{{Name: "server", Image: "app"}}}}}
	s.jobs[jobKey("default", "web")] = &Job{Spec: jobSpec, Revision: 1}
	original := &Allocation{ID: "original", Namespace: "default", JobName: "web", TaskGroupName: "api", Tasks: jobSpec.TaskGroups[0].Tasks, Node: drainingNode, Generation: 1, JobRevision: 1, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy, Diagnostic: lifecycle.Diagnostic{CreatedAt: s.now(), TransitionedAt: s.now()}}
	s.allocations = []*Allocation{original}

	if err := s.DrainNode(context.Background(), drainingNode.ID); err != nil {
		t.Fatal(err)
	}
	if !original.Draining || original.Phase != lifecycle.PhaseRunning {
		t.Fatalf("original after drain = draining %t, phase %s; want draining and running", original.Draining, original.Phase)
	}
	if len(s.allocations) != 2 {
		t.Fatalf("allocations after drain = %d, want original and replacement", len(s.allocations))
	}
	replacement := s.allocations[1]
	if replacement.Node != replacementNode {
		t.Fatalf("replacement node = %v, want %v", replacement.Node, replacementNode)
	}
	replacement.Health = lifecycle.HealthHealthy
	s.Reconcile(context.Background())
	if original.Phase != lifecycle.PhaseStopped {
		t.Fatalf("original phase after replacement healthy = %s, want stopped", original.Phase)
	}
}
