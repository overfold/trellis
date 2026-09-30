package server

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	"github.com/overfold/trellis/orchestrator/internal/spec"
)

func rollingPlanJob(count, parallel int) *Job {
	job := planTestJob("web", count, 2, spec.UpdateRolling)
	job.Spec.TaskGroups[0].Update.MaxParallel = parallel
	return job
}

func drainingPlanAllocation(id string, node *Node) *Allocation {
	allocation := planTestAllocation(id, node, lifecycle.PhaseRunning, 1)
	allocation.Draining = true
	allocation.DrainReason = "update"
	return allocation
}

func TestPlanRollingWaitsForCrossNodeStopBeforeStartingNextReplacement(t *testing.T) {
	nodes := []*Node{
		planTestNode(1, NodeStatusHealthy),
		planTestNode(2, NodeStatusHealthy),
		planTestNode(3, NodeStatusHealthy),
		planTestNode(4, NodeStatusHealthy),
	}
	oldA := drainingPlanAllocation("old-a", nodes[0])
	oldB := drainingPlanAllocation("old-b", nodes[1])
	newHealthy := planTestAllocation("new-healthy", nodes[2], lifecycle.PhaseRunning, 2)
	input := planTestInput(map[string]*Job{jobKey("default", "web"): rollingPlanJob(2, 1)}, nodes, oldA, oldB, newHealthy)

	plan, err := planReconciliation(input)
	if err != nil {
		t.Fatal(err)
	}
	var stops, starts int
	for _, action := range plan.Actions {
		switch action.Type {
		case ActionStop:
			stops++
		case ActionStart:
			starts++
		}
	}
	if stops != 1 || starts != 0 {
		t.Fatalf("actions = %#v, want one cross-node stop and no start", summarizeActions(plan.Actions))
	}
	if len(plan.NewAllocations) != 0 {
		t.Fatalf("new allocations = %#v, want none until the stop is durable", plan.NewAllocations)
	}

	oldA.Phase = lifecycle.PhaseStopped
	plan, err = planReconciliation(input)
	if err != nil {
		t.Fatal(err)
	}
	starts = 0
	for _, action := range plan.Actions {
		if action.Type == ActionStart {
			starts++
		}
	}
	if starts != 1 {
		t.Fatalf("actions after stop = %#v, want one replacement start", summarizeActions(plan.Actions))
	}
}

func TestReconcileRollingFailedStopKeepsSurgeSlotReserved(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	nodes := []*Node{
		{ID: planTestNode(1, NodeStatusHealthy).ID, Host: agent.host, Port: agent.port, Status: NodeStatusHealthy},
		{ID: planTestNode(2, NodeStatusHealthy).ID, Host: agent.host, Port: agent.port, Status: NodeStatusHealthy},
		{ID: planTestNode(3, NodeStatusHealthy).ID, Host: agent.host, Port: agent.port, Status: NodeStatusHealthy},
		{ID: planTestNode(4, NodeStatusHealthy).ID, Host: agent.host, Port: agent.port, Status: NodeStatusHealthy},
	}
	for _, node := range nodes {
		addTestNode(s, node, s.now())
	}
	s.jobs[jobKey("default", "web")] = rollingPlanJob(2, 1)
	oldA := drainingPlanAllocation("old-a", nodes[0])
	oldB := drainingPlanAllocation("old-b", nodes[1])
	newHealthy := planTestAllocation("new-healthy", nodes[2], lifecycle.PhaseRunning, 2)
	s.allocations = []*Allocation{oldA, oldB, newHealthy}
	agent.failStop = true

	s.Reconcile(context.Background())
	if oldA.Phase != lifecycle.PhaseStopping || oldA.NextRetryAt == nil {
		t.Fatalf("failed stop state = phase %s retry %v, want stopping with backoff", oldA.Phase, oldA.NextRetryAt)
	}
	if len(s.allocations) != 3 {
		t.Fatalf("allocations after failed stop = %d, want no excess replacement", len(s.allocations))
	}
	stopCalls := func() int {
		stops := 0
		for _, call := range agent.recordedCalls() {
			if call.method == http.MethodDelete && call.path == "/v1/allocations/old-a" {
				stops++
			}
		}
		return stops
	}
	stopsAfterFailure := stopCalls()
	s.Reconcile(context.Background())
	if stops := stopCalls(); stops != stopsAfterFailure {
		t.Fatalf("stop calls during backoff = %d, want %d", stops, stopsAfterFailure)
	}
	if len(s.allocations) != 3 {
		t.Fatalf("allocations during stop backoff = %d, want 3", len(s.allocations))
	}

	agent.failStop = false
	oldA.NextRetryAt = nil
	s.Reconcile(context.Background())
	if oldA.Phase != lifecycle.PhaseStopped || len(s.allocations) != 3 {
		t.Fatalf("successful stop pass = phase %s allocations %d, want stopped without a same-pass start", oldA.Phase, len(s.allocations))
	}
	s.Reconcile(context.Background())
	if len(s.allocations) != 4 {
		t.Fatalf("allocations after released slot = %d, want one replacement", len(s.allocations))
	}
	starts := 0
	for _, call := range agent.recordedCalls() {
		if call.method == http.MethodPost && call.path == "/v1/allocations" {
			starts++
		}
	}
	if starts != 1 {
		t.Fatalf("replacement starts = %d, want one after stop completion", starts)
	}
}

func TestPlanRollingMaxParallelReservesEveryLiveSlot(t *testing.T) {
	nodes := []*Node{
		planTestNode(1, NodeStatusHealthy), planTestNode(2, NodeStatusHealthy),
		planTestNode(3, NodeStatusHealthy), planTestNode(4, NodeStatusHealthy),
		planTestNode(5, NodeStatusHealthy), planTestNode(6, NodeStatusHealthy),
		planTestNode(7, NodeStatusHealthy), planTestNode(8, NodeStatusHealthy),
	}
	old := []*Allocation{
		drainingPlanAllocation("old-a", nodes[0]),
		drainingPlanAllocation("old-b", nodes[1]),
		drainingPlanAllocation("old-c", nodes[2]),
		drainingPlanAllocation("old-d", nodes[3]),
	}
	newA := planTestAllocation("new-a", nodes[4], lifecycle.PhaseRunning, 2)
	newB := planTestAllocation("new-b", nodes[5], lifecycle.PhaseRunning, 2)
	input := planTestInput(map[string]*Job{jobKey("default", "web"): rollingPlanJob(4, 2)}, nodes, old[0], old[1], old[2], old[3], newA, newB)

	plan, err := planReconciliation(input)
	if err != nil {
		t.Fatal(err)
	}
	var stops, starts int
	for _, action := range plan.Actions {
		switch action.Type {
		case ActionStop:
			stops++
		case ActionStart:
			starts++
		}
	}
	if stops != 2 || starts != 0 {
		t.Fatalf("actions at six live replicas = %#v, want two stops and no starts", summarizeActions(plan.Actions))
	}
	old[0].Phase, old[1].Phase = lifecycle.PhaseStopped, lifecycle.PhaseStopped
	plan, err = planReconciliation(input)
	if err != nil {
		t.Fatal(err)
	}
	starts = 0
	for _, action := range plan.Actions {
		if action.Type == ActionStart {
			starts++
		}
	}
	if starts != 2 || len(plan.NewAllocations) != 2 {
		t.Fatalf("actions after two stops = %#v, want two starts", summarizeActions(plan.Actions))
	}
}

func TestPlanRollingScaleChangesRespectNewSurgeBound(t *testing.T) {
	nodes := []*Node{
		planTestNode(1, NodeStatusHealthy), planTestNode(2, NodeStatusHealthy),
		planTestNode(3, NodeStatusHealthy), planTestNode(4, NodeStatusHealthy),
		planTestNode(5, NodeStatusHealthy), planTestNode(6, NodeStatusHealthy),
	}
	oldA := drainingPlanAllocation("old-a", nodes[0])
	oldB := drainingPlanAllocation("old-b", nodes[1])
	newHealthy := planTestAllocation("new-healthy", nodes[2], lifecycle.PhaseRunning, 2)

	t.Run("scale up admits only newly available slots", func(t *testing.T) {
		input := planTestInput(map[string]*Job{jobKey("default", "web"): rollingPlanJob(3, 1)}, nodes, oldA, oldB, newHealthy)
		plan, err := planReconciliation(input)
		if err != nil {
			t.Fatal(err)
		}
		if len(plan.NewAllocations) != 1 {
			t.Fatalf("scale-up replacements = %d, want one within count + max_parallel", len(plan.NewAllocations))
		}
	})

	t.Run("scale down starts nothing while above the new bound", func(t *testing.T) {
		oldC := drainingPlanAllocation("old-c", nodes[3])
		input := planTestInput(map[string]*Job{jobKey("default", "web"): rollingPlanJob(1, 1)}, nodes, oldA, oldB, oldC, newHealthy)
		plan, err := planReconciliation(input)
		if err != nil {
			t.Fatal(err)
		}
		if len(plan.NewAllocations) != 0 {
			t.Fatalf("scale-down replacements = %#v, want none", plan.NewAllocations)
		}
		for _, action := range plan.Actions {
			if action.Type == ActionStart {
				t.Fatalf("scale-down actions = %#v, want no starts", summarizeActions(plan.Actions))
			}
		}
	})
}

func TestPlanRollingLostNodeEventuallyReleasesSurgeSlot(t *testing.T) {
	lostNode := planTestNode(1, NodeStatusUnhealthy)
	healthyNodes := []*Node{planTestNode(2, NodeStatusHealthy), planTestNode(3, NodeStatusHealthy), planTestNode(4, NodeStatusHealthy)}
	nodes := append([]*Node{lostNode}, healthyNodes...)
	oldLost := drainingPlanAllocation("old-a", lostNode)
	oldLost.Phase = lifecycle.PhaseStopping
	retryAt := planNow.Add(time.Minute)
	oldLost.NextRetryAt = &retryAt
	oldHealthy := drainingPlanAllocation("old-b", healthyNodes[0])
	newHealthy := planTestAllocation("new-healthy", healthyNodes[1], lifecycle.PhaseRunning, 2)
	input := planTestInput(map[string]*Job{jobKey("default", "web"): rollingPlanJob(2, 1)}, nodes, oldLost, oldHealthy, newHealthy)

	plan, err := planReconciliation(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.NewAllocations) != 0 {
		t.Fatalf("replacement created before loss timeout: %#v", plan.NewAllocations)
	}

	input.Heartbeats[lostNode.ID] = planNow.Add(-DefaultAllocationLossTimeout)
	oldLost.NextRetryAt = nil
	plan, err = planReconciliation(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.NewAllocations) != 1 {
		t.Fatalf("replacements after terminal loss = %d, want one", len(plan.NewAllocations))
	}
	if len(plan.Updated) == 0 || plan.Updated[0].ID != oldLost.ID || plan.Updated[0].Phase != lifecycle.PhaseLost {
		t.Fatalf("updates after loss = %#v, want old allocation lost", summarizeUpdates(plan.Updated))
	}
}

func TestPlanRollingUnavailableOldReplicasStillConsumeSurge(t *testing.T) {
	nodes := []*Node{
		planTestNode(1, NodeStatusUnhealthy), planTestNode(2, NodeStatusUnhealthy),
		planTestNode(3, NodeStatusHealthy), planTestNode(4, NodeStatusHealthy),
	}
	oldA := planTestAllocation("old-a", nodes[0], lifecycle.PhaseRunning, 1)
	oldB := planTestAllocation("old-b", nodes[1], lifecycle.PhaseRunning, 1)
	input := planTestInput(map[string]*Job{jobKey("default", "web"): rollingPlanJob(2, 1)}, nodes, oldA, oldB)

	plan, err := planReconciliation(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.NewAllocations) != 1 {
		t.Fatalf("replacements while old nodes are unavailable = %d, want one surge slot", len(plan.NewAllocations))
	}
	if len(plan.Updated) != 2 || !plan.Updated[0].Draining || !plan.Updated[1].Draining {
		t.Fatalf("old allocations were not durably marked draining: %#v", summarizeUpdates(plan.Updated))
	}
}

func TestPlanRollingChargesReturnedLostOriginal(t *testing.T) {
	nodes := []*Node{
		planTestNode(1, NodeStatusHealthy), planTestNode(2, NodeStatusHealthy),
		planTestNode(3, NodeStatusHealthy), planTestNode(4, NodeStatusHealthy),
	}
	nodes[0].observedAllocations = []observedAllocation{{ID: "lost-original", Generation: 1, Phase: lifecycle.PhaseRunning}}
	lostOriginal := planTestAllocation("lost-original", nodes[0], lifecycle.PhaseLost, 1)
	oldHealthy := drainingPlanAllocation("old-b", nodes[1])
	newHealthy := planTestAllocation("new-healthy", nodes[2], lifecycle.PhaseRunning, 2)
	input := planTestInput(map[string]*Job{jobKey("default", "web"): rollingPlanJob(2, 1)}, nodes, lostOriginal, oldHealthy, newHealthy)

	plan, err := planReconciliation(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.NewAllocations) != 0 {
		t.Fatalf("new allocations = %#v, returned lost container must consume the final surge slot", plan.NewAllocations)
	}
	for _, action := range plan.Actions {
		if action.Type == ActionStart {
			t.Fatalf("actions = %#v, want no start while returned lost container is live", summarizeActions(plan.Actions))
		}
	}
}
