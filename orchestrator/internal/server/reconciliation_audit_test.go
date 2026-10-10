package server

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"testing"
	"time"

	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	"github.com/overfold/trellis/orchestrator/internal/spec"
)

func TestScaleDownWithUnavailableReplicas(t *testing.T) {
	for _, pendingCount := range []int{0, 2} {
		t.Run(fmt.Sprint(pendingCount), func(t *testing.T) {
			node := planTestNode(1, NodeStatusUnhealthy)
			allocations := []*Allocation{planTestAllocation("a", node, lifecycle.PhaseRunning, 1), planTestAllocation("b", node, lifecycle.PhaseRunning, 1)}
			for i := range pendingCount {
				allocations = append(allocations, planTestAllocation(fmt.Sprintf("pending-%d", i), nil, lifecycle.PhasePending, 1))
			}
			input := planTestInput(map[string]*Job{jobKey("default", "web"): planTestJob("web", 1, 1, "")}, []*Node{node}, allocations...)
			plan, err := planReconciliation(input)
			if err != nil {
				t.Fatal(err)
			}
			if len(plan.NewAllocations) != 0 || len(plan.Updated) != pendingCount {
				t.Fatalf("new=%d updated=%d, want only surplus pending capacity removed", len(plan.NewAllocations), len(plan.Updated))
			}
			for _, update := range plan.Updated {
				if update.Phase != lifecycle.PhaseStopped || update.Reason != "scaled_down" {
					t.Fatalf("unexpected update: %#v", update)
				}
			}
		})
	}
}

func TestRecreateWaitsForOldExecutionCleanup(t *testing.T) {
	for _, kind := range []string{"running", "stop retry", "restart", "failed siblings", "returned original", "previous incarnation"} {
		t.Run(kind, func(t *testing.T) {
			a, b := planTestNode(1, NodeStatusHealthy), planTestNode(2, NodeStatusHealthy)
			job := planTestJob("web", 1, 2, spec.UpdateRecreate)
			old := planTestAllocation("old", a, lifecycle.PhaseRunning, 1)
			switch kind {
			case "stop retry":
				old.Phase = lifecycle.PhaseStopping
				retry := planNow.Add(time.Minute)
				old.NextRetryAt = &retry
			case "restart":
				old.JobRevision = job.Revision
				old.Draining, old.DrainReason = true, "restart"
			case "failed siblings":
				old.Phase = lifecycle.PhaseFailed
			case "returned original":
				old.Phase = lifecycle.PhaseLost
			case "previous incarnation":
				old.JobRevision = job.Revision
				old.JobIncarnation = "deleted"
			}
			if kind == "failed siblings" || kind == "returned original" {
				a.observedAllocations = []observedAllocation{{ID: old.ID, Generation: old.Generation, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy, Tasks: map[string]bool{"server": true}}}
			}
			// Already placed replacements must not be redelivered either.
			placed := planTestAllocation("replacement", b, lifecycle.PhasePlaced, job.Revision)
			input := planTestInput(map[string]*Job{jobKey("default", "web"): job}, []*Node{a, b}, old, placed)
			plan, err := planReconciliation(input)
			if err != nil {
				t.Fatal(err)
			}
			for _, action := range plan.Actions {
				if action.Type == ActionStart {
					t.Fatalf("start admitted before cleanup: %#v", summarizeActions(plan.Actions))
				}
			}
			if len(plan.NewAllocations) != 0 || old.Generation != 1 {
				t.Fatal("replacement admitted or input generation changed")
			}
			old.Phase, old.NextRetryAt = lifecycle.PhaseStopped, nil
			a.observedAllocations, a.observedAt = nil, planNow
			input.Allocations = []*Allocation{old}
			converged, err := planReconciliation(input)
			if err != nil || len(converged.NewAllocations) != 1 || converged.NewAllocations[0].Phase != lifecycle.PhasePlaced {
				t.Fatalf("cleanup did not release capacity: plan=%#v err=%v", converged, err)
			}
		})
	}
}

func TestRetainedPlacementDiagnosticUsesPlacementOccupancy(t *testing.T) {
	a, b := planTestNode(1, NodeStatusHealthy), planTestNode(2, NodeStatusHealthy)
	a.CPUAllocatable, b.CPUAllocatable = 1000, 2000
	job := rollingPlanJob(2, 2)
	task := &job.Spec.TaskGroups[0].Tasks[0]
	task.Resources = &spec.ResourcesSpec{CPU: 1000, Memory: 64 << 20}
	task.Volumes = []spec.VolumeSpec{{Name: "data", HostPath: "@/data", ContainerPath: "/data"}}
	oldA := planTestAllocation("old-a", a, lifecycle.PhaseLost, 1)
	oldB := planTestAllocation("old-b", b, lifecycle.PhaseLost, 1)
	for _, old := range []*Allocation{oldA, oldB} {
		old.Tasks[0].Resources = &spec.ResourcesSpec{CPU: 1000, Memory: 64 << 20}
		old.Node.observedAllocations = []observedAllocation{{ID: old.ID, Generation: 1, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy, Tasks: map[string]bool{"server": true}}}
	}
	// Without retained occupancy the new volume binds to A and only one
	// replica fits. With occupancy it binds to B and still only one fits.
	// Neither original is released; diagnosing with B's retained CPU omitted
	// would falsely place the second replica and return a nil diagnostic.
	plan, err := planReconciliation(planTestInput(map[string]*Job{jobKey("default", "web"): job}, []*Node{a, b}, oldA, oldB))
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.NewAllocations) != 2 {
		t.Fatalf("allocations = %d, want one placed and one pending", len(plan.NewAllocations))
	}
	for i, allocation := range plan.NewAllocations {
		if i == 0 && (allocation.Phase != lifecycle.PhasePlaced || allocation.Node.ID != b.ID) || i == 1 && (allocation.Phase != lifecycle.PhasePending || allocation.Reason != "insufficient_capacity") {
			t.Fatalf("allocation %d: %#v", i, allocation)
		}
	}
}

func TestPlanObsoleteExecutionsBecomeLost(t *testing.T) {
	for _, kind := range []string{"deleted", "incarnation", "removed group", "recreate", "retry"} {
		t.Run(kind, func(t *testing.T) {
			dead := planTestNode(1, NodeStatusUnhealthy)
			worker := planTestNode(2, NodeStatusHealthy)
			job := rollingPlanJob(1, 1)
			old := planTestAllocation("old", dead, lifecycle.PhaseRunning, 1)
			jobs := map[string]*Job{jobKey("default", "web"): job}
			switch kind {
			case "deleted":
				delete(jobs, jobKey("default", "web"))
			case "incarnation":
				job.Incarnation = "new"
			case "removed group":
				old.TaskGroupName = "removed"
			case "recreate":
				job.Spec.TaskGroups[0].Update.Strategy = spec.UpdateRecreate
			case "retry":
				retry := planNow.Add(time.Hour)
				old.NextRetryAt = &retry
			}
			input := planTestInput(jobs, []*Node{dead, worker}, old)
			input.Heartbeats[dead.ID] = planNow.Add(-2 * DefaultAllocationLossTimeout)
			plan, err := planReconciliation(input)
			if err != nil {
				t.Fatal(err)
			}
			if len(plan.Updated) != 1 || plan.Updated[0].Phase != lifecycle.PhaseLost || plan.Updated[0].Generation != old.Generation {
				t.Fatalf("updates = %#v, want generation-preserving loss", summarizeUpdates(plan.Updated))
			}
			if kind != "deleted" && (len(plan.NewAllocations) != 1 || plan.NewAllocations[0].Phase != lifecycle.PhasePlaced) {
				t.Fatalf("replacement = %#v, want obsolete execution to release its slot", plan.NewAllocations)
			}
		})
	}
}

func TestPlanStoppingRetrySurvivesHealthRegression(t *testing.T) {
	node := planTestNode(1, NodeStatusHealthy)
	old := drainingPlanAllocation("old", node)
	old.Phase = lifecycle.PhaseStopping
	newReplica := planTestAllocation("new", node, lifecycle.PhaseRunning, 2)
	newReplica.Health = lifecycle.HealthUnhealthy
	input := planTestInput(map[string]*Job{jobKey("default", "web"): rollingPlanJob(1, 1)}, []*Node{node}, old, newReplica)
	for _, due := range []bool{false, true} {
		retry := planNow.Add(time.Second)
		if due {
			retry = planNow.Add(-time.Second)
		}
		old.NextRetryAt = &retry
		plan, err := planReconciliation(input)
		if err != nil {
			t.Fatal(err)
		}
		stops := 0
		for _, action := range plan.Actions {
			if action.Type == ActionStop && action.Allocation.ID == old.ID {
				stops++
			}
		}
		if stops != map[bool]int{false: 0, true: 1}[due] {
			t.Fatalf("due=%t actions=%#v", due, summarizeActions(plan.Actions))
		}
	}
}

func TestPlanReturnedOriginalUnblocksRollingUpdate(t *testing.T) {
	a, b := planTestNode(1, NodeStatusHealthy), planTestNode(2, NodeStatusHealthy)
	lost := planTestAllocation("lost", a, lifecycle.PhaseLost, 1)
	a.observedAllocations = []observedAllocation{{ID: lost.ID, Generation: lost.Generation, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy, Tasks: map[string]bool{"server": true}}}
	draining := drainingPlanAllocation("draining", b)
	input := planTestInput(map[string]*Job{jobKey("default", "web"): rollingPlanJob(1, 1)}, []*Node{a, b}, lost, draining)
	plan, err := planReconciliation(input)
	if err != nil {
		t.Fatal(err)
	}
	stopped := false
	for _, action := range plan.Actions {
		if action.Type == ActionStopObserved && action.ID == lost.ID && action.Generation == lost.Generation {
			stopped = true
		}
		if action.Type == ActionStop || action.Type == ActionStart {
			t.Fatalf("actions = %#v, must keep the healthy desired replica and reserve surge until cleanup", summarizeActions(plan.Actions))
		}
	}
	if !stopped {
		t.Fatal("returned original and draining execution deadlocked the full surge budget")
	}
	lost.Phase = lifecycle.PhaseStopped
	a.observedAllocations = nil
	plan, err = planReconciliation(input)
	if err != nil || len(plan.NewAllocations) != 1 || plan.NewAllocations[0].Phase != lifecycle.PhasePlaced {
		t.Fatalf("after cleanup: plan=%#v err=%v", plan, err)
	}
	// If the desired drain is unhealthy, keep the complete healthy original
	// and free the unhealthy drain instead, still waiting for stop completion.
	lost.Phase = lifecycle.PhaseLost
	a.observedAllocations = []observedAllocation{{ID: lost.ID, Generation: lost.Generation, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy, Tasks: map[string]bool{"server": true}}}
	draining.Health = lifecycle.HealthUnhealthy
	plan, err = planReconciliation(input)
	if err != nil {
		t.Fatal(err)
	}
	stopped = false
	for _, action := range plan.Actions {
		if action.Type == ActionStop && action.Allocation.ID == draining.ID {
			stopped = true
		}
		if action.Type == ActionStopObserved || action.Type == ActionStart {
			t.Fatalf("actions = %#v, must preserve the healthy returned bridge", summarizeActions(plan.Actions))
		}
	}
	if !stopped {
		t.Fatal("unhealthy drain blocked the retained bridge's replacement")
	}
}

func TestPlanReturnedOriginalPreservesAvailability(t *testing.T) {
	for _, healthy := range []bool{false, true} {
		a, b := planTestNode(1, NodeStatusHealthy), planTestNode(2, NodeStatusHealthy)
		job := rollingPlanJob(1, 2)
		job.Spec.TaskGroups[0].Constraints = []spec.ConstraintSpec{{Attribute: "target", Value: "yes"}}
		a.Labels = map[string]string{"target": "yes"}
		a.CPUAllocatable = job.Spec.TaskGroups[0].Tasks[0].Resources.CPU
		lost := planTestAllocation("lost", a, lifecycle.PhaseLost, 1)
		lost.Tasks = job.Spec.TaskGroups[0].Tasks
		health := lifecycle.HealthUnhealthy
		if healthy {
			health = lifecycle.HealthHealthy
		}
		a.observedAllocations = []observedAllocation{{ID: lost.ID, Generation: lost.Generation, Phase: lifecycle.PhaseRunning, Health: health, Tasks: map[string]bool{"server": true}}}
		draining := drainingPlanAllocation("draining", b)
		input := planTestInput(map[string]*Job{jobKey("default", "web"): job}, []*Node{a, b}, lost, draining)
		plan, err := planReconciliation(input)
		if err != nil {
			t.Fatal(err)
		}
		cleanup := false
		for _, action := range plan.Actions {
			if action.Type == ActionStop {
				t.Fatalf("healthy bridge=%t: stopped the available draining replica while the returned original is released", healthy)
			}
			if action.Type == ActionStopObserved && action.ID == lost.ID {
				cleanup = true
			}
		}
		if !cleanup {
			t.Fatalf("healthy bridge=%t: blocking/unhealthy original was not released", healthy)
		}
	}
}

func TestPlanReturnedOriginalNeedsCompleteObservation(t *testing.T) {
	a, b := planTestNode(1, NodeStatusHealthy), planTestNode(2, NodeStatusHealthy)
	lost := planTestAllocation("lost", a, lifecycle.PhaseLost, 1)
	lost.Tasks = append(lost.Tasks, spec.TaskSpec{Name: "sidecar", Image: "app"})
	a.observedAllocations = []observedAllocation{{ID: lost.ID, Generation: 1, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy, Tasks: map[string]bool{"server": true}}}
	draining := drainingPlanAllocation("draining", b)
	input := planTestInput(map[string]*Job{jobKey("default", "web"): rollingPlanJob(1, 1)}, []*Node{a, b}, lost, draining)
	plan, err := planReconciliation(input)
	if err != nil {
		t.Fatal(err)
	}
	cleanup := false
	for _, action := range plan.Actions {
		if action.Type == ActionStop || action.Type == ActionStart {
			t.Fatalf("incomplete original cannot replace available capacity or release its slot: %#v", summarizeActions(plan.Actions))
		}
		if action.Type == ActionStopObserved && action.ID == lost.ID {
			cleanup = true
		}
	}
	if !cleanup {
		t.Fatal("incomplete original was retained as healthy capacity")
	}
}

func TestPlanFailedSiblingOccupancyUntilCleanup(t *testing.T) {
	for _, resource := range []string{"cpu", "memory", "port"} {
		t.Run(resource, func(t *testing.T) {
			node := planTestNode(1, NodeStatusHealthy)
			job := rollingPlanJob(1, 1)
			group := &job.Spec.TaskGroups[0]
			sibling := group.Tasks[0]
			sibling.Name = "sidecar"
			sibling.Resources = &spec.ResourcesSpec{CPU: 30, Memory: 64 << 20}
			group.Tasks = append(group.Tasks, sibling)
			switch resource {
			case "cpu":
				node.CPUAllocatable = group.Tasks[0].Resources.CPU + sibling.Resources.CPU
			case "memory":
				node.MemoryAllocatable = int64(group.Tasks[0].Resources.Memory + sibling.Resources.Memory)
			case "port":
				group.Tasks[0].Networking = &spec.TaskNetworkingSpec{Mode: spec.TaskNetworkHost, Ports: []spec.PortSpec{{Port: 8080}}}
			}
			failed := planTestAllocation("failed", node, lifecycle.PhaseFailed, 1)
			failed.Tasks = group.Tasks
			node.observedAt = planNow
			node.observedAllocations = []observedAllocation{{ID: failed.ID, Generation: failed.Generation, Phase: lifecycle.PhaseFailed}}
			input := planTestInput(map[string]*Job{jobKey("default", "web"): job}, []*Node{node}, failed)
			plan, err := planReconciliation(input)
			if err != nil {
				t.Fatal(err)
			}
			for _, action := range plan.Actions {
				if action.Type == ActionStart {
					t.Fatalf("started over live siblings: %#v", summarizeActions(plan.Actions))
				}
			}
			node.observedAllocations[0].RetainedLogs = true
			plan, err = planReconciliation(input)
			if err != nil || len(plan.NewAllocations) != 1 || plan.NewAllocations[0].Phase != lifecycle.PhasePlaced {
				t.Fatalf("cleanup did not release occupancy: plan=%#v err=%v", plan, err)
			}
			// Another generation cannot resurrect this terminal record. Its
			// unknown reservations gate the node until orphan cleanup instead.
			node.observedAllocations[0].RetainedLogs = false
			node.observedAllocations[0].Generation++
			plan, err = planReconciliation(input)
			if err != nil || len(plan.NewAllocations) != 1 || plan.NewAllocations[0].Phase != lifecycle.PhasePending || failed.Phase != lifecycle.PhaseFailed {
				t.Fatalf("stale generation did not reserve its node pending cleanup: plan=%#v err=%v", plan, err)
			}
		})
	}
}

func TestPlanScaleDownKeepsHealthyReplica(t *testing.T) {
	for _, draining := range []bool{false, true} {
		node := planTestNode(1, NodeStatusHealthy)
		unhealthy := planTestAllocation("a-unhealthy", node, lifecycle.PhaseRunning, 2)
		unhealthy.Health = lifecycle.HealthUnhealthy
		healthy := planTestAllocation("z-healthy", node, lifecycle.PhaseRunning, 2)
		if draining {
			unhealthy.ID, healthy.ID = "z-unhealthy", "a-healthy"
			unhealthy.Draining, healthy.Draining = true, true
		}
		input := planTestInput(map[string]*Job{jobKey("default", "web"): rollingPlanJob(1, 1)}, []*Node{node}, unhealthy, healthy)
		plan, err := planReconciliation(input)
		if err != nil {
			t.Fatal(err)
		}
		var stopped []string
		for _, action := range plan.Actions {
			if action.Type == ActionStop {
				stopped = append(stopped, action.Allocation.ID)
			}
		}
		if len(stopped) != 1 || stopped[0] != unhealthy.ID {
			t.Fatalf("draining=%t: actions = %#v, want unhealthy replica stopped despite ID ordering", draining, summarizeActions(plan.Actions))
		}
	}
}

func TestPlanMaxParallelDoesNotOverflow(t *testing.T) {
	node := planTestNode(1, NodeStatusHealthy)
	old := drainingPlanAllocation("old", node)
	input := planTestInput(map[string]*Job{jobKey("default", "web"): rollingPlanJob(1, math.MaxInt)}, []*Node{node}, old)
	plan, err := planReconciliation(input)
	if err != nil || len(plan.NewAllocations) != 1 || plan.NewAllocations[0].Phase != lifecycle.PhasePlaced {
		t.Fatalf("MaxInt parallel blocked rollout: plan=%#v err=%v", plan, err)
	}
}

func TestRetainedCleanupFailureGatesSameNodeStarts(t *testing.T) {
	s, agent := newTestServerWithAgent()
	t.Cleanup(agent.server.Close)
	node := planTestNode(1, NodeStatusHealthy)
	node.Host, node.Port = agent.host, agent.port
	addTestNode(s, node, s.now())
	alloc := planTestAllocation("replacement", node, lifecycle.PhasePlaced, 2)
	job := rollingPlanJob(1, 1)
	job.Spec.TaskGroups[0].Tasks[0].Networking = &spec.TaskNetworkingSpec{Mode: spec.TaskNetworkHost}
	s.jobs[jobKey("default", "web")] = job
	s.allocations = []*Allocation{alloc}
	agent.failStop = true
	actions := []Action{{Type: ActionStopObserved, Node: node, ID: "original", Generation: 7}, {Type: ActionStart, Allocation: alloc}}
	<-s.dispatchReconcileActions(context.Background(), actions, true)
	if calls := agent.recordedCalls(); len(calls) != 1 || calls[0].method != http.MethodDelete {
		t.Fatalf("calls = %#v, cleanup failure must gate the dependent start", calls)
	}
	agent.mu.Lock()
	agent.failStop = false
	agent.mu.Unlock()
	<-s.dispatchReconcileActions(context.Background(), actions, true)
	if calls := agent.recordedCalls(); len(calls) != 3 || calls[2].method != http.MethodPost {
		t.Fatalf("calls = %#v, successful cleanup must allow start", calls)
	}
}

func TestRollingReplacementLimitSurvivesStoppingAndLoss(t *testing.T) {
	node := planTestNode(1, NodeStatusHealthy)
	job := rollingPlanJob(2, 1)
	old := drainingPlanAllocation("old", node)
	old.Phase = lifecycle.PhaseStopping
	lost := planTestAllocation("lost-new", node, lifecycle.PhaseLost, 2)
	input := planTestInput(map[string]*Job{jobKey("default", "web"): job}, []*Node{node}, old, lost)
	input.Policy.RetainTerminal = 0
	for _, phase := range []lifecycle.Phase{lifecycle.PhaseStopping, lifecycle.PhaseStopped, lifecycle.PhaseLost} {
		old.Phase = phase
		plan, err := planReconciliation(input)
		if err != nil {
			t.Fatal(err)
		}
		if len(plan.NewAllocations) != 1 {
			t.Fatalf("old=%s: placements=%d, want one unhealthy replacement", phase, len(plan.NewAllocations))
		}
		for _, pruned := range plan.Pruned {
			if pruned.ID == old.ID {
				t.Fatalf("old=%s: pruned unfinished rollout evidence", phase)
			}
		}
		inFlight := plan.NewAllocations[0]
		input.Allocations = []*Allocation{old, lost, inFlight}
		waiting, err := planReconciliation(input)
		if err != nil || len(waiting.NewAllocations) != 0 {
			t.Fatalf("old=%s: admitted second unhealthy replacement: %v %v", phase, waiting, err)
		}
		inFlight.Phase, inFlight.Health = lifecycle.PhaseRunning, lifecycle.HealthHealthy
		ready, err := planReconciliation(input)
		if err != nil || len(ready.NewAllocations) != 1 {
			t.Fatalf("old=%s: healthy replacement did not release parallel slot: %v %v", phase, ready, err)
		}
		if phase != lifecycle.PhaseStopping {
			second := ready.NewAllocations[0]
			second.Phase, second.Health = lifecycle.PhaseRunning, lifecycle.HealthHealthy
			input.Allocations = []*Allocation{old, lost, inFlight, second}
			complete, err := planReconciliation(input)
			if err != nil || len(complete.Pruned) != 2 {
				t.Fatalf("completed rollout did not release terminal evidence: %v %v", complete, err)
			}
		}
		input.Allocations = []*Allocation{old, lost}
	}
	input.Allocations = nil
	initial, err := planReconciliation(input)
	if err != nil || len(initial.NewAllocations) != 2 {
		t.Fatalf("initial deployment was replacement-throttled: %v %v", initial, err)
	}
}

func TestOrphanInventoryBlocksOnlyItsNodeUntilCleanup(t *testing.T) {
	for _, elapsed := range []time.Duration{leaderRecoveryGrace - time.Nanosecond, leaderRecoveryGrace} {
		for _, logsOnly := range []bool{false, true} {
			t.Run(fmt.Sprintf("elapsed=%s/logs=%t", elapsed, logsOnly), func(t *testing.T) {
				a, b := planTestNode(1, NodeStatusHealthy), planTestNode(2, NodeStatusHealthy)
				a.CPUAllocatable, b.CPUAllocatable = 1000, 1000
				job := planTestJob("web", 2, 1, spec.UpdateRecreate)
				job.Spec.TaskGroups[0].Tasks[0].Resources = &spec.ResourcesSpec{CPU: 1000, Memory: 64 << 20}
				a.observedAllocations = []observedAllocation{{ID: "pruned", Generation: 7, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy, RetainedLogs: logsOnly}}
				input := planTestInput(map[string]*Job{jobKey("default", "web"): job}, []*Node{a, b})
				input.LeaderSince = planNow.Add(-elapsed)
				plan, err := planReconciliation(input)
				if err != nil {
					t.Fatal(err)
				}
				placed := 0
				for _, allocation := range plan.NewAllocations {
					if allocation.Node == nil {
						continue
					}
					placed++
					if !logsOnly && allocation.Node.ID == a.ID {
						t.Fatal("orphan execution treated as free CPU/memory/ports")
					}
				}
				want := 1
				if logsOnly {
					want = 2
				}
				if placed != want {
					t.Fatalf("placed=%d want=%d (independent-node control)", placed, want)
				}
				stops := 0
				for _, action := range plan.Actions {
					if action.Type == ActionStopObserved {
						stops++
					}
				}
				wantStops := 0
				if elapsed >= leaderRecoveryGrace {
					wantStops = 1
				}
				if stops != wantStops {
					t.Fatalf("cleanup actions=%d want=%d at grace boundary", stops, wantStops)
				}
			})
		}
	}
}

func TestOrphanStopOutcomeControlsStartAdmission(t *testing.T) {
	s, agent := newTestServerWithAgent()
	t.Cleanup(agent.server.Close)
	node := planTestNode(1, NodeStatusHealthy)
	node.Host, node.Port = agent.host, agent.port
	addTestNode(s, node, s.now())
	s.leaderSince = s.now().Add(-leaderRecoveryGrace)
	job := planTestJob("web", 1, 1, spec.UpdateRecreate)
	job.Spec.TaskGroups[0].Tasks[0].Networking = &spec.TaskNetworkingSpec{Mode: spec.TaskNetworkHost}
	s.jobs[jobKey("default", "web")] = job
	allocation := planTestAllocation("queued", node, lifecycle.PhasePlaced, 1)
	s.allocations = []*Allocation{allocation}
	node.observedAllocations = []observedAllocation{{ID: "pruned", Generation: 7, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy}}
	// A start planned before the orphan heartbeat must also be rejected.
	if err := s.Execute(t.Context(), &Action{Type: ActionStart, Allocation: allocation}); err == nil || len(agent.recordedCalls()) != 0 {
		t.Fatal("stale planned start ignored current orphan inventory")
	}
	agent.failStop = true
	s.Reconcile(t.Context())
	if calls := agent.recordedCalls(); len(calls) != 1 || calls[0].method != http.MethodDelete {
		t.Fatalf("failed cleanup calls=%v, want stop only", calls)
	}
	agent.mu.Lock()
	agent.failStop = false
	agent.mu.Unlock()
	s.Reconcile(t.Context())
	s.Reconcile(t.Context())
	if calls := agent.recordedCalls(); len(calls) != 3 || calls[1].method != http.MethodDelete || calls[2].method != http.MethodPost {
		t.Fatalf("cleanup recovery calls=%v, want failed stop, successful stop, start", calls)
	}
}
