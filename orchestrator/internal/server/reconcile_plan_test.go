package server

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/internal/api"
	"github.com/overfold/trellis/internal/lifecycle"
	"github.com/overfold/trellis/internal/spec"
)

var planNow = time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)

func planTestNode(id byte, status NodeStatus) *Node {
	return &Node{ID: uuid.UUID{id}, Host: "10.0.0.1", Port: 8127, Status: status, LastHeartbeat: planNow}
}

func planTestJob(name string, count, revision int, strategy spec.UpdateStrategy) *Job {
	group := spec.TaskGroupSpec{Name: "app", Count: count, Tasks: []spec.TaskSpec{{Name: "server", Image: "app"}}}
	if strategy != "" {
		group.Update = &spec.UpdateSpec{Strategy: strategy}
	}
	return &Job{Spec: &spec.JobSpec{Namespace: "default", Name: name, TaskGroups: []spec.TaskGroupSpec{group}}, Revision: revision}
}

func planTestAllocation(id string, node *Node, phase lifecycle.Phase, revision int) *Allocation {
	return &Allocation{ID: id, Namespace: "default", JobName: "web", TaskGroupName: "app", Tasks: []spec.TaskSpec{{Name: "server", Image: "app"}}, Node: node, Generation: 1, JobRevision: revision, Phase: phase, Health: lifecycle.HealthHealthy, Diagnostic: lifecycle.Diagnostic{CreatedAt: planNow.Add(-time.Hour), TransitionedAt: planNow.Add(-time.Hour)}}
}

func planTestInput(jobs map[string]*Job, nodes []*Node, allocations ...*Allocation) *reconcilePlanInput {
	nodeMap := make(map[uuid.UUID]*Node, len(nodes))
	for _, node := range nodes {
		nodeMap[node.ID] = node
	}
	suffix := 0
	return &reconcilePlanInput{
		Now:                   planNow,
		LeaderSince:           planNow.Add(-time.Hour),
		Limits:                spec.DefaultLimits(),
		Policy:                DefaultReplacementPolicy(),
		AllocationLossTimeout: DefaultAllocationLossTimeout,
		Jobs:                  jobs,
		Nodes:                 nodeMap,
		Allocations:           allocations,
		Backoffs:              map[string]*ReplacementBackoff{},
		VolumeOwners:          map[string]uuid.UUID{},
		NetworkPorts:          map[string]int{},
		NewAllocationSuffix: func() string {
			suffix++
			return fmt.Sprintf("%08d", suffix)
		},
	}
}

type plannedAction struct {
	Type       ActionType
	Allocation string
}

func summarizeActions(actions []Action) []plannedAction {
	summary := make([]plannedAction, 0, len(actions))
	for _, action := range actions {
		id := action.ID
		if action.Allocation != nil {
			id = action.Allocation.ID
		}
		summary = append(summary, plannedAction{Type: action.Type, Allocation: id})
	}
	return summary
}

type plannedUpdate struct {
	ID       string
	Phase    lifecycle.Phase
	Draining bool
	Reason   string
}

func summarizeUpdates(allocations []*Allocation) []plannedUpdate {
	summary := make([]plannedUpdate, 0, len(allocations))
	for _, allocation := range allocations {
		summary = append(summary, plannedUpdate{ID: allocation.ID, Phase: allocation.Phase, Draining: allocation.Draining, Reason: allocation.Reason})
	}
	return summary
}

func TestPlanReconciliation(t *testing.T) {
	healthy := planTestNode(1, NodeStatusHealthy)
	tests := []struct {
		name        string
		input       func() *reconcilePlanInput
		actions     []plannedAction
		updates     []plannedUpdate
		created     []plannedUpdate
		diagnostics []string
	}{
		{
			name: "places missing replicas on a healthy node",
			input: func() *reconcilePlanInput {
				return planTestInput(map[string]*Job{jobKey("default", "web"): planTestJob("web", 2, 1, "")}, []*Node{healthy})
			},
			actions: []plannedAction{{ActionStart, "default-web-app-00000001"}, {ActionStart, "default-web-app-00000002"}},
			created: []plannedUpdate{{ID: "default-web-app-00000001", Phase: lifecycle.PhasePlaced}, {ID: "default-web-app-00000002", Phase: lifecycle.PhasePlaced}},
		},
		{
			name: "converged group plans nothing",
			input: func() *reconcilePlanInput {
				return planTestInput(map[string]*Job{jobKey("default", "web"): planTestJob("web", 1, 1, "")}, []*Node{healthy},
					planTestAllocation("a", healthy, lifecycle.PhaseRunning, 1))
			},
		},
		{
			name: "stops the highest surplus replica",
			input: func() *reconcilePlanInput {
				return planTestInput(map[string]*Job{jobKey("default", "web"): planTestJob("web", 1, 1, "")}, []*Node{healthy},
					planTestAllocation("b", healthy, lifecycle.PhaseRunning, 1), planTestAllocation("a", healthy, lifecycle.PhaseRunning, 1))
			},
			actions: []plannedAction{{ActionStop, "b"}},
		},
		{
			name: "stops allocations of a deleted job",
			input: func() *reconcilePlanInput {
				return planTestInput(map[string]*Job{}, []*Node{healthy}, planTestAllocation("a", healthy, lifecycle.PhaseRunning, 1))
			},
			actions: []plannedAction{{ActionStop, "a"}},
		},
		{
			name: "recreate strategy stops outdated revisions and replaces them",
			input: func() *reconcilePlanInput {
				return planTestInput(map[string]*Job{jobKey("default", "web"): planTestJob("web", 1, 2, "")}, []*Node{healthy},
					planTestAllocation("a", healthy, lifecycle.PhaseRunning, 1))
			},
			actions: []plannedAction{{ActionStop, "a"}, {ActionStart, "default-web-app-00000001"}},
			created: []plannedUpdate{{ID: "default-web-app-00000001", Phase: lifecycle.PhasePlaced}},
		},
		{
			name: "rolling strategy drains outdated revisions before replacing them",
			input: func() *reconcilePlanInput {
				return planTestInput(map[string]*Job{jobKey("default", "web"): planTestJob("web", 1, 2, spec.UpdateRolling)}, []*Node{healthy},
					planTestAllocation("a", healthy, lifecycle.PhaseRunning, 1))
			},
			actions: []plannedAction{{ActionDrain, "a"}, {ActionStart, "default-web-app-00000001"}},
			updates: []plannedUpdate{{ID: "a", Phase: lifecycle.PhaseRunning, Draining: true}},
			created: []plannedUpdate{{ID: "default-web-app-00000001", Phase: lifecycle.PhasePlaced}},
		},
		{
			name: "stops obsolete pending allocations",
			input: func() *reconcilePlanInput {
				pending := planTestAllocation("a", nil, lifecycle.PhasePending, 1)
				return planTestInput(map[string]*Job{jobKey("default", "web"): planTestJob("web", 1, 2, "")}, nil, pending)
			},
			updates: []plannedUpdate{{ID: "a", Phase: lifecycle.PhaseStopped, Reason: "job_changed"}},
		},
		{
			name: "marks allocations lost after the node loss timeout",
			input: func() *reconcilePlanInput {
				gone := planTestNode(2, NodeStatusUnhealthy)
				gone.LastHeartbeat = planNow.Add(-DefaultAllocationLossTimeout)
				return planTestInput(map[string]*Job{jobKey("default", "web"): planTestJob("web", 1, 1, "")}, []*Node{gone},
					planTestAllocation("a", gone, lifecycle.PhaseRunning, 1))
			},
			updates: []plannedUpdate{{ID: "a", Phase: lifecycle.PhaseLost, Reason: "node_unavailable"}},
		},
		{
			name: "keeps allocations on a silent node within the leader recovery grace",
			input: func() *reconcilePlanInput {
				gone := planTestNode(2, NodeStatusUnhealthy)
				gone.LastHeartbeat = planNow.Add(-DefaultAllocationLossTimeout)
				input := planTestInput(map[string]*Job{jobKey("default", "web"): planTestJob("web", 1, 1, "")}, []*Node{gone},
					planTestAllocation("a", gone, lifecycle.PhaseRunning, 1))
				input.LeaderSince = planNow.Add(-time.Second)
				return input
			},
		},
		{
			name: "evacuates allocations from a draining node",
			input: func() *reconcilePlanInput {
				draining := planTestNode(2, NodeStatusDraining)
				return planTestInput(map[string]*Job{jobKey("default", "web"): planTestJob("web", 1, 1, "")}, []*Node{healthy, draining},
					planTestAllocation("a", draining, lifecycle.PhaseRunning, 1))
			},
			actions: []plannedAction{{ActionDrain, "a"}, {ActionStart, "default-web-app-00000001"}},
			updates: []plannedUpdate{{ID: "a", Phase: lifecycle.PhaseRunning, Draining: true}},
			created: []plannedUpdate{{ID: "default-web-app-00000001", Phase: lifecycle.PhasePlaced}},
		},
		{
			name: "redelivers an unacknowledged resume",
			input: func() *reconcilePlanInput {
				resumed := planTestAllocation("a", healthy, lifecycle.PhaseRunning, 1)
				resumed.DrainSequence = 2
				resumedAcknowledged := planTestAllocation("b", healthy, lifecycle.PhaseRunning, 1)
				resumedAcknowledged.DrainSequence = 2
				input := planTestInput(map[string]*Job{jobKey("default", "web"): planTestJob("web", 2, 1, "")}, []*Node{healthy}, resumed, resumedAcknowledged)
				input.DeliveredResumes = map[resumeDeliveryKey]uint64{{allocation: "b", generation: 1}: 2, {allocation: "a", generation: 1}: 1}
				return input
			},
			actions: []plannedAction{{ActionResume, "a"}},
		},
		{
			name: "stops observed allocations that are no longer desired",
			input: func() *reconcilePlanInput {
				observing := planTestNode(1, NodeStatusHealthy)
				observing.observedAllocations = []observedAllocation{{ID: "a", Generation: 1, Phase: lifecycle.PhaseRunning}, {ID: "orphan", Generation: 3, Phase: lifecycle.PhaseRunning}}
				return planTestInput(map[string]*Job{jobKey("default", "web"): planTestJob("web", 1, 1, "")}, []*Node{observing},
					planTestAllocation("a", observing, lifecycle.PhaseRunning, 1))
			},
			actions: []plannedAction{{ActionStopObserved, "orphan"}},
		},
		{
			name: "skips jobs that exceed operator limits and stops their allocations",
			input: func() *reconcilePlanInput {
				input := planTestInput(map[string]*Job{jobKey("default", "web"): planTestJob("web", 3, 1, "")}, []*Node{healthy},
					planTestAllocation("a", healthy, lifecycle.PhaseRunning, 1))
				input.Limits.MaxReplicasPerTaskGroup = 2
				return input
			},
			actions:     []plannedAction{{ActionStop, "a"}},
			diagnostics: []string{"skip invalid job during reconciliation"},
		},
		{
			name: "skips jobs beyond the namespace allocation limit",
			input: func() *reconcilePlanInput {
				input := planTestInput(map[string]*Job{
					jobKey("default", "first"):  planTestJob("first", 1, 1, ""),
					jobKey("default", "second"): planTestJob("second", 1, 1, ""),
				}, []*Node{healthy})
				input.Limits.MaxDesiredAllocationsPerNamespace = 1
				return input
			},
			actions:     []plannedAction{{ActionStart, "default-first-app-00000001"}},
			created:     []plannedUpdate{{ID: "default-first-app-00000001", Phase: lifecycle.PhasePlaced}},
			diagnostics: []string{"skip job exceeding namespace allocation limit during reconciliation"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan, err := planReconciliation(test.input())
			if err != nil {
				t.Fatalf("plan: %v", err)
			}
			if actions := summarizeActions(plan.Actions); !reflect.DeepEqual(actions, nonNil(test.actions)) {
				t.Errorf("actions = %+v, want %+v", actions, test.actions)
			}
			if updates := summarizeUpdates(plan.Updated); !reflect.DeepEqual(updates, nonNil(test.updates)) {
				t.Errorf("updates = %+v, want %+v", updates, test.updates)
			}
			if created := summarizeUpdates(plan.NewAllocations); !reflect.DeepEqual(created, nonNil(test.created)) {
				t.Errorf("created = %+v, want %+v", created, test.created)
			}
			if got := len(plan.Commit.Allocations); got != len(test.updates)+len(test.created) {
				t.Errorf("committed allocations = %d, want %d", got, len(test.updates)+len(test.created))
			}
			diagnostics := make([]string, 0, len(plan.Diagnostics))
			for _, diagnostic := range plan.Diagnostics {
				diagnostics = append(diagnostics, diagnostic.Message)
			}
			if !reflect.DeepEqual(diagnostics, nonNil(test.diagnostics)) {
				t.Errorf("diagnostics = %q, want %q", diagnostics, test.diagnostics)
			}
			for _, allocation := range plan.NewAllocations {
				if allocation.Node != nil && allocation.Node != healthy {
					t.Errorf("new allocation %s node is not the canonical input node", allocation.ID)
				}
			}
		})
	}
}

func nonNil[T any](values []T) []T {
	if values == nil {
		return []T{}
	}
	return values
}

func TestPlanReconciliationDoesNotMutateInputs(t *testing.T) {
	healthy := planTestNode(1, NodeStatusHealthy)
	job := planTestJob("web", 2, 2, spec.UpdateRolling)
	job.Spec.TaskGroups[0].Tasks[0].Volumes = []spec.VolumeSpec{{Name: "data", HostPath: "@/data", ContainerPath: "/data"}}
	outdated := planTestAllocation("a", healthy, lifecycle.PhaseRunning, 1)
	failed := planTestAllocation("b", healthy, lifecycle.PhaseFailed, 1)
	input := planTestInput(map[string]*Job{jobKey("default", "web"): job}, []*Node{healthy}, outdated, failed)
	input.Backoffs[replacementBackoffKey("default", "old", "app")] = &ReplacementBackoff{Namespace: "default", JobName: "old", TaskGroupName: "app", JobRevision: 1}
	before := func() string {
		raw, err := json.Marshal(struct {
			Jobs         map[string]*Job
			Nodes        map[uuid.UUID]*Node
			Allocations  []*Allocation
			Backoffs     map[string]*ReplacementBackoff
			VolumeOwners map[string]uuid.UUID
		}{input.Jobs, input.Nodes, input.Allocations, input.Backoffs, input.VolumeOwners})
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	snapshot := before()

	plan, err := planReconciliation(input)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if after := before(); after != snapshot {
		t.Fatalf("planning mutated its inputs:\nbefore %s\nafter  %s", snapshot, after)
	}
	if job.Spec.TaskGroups[0].Tasks[0].Resources != nil {
		t.Fatal("planning resolved resource defaults into the stored job spec")
	}
	if len(plan.NewAllocations) == 0 || plan.NewAllocations[0].Tasks[0].Resources == nil {
		t.Fatal("new allocation did not receive the canonical task resources")
	}
	if len(plan.Commit.VolumeRegistrations) != 1 || plan.Commit.VolumeRegistrations[0].Name != "data" {
		t.Fatalf("volume registrations = %#v, want the claimed volume", plan.Commit.VolumeRegistrations)
	}
	if len(plan.Commit.DeleteBackoffs) != 1 || plan.Commit.DeleteBackoffs[0].JobName != "old" {
		t.Fatalf("deleted backoffs = %#v, want the orphaned record", plan.Commit.DeleteBackoffs)
	}
	for _, updated := range plan.Updated {
		if plan.Source[updated] == nil || plan.Source[updated] == updated {
			t.Fatalf("updated allocation %s is not a working copy of an input snapshot", updated.ID)
		}
	}
}

func TestPlanReconciliationIsDeterministic(t *testing.T) {
	nodes := []*Node{planTestNode(3, NodeStatusHealthy), planTestNode(1, NodeStatusHealthy), planTestNode(2, NodeStatusDraining)}
	nodes[0].observedAllocations = []observedAllocation{{ID: "orphan-3", Generation: 1, Phase: lifecycle.PhaseRunning}}
	nodes[1].observedAllocations = []observedAllocation{{ID: "orphan-1", Generation: 1, Phase: lifecycle.PhaseRunning}}
	jobs := map[string]*Job{
		jobKey("default", "web"):   planTestJob("web", 3, 2, spec.UpdateRolling),
		jobKey("default", "batch"): planTestJob("batch", 2, 1, ""),
	}
	failed := planTestAllocation("f", nodes[1], lifecycle.PhaseFailed, 2)
	failed.TransitionedAt = planNow.Add(-time.Second)
	allocations := func() []*Allocation {
		return []*Allocation{
			planTestAllocation("c", nodes[2], lifecycle.PhaseRunning, 2),
			planTestAllocation("a", nodes[0], lifecycle.PhaseRunning, 1),
			failed,
			planTestAllocation("b", nodes[1], lifecycle.PhaseStarting, 2),
		}
	}
	encode := func(plan *reconcilePlan) string {
		raw, err := json.Marshal(struct {
			Commit  ReconciliationCommit
			Actions []plannedAction
			Events  []api.ClusterEvent
		}{plan.Commit, summarizeActions(plan.Actions), plan.Events})
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	var first string
	for i := 0; i < 20; i++ {
		plan, err := planReconciliation(planTestInput(jobs, nodes, allocations()...))
		if err != nil {
			t.Fatalf("plan: %v", err)
		}
		if i == 0 {
			first = encode(plan)
			if len(plan.Events) != 1 || plan.Events[0].Type != api.EventJobReplacementDelayed {
				t.Fatalf("events = %#v, want one delayed replacement", plan.Events)
			}
			continue
		}
		if got := encode(plan); got != first {
			t.Fatalf("plan %d differs:\n%s\nwant\n%s", i, got, first)
		}
	}
}
