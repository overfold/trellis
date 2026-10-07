package server

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	"github.com/overfold/trellis/orchestrator/internal/spec"
)

var (
	lostNodeAID = uuid.MustParse("00000000-0000-0000-0000-00000000000a")
	lostNodeBID = uuid.MustParse("00000000-0000-0000-0000-00000000000b")
)

type lostReturnFixture struct {
	s        *Server
	agent    *testAgent
	nodeA    *Node
	original *Allocation
	tasks    []spec.TaskSpec
}

// newLostReturnFixture models a returning node A that still runs the container
// of a lost allocation. Node B exists only when withNodeB is set.
func newLostReturnFixture(t *testing.T, tasks []spec.TaskSpec, withNodeB bool) *lostReturnFixture {
	t.Helper()
	s, agent := newTestServerWithAgent()
	t.Cleanup(agent.server.Close)
	now := s.now()
	s.leaderSince = now.Add(-leaderRecoveryGrace - time.Second)
	nodeA := &Node{ID: lostNodeAID, Host: agent.host, Port: agent.port, Status: NodeStatusHealthy}
	addTestNode(s, nodeA, now)
	if withNodeB {
		addTestNode(s, &Node{ID: lostNodeBID, Host: agent.host, Port: agent.port, Status: NodeStatusHealthy}, now)
	}
	s.jobs[jobKey("default", "web")] = &Job{Spec: canonicalTestSpec(&spec.JobSpec{
		Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "app", Count: 1, Tasks: tasks}},
	}), Revision: 1}
	original := &Allocation{
		ID: "original", Namespace: "default", JobName: "web", TaskGroupName: "app", Tasks: tasks,
		Node: nodeA, Generation: 1, JobRevision: 1, Phase: lifecycle.PhaseLost, Health: lifecycle.HealthUnknown,
		Diagnostic: lifecycle.Diagnostic{CreatedAt: now, TransitionedAt: now, Reason: "node_unavailable"},
	}
	s.allocations = []*Allocation{original}
	return &lostReturnFixture{s: s, agent: agent, nodeA: nodeA, original: original, tasks: tasks}
}

func (f *lostReturnFixture) addAllocation(id string, node *Node, phase lifecycle.Phase) *Allocation {
	now := f.s.now()
	allocation := &Allocation{
		ID: id, Namespace: "default", JobName: "web", TaskGroupName: "app", Tasks: f.tasks,
		Node: node, Generation: 1, JobRevision: 1, Phase: phase, Health: lifecycle.HealthUnknown,
		Diagnostic: lifecycle.Diagnostic{CreatedAt: now, TransitionedAt: now},
	}
	if phase == lifecycle.PhaseRunning {
		allocation.Health = lifecycle.HealthHealthy
	}
	f.s.allocations = append(f.s.allocations, allocation)
	return allocation
}

// heartbeatA reports the original's container as running on node A.
func (f *lostReturnFixture) heartbeatA(t *testing.T) {
	t.Helper()
	statuses := []nodeapi.AllocationStatus{{ID: "original", Generation: 1, Task: "app", Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy}}
	if err := heartbeatAndApply(t, f.s, f.nodeA.ID, statuses, "test", nodeResourceObservation{}); err != nil {
		t.Fatal(err)
	}
	if f.original.Phase != lifecycle.PhaseLost {
		t.Fatalf("original phase after heartbeat = %s, want lost", f.original.Phase)
	}
}

func (f *lostReturnFixture) pinToNodeA(t *testing.T) {
	t.Helper()
	if err := f.s.state.PutVolumeRegistration(context.Background(), &VolumeRegistration{Namespace: "default", Name: "data", NodeID: f.nodeA.ID}); err != nil {
		t.Fatal(err)
	}
}

func (f *lostReturnFixture) replacements() []*Allocation {
	var result []*Allocation
	for _, allocation := range activeAllocations(f.s) {
		if allocation.ID != "original" {
			result = append(result, allocation)
		}
	}
	return result
}

type recordedOperation struct {
	method, id string
	generation uint64
}

func recordedOperations(t *testing.T, agent *testAgent) []recordedOperation {
	t.Helper()
	var operations []recordedOperation
	for _, call := range agent.recordedCalls() {
		switch {
		case call.method == http.MethodDelete:
			var request nodeapi.StopAllocationRequest
			if err := json.Unmarshal(call.body, &request); err != nil {
				t.Fatal(err)
			}
			operations = append(operations, recordedOperation{method: call.method, id: request.AllocationID, generation: request.Generation})
		case call.method == http.MethodPost && call.path == "/v1/allocations":
			var request nodeapi.AllocationRequest
			if err := json.Unmarshal(call.body, &request); err != nil {
				t.Fatal(err)
			}
			operations = append(operations, recordedOperation{method: call.method, id: request.AllocationID, generation: request.Generation})
		default:
			operations = append(operations, recordedOperation{method: call.method, id: call.path})
		}
	}
	return operations
}

// originalStops counts the stops of the lost original's container.
func originalStops(operations []recordedOperation) int {
	stops := 0
	for _, operation := range operations {
		if operation.method == http.MethodDelete && operation.id == "original" {
			stops++
		}
	}
	return stops
}

func startsOf(operations []recordedOperation) []recordedOperation {
	var starts []recordedOperation
	for _, operation := range operations {
		if operation.method == http.MethodPost {
			starts = append(starts, operation)
		}
	}
	return starts
}

func TestReconcileKeepsLostOriginalUntilReplacementRuns(t *testing.T) {
	tasks := []spec.TaskSpec{{Name: "app", Image: "app"}}
	t.Run("replacement starting on another node", func(t *testing.T) {
		f := newLostReturnFixture(t, tasks, true)
		f.addAllocation("replacement", f.s.nodes[lostNodeBID], lifecycle.PhaseStarting)
		f.heartbeatA(t)

		f.s.Reconcile(context.Background())
		operations := recordedOperations(t, f.agent)
		if originalStops(operations) != 0 {
			t.Fatalf("operations = %#v, want the original kept while its replacement starts", operations)
		}
		if f.original.Phase != lifecycle.PhaseLost {
			t.Fatalf("original phase = %s, want lost", f.original.Phase)
		}
	})
	t.Run("replacement placed in the same pass", func(t *testing.T) {
		f := newLostReturnFixture(t, tasks, true)
		f.heartbeatA(t)

		f.s.Reconcile(context.Background())
		operations := recordedOperations(t, f.agent)
		if originalStops(operations) != 0 || len(startsOf(operations)) != 1 {
			t.Fatalf("operations = %#v, want one replacement start and the original kept", operations)
		}
		if replacements := f.replacements(); len(replacements) != 1 {
			t.Fatalf("replacements = %#v, want one replacement: lost does not count toward the group", replacements)
		}
	})
	t.Run("replacement pending for lack of capacity", func(t *testing.T) {
		f := newLostReturnFixture(t, []spec.TaskSpec{{Name: "app", Image: "app", Resources: &spec.ResourcesSpec{CPU: 600, Memory: 64 << 20}}}, false)
		f.heartbeatA(t)
		// Node A cannot fit the replacement even without the original.
		f.nodeA.CPUAllocatable = 500

		f.s.Reconcile(context.Background())
		if operations := recordedOperations(t, f.agent); len(operations) != 0 {
			t.Fatalf("operations = %#v, want none: the original is kept and nothing fits", operations)
		}
		if replacements := f.replacements(); len(replacements) != 1 || replacements[0].Phase != lifecycle.PhasePending || replacements[0].Reason != "insufficient_capacity" {
			t.Fatalf("replacements = %#v, want one pending capacity diagnostic", replacements)
		}
	})
	t.Run("original reported starting is not retained", func(t *testing.T) {
		f := newLostReturnFixture(t, tasks, true)
		if err := heartbeatAndApply(t, f.s, f.nodeA.ID, []nodeapi.AllocationStatus{
			{ID: "original", Generation: 1, Task: "app", Phase: lifecycle.PhaseStarting, Health: lifecycle.HealthUnknown},
		}, "test", nodeResourceObservation{}); err != nil {
			t.Fatal(err)
		}

		f.s.Reconcile(context.Background())
		if operations := recordedOperations(t, f.agent); originalStops(operations) != 1 {
			t.Fatalf("operations = %#v, want the non-running original stopped", operations)
		}
	})
}

func TestReconcileStopsLostOriginalOnceReplacementRuns(t *testing.T) {
	f := newLostReturnFixture(t, []spec.TaskSpec{{Name: "app", Image: "app"}}, true)
	replacement := f.addAllocation("replacement", f.s.nodes[lostNodeBID], lifecycle.PhaseStarting)
	f.heartbeatA(t)
	f.s.Reconcile(context.Background())
	if operations := recordedOperations(t, f.agent); originalStops(operations) != 0 {
		t.Fatalf("operations before the replacement runs = %#v, want the original kept", operations)
	}

	replacement.Phase = lifecycle.PhaseRunning
	replacement.Health = lifecycle.HealthHealthy
	f.s.Reconcile(context.Background())
	operations := recordedOperations(t, f.agent)
	if originalStops(operations) != 1 {
		t.Fatalf("operations = %#v, want the original stopped once its replacement runs", operations)
	}
	for _, operation := range operations {
		if operation.method == http.MethodDelete && operation.id == "original" && operation.generation != 1 {
			t.Fatalf("stop = %#v, want original generation 1", operation)
		}
	}
	if f.original.Phase != lifecycle.PhaseStopped || replacement.Phase != lifecycle.PhaseRunning {
		t.Fatalf("phases after reconcile: original=%s replacement=%s, want stopped/running", f.original.Phase, replacement.Phase)
	}
}

func TestReconcileStopsLostOriginalOfOutdatedRecreateRevision(t *testing.T) {
	f := newLostReturnFixture(t, []spec.TaskSpec{{Name: "app", Image: "app"}}, true)
	f.s.jobs[jobKey("default", "web")].Revision = 2
	f.heartbeatA(t)

	f.s.Reconcile(context.Background())
	if operations := recordedOperations(t, f.agent); originalStops(operations) != 1 {
		t.Fatalf("operations = %#v, want the outdated original stopped as recreate stops outdated allocations", operations)
	}
}

func TestReconcileKeepsLostOriginalBesideReplacementPinnedToItsNode(t *testing.T) {
	tasks := []spec.TaskSpec{{Name: "app", Image: "app", Volumes: []spec.VolumeSpec{{Name: "data", HostPath: "@/data", ContainerPath: "/data"}}}}
	f := newLostReturnFixture(t, tasks, true)
	f.pinToNodeA(t)
	f.heartbeatA(t)

	f.s.Reconcile(context.Background())
	operations := recordedOperations(t, f.agent)
	if originalStops(operations) != 0 || len(startsOf(operations)) != 1 {
		t.Fatalf("operations = %#v, want the replacement started beside the kept original", operations)
	}
	replacements := f.replacements()
	if len(replacements) != 1 || replacements[0].Node == nil || replacements[0].Node.ID != f.nodeA.ID {
		t.Fatalf("replacements = %#v, want one replacement on the volume's node", replacements)
	}

	replacement := replacements[0]
	replacement.Phase = lifecycle.PhaseRunning
	replacement.Health = lifecycle.HealthHealthy
	f.s.Reconcile(context.Background())
	if operations := recordedOperations(t, f.agent); originalStops(operations) != 1 {
		t.Fatalf("operations = %#v, want the original stopped once the pinned replacement runs", operations)
	}
}

func TestReconcileLostOriginalHostPortConflict(t *testing.T) {
	portTasks := func(volume bool) []spec.TaskSpec {
		task := spec.TaskSpec{Name: "app", Image: "app", Networking: &spec.TaskNetworkingSpec{Mode: spec.TaskNetworkHost, Ports: []spec.PortSpec{{Port: 8080}}}}
		if volume {
			task.Volumes = []spec.VolumeSpec{{Name: "data", HostPath: "@/data", ContainerPath: "/data"}}
		}
		return []spec.TaskSpec{task}
	}

	t.Run("replacement avoids the original's port on another node", func(t *testing.T) {
		f := newLostReturnFixture(t, portTasks(false), true)
		f.heartbeatA(t)

		f.s.Reconcile(context.Background())
		operations := recordedOperations(t, f.agent)
		if originalStops(operations) != 0 || len(startsOf(operations)) != 1 {
			t.Fatalf("operations = %#v, want one replacement start and the original kept", operations)
		}
		if replacements := f.replacements(); len(replacements) != 1 || replacements[0].Node == nil || replacements[0].Node.ID != lostNodeBID {
			t.Fatalf("replacements = %#v, want the replacement on node B, away from the original's port", replacements)
		}
	})
	t.Run("replacement pinned to the original's node releases it first", func(t *testing.T) {
		f := newLostReturnFixture(t, portTasks(true), true)
		f.pinToNodeA(t)
		f.heartbeatA(t)

		f.s.Reconcile(context.Background())
		operations := recordedOperations(t, f.agent)
		if len(operations) != 2 || operations[0].method != http.MethodDelete || operations[0].id != "original" || operations[1].method != http.MethodPost {
			t.Fatalf("operations = %#v, want the original stopped before its pinned replacement starts", operations)
		}
		if replacements := f.replacements(); len(replacements) != 1 || replacements[0].Node == nil || replacements[0].Node.ID != f.nodeA.ID {
			t.Fatalf("replacements = %#v, want one replacement on the volume's node", replacements)
		}
	})
	t.Run("replacement already placed on the original's node", func(t *testing.T) {
		f := newLostReturnFixture(t, portTasks(false), false)
		f.addAllocation("replacement", f.nodeA, lifecycle.PhaseStarting)
		f.heartbeatA(t)

		f.s.Reconcile(context.Background())
		operations := recordedOperations(t, f.agent)
		if len(operations) == 0 || operations[0].method != http.MethodDelete || operations[0].id != "original" {
			t.Fatalf("operations = %#v, want the original stopped first so the placed replacement can bind its port", operations)
		}
	})
	t.Run("other group's replacement pinned to the original's node", func(t *testing.T) {
		f := newLostReturnFixture(t, portTasks(false), false)
		f.pinToNodeA(t)
		other := portTasks(true)
		other[0].Name = "worker"
		job := f.s.jobs[jobKey("default", "web")]
		job.Spec.TaskGroups = append(job.Spec.TaskGroups, spec.TaskGroupSpec{Name: "worker", Count: 1, Tasks: other})
		canonicalTestSpec(job.Spec)
		f.heartbeatA(t)

		f.s.Reconcile(context.Background())
		operations := recordedOperations(t, f.agent)
		if len(operations) != 2 || operations[0].method != http.MethodDelete || operations[0].id != "original" || operations[1].method != http.MethodPost {
			t.Fatalf("operations = %#v, want the blocking original stopped before the worker starts", operations)
		}
	})
}

func TestScheduleAroundRetainedDoesNotMutateInputs(t *testing.T) {
	node := &Node{ID: lostNodeAID, Status: NodeStatusHealthy}
	tasks := []spec.TaskSpec{{Name: "app", Image: "app", Networking: &spec.TaskNetworkingSpec{Mode: spec.TaskNetworkHost, Ports: []spec.PortSpec{{Port: 8080}}}, Volumes: []spec.VolumeSpec{{Name: "data", HostPath: "@/data", ContainerPath: "/data"}}}}
	original := &Allocation{ID: "original", Namespace: "default", JobName: "web", TaskGroupName: "app", Tasks: tasks, Node: node, Generation: 1, Phase: lifecycle.PhaseLost}
	owners := map[string]uuid.UUID{}
	valid := []*Allocation{}
	intent := PlacementIntent{Namespace: "default", JobName: "web", TaskGroupName: "app", Count: 1, Nodes: []*Node{node}, Allocations: valid, Tasks: tasks, VolumeOwners: owners}

	retained := []*retainedOriginal{{allocation: original, node: node}}
	placements, released := scheduleAroundRetained(intent, retained)
	if len(placements) != 1 || placements[0].NodeID != node.ID || len(released) != 1 || released[0].allocation != original {
		t.Fatalf("placements=%#v released=%#v, want one placement after releasing the original", placements, released)
	}
	if len(valid) != 0 || cap(valid) != 0 || original.Phase != lifecycle.PhaseLost {
		t.Fatalf("scheduler inputs changed: valid=%#v original=%s", valid, original.Phase)
	}
	if len(owners) != 0 {
		t.Fatalf("volume owners = %#v, want the caller's map untouched", owners)
	}
	if claims := placements[0].VolumeClaims; len(claims) != 1 || claims[0].Name != "data" || claims[0].NodeID != node.ID {
		t.Fatalf("volume claims = %#v, want the placement to claim the volume on its node", claims)
	}

	// The same inputs always yield the same decision.
	again := []*retainedOriginal{{allocation: original, node: node}}
	placementsAgain, releasedAgain := scheduleAroundRetained(intent, again)
	if len(placementsAgain) != len(placements) || len(releasedAgain) != len(released) {
		t.Fatalf("repeated decision differs: %#v/%#v", placementsAgain, releasedAgain)
	}
}

func TestReconcileAllocationLossTimeout(t *testing.T) {
	for _, tc := range []struct {
		name          string
		timeout       time.Duration
		leaderFor     time.Duration
		silentFor     time.Duration
		wantLost      bool
		wantNodeState NodeStatus
	}{
		{name: "default before timeout", silentFor: 44 * time.Second, leaderFor: time.Hour, wantNodeState: NodeStatusUnhealthy},
		{name: "default at timeout", silentFor: DefaultAllocationLossTimeout, leaderFor: time.Hour, wantLost: true, wantNodeState: NodeStatusUnhealthy},
		{name: "configured before timeout", timeout: 5 * time.Minute, silentFor: 4 * time.Minute, leaderFor: time.Hour, wantNodeState: NodeStatusUnhealthy},
		{name: "configured at timeout", timeout: 5 * time.Minute, silentFor: 5 * time.Minute, leaderFor: time.Hour, wantLost: true, wantNodeState: NodeStatusUnhealthy},
		{name: "leader recovery grace", silentFor: time.Hour, leaderFor: leaderRecoveryGrace - time.Second, wantNodeState: NodeStatusUnhealthy},
		// Heartbeat times are leader observations that are not replicated, so a
		// new leader measures a node's silence from the start of its own term.
		{name: "silence counted from term start", silentFor: time.Hour, leaderFor: DefaultAllocationLossTimeout - time.Second, wantNodeState: NodeStatusUnhealthy},
		{name: "silent for timeout since term start", silentFor: time.Hour, leaderFor: DefaultAllocationLossTimeout, wantLost: true, wantNodeState: NodeStatusUnhealthy},
		{name: "never heartbeated to this leader", silentFor: -1, leaderFor: DefaultAllocationLossTimeout, wantLost: true, wantNodeState: NodeStatusUnhealthy},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, agent := newTestServerWithAgent()
			defer agent.server.Close()
			if tc.timeout != 0 {
				s.reconciliation.AllocationLossTimeout = tc.timeout
			}
			now := s.now()
			s.leaderSince = now.Add(-tc.leaderFor)
			node := &Node{ID: lostNodeAID, Host: agent.host, Port: agent.port, Status: NodeStatusHealthy}
			var heartbeat time.Time
			if tc.silentFor >= 0 {
				heartbeat = now.Add(-tc.silentFor)
			}
			addTestNode(s, node, heartbeat)
			tasks := []spec.TaskSpec{{Name: "app", Image: "app"}}
			s.jobs[jobKey("default", "web")] = &Job{Spec: canonicalTestSpec(&spec.JobSpec{
				Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "app", Count: 1, Tasks: tasks}},
			}), Revision: 1}
			allocation := &Allocation{
				ID: "alloc", Namespace: "default", JobName: "web", TaskGroupName: "app", Tasks: tasks,
				Node: node, Generation: 1, JobRevision: 1, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy,
				Diagnostic: lifecycle.Diagnostic{CreatedAt: now, TransitionedAt: now},
			}
			s.allocations = []*Allocation{allocation}

			s.Reconcile(context.Background())
			if node.Status != tc.wantNodeState {
				t.Fatalf("node status = %s, want %s", node.Status, tc.wantNodeState)
			}
			if lost := allocation.Phase == lifecycle.PhaseLost; lost != tc.wantLost {
				t.Fatalf("allocation phase = %s, want lost=%t", allocation.Phase, tc.wantLost)
			}
		})
	}
}

func TestValidateReconciliationSettings(t *testing.T) {
	valid := DefaultReconciliationSettings()
	if err := valid.Validate(); err != nil {
		t.Fatalf("default settings are invalid: %v", err)
	}
	for name, mutate := range map[string]func(*ReconciliationSettings){
		"zero loss timeout":         func(r *ReconciliationSettings) { r.AllocationLossTimeout = 0 },
		"loss timeout below min":    func(r *ReconciliationSettings) { r.AllocationLossTimeout = MinAllocationLossTimeout - time.Second },
		"loss timeout above max":    func(r *ReconciliationSettings) { r.AllocationLossTimeout = MaxAllocationLossTimeout + time.Second },
		"backoff base below min":    func(r *ReconciliationSettings) { r.ReplacementBackoffBase = MinReplacementBackoff - 1 },
		"backoff max below base":    func(r *ReconciliationSettings) { r.ReplacementBackoffMax = r.ReplacementBackoffBase - 1 },
		"backoff max above max":     func(r *ReconciliationSettings) { r.ReplacementBackoffMax = MaxReplacementBackoff + 1 },
		"stable after below min":    func(r *ReconciliationSettings) { r.ReplacementStableAfter = MinReplacementStableAfter - 1 },
		"negative retention":        func(r *ReconciliationSettings) { r.TerminalAllocationRetention = -1 },
		"retention above the bound": func(r *ReconciliationSettings) { r.TerminalAllocationRetention = MaxTerminalAllocationRetention + 1 },
	} {
		settings := valid
		mutate(&settings)
		if err := settings.Validate(); err == nil {
			t.Errorf("%s: settings %+v accepted", name, settings)
		}
	}
	for name, mutate := range map[string]func(*ReconciliationSettings){
		"minimum loss timeout":  func(r *ReconciliationSettings) { r.AllocationLossTimeout = MinAllocationLossTimeout },
		"maximum loss timeout":  func(r *ReconciliationSettings) { r.AllocationLossTimeout = MaxAllocationLossTimeout },
		"equal backoff":         func(r *ReconciliationSettings) { r.ReplacementBackoffMax = r.ReplacementBackoffBase },
		"no terminal retention": func(r *ReconciliationSettings) { r.TerminalAllocationRetention = 0 },
		"maximum retention":     func(r *ReconciliationSettings) { r.TerminalAllocationRetention = MaxTerminalAllocationRetention },
	} {
		settings := valid
		mutate(&settings)
		if err := settings.Validate(); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	if DefaultAllocationLossTimeout != 45*time.Second || MinAllocationLossTimeout != 3*heartbeatInterval {
		t.Fatalf("defaults changed: default=%s min=%s", DefaultAllocationLossTimeout, MinAllocationLossTimeout)
	}
}
