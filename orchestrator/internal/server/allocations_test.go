package server

import (
	"context"
	"testing"
	"time"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/catalog"
	"github.com/clofour/trellis/internal/lifecycle"
	"github.com/clofour/trellis/internal/spec"
	"github.com/google/uuid"
)

func TestListAllocationsWithFilters(t *testing.T) {
	acmeNode := &Node{ID: uuid.MustParse("11111111-1111-1111-1111-111111111111"), Host: "10.0.0.1"}
	stagingNode := &Node{ID: uuid.MustParse("22222222-2222-2222-2222-222222222222"), Host: "10.0.1.1"}

	s := &Server{
		jobs: map[string]*Job{
			jobKey("acme", "web"): {
				Spec: &spec.JobSpec{Namespace: "acme", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "frontend", Labels: map[string]string{"trellis.expose": "true", "trellis/domain": "example.com"}}}},
			},
			jobKey("acme", "db"): {
				Spec: &spec.JobSpec{Namespace: "acme", Name: "db", TaskGroups: []spec.TaskGroupSpec{{Name: "primary", Labels: map[string]string{"trellis/engine": "postgres"}}}},
			},
			jobKey("staging", "web"): {
				Spec: &spec.JobSpec{Namespace: "staging", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "frontend", Labels: map[string]string{"trellis.expose": "true"}}}},
			},
		},
		allocations: []*Allocation{
			{Namespace: "acme", JobName: "web", TaskGroupName: "frontend", ID: "acme-web-1", Generation: 1, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy, Node: acmeNode},
			{Namespace: "acme", JobName: "db", TaskGroupName: "primary", ID: "acme-db-1", Generation: 1, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy, Node: acmeNode},
			{Namespace: "staging", JobName: "web", TaskGroupName: "frontend", ID: "staging-web-1", Generation: 1, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy, Node: stagingNode},
		},
	}

	if got := s.ListAllocations("acme", nil); len(got) != 2 {
		t.Fatalf("expected 2 acme allocations, got %d", len(got))
	}
	if got := s.ListAllocations("acme", &AllocationListFilter{Job: "web"}); len(got) != 1 || got[0].Job != "web" {
		t.Fatalf("expected one web allocation, got %#v", got)
	}
	if got := s.ListAllocations("acme", &AllocationListFilter{Label: "trellis.expose:true"}); len(got) != 1 || got[0].Labels["trellis/domain"] != "example.com" {
		t.Fatalf("expected exposed allocation with domain label, got %#v", got)
	}
	if got := s.ListAllocations("acme", &AllocationListFilter{Label: "trellis/engine"}); len(got) != 1 || got[0].Job != "db" {
		t.Fatalf("expected db allocation by label existence, got %#v", got)
	}
	if got := s.ListAllocations("", &AllocationListFilter{Job: "web", Label: "trellis.expose:true"}); len(got) != 2 {
		t.Fatalf("expected two exposed web allocations across namespaces, got %d", len(got))
	}
}

func TestListJobsIncludesAllocationDiagnostics(t *testing.T) {
	nodeID := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	s := &Server{
		jobs: map[string]*Job{
			jobKey("default", "web"): {
				Spec: &spec.JobSpec{
					Namespace:  "default",
					Name:       "web",
					TaskGroups: []spec.TaskGroupSpec{{Name: "frontend", Count: 1}},
				},
				Revision: 2,
			},
		},
		allocations: []*Allocation{
			{
				Namespace:     "default",
				JobName:       "web",
				TaskGroupName: "frontend",
				ID:            "default-web-frontend-deadbeef",
				Node:          &Node{ID: nodeID},
				Generation:    3,
				JobRevision:   2,
				Phase:         lifecycle.PhaseFailed,
				Health:        lifecycle.HealthUnhealthy,
				Diagnostic: lifecycle.Diagnostic{
					Reason:  "start_failed",
					Message: "container exited",
					Attempt: 2,
				},
			},
			{
				Namespace:     "default",
				JobName:       "web",
				TaskGroupName: "frontend",
				ID:            "default-web-frontend-old",
				JobRevision:   1,
				Phase:         lifecycle.PhaseRunning,
				Health:        lifecycle.HealthHealthy,
				Draining:      true,
			},
		},
	}

	jobs := s.ListJobs("default")
	if len(jobs) != 1 || len(jobs[0].Allocations) != 2 {
		t.Fatalf("expected two job allocations, got %#v", jobs)
	}
	if jobs[0].Running != 0 || jobs[0].Healthy != 0 {
		t.Fatalf("old draining allocation counted toward revision: %#v", jobs[0])
	}
	allocation := jobs[0].Allocations[0]
	if allocation.JobRevision != 2 || allocation.NodeID != nodeID || allocation.Reason != "start_failed" || allocation.Message != "container exited" {
		t.Fatalf("allocation diagnostics missing from job list: %#v", allocation)
	}
}

func TestAllocationEvents(t *testing.T) {
	now := time.Now().UTC()
	alloc := &Allocation{Namespace: "acme", JobName: "web", TaskGroupName: "frontend", ID: "acme-web-1", Phase: lifecycle.PhasePlaced,
		Generation: 1,
		Health:     lifecycle.HealthUnknown}

	s := &Server{
		jobs:        map[string]*Job{},
		allocations: []*Allocation{alloc},
	}

	// No events yet.
	events, ok := s.AllocationEvents("acme", "acme-web-1")
	if !ok {
		t.Fatal("expected allocation to be found")
	}
	if len(events) != 0 {
		t.Fatalf("expected 0 events before any transition, got %d", len(events))
	}

	// Drive through a few transitions.
	if err := alloc.Transition(lifecycle.PhaseStarting, now, "scheduled", ""); err != nil {
		t.Fatal(err)
	}
	if err := alloc.Transition(lifecycle.PhaseRunning, now.Add(time.Second), "", ""); err != nil {
		t.Fatal(err)
	}
	if err := alloc.Transition(lifecycle.PhaseFailed, now.Add(2*time.Second), "oom", "container exited"); err != nil {
		t.Fatal(err)
	}

	events, ok = s.AllocationEvents("acme", "acme-web-1")
	if !ok {
		t.Fatal("expected allocation to be found")
	}
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}
	if events[0].Phase != lifecycle.PhaseStarting || events[0].Reason != "scheduled" {
		t.Errorf("event 0: unexpected %+v", events[0])
	}
	if events[1].Phase != lifecycle.PhaseRunning {
		t.Errorf("event 1: unexpected %+v", events[1])
	}
	if events[2].Phase != lifecycle.PhaseFailed || events[2].Reason != "oom" || events[2].Message != "container exited" {
		t.Errorf("event 2: unexpected %+v", events[2])
	}

	// Namespace mismatch returns not-found.
	if _, ok := s.AllocationEvents("staging", "acme-web-1"); ok {
		t.Fatal("expected not-found for wrong namespace")
	}

	// Unknown allocation returns not-found.
	if _, ok := s.AllocationEvents("acme", "nonexistent"); ok {
		t.Fatal("expected not-found for unknown allocation")
	}
}

func TestAllocationEndpointUsesObservedNetworkAddress(t *testing.T) {
	node := &Node{ID: uuid.MustParse("44444444-4444-4444-4444-444444444444"), Host: "node-a"}
	tasks := []spec.TaskSpec{{
		Name: "app",
		Networking: &spec.TaskNetworkingSpec{Mode: spec.TaskNetworkWireGuard},
	}}
	allocation := &Allocation{
		Namespace:     "demo",
		JobName:       "web",
		TaskGroupName: "web",
		ID:            "demo-web-1",
		Node:          node,
		Tasks:         tasks,
		Endpoints:     []api.AllocationEndpoint{{Task: "app", Address: "10.86.213.2"}},
		Generation:    1,
		JobRevision:   1,
		Phase:         lifecycle.PhaseRunning,
		Health:        lifecycle.HealthHealthy,
	}
	s := &Server{
		jobs: map[string]*Job{
			jobKey("demo", "web"): {
				Spec: &spec.JobSpec{Namespace: "demo", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "web", Count: 1, Tasks: tasks}}},
				Revision: 1,
			},
		},
		allocations: []*Allocation{allocation},
		catalog:     catalog.New(),
	}

	listed := s.ListAllocations("demo", nil)
	if len(listed) != 1 || listed[0].Address != "10.86.213.2" || len(listed[0].Endpoints) != 1 || listed[0].Endpoints[0].Task != "app" {
		t.Fatalf("allocation endpoint = %#v, want observed namespace task address", listed)
	}
	status, ok := s.GetJob("demo", "web")
	if !ok || len(status.Allocations) != 1 || status.Allocations[0].Address != "10.86.213.2" || len(status.Allocations[0].Endpoints) != 1 {
		t.Fatalf("job status endpoint = %#v, want observed namespace task endpoint", status)
	}

	s.refreshCatalog()
	services := s.ListServices("demo", nil)
	if len(services) != 1 || services[0].Address != "10.86.213.2" {
		t.Fatalf("catalog endpoint = %#v, want observed namespace address", services)
	}
}

func TestAllocationEndpointDoesNotFallBackForNamespaceNetworking(t *testing.T) {
	allocation := &Allocation{
		Namespace:     "demo",
		JobName:       "web",
		TaskGroupName: "web",
		ID:            "demo-web-1",
		Node:          &Node{ID: uuid.MustParse("55555555-5555-5555-5555-555555555555"), Host: "node-a"},
		Tasks: []spec.TaskSpec{{
			Name: "app",
			Networking: &spec.TaskNetworkingSpec{Mode: spec.TaskNetworkWireGuard},
		}},
		Generation: 1,
		Phase:      lifecycle.PhaseRunning,
		Health:     lifecycle.HealthHealthy,
	}
	s := &Server{allocations: []*Allocation{allocation}, catalog: catalog.New()}

	listed := s.ListAllocations("demo", nil)
	if len(listed) != 1 || listed[0].Address != "" {
		t.Fatalf("allocation endpoint = %#v, want no node fallback for namespace task", listed)
	}
	s.refreshCatalog()
	if services := s.ListServices("demo", nil); len(services) != 0 {
		t.Fatalf("catalog = %#v, want no endpoint until namespace address is observed", services)
	}
}

func TestAllocationEndpointFallsBackToNodeForHostNetworking(t *testing.T) {
	allocation := &Allocation{
		Namespace:     "demo",
		JobName:       "web",
		TaskGroupName: "web",
		ID:            "demo-web-1",
		Node:          &Node{ID: uuid.MustParse("66666666-6666-6666-6666-666666666666"), Host: "node-a"},
		Tasks: []spec.TaskSpec{{
			Name: "app",
			Networking: &spec.TaskNetworkingSpec{Mode: spec.TaskNetworkHost, Ports: []spec.PortSpec{{Port: 8080}}},
		}},
		Generation: 1,
		Phase:      lifecycle.PhaseRunning,
		Health:     lifecycle.HealthHealthy,
	}
	s := &Server{allocations: []*Allocation{allocation}}

	listed := s.ListAllocations("demo", nil)
	if len(listed) != 1 || listed[0].Address != "node-a" || len(listed[0].Endpoints) != 1 || len(listed[0].Endpoints[0].Ports) != 1 {
		t.Fatalf("allocation endpoint = %#v, want host task node fallback and port", listed)
	}
}

func TestAllocationEndpointKeepsDistinctTaskAddresses(t *testing.T) {
	allocation := &Allocation{
		Namespace:     "demo",
		JobName:       "web",
		TaskGroupName: "web",
		ID:            "demo-web-1",
		Node:          &Node{ID: uuid.MustParse("77777777-7777-7777-7777-777777777777"), Host: "node-a"},
		Tasks: []spec.TaskSpec{
			{Name: "app", Networking: &spec.TaskNetworkingSpec{Mode: spec.TaskNetworkWireGuard}},
			{Name: "sidecar", Networking: &spec.TaskNetworkingSpec{Mode: spec.TaskNetworkWireGuard}},
		},
		Endpoints: []api.AllocationEndpoint{
			{Task: "sidecar", Address: "10.86.213.17"},
			{Task: "app", Address: "10.86.213.2"},
		},
		Generation: 1,
		Phase:      lifecycle.PhaseRunning,
		Health:     lifecycle.HealthHealthy,
	}
	s := &Server{allocations: []*Allocation{allocation}, catalog: catalog.New()}

	listed := s.ListAllocations("demo", nil)
	if len(listed) != 1 || listed[0].Address != "" || len(listed[0].Endpoints) != 2 {
		t.Fatalf("allocation endpoint = %#v, want ambiguous legacy address plus two task endpoints", listed)
	}
	if listed[0].Endpoints[0].Task != "app" || listed[0].Endpoints[0].Address != "10.86.213.2" ||
		listed[0].Endpoints[1].Task != "sidecar" || listed[0].Endpoints[1].Address != "10.86.213.17" {
		t.Fatalf("task endpoints = %#v, want addresses matched by task", listed[0].Endpoints)
	}
	s.refreshCatalog()
	if services := s.ListServices("demo", nil); len(services) != 0 {
		t.Fatalf("catalog = %#v, want ambiguous allocation omitted from allocation-level discovery", services)
	}
}


func TestHeartbeatPreservesTaskEndpointIdentity(t *testing.T) {
	nodeID := uuid.MustParse("88888888-8888-8888-8888-888888888888")
	node := &Node{ID: nodeID, Host: "node-a", Status: NodeStatusHealthy}
	allocation := &Allocation{
		Namespace:     "demo",
		JobName:       "web",
		TaskGroupName: "web",
		ID:            "demo-web-1",
		Node:          node,
		Tasks: []spec.TaskSpec{
			{Name: "app", Networking: &spec.TaskNetworkingSpec{Mode: spec.TaskNetworkWireGuard}},
			{Name: "sidecar", Networking: &spec.TaskNetworkingSpec{Mode: spec.TaskNetworkWireGuard}},
		},
		Generation: 1,
		Phase:      lifecycle.PhaseRunning,
		Health:     lifecycle.HealthHealthy,
	}
	s := &Server{
		state:       NewStateController(memoryStore{}, "test"),
		nodes:       map[uuid.UUID]*Node{nodeID: node},
		allocations: []*Allocation{allocation},
		catalog:     catalog.New(),
	}
	actual := []api.AllocationStatus{
		{ID: allocation.ID, Generation: 1, Task: "sidecar", Address: "10.86.213.17", Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy},
		{ID: allocation.ID, Generation: 1, Task: "app", Address: "10.86.213.2", Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy},
	}
	if err := s.Heartbeat(context.Background(), nodeID, actual, "test", nil, nil); err != nil {
		t.Fatal(err)
	}
	if len(allocation.Endpoints) != 2 ||
		allocation.Endpoints[0].Task != "app" || allocation.Endpoints[0].Address != "10.86.213.2" ||
		allocation.Endpoints[1].Task != "sidecar" || allocation.Endpoints[1].Address != "10.86.213.17" {
		t.Fatalf("heartbeat endpoints = %#v, want stable task/address pairs", allocation.Endpoints)
	}

	actual[1].Address = ""
	if err := s.Heartbeat(context.Background(), nodeID, actual, "test", nil, nil); err != nil {
		t.Fatal(err)
	}
	if got := allocationEndpointAddress(allocation); got != "" {
		t.Fatalf("allocation address after missing task observation = %q, want empty", got)
	}
	if allocation.Endpoints[0].Address != "" {
		t.Fatalf("stale app address was retained: %#v", allocation.Endpoints)
	}
}

func TestHeartbeatRequiresEveryTaskForRunningHealth(t *testing.T) {
	nodeID := uuid.MustParse("99999999-9999-9999-9999-999999999999")
	node := &Node{ID: nodeID, Host: "node-a", Status: NodeStatusHealthy}
	allocation := &Allocation{
		ID: "demo-web-1", Node: node, Generation: 1,
		Tasks: []spec.TaskSpec{{Name: "app"}, {Name: "sidecar"}},
		Phase: lifecycle.PhaseStarting, Health: lifecycle.HealthUnknown,
	}
	s := &Server{
		state: NewStateController(memoryStore{}, "test"),
		nodes: map[uuid.UUID]*Node{nodeID: node}, allocations: []*Allocation{allocation},
		catalog: catalog.New(),
	}
	app := api.AllocationStatus{ID: allocation.ID, Generation: 1, Task: "app", Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy}
	sidecar := api.AllocationStatus{ID: allocation.ID, Generation: 1, Task: "sidecar", Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy}
	for _, actual := range [][]api.AllocationStatus{{app}, {app, app}} {
		if err := s.Heartbeat(context.Background(), nodeID, actual, "test", nil, nil); err != nil {
			t.Fatal(err)
		}
		if allocation.Phase != lifecycle.PhaseStarting || allocation.Health != lifecycle.HealthUnknown {
			t.Fatalf("partial heartbeat: phase=%s health=%s, want starting/unknown", allocation.Phase, allocation.Health)
		}
	}
	if err := s.Heartbeat(context.Background(), nodeID, []api.AllocationStatus{app, sidecar}, "test", nil, nil); err != nil {
		t.Fatal(err)
	}
	if allocation.Phase != lifecycle.PhaseRunning || allocation.Health != lifecycle.HealthHealthy {
		t.Fatalf("complete heartbeat: phase=%s health=%s, want running/healthy", allocation.Phase, allocation.Health)
	}
}

func TestHeartbeatMissingTaskRetriesRunningAllocation(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()

	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy, LastHeartbeat: s.now()}
	s.nodes[node.ID] = node
	tasks := []spec.TaskSpec{{Name: "app", Image: "app"}, {Name: "sidecar", Image: "sidecar"}}
	jobSpec := &spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "web", Count: 1, Tasks: tasks}}}
	s.jobs[jobKey("default", "web")] = &Job{Spec: jobSpec, Revision: 1}
	allocation := &Allocation{
		ID: "default-web-1", Namespace: "default", JobName: "web", TaskGroupName: "web",
		Node: node, Tasks: tasks, Generation: 1, JobRevision: 1,
		Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy,
	}
	s.allocations = []*Allocation{allocation}
	app := api.AllocationStatus{ID: allocation.ID, Generation: 1, Task: "app", Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy}
	sidecar := api.AllocationStatus{ID: allocation.ID, Generation: 1, Task: "sidecar", Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy}
	if err := s.Heartbeat(context.Background(), node.ID, []api.AllocationStatus{app}, "test", nil, nil); err != nil {
		t.Fatal(err)
	}
	if allocation.Phase != lifecycle.PhaseStarting || allocation.Health != lifecycle.HealthUnknown {
		t.Fatalf("partial heartbeat: phase=%s health=%s, want starting/unknown", allocation.Phase, allocation.Health)
	}
	persisted, err := s.state.ListAllocations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if persisted[allocation.ID] == nil || persisted[allocation.ID].Phase != lifecycle.PhaseStarting {
		t.Fatalf("persisted partial observation = %#v, want starting", persisted[allocation.ID])
	}

	s.Reconcile(context.Background())
	if allocation.Phase != lifecycle.PhaseRunning || len(s.allocations) != 1 {
		t.Fatalf("after retry: phase=%s allocations=%d, want running allocation reused", allocation.Phase, len(s.allocations))
	}
	if err := s.Heartbeat(context.Background(), node.ID, []api.AllocationStatus{app, sidecar}, "test", nil, nil); err != nil {
		t.Fatal(err)
	}
	if allocation.Phase != lifecycle.PhaseRunning || allocation.Health != lifecycle.HealthHealthy {
		t.Fatalf("complete heartbeat: phase=%s health=%s, want running/healthy", allocation.Phase, allocation.Health)
	}
}
