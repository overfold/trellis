package server

import (
	"math"
	"slices"
	"testing"

	"github.com/google/uuid"
	"github.com/overfold/trellis/internal/spec"
)

func TestScheduleBalancesAndSkipsUnhealthyNodes(t *testing.T) {
	a := &Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000001"), Status: NodeStatusHealthy}
	b := &Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000002"), Status: NodeStatusHealthy}
	bad := &Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000003"), Status: NodeStatusUnhealthy}

	placements := Schedule(&PlacementIntent{Count: 4, Nodes: []*Node{bad, b, a}})
	if len(placements) != 4 {
		t.Fatalf("got %d placements, want 4", len(placements))
	}
	counts := map[uuid.UUID]int{}
	for _, placement := range placements {
		counts[placement.NodeID]++
	}
	if counts[a.ID] != 2 || counts[b.ID] != 2 || counts[bad.ID] != 0 {
		t.Fatalf("unexpected placement counts: %#v", counts)
	}
}

func TestScheduleRequiresNodeCapabilities(t *testing.T) {
	runsc := &Node{ID: uuid.New(), Status: NodeStatusHealthy, Capabilities: []spec.NodeCapability{spec.CapabilityRunsc}}
	plain := &Node{ID: uuid.New(), Status: NodeStatusHealthy}

	placements := Schedule(&PlacementIntent{Count: 1, Nodes: []*Node{plain, runsc}, RequiredCapabilities: []spec.NodeCapability{spec.CapabilityRunsc}})
	if len(placements) != 1 || placements[0].NodeID != runsc.ID {
		t.Fatalf("placements = %#v, want runsc node", placements)
	}
}

func TestScheduleRequiresRegisteredNamespaceVolumeOwner(t *testing.T) {
	a := &Node{ID: uuid.New(), Status: NodeStatusHealthy}
	b := &Node{ID: uuid.New(), Status: NodeStatusHealthy}
	tasks := []spec.TaskSpec{{Name: "app", Volumes: []spec.VolumeSpec{{Name: "uploads", HostPath: "@/uploads", ContainerPath: "/data"}}}}
	owners := map[string]uuid.UUID{volumeRegistrationKey("default", "uploads"): a.ID}
	placements := Schedule(&PlacementIntent{Namespace: "default", Count: 1, Nodes: []*Node{b, a}, Tasks: tasks, VolumeOwners: owners})
	if len(placements) != 1 || placements[0].NodeID != a.ID {
		t.Fatalf("expected placement on registered volume owner, got %#v", placements)
	}
}

func TestScheduleClaimsNewVolumeOnFirstPlacement(t *testing.T) {
	a := &Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000001"), Status: NodeStatusHealthy}
	b := &Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000002"), Status: NodeStatusHealthy}
	tasks := []spec.TaskSpec{{Name: "app", Volumes: []spec.VolumeSpec{{Name: "database", HostPath: "@/database", ContainerPath: "/data"}}}}
	owners := map[string]uuid.UUID{}
	placements := Schedule(&PlacementIntent{Namespace: "acme", Count: 2, Nodes: []*Node{a, b}, Tasks: tasks, VolumeOwners: owners})
	if len(placements) != 2 || placements[0].NodeID != placements[1].NodeID {
		t.Fatalf("volume-sharing replicas must stay on first owner: %#v", placements)
	}
	if len(owners) != 0 {
		t.Fatalf("Schedule mutated volume owners: %#v", owners)
	}
	wantClaim := VolumeClaim{Namespace: "acme", Name: "database", NodeID: placements[0].NodeID}
	if len(placements[0].VolumeClaims) != 1 || placements[0].VolumeClaims[0] != wantClaim {
		t.Fatalf("first placement claims = %#v, want %#v", placements[0].VolumeClaims, wantClaim)
	}
	if len(placements[1].VolumeClaims) != 0 {
		t.Fatalf("second placement repeated volume claim: %#v", placements[1].VolumeClaims)
	}
}

func TestScheduleDoesNotAssignNilVolumeOwners(t *testing.T) {
	intent := &PlacementIntent{Count: 1, Nodes: []*Node{{ID: uuid.New(), Status: NodeStatusHealthy}}}
	if placements := Schedule(intent); len(placements) != 1 {
		t.Fatalf("placements = %#v, want one", placements)
	}
	if intent.VolumeOwners != nil {
		t.Fatalf("Schedule assigned the nil volume owners map: %#v", intent.VolumeOwners)
	}
}

func TestScheduleIgnoresNodeAdvertisedVolumeWithoutDurableRegistration(t *testing.T) {
	a := &Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000001"), Status: NodeStatusHealthy, Volumes: []string{"acme/database"}}
	b := &Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000002"), Status: NodeStatusHealthy}
	tasks := []spec.TaskSpec{{Volumes: []spec.VolumeSpec{{Name: "database", HostPath: "@/database", ContainerPath: "/data"}}}}
	owners := map[string]uuid.UUID{}
	placements := Schedule(&PlacementIntent{Namespace: "acme", Count: 1, Nodes: []*Node{b, a}, Tasks: tasks, VolumeOwners: owners})
	if len(placements) != 1 {
		t.Fatalf("expected first placement, got %#v", placements)
	}
	if placements[0].NodeID != a.ID {
		// Deterministic node ordering chooses a here because of the fixed UUIDs;
		// the assertion makes clear that the stale advertisement did not become
		// authority. Ownership comes from the explicit placement claim.
		t.Fatalf("unexpected deterministic first placement: %#v", placements)
	}
	if len(owners) != 0 {
		t.Fatalf("Schedule mutated volume owners: %#v", owners)
	}
	wantClaim := VolumeClaim{Namespace: "acme", Name: "database", NodeID: a.ID}
	if len(placements[0].VolumeClaims) != 1 || placements[0].VolumeClaims[0] != wantClaim {
		t.Fatalf("first placement claims = %#v, want %#v", placements[0].VolumeClaims, wantClaim)
	}
}

func TestScheduleResourceCapacityAtIntegerBoundaries(t *testing.T) {
	tests := []struct {
		name       string
		node       *Node
		existing   *spec.ResourcesSpec
		requested  *spec.ResourcesSpec
		wantPlaced bool
	}{
		{
			name:     "CPU exact MaxInt capacity",
			node:     &Node{ID: uuid.New(), Status: NodeStatusHealthy, CPUAllocatable: math.MaxInt},
			existing: &spec.ResourcesSpec{CPU: math.MaxInt - 1}, requested: &spec.ResourcesSpec{CPU: 1}, wantPlaced: true,
		},
		{
			name:     "CPU above MaxInt capacity",
			node:     &Node{ID: uuid.New(), Status: NodeStatusHealthy, CPUAllocatable: math.MaxInt},
			existing: &spec.ResourcesSpec{CPU: math.MaxInt}, requested: &spec.ResourcesSpec{CPU: 1}, wantPlaced: false,
		},
		{
			name:     "memory exact MaxInt64 capacity",
			node:     &Node{ID: uuid.New(), Status: NodeStatusHealthy, MemoryAllocatable: math.MaxInt64},
			existing: &spec.ResourcesSpec{Memory: spec.ByteSize(math.MaxInt64 - 1)}, requested: &spec.ResourcesSpec{Memory: 1}, wantPlaced: true,
		},
		{
			name:     "memory above MaxInt64 capacity",
			node:     &Node{ID: uuid.New(), Status: NodeStatusHealthy, MemoryAllocatable: math.MaxInt64},
			existing: &spec.ResourcesSpec{Memory: spec.ByteSize(math.MaxInt64)}, requested: &spec.ResourcesSpec{Memory: 1}, wantPlaced: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			existing := &Allocation{Node: tt.node, Tasks: []spec.TaskSpec{{Resources: tt.existing}}}
			placements := Schedule(&PlacementIntent{Count: 1, Nodes: []*Node{tt.node}, Allocations: []*Allocation{existing}, Tasks: []spec.TaskSpec{{Resources: tt.requested}}})
			if gotPlaced := len(placements) == 1; gotPlaced != tt.wantPlaced {
				t.Fatalf("placements = %#v, want placed %t", placements, tt.wantPlaced)
			}
		})
	}
}

func TestScheduleRejectsOverflowingTaskGroupResources(t *testing.T) {
	tests := []struct {
		name  string
		node  *Node
		tasks []spec.TaskSpec
	}{
		{
			name: "CPU",
			node: &Node{ID: uuid.New(), Status: NodeStatusHealthy, CPUAllocatable: math.MaxInt},
			tasks: []spec.TaskSpec{
				{Resources: &spec.ResourcesSpec{CPU: math.MaxInt}},
				{Resources: &spec.ResourcesSpec{CPU: 1}},
			},
		},
		{
			name: "memory",
			node: &Node{ID: uuid.New(), Status: NodeStatusHealthy, MemoryAllocatable: math.MaxInt64},
			tasks: []spec.TaskSpec{
				{Resources: &spec.ResourcesSpec{Memory: spec.ByteSize(math.MaxInt64)}},
				{Resources: &spec.ResourcesSpec{Memory: 1}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if placements := Schedule(&PlacementIntent{Count: 1, Nodes: []*Node{tt.node}, Tasks: tt.tasks}); len(placements) != 0 {
				t.Fatalf("placed a task group with overflowing resource total: %#v", placements)
			}
		})
	}
}

func TestPlacementUtilizationDoesNotWrapAtIntegerBoundaries(t *testing.T) {
	cpuNode := &Node{CPUAllocatable: math.MaxInt}
	if utilization := placementUtilization(cpuNode, math.MaxInt, 1, 0, 0); !math.IsInf(utilization, 1) {
		t.Fatalf("overflowing CPU utilization = %v, want +Inf", utilization)
	}
	memoryNode := &Node{MemoryAllocatable: math.MaxInt64}
	if utilization := placementUtilization(memoryNode, 0, 0, math.MaxInt64, 1); !math.IsInf(utilization, 1) {
		t.Fatalf("overflowing memory utilization = %v, want +Inf", utilization)
	}
}

func TestScheduleRespectsResourcesAndDrainingNodes(t *testing.T) {
	a := &Node{ID: uuid.New(), Status: NodeStatusHealthy, CPUAllocatable: 1000, MemoryAllocatable: 1024}
	b := &Node{ID: uuid.New(), Status: NodeStatusHealthy, CPUAllocatable: 2000, MemoryAllocatable: 2048}
	draining := &Node{ID: uuid.New(), Status: NodeStatusDraining, CPUAllocatable: 10000, MemoryAllocatable: 10000}
	tasks := []spec.TaskSpec{{Resources: &spec.ResourcesSpec{CPU: 750, Memory: 700}}}
	placements := Schedule(&PlacementIntent{Count: 4, Nodes: []*Node{a, b, draining}, Tasks: tasks})
	if len(placements) != 3 {
		t.Fatalf("got %d placements, want 3", len(placements))
	}
	counts := map[uuid.UUID]int{}
	for _, placement := range placements {
		counts[placement.NodeID]++
	}
	if counts[a.ID] != 1 || counts[b.ID] != 2 || counts[draining.ID] != 0 {
		t.Fatalf("unexpected placements: %#v", counts)
	}
}

func TestScheduleTreatsTaskGroupAsOneResourceUnit(t *testing.T) {
	node := &Node{ID: uuid.New(), Status: NodeStatusHealthy}
	node.CPUAllocatable, node.MemoryAllocatable = 1000, 1024
	tasks := []spec.TaskSpec{
		{Name: "app", Resources: &spec.ResourcesSpec{CPU: 600, Memory: 256}},
		{Name: "proxy", Resources: &spec.ResourcesSpec{CPU: 500, Memory: 256}},
	}
	placements := Schedule(&PlacementIntent{Count: 1, Nodes: []*Node{node}, Tasks: tasks})
	if len(placements) != 0 {
		t.Fatalf("placed a group whose aggregate task resources exceed the node: %#v", placements)
	}
}

func spreadTestNode(id string) *Node {
	return &Node{ID: uuid.MustParse(id), Status: NodeStatusHealthy, CPUAllocatable: 4000, MemoryAllocatable: 8 << 30}
}

func spreadTestTasks() []spec.TaskSpec {
	return []spec.TaskSpec{{Name: "server", Resources: &spec.ResourcesSpec{CPU: 500, Memory: 512 << 20}}}
}

func placementCounts(placements []Placement) map[uuid.UUID]int {
	counts := map[uuid.UUID]int{}
	for _, placement := range placements {
		counts[placement.NodeID]++
	}
	return counts
}

func TestScheduleSpreadsTaskGroupReplicas(t *testing.T) {
	a := spreadTestNode("00000000-0000-0000-0000-000000000001")
	b := spreadTestNode("00000000-0000-0000-0000-000000000002")
	c := spreadTestNode("00000000-0000-0000-0000-000000000003")

	placements := Schedule(&PlacementIntent{Namespace: "default", JobName: "web", TaskGroupName: "api", Count: 3, Nodes: []*Node{a, b, c}, Tasks: spreadTestTasks()})
	if len(placements) != 3 {
		t.Fatalf("got %d placements, want 3", len(placements))
	}
	counts := placementCounts(placements)
	if counts[a.ID] != 1 || counts[b.ID] != 1 || counts[c.ID] != 1 {
		t.Fatalf("replicas were not spread one per node: %#v", counts)
	}
}

func TestScheduleSpreadsAroundExistingReplicas(t *testing.T) {
	a := spreadTestNode("00000000-0000-0000-0000-000000000001")
	b := spreadTestNode("00000000-0000-0000-0000-000000000002")
	c := spreadTestNode("00000000-0000-0000-0000-000000000003")
	tasks := spreadTestTasks()
	replica := func(node *Node) *Allocation {
		return &Allocation{Namespace: "default", JobName: "web", TaskGroupName: "api", Node: node, Tasks: tasks}
	}
	existing := []*Allocation{replica(a), replica(a), replica(c)}

	placements := Schedule(&PlacementIntent{
		Namespace: "default", JobName: "web", TaskGroupName: "api", Count: 3,
		Nodes: []*Node{a, b, c}, Allocations: existing, DesiredAllocations: existing, Tasks: tasks,
	})
	counts := placementCounts(placements)
	if len(placements) != 3 || counts[a.ID] != 0 || counts[b.ID] != 2 || counts[c.ID] != 1 {
		t.Fatalf("placements = %#v, want two on the empty node and one on the node with one replica", counts)
	}
}

func TestScheduleDoesNotCountDrainingReplicasForSpread(t *testing.T) {
	a := spreadTestNode("00000000-0000-0000-0000-000000000001")
	b := spreadTestNode("00000000-0000-0000-0000-000000000002")
	c := spreadTestNode("00000000-0000-0000-0000-000000000003")
	tasks := spreadTestTasks()
	draining := &Allocation{Namespace: "default", JobName: "web", TaskGroupName: "api", Node: a, Tasks: tasks, Draining: true}
	current := &Allocation{Namespace: "default", JobName: "web", TaskGroupName: "api", Node: b, Tasks: tasks}
	allocations := []*Allocation{draining, current}

	placements := Schedule(&PlacementIntent{
		Namespace: "default", JobName: "web", TaskGroupName: "api", Count: 1,
		Nodes: []*Node{a, b, c}, Allocations: allocations, DesiredAllocations: allocations, Tasks: tasks,
	})
	// The draining replica frees its spread slot but still occupies
	// resources, so best fit prefers its node over the empty one.
	if len(placements) != 1 || placements[0].NodeID != a.ID {
		t.Fatalf("placements = %#v, want the node whose only replica is draining", placements)
	}
}

func TestScheduleUsesBestFitAmongEquallySpreadNodes(t *testing.T) {
	a := spreadTestNode("00000000-0000-0000-0000-000000000001")
	b := spreadTestNode("00000000-0000-0000-0000-000000000002")
	other := &Allocation{Namespace: "default", JobName: "db", TaskGroupName: "primary", Node: b, Tasks: []spec.TaskSpec{{Name: "db", Resources: &spec.ResourcesSpec{CPU: 2000, Memory: 1 << 30}}}}

	placements := Schedule(&PlacementIntent{
		Namespace: "default", JobName: "web", TaskGroupName: "api", Count: 1,
		Nodes: []*Node{a, b}, Allocations: []*Allocation{other}, Tasks: spreadTestTasks(),
	})
	if len(placements) != 1 || placements[0].NodeID != b.ID {
		t.Fatalf("placements = %#v, want the more utilized node", placements)
	}
}

func TestScheduleColocatesReplicasUnderCapacityPressure(t *testing.T) {
	large := spreadTestNode("00000000-0000-0000-0000-000000000001")
	small := spreadTestNode("00000000-0000-0000-0000-000000000002")
	small.CPUAllocatable = 500
	tasks := spreadTestTasks()

	placements := Schedule(&PlacementIntent{Namespace: "default", JobName: "web", TaskGroupName: "api", Count: 4, Nodes: []*Node{large, small}, Tasks: tasks})
	counts := placementCounts(placements)
	if len(placements) != 4 || counts[large.ID] != 3 || counts[small.ID] != 1 {
		t.Fatalf("placements = %#v, want one replica on the small node and the rest co-located on the large node", counts)
	}
}

func TestScheduleIsDeterministic(t *testing.T) {
	a := spreadTestNode("00000000-0000-0000-0000-000000000001")
	b := spreadTestNode("00000000-0000-0000-0000-000000000002")
	c := spreadTestNode("00000000-0000-0000-0000-000000000003")
	tasks := spreadTestTasks()
	intent := func(nodes ...*Node) *PlacementIntent {
		return &PlacementIntent{Namespace: "default", JobName: "web", TaskGroupName: "api", Count: 5, Nodes: nodes, Tasks: tasks}
	}

	nodes := []*Node{c, a, b}
	want := Schedule(intent(nodes...))
	wantOrder := []uuid.UUID{a.ID, b.ID, c.ID, a.ID, b.ID}
	if len(want) != len(wantOrder) {
		t.Fatalf("got %d placements, want %d", len(want), len(wantOrder))
	}
	for i, placement := range want {
		if placement.NodeID != wantOrder[i] {
			t.Fatalf("placement %d on %s, want %s (ties break by lowest node ID)", i, placement.NodeID, wantOrder[i])
		}
	}
	shuffled := Schedule(intent(b, c, a))
	for i := range want {
		if shuffled[i].NodeID != want[i].NodeID {
			t.Fatalf("node input order changed placement %d: %s != %s", i, shuffled[i].NodeID, want[i].NodeID)
		}
	}
	if nodes[0] != c || nodes[1] != a || nodes[2] != b {
		t.Fatal("Schedule reordered the intent's nodes")
	}
}

func TestScheduleSeparatesDesiredReplicasFromOccupancy(t *testing.T) {
	a := &Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000002"), Status: NodeStatusHealthy}
	b := &Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000001"), Status: NodeStatusHealthy}
	obsolete := &Allocation{Namespace: "default", JobName: "web", TaskGroupName: "api", Node: a}
	desired := &Allocation{Namespace: "default", JobName: "web", TaskGroupName: "api", Node: b}

	placements := Schedule(&PlacementIntent{
		Namespace: "default", JobName: "web", TaskGroupName: "api", Count: 1,
		Nodes:              []*Node{a, b},
		Allocations:        []*Allocation{obsolete, desired},
		DesiredAllocations: []*Allocation{desired},
	})
	if len(placements) != 1 || placements[0].NodeID != a.ID {
		t.Fatalf("placements = %#v, want node without a desired replica", placements)
	}
}

func TestScheduleAvoidsOccupiedHostPorts(t *testing.T) {
	a := &Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000001"), Status: NodeStatusHealthy}
	b := &Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000002"), Status: NodeStatusHealthy}
	tasks := []spec.TaskSpec{{Name: "app", Networking: &spec.TaskNetworkingSpec{Mode: spec.TaskNetworkHost, Ports: []spec.PortSpec{{Port: 8080}}}}}
	occupied := &Allocation{Node: a, Tasks: tasks}

	placements := Schedule(&PlacementIntent{Count: 2, Nodes: []*Node{a, b}, Allocations: []*Allocation{occupied}, Tasks: tasks})
	if len(placements) != 1 || placements[0].NodeID != b.ID {
		t.Fatalf("expected only the free node, got %#v", placements)
	}
}

func TestScheduleReservesHostPortsWithinBatch(t *testing.T) {
	a := &Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000001"), Status: NodeStatusHealthy}
	b := &Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000002"), Status: NodeStatusHealthy}
	tasks := []spec.TaskSpec{{Name: "app", Networking: &spec.TaskNetworkingSpec{Mode: spec.TaskNetworkHost, Ports: []spec.PortSpec{{Port: 8080}}}}}

	placements := Schedule(&PlacementIntent{Count: 3, Nodes: []*Node{a, b}, Tasks: tasks})
	if len(placements) != 2 || placements[0].NodeID != a.ID || placements[1].NodeID != b.ID {
		t.Fatalf("expected one placement per node, got %#v", placements)
	}
}

func TestScheduleEnforcesNodePortsAcrossNetworkModes(t *testing.T) {
	a := &Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000001"), Status: NodeStatusHealthy}
	b := &Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000002"), Status: NodeStatusHealthy}
	published := func(port, hostPort int) []spec.TaskSpec {
		return []spec.TaskSpec{{Name: "app", Networking: &spec.TaskNetworkingSpec{Mode: spec.TaskNetworkWireGuard, Ports: []spec.PortSpec{{Port: port, HostPort: hostPort}}}}}
	}
	host := func(port int) []spec.TaskSpec {
		return []spec.TaskSpec{{Name: "app", Networking: &spec.TaskNetworkingSpec{Mode: spec.TaskNetworkHost, Ports: []spec.PortSpec{{Port: port}}}}}
	}
	for _, test := range []struct {
		name      string
		occupying []spec.TaskSpec
		requested []spec.TaskSpec
		wantNodes []uuid.UUID
	}{
		{name: "published port conflicts with host port", occupying: host(80), requested: published(8080, 80), wantNodes: []uuid.UUID{b.ID}},
		{name: "host port conflicts with published port", occupying: published(8080, 80), requested: host(80), wantNodes: []uuid.UUID{b.ID}},
		{name: "published ports conflict", occupying: published(8080, 80), requested: published(9090, 80), wantNodes: []uuid.UUID{b.ID}},
		{name: "same listen port on different node ports", occupying: published(8080, 80), requested: published(8080, 81), wantNodes: []uuid.UUID{a.ID, b.ID}},
		{name: "listen port does not occupy the node", occupying: published(8080, 80), requested: host(8080), wantNodes: []uuid.UUID{a.ID, b.ID}},
	} {
		t.Run(test.name, func(t *testing.T) {
			occupied := &Allocation{Node: a, Tasks: test.occupying}
			placements := Schedule(&PlacementIntent{Count: 2, Nodes: []*Node{a, b}, Allocations: []*Allocation{occupied}, Tasks: test.requested})
			var nodes []uuid.UUID
			for _, placement := range placements {
				nodes = append(nodes, placement.NodeID)
			}
			if !slices.Equal(nodes, test.wantNodes) {
				t.Fatalf("placements = %v, want %v", nodes, test.wantNodes)
			}
		})
	}
}

func TestScheduleStacksReplicasWhenOnlyOneNodeFits(t *testing.T) {
	a := &Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000001"), Status: NodeStatusHealthy, CPUAllocatable: 1000}
	b := &Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000002"), Status: NodeStatusHealthy, CPUAllocatable: 50}
	tasks := []spec.TaskSpec{{Name: "server", Resources: &spec.ResourcesSpec{CPU: 100}}}

	placements := Schedule(&PlacementIntent{Namespace: "default", JobName: "web", TaskGroupName: "api", Count: 2, Nodes: []*Node{a, b}, Tasks: tasks})
	if len(placements) != 2 {
		t.Fatalf("got %d placements, want 2", len(placements))
	}
	for _, placement := range placements {
		if placement.NodeID != a.ID {
			t.Fatalf("replica placed on node without capacity: %#v", placements)
		}
	}
}

func TestScheduleFiltersNodesByConstraints(t *testing.T) {
	amd64 := &Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000001"), Status: NodeStatusHealthy, OS: "linux", Arch: "amd64"}
	arm64 := &Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000002"), Status: NodeStatusHealthy, OS: "linux", Arch: "arm64"}

	placements := Schedule(&PlacementIntent{
		Count: 1, Nodes: []*Node{amd64, arm64},
		Constraints: []spec.ConstraintSpec{{Attribute: "os", Value: "linux"}, {Attribute: "arch", Value: "arm64"}},
	})
	if len(placements) != 1 || placements[0].NodeID != arm64.ID {
		t.Fatalf("unexpected placements: %#v", placements)
	}
}

func TestScheduleProducesNoPlacementsWithoutConstraintMatch(t *testing.T) {
	node := &Node{ID: uuid.New(), Status: NodeStatusHealthy, OS: "linux", Arch: "amd64"}
	placements := Schedule(&PlacementIntent{
		Count: 1, Nodes: []*Node{node},
		Constraints: []spec.ConstraintSpec{{Attribute: "os", Value: "windows"}},
	})
	if len(placements) != 0 {
		t.Fatalf("unexpected placements: %#v", placements)
	}
}

func TestScheduleFiltersNodesByLabel(t *testing.T) {
	gpu := &Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000001"), Status: NodeStatusHealthy, OS: "linux", Arch: "amd64", Labels: map[string]string{"gpu": "true", "region": "us-east"}}
	cpu := &Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000002"), Status: NodeStatusHealthy, OS: "linux", Arch: "amd64", Labels: map[string]string{"region": "us-east"}}

	placements := Schedule(&PlacementIntent{
		Count: 1, Nodes: []*Node{gpu, cpu},
		Constraints: []spec.ConstraintSpec{{Attribute: "gpu", Value: "true"}},
	})
	if len(placements) != 1 || placements[0].NodeID != gpu.ID {
		t.Fatalf("unexpected placements: %#v", placements)
	}
}

func TestScheduleFiltersNodesByLabelAbsence(t *testing.T) {
	withLabel := &Node{ID: uuid.New(), Status: NodeStatusHealthy, Labels: map[string]string{"zone": "a"}}
	withoutLabel := &Node{ID: uuid.New(), Status: NodeStatusHealthy}

	// Count=2 with only one eligible node: both placements land on the matching node.
	placements := Schedule(&PlacementIntent{
		Count: 2, Nodes: []*Node{withLabel, withoutLabel},
		Constraints: []spec.ConstraintSpec{{Attribute: "zone", Value: "a"}},
	})
	if len(placements) != 2 {
		t.Fatalf("expected 2 placements, got %d: %#v", len(placements), placements)
	}
	for _, p := range placements {
		if p.NodeID != withLabel.ID {
			t.Fatalf("placement landed on unlabeled node: %#v", placements)
		}
	}
}

func TestScheduleCombinesBuiltinAndLabelConstraints(t *testing.T) {
	match := &Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000001"), Status: NodeStatusHealthy, OS: "linux", Arch: "amd64", Labels: map[string]string{"disk": "ssd"}}
	wrongDisk := &Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000002"), Status: NodeStatusHealthy, OS: "linux", Arch: "amd64", Labels: map[string]string{"disk": "hdd"}}
	wrongArch := &Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000003"), Status: NodeStatusHealthy, OS: "linux", Arch: "arm64", Labels: map[string]string{"disk": "ssd"}}

	placements := Schedule(&PlacementIntent{
		Count: 1, Nodes: []*Node{match, wrongDisk, wrongArch},
		Constraints: []spec.ConstraintSpec{
			{Attribute: "arch", Value: "amd64"},
			{Attribute: "disk", Value: "ssd"},
		},
	})
	if len(placements) != 1 || placements[0].NodeID != match.ID {
		t.Fatalf("unexpected placements: %#v", placements)
	}
}
