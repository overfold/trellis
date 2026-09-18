package server

import (
	"testing"

	"github.com/clofour/trellis/internal/spec"
	"github.com/google/uuid"
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
	if owner := owners[volumeRegistrationKey("acme", "database")]; owner != placements[0].NodeID {
		t.Fatalf("first placement did not claim durable owner: got %s want %s", owner, placements[0].NodeID)
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
		// authority. Ownership comes from the map populated by Schedule.
		t.Fatalf("unexpected deterministic first placement: %#v", placements)
	}
	if owners[volumeRegistrationKey("acme", "database")] != a.ID {
		t.Fatalf("first placement was not recorded in ownership map: %#v", owners)
	}
}

func TestScheduleRespectsResourcesAndDrainingNodes(t *testing.T) {
	a := &Node{ID: uuid.New(), Status: NodeStatusHealthy, CPU: 1000, Memory: 1024}
	b := &Node{ID: uuid.New(), Status: NodeStatusHealthy, CPU: 2000, Memory: 2048}
	draining := &Node{ID: uuid.New(), Status: NodeStatusDraining, CPU: 10000, Memory: 10000}
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
	node.CPU, node.Memory = 1000, 1024
	tasks := []spec.TaskSpec{
		{Name: "app", Resources: &spec.ResourcesSpec{CPU: 600, Memory: 256}},
		{Name: "proxy", Resources: &spec.ResourcesSpec{CPU: 500, Memory: 256}},
	}
	placements := Schedule(&PlacementIntent{Count: 1, Nodes: []*Node{node}, Tasks: tasks})
	if len(placements) != 0 {
		t.Fatalf("placed a group whose aggregate task resources exceed the node: %#v", placements)
	}
}

func TestScheduleSpreadsTaskGroupReplicas(t *testing.T) {
	a := &Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000001"), Status: NodeStatusHealthy}
	b := &Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000002"), Status: NodeStatusHealthy}

	placements := Schedule(&PlacementIntent{Namespace: "default", JobName: "web", TaskGroupName: "api", Count: 2, Nodes: []*Node{a, b}})
	if len(placements) != 2 {
		t.Fatalf("got %d placements, want 2", len(placements))
	}
	if placements[0].NodeID == placements[1].NodeID {
		t.Fatalf("replicas were not spread: %#v", placements)
	}
}

func TestScheduleStacksReplicasWhenOnlyOneNodeFits(t *testing.T) {
	a := &Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000001"), Status: NodeStatusHealthy, CPU: 1000}
	b := &Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000002"), Status: NodeStatusHealthy, CPU: 50}
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
