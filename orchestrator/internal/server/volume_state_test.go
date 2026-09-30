package server

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/internal/spec"
)

func TestVolumeRegistrationRoundTripAndPinsScheduler(t *testing.T) {
	ctx := context.Background()
	controller := NewStateController(memoryStore{}, "test")
	owner := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	if err := controller.PutVolumeRegistration(ctx, &VolumeRegistration{Namespace: "acme", Name: "database", NodeID: owner}); err != nil {
		t.Fatal(err)
	}
	owners, err := controller.ListVolumeRegistrations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if owners[volumeRegistrationKey("acme", "database")] != owner {
		t.Fatalf("unexpected registrations: %#v", owners)
	}

	other := &Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000002"), Status: NodeStatusHealthy}
	tasks := []spec.TaskSpec{{Volumes: []spec.VolumeSpec{{Name: "database", HostPath: "@/database", ContainerPath: "/data"}}}}
	placements := Schedule(&PlacementIntent{Namespace: "acme", Count: 1, Nodes: []*Node{other}, Tasks: tasks, VolumeOwners: owners})
	if len(placements) != 0 {
		t.Fatalf("volume was reassigned while its owner was absent: %#v", placements)
	}
}
