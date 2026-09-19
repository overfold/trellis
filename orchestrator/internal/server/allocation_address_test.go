package server

import (
	"context"
	"testing"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/catalog"
	"github.com/clofour/trellis/internal/lifecycle"
	"github.com/clofour/trellis/internal/spec"
	"github.com/google/uuid"
)

func TestHeartbeatPublishesNamespaceAllocationAddress(t *testing.T) {
	ctx := context.Background()
	nodeID := uuid.MustParse("44444444-4444-4444-4444-444444444444")
	node := &Node{ID: nodeID, Host: "node-a", Status: NodeStatusHealthy}
	allocation := &Allocation{
		Namespace:     "demo-production",
		JobName:       "dmeo",
		TaskGroupName: "dmeo",
		ID:            "demo-production-dmeo-dmeo-deadbeef",
		Generation:    1,
		JobRevision:   1,
		Tasks: []spec.TaskSpec{{
			Name:       "dmeo",
			Networking: &spec.TaskNetworkingSpec{Mode: spec.TaskNetworkWireGuard},
		}},
		Phase:  lifecycle.PhaseRunning,
		Health: lifecycle.HealthUnknown,
		Node:   node,
	}
	s := &Server{
		state:       newNopStateController(),
		nodes:       map[uuid.UUID]*Node{nodeID: node},
		jobs: map[string]*Job{
			jobKey("demo-production", "dmeo"): {
				Spec: &spec.JobSpec{
					Namespace: "demo-production",
					Name:      "dmeo",
					TaskGroups: []spec.TaskGroupSpec{{
						Name:  "dmeo",
						Count: 1,
						Tasks: allocation.Tasks,
					}},
				},
				Revision: 1,
			},
		},
		allocations: []*Allocation{allocation},
		catalog:     catalog.New(),
	}

	err := s.Heartbeat(ctx, nodeID, []api.AllocationStatus{{
		ID:         allocation.ID,
		Generation: 1,
		Task:       "dmeo",
		Phase:      lifecycle.PhaseRunning,
		Health:     lifecycle.HealthHealthy,
		Address:    "10.86.213.2",
	}}, "", nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	allocations := s.ListAllocations("demo-production", nil)
	if len(allocations) != 1 {
		t.Fatalf("ListAllocations() = %#v, want one allocation", allocations)
	}
	if got := allocations[0].Address; got != "10.86.213.2" {
		t.Fatalf("allocation address = %q, want namespace endpoint 10.86.213.2", got)
	}

	status, ok := s.GetJob("demo-production", "dmeo")
	if !ok || len(status.Allocations) != 1 {
		t.Fatalf("GetJob() = %#v, %v; want one allocation", status, ok)
	}
	if got := status.Allocations[0].Address; got != "10.86.213.2" {
		t.Fatalf("job allocation address = %q, want namespace endpoint 10.86.213.2", got)
	}

	services := s.ListServices("demo-production", nil)
	if len(services) != 1 || services[0].Address != "10.86.213.2" {
		t.Fatalf("service catalog = %#v, want namespace endpoint 10.86.213.2", services)
	}
}

func TestAllocationEndpointAddressUsesNodeForHostNetworking(t *testing.T) {
	allocation := &Allocation{
		Node: &Node{Host: "node-a"},
		Tasks: []spec.TaskSpec{{
			Name:       "web",
			Networking: &spec.TaskNetworkingSpec{Mode: spec.TaskNetworkHost},
		}},
	}
	if got := allocationEndpointAddressLocked(allocation); got != "node-a" {
		t.Fatalf("allocationEndpointAddressLocked() = %q, want node-a", got)
	}
}

func TestAllocationEndpointAddressRejectsAmbiguousNamespaceTasks(t *testing.T) {
	allocation := &Allocation{
		Address: "10.86.213.2",
		Node:    &Node{Host: "node-a"},
		Tasks: []spec.TaskSpec{
			{Name: "app", Networking: &spec.TaskNetworkingSpec{Mode: spec.TaskNetworkWireGuard}},
			{Name: "sidecar", Networking: &spec.TaskNetworkingSpec{Mode: spec.TaskNetworkWireGuard}},
		},
	}
	if got := allocationEndpointAddressLocked(allocation); got != "" {
		t.Fatalf("allocationEndpointAddressLocked() = %q, want empty for multiple namespace task endpoints", got)
	}
}
