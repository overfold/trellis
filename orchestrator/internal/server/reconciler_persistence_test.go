package server

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/internal/lifecycle"
	"github.com/overfold/trellis/internal/spec"
	"github.com/overfold/trellis/internal/state"
)

type failingBatchStore struct {
	memoryStore
	batches [][]state.Mutation
}

func (s *failingBatchStore) Batch(_ context.Context, mutations []state.Mutation) error {
	s.batches = append(s.batches, append([]state.Mutation(nil), mutations...))
	return errors.New("storage unavailable")
}

type controlledBatchStore struct {
	memoryStore
	started chan struct{}
	release chan struct{}
	fail    bool
}

func (s *controlledBatchStore) Batch(ctx context.Context, mutations []state.Mutation) error {
	close(s.started)
	if s.release != nil {
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if s.fail {
		return errors.New("storage unavailable")
	}
	return s.memoryStore.Batch(ctx, mutations)
}

func newObsoletePendingReconcileServer(t *testing.T, store state.Store) (*Server, *Allocation) {
	t.Helper()
	controller := NewStateController(store, "test")
	s := NewServer(slog.Default(), nil, controller, store, "test", "")
	s.jobs[jobKey("default", "web")] = &Job{
		Spec: &spec.JobSpec{
			Namespace: "default",
			Name:      "web",
			TaskGroups: []spec.TaskGroupSpec{{
				Name:  "api",
				Count: 1,
				Tasks: []spec.TaskSpec{{Name: "server", Image: "app:v2"}},
			}},
		},
		Revision: 2,
	}
	allocation := &Allocation{
		ID:            "old",
		Namespace:     "default",
		JobName:       "web",
		TaskGroupName: "api",
		Tasks:         []spec.TaskSpec{{Name: "server", Image: "app:v1"}},
		Generation:    1,
		JobRevision:   1,
		Phase:         lifecycle.PhasePending,
		Health:        lifecycle.HealthUnknown,
	}
	s.allocations = []*Allocation{allocation}
	if err := controller.PutAllocation(context.Background(), allocation); err != nil {
		t.Fatalf("seed allocation: %v", err)
	}
	return s, allocation
}

func TestReconcileDoesNotHoldServerLockDuringAllocationPersistence(t *testing.T) {
	store := &controlledBatchStore{
		memoryStore: memoryStore{},
		started:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	s, allocation := newObsoletePendingReconcileServer(t, store)
	done := make(chan struct{})
	go func() {
		s.Reconcile(context.Background())
		close(done)
	}()

	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("reconciliation did not reach allocation persistence")
	}

	locked := make(chan struct{})
	go func() {
		s.mu.Lock()
		_ = len(s.allocations)
		s.mu.Unlock()
		close(locked)
	}()
	select {
	case <-locked:
	case <-time.After(time.Second):
		close(store.release)
		t.Fatal("server lock remained held while allocation persistence blocked")
	}
	if allocation.Phase != lifecycle.PhasePending {
		close(store.release)
		t.Fatalf("in-memory phase advanced before persistence: %s", allocation.Phase)
	}

	close(store.release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reconciliation did not complete after persistence resumed")
	}
	if allocation.Phase != lifecycle.PhaseStopped {
		t.Fatalf("in-memory phase after persistence = %s, want stopped", allocation.Phase)
	}
}

func TestReconcilePersistenceFailureDoesNotAdvanceMemory(t *testing.T) {
	store := &controlledBatchStore{
		memoryStore: memoryStore{},
		started:     make(chan struct{}),
		fail:        true,
	}
	s, allocation := newObsoletePendingReconcileServer(t, store)

	s.Reconcile(context.Background())

	if allocation.Phase != lifecycle.PhasePending {
		t.Fatalf("in-memory phase after failed persistence = %s, want pending", allocation.Phase)
	}
	persisted, err := s.state.ListAllocations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if persisted[allocation.ID] == nil || persisted[allocation.ID].Phase != lifecycle.PhasePending {
		t.Fatalf("persisted allocation after failed update = %#v, want pending", persisted[allocation.ID])
	}
}

func TestReconcileCommitsVolumeRegistrationWithAllocation(t *testing.T) {
	store := &failingBatchStore{memoryStore: memoryStore{}}
	controller := NewStateController(store, "test")
	s := NewServer(slog.Default(), nil, controller, store, "test", "")
	node := &Node{ID: uuid.New(), Status: NodeStatusHealthy, LastHeartbeat: time.Now()}
	s.nodes[node.ID] = node
	s.jobs[jobKey("acme", "database")] = &Job{
		Spec: &spec.JobSpec{
			Namespace: "acme",
			Name:      "database",
			TaskGroups: []spec.TaskGroupSpec{{
				Name:  "db",
				Count: 1,
				Tasks: []spec.TaskSpec{{
					Name:  "database",
					Image: "database:latest",
					Volumes: []spec.VolumeSpec{{
						Name:          "data",
						HostPath:      "@/data",
						ContainerPath: "/var/lib/database",
					}},
				}},
			}},
		},
		Revision: 1,
	}

	s.Reconcile(context.Background())

	if len(s.allocations) != 0 {
		t.Fatalf("in-memory allocations after failed commit = %d, want 0", len(s.allocations))
	}
	registrations, err := controller.ListVolumeRegistrations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(registrations) != 0 {
		t.Fatalf("volume registrations after failed commit = %v, want none", registrations)
	}
	allocations, err := controller.ListAllocations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(allocations) != 0 {
		t.Fatalf("persisted allocations after failed commit = %v, want none", allocations)
	}
	if len(store.batches) != 1 {
		t.Fatalf("batch count = %d, want 1", len(store.batches))
	}
	var allocationMutation, volumeMutation bool
	for _, mutation := range store.batches[0] {
		allocationMutation = allocationMutation || strings.Contains(mutation.Key, "/allocations/")
		volumeMutation = volumeMutation || strings.Contains(mutation.Key, "/volume-registrations/")
	}
	if !allocationMutation || !volumeMutation {
		t.Fatalf("reconciliation batch = %#v, want allocation and volume registration", store.batches[0])
	}
}

func TestReconcileCommitsNetworkPortRegistrationWithAllocation(t *testing.T) {
	store := &failingBatchStore{memoryStore: memoryStore{}}
	controller := NewStateController(store, "test")
	s := NewServer(slog.Default(), nil, controller, store, "test", "")
	s.wireGuardPortCount = 8
	s.networkPorts = map[string]int{"existing": 7}
	node := &Node{ID: uuid.New(), Status: NodeStatusHealthy, LastHeartbeat: time.Now(), Capabilities: []spec.NodeCapability{spec.CapabilityNamespaceNetworking}}
	s.nodes[node.ID] = node
	s.jobs[jobKey("acme", "web")] = &Job{
		Spec: &spec.JobSpec{
			Namespace: "acme",
			Name:      "web",
			TaskGroups: []spec.TaskGroupSpec{{
				Name:  "api",
				Count: 1,
				Tasks: []spec.TaskSpec{{Name: "server", Image: "app", Networking: &spec.TaskNetworkingSpec{Mode: spec.TaskNetworkWireGuard}}},
			}},
		},
		Revision: 1,
	}

	s.Reconcile(context.Background())

	if len(s.allocations) != 0 {
		t.Fatalf("in-memory allocations after failed commit = %d, want 0", len(s.allocations))
	}
	if !reflect.DeepEqual(s.networkPorts, map[string]int{"existing": 7}) {
		t.Fatalf("in-memory network ports after failed commit = %v, want unchanged", s.networkPorts)
	}
	registrations, err := controller.ListNetworkPortRegistrations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(registrations) != 0 {
		t.Fatalf("network port registrations after failed commit = %v, want none", registrations)
	}
	allocations, err := controller.ListAllocations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(allocations) != 0 {
		t.Fatalf("persisted allocations after failed commit = %v, want none", allocations)
	}
	if len(store.batches) != 1 {
		t.Fatalf("batch count = %d, want 1", len(store.batches))
	}
	var allocationMutation, networkPortMutation bool
	for _, mutation := range store.batches[0] {
		allocationMutation = allocationMutation || strings.Contains(mutation.Key, "/allocations/")
		networkPortMutation = networkPortMutation || strings.Contains(mutation.Key, "/network-port-registrations/")
	}
	if !allocationMutation || !networkPortMutation {
		t.Fatalf("reconciliation batch = %#v, want allocation and network port registration", store.batches[0])
	}
}

func TestReconcileAppliesVolumeClaimsAcrossTaskGroups(t *testing.T) {
	store := memoryStore{}
	controller := NewStateController(store, "test")
	s := NewServer(slog.Default(), nil, controller, store, "test", "")
	agent := newTestAgent()
	t.Cleanup(agent.server.Close)
	s.client = newTestAgentClient()
	a := &Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000001"), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy, LastHeartbeat: time.Now(), Labels: map[string]string{"zone": "a"}}
	b := &Node{ID: uuid.MustParse("00000000-0000-0000-0000-000000000002"), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy, LastHeartbeat: time.Now(), Labels: map[string]string{"zone": "b"}}
	s.nodes[a.ID], s.nodes[b.ID] = a, b
	volume := []spec.VolumeSpec{{Name: "data", HostPath: "@/data", ContainerPath: "/data"}}
	s.jobs[jobKey("acme", "database")] = &Job{
		Spec: &spec.JobSpec{
			Namespace: "acme",
			Name:      "database",
			TaskGroups: []spec.TaskGroupSpec{
				{Name: "first", Count: 1, Constraints: []spec.ConstraintSpec{{Attribute: "zone", Value: "a"}}, Tasks: []spec.TaskSpec{{Name: "first", Image: "app", Volumes: volume}}},
				{Name: "second", Count: 1, Constraints: []spec.ConstraintSpec{{Attribute: "zone", Value: "b"}}, Tasks: []spec.TaskSpec{{Name: "second", Image: "app", Volumes: volume}}},
			},
		},
		Revision: 1,
	}

	s.Reconcile(context.Background())

	if len(s.allocations) != 1 || s.allocations[0].Node != a {
		t.Fatalf("allocations = %#v, want only first task group on volume owner", s.allocations)
	}
	registrations, err := controller.ListVolumeRegistrations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if owner := registrations[volumeRegistrationKey("acme", "data")]; owner != a.ID {
		t.Fatalf("volume owner = %s, want %s", owner, a.ID)
	}
}
