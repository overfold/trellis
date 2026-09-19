package server

import (
	"context"
	"testing"

	"github.com/clofour/trellis/internal/lifecycle"
	"github.com/clofour/trellis/internal/spec"
	"github.com/clofour/trellis/internal/state"
)

type memoryStore map[string][]byte

func (m memoryStore) Get(_ context.Context, key string) ([]byte, error) { return m[key], nil }
func (m memoryStore) List(_ context.Context, prefix string) (map[string][]byte, error) {
	result := map[string][]byte{}
	for key, value := range m {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			result[key] = value
		}
	}
	return result, nil
}
func (m memoryStore) Put(_ context.Context, key string, value []byte) error {
	m[key] = value
	return nil
}
func (m memoryStore) Delete(_ context.Context, key string) error { delete(m, key); return nil }

var _ state.Store = memoryStore{}

func TestStateControllerRoundTripsDurableLeaderState(t *testing.T) {
	ctx := context.Background()
	controller := NewStateController(memoryStore{}, "test")
	job := &Job{Spec: &spec.JobSpec{Name: "web"}, Revision: 3}
	if err := controller.PutJob(ctx, "web", job); err != nil {
		t.Fatal(err)
	}
	jobs, err := controller.ListJobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if jobs["web"] == nil || jobs["web"].Revision != 3 {
		t.Fatalf("unexpected jobs: %#v", jobs)
	}
	allocation := &Allocation{ID: "web-1", JobName: "web", JobRevision: 3,
		Generation: 1,
		Phase:      lifecycle.PhasePlaced,
		Health:     lifecycle.HealthUnknown,
		Address:    "10.86.213.2"}
	if err := controller.PutAllocation(ctx, allocation); err != nil {
		t.Fatal(err)
	}
	allocations, err := controller.ListAllocations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if allocations["web-1"] == nil {
		t.Fatalf("unexpected allocations: %#v", allocations)
	}
	if allocations["web-1"].Address != "10.86.213.2" {
		t.Fatalf("allocation address was not persisted: %#v", allocations["web-1"])
	}
	if err := controller.DeleteAllocation(ctx, "web-1"); err != nil {
		t.Fatal(err)
	}
	allocations, err = controller.ListAllocations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(allocations) != 0 {
		t.Fatalf("allocation was not deleted: %#v", allocations)
	}
}
