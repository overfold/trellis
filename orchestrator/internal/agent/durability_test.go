package agent

import (
	"context"
	"errors"
	"testing"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/storage"
)

func TestEpochFenceSurvivesRestart(t *testing.T) {
	local := storage.NewLocalStorage(t.TempDir())
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	first := &Agent{local: local}
	if err := first.AcceptEpoch(7); err != nil {
		t.Fatal(err)
	}
	second := &Agent{local: local, allocations: map[string]*Allocation{}}
	var epoch uint64
	if err := local.Get("agent/control-epoch", &epoch); err != nil {
		t.Fatal(err)
	}
	second.epoch = epoch
	if err := second.AcceptEpoch(6); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("expected stale epoch, got %v", err)
	}
}

func TestPrepareStartRejectsObsoleteGenerationAndConflict(t *testing.T) {
	agent := &Agent{allocations: map[string]*Allocation{
		"task": {ID: "task", AllocationID: "alloc", Generation: 3, JobRevision: 7, ExecutionHash: "same"},
	}}
	if err := agent.PrepareStart(context.Background(), &api.AllocationRequest{AllocationID: "alloc", Generation: 2, Epoch: 1}); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("expected stale generation, got %v", err)
	}
	if err := agent.PrepareStart(context.Background(), &api.AllocationRequest{AllocationID: "alloc", Generation: 3, JobRevision: 7, Epoch: 1, ExecutionHash: "different"}); !errors.Is(err, ErrExecutionConflict) {
		t.Fatalf("expected metadata conflict, got %v", err)
	}
	if err := agent.PrepareStart(context.Background(), &api.AllocationRequest{AllocationID: "alloc", Generation: 3, JobRevision: 8, Epoch: 1, ExecutionHash: "same"}); !errors.Is(err, ErrExecutionConflict) {
		t.Fatalf("expected revision conflict, got %v", err)
	}
	if err := agent.PrepareStart(context.Background(), &api.AllocationRequest{AllocationID: "alloc", Generation: 3, JobRevision: 7, Epoch: 1, ExecutionHash: "same"}); err != nil {
		t.Fatalf("expected matching retry to succeed, got %v", err)
	}
}

func TestAgentMutationsRequirePositiveFences(t *testing.T) {
	agent := &Agent{
		allocations: map[string]*Allocation{
			"task": {ID: "task", AllocationID: "alloc", Generation: 1, Status: "running"},
		},
		operations: make(map[string]*allocationOperation),
	}

	tests := []struct {
		name string
		run  func() error
	}{
		{name: "start epoch", run: func() error {
			return agent.PrepareStart(context.Background(), &api.AllocationRequest{AllocationID: "alloc", Generation: 1})
		}},
		{name: "stop epoch", run: func() error {
			return agent.StopGroup(context.Background(), &api.StopAllocationRequest{AllocationID: "alloc", Generation: 1})
		}},
		{name: "drain epoch", run: func() error {
			return agent.DrainGroup(&api.DrainAllocationRequest{AllocationID: "alloc", Generation: 1})
		}},
		{name: "resume epoch", run: func() error {
			return agent.ResumeGroup(&api.DrainAllocationRequest{AllocationID: "alloc", Generation: 1})
		}},
		{name: "network plan epoch", run: func() error {
			return agent.UpdateNetworkPlan(context.Background(), &api.NetworkPlanRequest{Namespace: "default"})
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.run(); !errors.Is(err, ErrInvalidEpoch) {
				t.Fatalf("error = %v, want %v", err, ErrInvalidEpoch)
			}
		})
	}

	if err := agent.StopGroup(context.Background(), &api.StopAllocationRequest{AllocationID: "alloc", Epoch: 1}); !errors.Is(err, ErrInvalidGeneration) {
		t.Fatalf("zero-generation stop error = %v, want %v", err, ErrInvalidGeneration)
	}
	if agent.epoch != 0 {
		t.Fatalf("invalid stop advanced epoch to %d", agent.epoch)
	}
	if agent.allocations["task"] == nil {
		t.Fatal("invalid stop removed allocation")
	}
}
