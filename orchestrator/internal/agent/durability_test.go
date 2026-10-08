package agent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	"github.com/overfold/trellis/orchestrator/internal/runtime"
	"github.com/overfold/trellis/orchestrator/internal/storage"
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

func TestStopWatermarkSurvivesRestartAndLogPruning(t *testing.T) {
	for _, retain := range []bool{false, true} {
		record := recoveryTestAllocation(0)
		record.Generation = 3
		record.Ports = nil
		rt := &retainedLogsRuntime{listingRecoveryRuntime: &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{status: runtime.StatusRunning}}, logs: map[string]string{"task": "logs"}}
		rt.containers = []runtime.ContainerInfo{{ID: record.ID, Status: runtime.StatusRunning, Labels: recoveryTestLabels(record)}}
		first, local := newRecoveryTestAgent(t, rt, record)
		if err := first.recover(context.Background()); err != nil {
			t.Fatal(err)
		}
		stop := &nodeapi.StopAllocationRequest{AllocationID: "allocation", Generation: 3, Epoch: 1, RetainLogs: retain}
		if err := first.StopGroup(context.Background(), stop); err != nil {
			t.Fatal(err)
		}
		rt.containers = nil
		if len(first.allocations) != 0 || (retain && len(first.retainedLogs) != 1) {
			t.Fatalf("stop cleanup: tasks=%v logs=%v", first.allocations, first.retainedLogs)
		}
		stop.RetainLogs = false
		if err := first.StopGroup(context.Background(), stop); err != nil {
			t.Fatal(err)
		}
		// An older duplicate stop must not lower the watermark.
		stop.Generation = 1
		if err := first.StopGroup(context.Background(), stop); err != nil {
			t.Fatal(err)
		}
		second := newOperationTestAgent(t, rt)
		second.ConfigureDurability(local, "test")
		if err := second.recover(context.Background()); err != nil {
			t.Fatal(err)
		}
		for _, generation := range []uint64{1, 3} {
			if err := second.StartGroup(context.Background(), &nodeapi.AllocationRequest{AllocationID: "allocation", Generation: generation, Epoch: 1}); !errors.Is(err, ErrStaleGeneration) {
				t.Fatalf("generation %d: %v", generation, err)
			}
		}
		if err := second.fenceStart(&nodeapi.AllocationRequest{AllocationID: "allocation", Generation: 4}); err != nil {
			t.Fatalf("newer generation: %v", err)
		}
	}
}

func TestRecoveryRejectsInvalidStopWatermarksAndIgnoresTemporaryFiles(t *testing.T) {
	for _, value := range []any{uint64(0), "invalid", uint64(3)} {
		agent, local := newRecoveryTestAgent(t, &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{}})
		key := "agent/stopped-generations/" + allocationFileName("allocation")
		if err := local.Put(key, value); err != nil {
			t.Fatal(err)
		}
		if err := local.Put("agent/allocations/.tmp-interrupted", "not an allocation"); err != nil {
			t.Fatal(err)
		}
		if err := local.Put("agent/stopped-generations/.tmp-interrupted", "not a watermark"); err != nil {
			t.Fatal(err)
		}
		err := agent.recover(context.Background())
		if (err == nil) != (value == uint64(3)) {
			t.Fatalf("watermark %v: recover = %v", value, err)
		}
		if value == uint64(3) {
			if err := local.Delete(controlEpochKey); err != nil {
				t.Fatal(err)
			}
			if err := agent.recover(context.Background()); err == nil {
				t.Fatal("accepted missing epoch with durable stop fencing state")
			}
		}
	}
}

func TestStopWatermarkPersistencePrecedesTaskCleanup(t *testing.T) {
	rt := &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{}}
	record := recoveryTestAllocation(0)
	agent, local := newRecoveryTestAgent(t, rt, record)
	agent.allocations[record.ID] = record
	// A file in place of the watermark directory makes its publication fail.
	if err := local.Put("agent/stopped-generations", "blocked"); err != nil {
		t.Fatal(err)
	}
	if err := agent.StopGroup(context.Background(), &nodeapi.StopAllocationRequest{AllocationID: record.AllocationID, Generation: record.Generation, Epoch: 1}); err == nil {
		t.Fatal("stop acknowledged without a durable generation fence")
	}
	var stored Allocation
	if err := local.Get(allocationRecordKey(record.ID), &stored); err != nil || stored.Status != "running" || rt.stopCount != 0 || rt.removeCount != 0 {
		t.Fatalf("task cleanup preceded durable fence: record=%+v error=%v stops=%d removes=%d", stored, err, rt.stopCount, rt.removeCount)
	}
}

func TestPrepareStartRejectsObsoleteGenerationAndConflict(t *testing.T) {
	agent := &Agent{allocations: map[string]*Allocation{
		"task": {ID: "task", AllocationID: "alloc", Generation: 3, JobRevision: 7, ExecutionHash: "same"},
	}}
	if err := agent.fenceStart(&nodeapi.AllocationRequest{AllocationID: "alloc", Generation: 2, Epoch: 1}); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("expected stale generation, got %v", err)
	}
	if err := agent.fenceStart(&nodeapi.AllocationRequest{AllocationID: "alloc", Generation: 3, JobRevision: 7, Epoch: 1, ExecutionHash: "different"}); !errors.Is(err, ErrExecutionConflict) {
		t.Fatalf("expected metadata conflict, got %v", err)
	}
	if err := agent.fenceStart(&nodeapi.AllocationRequest{AllocationID: "alloc", Generation: 3, JobRevision: 8, Epoch: 1, ExecutionHash: "same"}); !errors.Is(err, ErrExecutionConflict) {
		t.Fatalf("expected revision conflict, got %v", err)
	}
	if err := agent.fenceStart(&nodeapi.AllocationRequest{AllocationID: "alloc", Generation: 3, JobRevision: 7, Epoch: 1, ExecutionHash: "same"}); err != nil {
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
			return agent.StartGroup(context.Background(), &nodeapi.AllocationRequest{AllocationID: "alloc", Generation: 1})
		}},
		{name: "stop epoch", run: func() error {
			return agent.StopGroup(context.Background(), &nodeapi.StopAllocationRequest{AllocationID: "alloc", Generation: 1})
		}},
		{name: "drain epoch", run: func() error {
			return agent.DrainGroup(context.Background(), &nodeapi.DrainAllocationRequest{AllocationID: "alloc", Generation: 1})
		}},
		{name: "resume epoch", run: func() error {
			return agent.ResumeGroup(&nodeapi.DrainAllocationRequest{AllocationID: "alloc", Generation: 1})
		}},
		{name: "network plan epoch", run: func() error {
			return agent.UpdateNetworkPlan(context.Background(), &nodeapi.NetworkPlanRequest{Namespace: "default"})
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.run(); !errors.Is(err, ErrInvalidEpoch) {
				t.Fatalf("error = %v, want %v", err, ErrInvalidEpoch)
			}
		})
	}

	if err := agent.StopGroup(context.Background(), &nodeapi.StopAllocationRequest{AllocationID: "alloc", Epoch: 1}); !errors.Is(err, ErrInvalidGeneration) {
		t.Fatalf("zero-generation stop error = %v, want %v", err, ErrInvalidGeneration)
	}
	if agent.epoch != 0 {
		t.Fatalf("invalid stop advanced epoch to %d", agent.epoch)
	}
	if agent.allocations["task"] == nil {
		t.Fatal("invalid stop removed allocation")
	}
}

func TestRecoverAcceptsEmptyFirstBoot(t *testing.T) {
	local := storage.NewLocalStorage(t.TempDir())
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	rt := &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{}}
	agent := newOperationTestAgent(t, rt)
	agent.ConfigureDurability(local, "test")

	if err := agent.recover(context.Background()); err != nil {
		t.Fatalf("recover empty first boot: %v", err)
	}
	var epoch uint64
	if err := local.Get("agent/control-epoch", &epoch); err != nil || epoch != 0 {
		t.Fatalf("initialized control epoch = %d, error = %v", epoch, err)
	}
}

func TestInitRejectsIncompleteDurableFencingState(t *testing.T) {
	validAllocation := Allocation{
		ID: "task", ContainerID: "task", AllocationID: "allocation",
		Generation: 1, JobRevision: 1, ExecutionHash: "hash",
	}

	tests := []struct {
		name    string
		prepare func(*testing.T, string, *storage.LocalStorage) runtime.ContainerRuntime
		want    string
	}{
		{
			name: "malformed control epoch",
			prepare: func(t *testing.T, root string, _ *storage.LocalStorage) runtime.ContainerRuntime {
				if err := os.MkdirAll(filepath.Join(root, "agent"), 0o750); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(root, "agent", "control-epoch"), []byte("{"), 0o600); err != nil {
					t.Fatal(err)
				}
				return &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{}}
			},
			want: "read control-plane epoch",
		},
		{
			name: "missing control epoch with allocation record",
			prepare: func(t *testing.T, _ string, local *storage.LocalStorage) runtime.ContainerRuntime {
				if err := local.Put(allocationRecordKey(validAllocation.ID), &validAllocation); err != nil {
					t.Fatal(err)
				}
				return &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{}}
			},
			want: "control-plane epoch agent/control-epoch is missing while 1 allocation recovery records exist",
		},
		{
			name: "malformed allocation record",
			prepare: func(t *testing.T, root string, local *storage.LocalStorage) runtime.ContainerRuntime {
				if err := local.Put("agent/control-epoch", uint64(1)); err != nil {
					t.Fatal(err)
				}
				allocationDir := filepath.Join(root, "agent", "allocations")
				if err := os.MkdirAll(allocationDir, 0o750); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(allocationDir, "broken"), []byte("{"), 0o600); err != nil {
					t.Fatal(err)
				}
				return &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{}}
			},
			want: "decode allocation recovery record",
		},
		{
			name: "unreadable allocation record",
			prepare: func(t *testing.T, root string, local *storage.LocalStorage) runtime.ContainerRuntime {
				if err := local.Put("agent/control-epoch", uint64(1)); err != nil {
					t.Fatal(err)
				}
				allocationDir := filepath.Join(root, "agent", "allocations")
				if err := os.MkdirAll(allocationDir, 0o750); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Join(root, "missing"), filepath.Join(allocationDir, "unreadable")); err != nil {
					t.Fatal(err)
				}
				return &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{}}
			},
			want: "read allocation recovery records",
		},
		{
			name: "missing control epoch with allocation record and runtime listing failure",
			prepare: func(t *testing.T, _ string, local *storage.LocalStorage) runtime.ContainerRuntime {
				if err := local.Put(allocationRecordKey(validAllocation.ID), &validAllocation); err != nil {
					t.Fatal(err)
				}
				return &listingRecoveryRuntime{
					reconcilerRuntime: &reconcilerRuntime{},
					listErr:           errors.New("runtime unavailable"),
				}
			},
			want: "control-plane epoch agent/control-epoch is missing while 1 allocation recovery records exist",
		},
		{
			name: "missing control epoch with runtime container",
			prepare: func(_ *testing.T, _ string, _ *storage.LocalStorage) runtime.ContainerRuntime {
				return &listingRecoveryRuntime{
					reconcilerRuntime: &reconcilerRuntime{status: runtime.StatusRunning},
					containers:        []runtime.ContainerInfo{{ID: "task", Status: runtime.StatusRunning}},
				}
			},
			want: `control-plane epoch agent/control-epoch is missing while managed runtime container "task" exists`,
		},
		{
			name: "mis-keyed allocation record",
			prepare: func(t *testing.T, _ string, local *storage.LocalStorage) runtime.ContainerRuntime {
				if err := local.Put("agent/control-epoch", uint64(1)); err != nil {
					t.Fatal(err)
				}
				if err := local.Put(allocationRecordKey("other"), &validAllocation); err != nil {
					t.Fatal(err)
				}
				return &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{}}
			},
			want: "record name does not match allocation ID",
		},
		{
			name: "runtime container without durable record",
			prepare: func(t *testing.T, _ string, local *storage.LocalStorage) runtime.ContainerRuntime {
				if err := local.Put("agent/control-epoch", uint64(1)); err != nil {
					t.Fatal(err)
				}
				return &listingRecoveryRuntime{
					reconcilerRuntime: &reconcilerRuntime{status: runtime.StatusRunning},
					containers: []runtime.ContainerInfo{{
						ID: "task", Status: runtime.StatusRunning,
						Labels: map[string]string{
							"trellis.allocation-id":         "allocation",
							"trellis.allocation-generation": "1",
							"trellis.job-revision":          "1",
							"trellis.execution-hash":        "hash",
						},
					}},
				}
			},
			want: `managed runtime container "task" (labelled allocation "allocation", generation "1") has no durable allocation record`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			local := storage.NewLocalStorage(root)
			if err := local.Init(); err != nil {
				t.Fatal(err)
			}
			agent := newOperationTestAgent(t, tt.prepare(t, root, local))
			agent.ConfigureDurability(local, "test")
			err := agent.Init(context.Background())
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Init error = %v, want containing %q", err, tt.want)
			}
			if !strings.Contains(err.Error(), "Agent recovery refused") {
				t.Fatalf("Init error = %v, want it to point to the operator recovery steps", err)
			}
		})
	}
}
