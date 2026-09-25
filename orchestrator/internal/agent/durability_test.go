package agent

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/health"
	"github.com/clofour/trellis/internal/runtime"
	"github.com/clofour/trellis/internal/spec"
	"github.com/clofour/trellis/internal/storage"
)

type recoveryProbeRuntime struct {
	*runtime.InjectedRuntime
	execs          chan []string
	namespaceDials chan struct{}
}

func (r *recoveryProbeRuntime) Exec(_ context.Context, _ string, command []string) (int, error) {
	select {
	case r.execs <- command:
	default:
	}
	return 0, nil
}

func (r *recoveryProbeRuntime) NetworkNamespace(context.Context, string) (string, error) {
	select {
	case r.namespaceDials <- struct{}{}:
	default:
	}
	return "", fmt.Errorf("runsc probe must not use a Linux network namespace")
}

func TestRunscProbeRecoversFromReadyAllocation(t *testing.T) {
	dataDir := t.TempDir()
	local := storage.NewLocalStorage(dataDir)
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	runtimePath := filepath.Join(dataDir, "runtime.json")
	firstRuntime, err := runtime.NewInjectedRuntime(runtimePath, "")
	if err != nil {
		t.Fatal(err)
	}
	first := newOperationTestAgent(t, firstRuntime)
	first.ConfigureDurability(local, "test")
	cancelled, cancelFirst := context.WithCancel(context.Background())
	cancelFirst()
	first.health.SetContext(cancelled)
	request := operationTestRequest()
	request.Runtime = "runsc"
	request.Tasks = []spec.TaskSpec{{
		Name: "first", Image: "image",
		HealthCheck: &spec.HealthCheckSpec{Type: "http", Port: 8080, Path: "/health", Interval: time.Millisecond},
	}}
	if err := first.RunGroup(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	id := "allocation-g2-first"
	var persisted Allocation
	if err := local.Get(allocationRecordKey(id), &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Runtime != "runsc" {
		t.Fatalf("ready allocation runtime = %q, want runsc", persisted.Runtime)
	}

	recoveredRuntime, err := runtime.NewInjectedRuntime(runtimePath, "")
	if err != nil {
		t.Fatal(err)
	}
	probeRuntime := &recoveryProbeRuntime{InjectedRuntime: recoveredRuntime, execs: make(chan []string, 1), namespaceDials: make(chan struct{}, 1)}
	second := newOperationTestAgent(t, probeRuntime)
	second.ConfigureDurability(storage.NewLocalStorage(dataDir), "test")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	second.health.SetContext(ctx)
	if err := second.recover(ctx); err != nil {
		t.Fatal(err)
	}
	recovered := second.allocations[id]
	if recovered == nil {
		t.Fatal("allocation was not recovered")
	}
	if got := recovered.Runtime; got != "runsc" {
		t.Fatalf("recovered allocation runtime = %q, want runsc", got)
	}
	select {
	case command := <-probeRuntime.execs:
		want := []string{health.ProbePath, "__health-probe", "http", "8080", "/health", "5s"}
		if !slices.Equal(command, want) {
			t.Fatalf("recovered probe command = %q, want %q", command, want)
		}
	case <-probeRuntime.namespaceDials:
		t.Fatal("recovered runsc probe attempted a Linux namespace dial")
	case <-time.After(2 * time.Second):
		t.Fatal("recovered runsc probe did not execute")
	}
}

func TestEpochFenceSurvivesRestart(t *testing.T) {
	local := storage.NewLocalStorage(t.TempDir())
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	first := &Agent{local: local}
	if err := first.AcceptEpoch(7); err != nil {
		t.Fatal(err)
	}
	second := &Agent{local: local, allocations: map[string]*Allocation{}, orphans: map[string]int{}}
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
		"task": {ID: "task", AllocationID: "alloc", Generation: 3, ExecutionHash: "same"},
	}}
	if err := agent.PrepareStart(context.Background(), &api.AllocationRequest{AllocationID: "alloc", Generation: 2}); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("expected stale generation, got %v", err)
	}
	if err := agent.PrepareStart(context.Background(), &api.AllocationRequest{AllocationID: "alloc", Generation: 3, ExecutionHash: "different"}); !errors.Is(err, ErrExecutionConflict) {
		t.Fatalf("expected metadata conflict, got %v", err)
	}
}

func TestLeaderUnavailableDoesNotConfirmOrphan(t *testing.T) {
	agent := &Agent{allocations: map[string]*Allocation{
		"task": {ID: "task", AllocationID: "alloc", Generation: 1},
	}, orphans: map[string]int{}}
	agent.reconcileDesired(context.Background(), &api.HeartbeatResponse{OrphanConfirmation: false})
	if len(agent.allocations) != 1 || len(agent.orphans) != 0 {
		t.Fatal("an unconfirmed allocation was considered orphaned")
	}
}
