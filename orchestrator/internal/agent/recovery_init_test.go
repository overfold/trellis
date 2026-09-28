package agent

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/clofour/trellis/internal/client"
	"github.com/clofour/trellis/internal/runtime"
	"github.com/clofour/trellis/internal/storage"
)

// lockedListingRuntime lets a test change what the runtime lists while the
// agent's background recovery retry reads it.
type lockedListingRuntime struct {
	reconcilerRuntime
	mu         sync.Mutex
	listErr    error
	containers []runtime.ContainerInfo
	actions    int
}

func (r *lockedListingRuntime) ListManaged(context.Context, string) ([]runtime.ContainerInfo, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.listErr != nil {
		return nil, r.listErr
	}
	return slices.Clone(r.containers), nil
}

func (r *lockedListingRuntime) Inspect(context.Context, string) (*runtime.ContainerInfo, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return &runtime.ContainerInfo{Status: r.status}, nil
}

func (r *lockedListingRuntime) Stop(context.Context, string) error    { return r.act() }
func (r *lockedListingRuntime) Remove(context.Context, string) error  { return r.act() }
func (r *lockedListingRuntime) Restart(context.Context, string) error { return r.act() }

func (r *lockedListingRuntime) act() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.actions++
	return nil
}

func (r *lockedListingRuntime) list(listErr error, status runtime.ContainerStatus, containers ...runtime.ContainerInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.listErr, r.status, r.containers = listErr, status, containers
}

func (r *lockedListingRuntime) actionCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.actions
}

// newInitTestAgent returns an agent whose server client records requests on
// the returned channel, so a test can see the heartbeat loop start.
func newInitTestAgent(t *testing.T, rt runtime.ContainerRuntime, epoch uint64, records ...*Allocation) (*Agent, *storage.LocalStorage, <-chan string) {
	t.Helper()
	agent, local := newRecoveryTestAgent(t, rt, records...)
	if err := local.Put(controlEpochKey, epoch); err != nil {
		t.Fatal(err)
	}
	requests := make(chan string, 16)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case requests <- r.URL.Path:
		default:
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("{}"))
	}))
	t.Cleanup(server.Close)
	agent.server = client.NewServerClient("", server.URL, nil)
	return agent, local, requests
}

func initTestContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return ctx
}

func waitFor(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (m *recoveringNetworkManager) detachedByID() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.byID)
}

// A temporary runtime listing failure during an agent restart must not stop
// the agent: its recorded allocations keep being reported until a later
// listing classifies them.
func TestInitListingFailureKeepsRecordedAllocationsUntilRetryObservesThem(t *testing.T) {
	rt := &lockedListingRuntime{listErr: errors.New("containerd unavailable")}
	record := recoveryTestAllocation(18096)
	agent, local, requests := newInitTestAgent(t, rt, 7, record)
	manager := newRecoveringNetworkManager("orphan")
	agent.SetNetworkManager(manager)

	if err := agent.Init(initTestContext(t)); err != nil {
		t.Fatalf("Init with failed runtime listing: %v", err)
	}
	// The recorded epoch fences stale leaders from the start.
	if err := agent.AcceptEpoch(6); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("AcceptEpoch(6) = %v, want stale epoch against the recorded epoch 7", err)
	}
	if allocation, ok := agent.snapshotAllocation("task"); !ok || !allocation.unobserved || allocation.Status != "running" {
		t.Fatalf("recorded allocation after failed listing = %+v, want kept unobserved", allocation)
	}
	if !portClaimed(agent, 18096) {
		t.Fatal("recorded allocation lost its port claim")
	}
	if statuses := agent.allocationStatuses(); len(statuses) != 1 || statuses[0].ID != "allocation" || statuses[0].Health != "unknown" {
		t.Fatalf("heartbeat allocations = %+v, want the recorded allocation with unknown health", statuses)
	}
	select {
	case <-requests:
	case <-time.After(5 * time.Second):
		t.Fatal("heartbeat loop did not contact the control plane")
	}
	if detached := manager.detachedByID(); len(detached) != 0 {
		t.Fatalf("orphaned resource sweep ran before any listing completed: %v", detached)
	}
	var persisted Allocation
	if err := local.Get(allocationRecordKey("task"), &persisted); err != nil {
		t.Fatalf("allocation record after failed listing: %v", err)
	}

	rt.list(nil, runtime.StatusRunning, runtime.ContainerInfo{ID: "task", Status: runtime.StatusRunning, Labels: recoveryTestLabels(record)})
	waitFor(t, "the recovery retry to observe the allocation", func() bool {
		allocation, ok := agent.snapshotAllocation("task")
		return ok && !allocation.unobserved && allocation.Status == "running" && allocation.Health == "healthy"
	})
	waitFor(t, "the orphaned resource sweep", func() bool {
		return slices.Equal(manager.detachedByID(), []string{"orphan"})
	})
	if agent.recoveryPending() {
		t.Fatal("recovery still pending after a complete listing")
	}
	select {
	case err := <-agent.Failed():
		t.Fatalf("recovery failed: %v", err)
	default:
	}
	if actions := rt.actionCount(); actions != 0 {
		t.Fatalf("running container was acted on %d times", actions)
	}
}

// Without a recorded epoch only an empty first boot may start, and a failed
// listing cannot prove the node is empty.
func TestInitRefusesMissingEpochWhenListingFails(t *testing.T) {
	root := t.TempDir()
	local := storage.NewLocalStorage(root)
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	rt := &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{}, listErr: errors.New("containerd unavailable")}
	agent := newOperationTestAgent(t, rt)
	agent.ConfigureDurability(local, "test")

	err := agent.Init(context.Background())
	for _, want := range []string{"control-plane epoch agent/control-epoch is missing", "confirm an empty first boot", "containerd unavailable", "Agent recovery refused"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("Init error = %v, want containing %q", err, want)
		}
	}
	var epoch uint64
	if err := local.Get(controlEpochKey, &epoch); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("control epoch after refused Init = %d (%v), want it left missing", epoch, err)
	}
}

// A container found by a late listing without an allocation record is refused
// as it is at startup: it is neither adopted nor recorded, and the agent
// reports the failure so the process stops.
func TestInitRecoveryRetryRefusesUnrecordedContainer(t *testing.T) {
	rt := &lockedListingRuntime{listErr: errors.New("containerd unavailable")}
	record := recoveryTestAllocation(18097)
	agent, local, _ := newInitTestAgent(t, rt, 3, record)
	manager := newRecoveringNetworkManager("orphan")
	agent.SetNetworkManager(manager)

	if err := agent.Init(initTestContext(t)); err != nil {
		t.Fatalf("Init with failed runtime listing: %v", err)
	}
	stray := recoveryTestAllocation(0)
	stray.ID, stray.ContainerID, stray.AllocationID = "stray", "stray", "stray-allocation"
	rt.list(nil, runtime.StatusRunning,
		runtime.ContainerInfo{ID: "task", Status: runtime.StatusRunning, Labels: recoveryTestLabels(record)},
		runtime.ContainerInfo{ID: "stray", Status: runtime.StatusRunning, Labels: recoveryTestLabels(stray)},
	)

	select {
	case err := <-agent.Failed():
		for _, want := range []string{`"stray"`, `labelled allocation "stray-allocation"`, "no durable allocation record", "Agent recovery refused"} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("recovery error = %v, want containing %s", err, want)
			}
		}
	case <-time.After(15 * time.Second):
		t.Fatal("recovery retry did not refuse the unrecorded container")
	}
	if _, adopted := agent.snapshotAllocation("stray"); adopted {
		t.Fatal("unrecorded container was adopted from its labels")
	}
	var persisted Allocation
	if err := local.Get(allocationRecordKey("stray"), &persisted); err == nil {
		t.Fatal("unrecorded container was recorded from its labels")
	}
	if detached := manager.detachedByID(); len(detached) != 0 {
		t.Fatalf("orphaned resource sweep ran despite an unrecorded container: %v", detached)
	}
	if actions := rt.actionCount(); actions != 0 {
		t.Fatalf("containers were acted on %d times", actions)
	}

	// Restarting refuses the same state at Init.
	restarted := newOperationTestAgent(t, rt)
	restarted.ConfigureDurability(local, "test")
	err := restarted.Init(context.Background())
	if err == nil || !strings.Contains(err.Error(), `managed runtime container "stray"`) {
		t.Fatalf("restarted Init error = %v, want the unrecorded container refused", err)
	}
}
