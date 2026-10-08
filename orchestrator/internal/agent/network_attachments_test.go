package agent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/overfold/trellis/orchestrator/internal/client"
	"github.com/overfold/trellis/orchestrator/internal/network"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	"github.com/overfold/trellis/orchestrator/internal/runtime"
	"github.com/overfold/trellis/orchestrator/internal/spec"
	"github.com/overfold/trellis/orchestrator/internal/storage"
)

// recoveringNetworkManager records attachments by allocation ID the way the
// WireGuard manager journals them, so it can detach them by ID alone.
type recoveringNetworkManager struct {
	mu           sync.Mutex
	attached     map[string]bool
	byID         []string
	detached     []string
	attachErr    error
	detachErr    error
	listErr      error
	beforeAttach func(network.AttachRequest)
	beforeDetach func(context.Context, string) error
}

func newRecoveringNetworkManager(attached ...string) *recoveringNetworkManager {
	m := &recoveringNetworkManager{attached: map[string]bool{}}
	for _, id := range attached {
		m.attached[id] = true
	}
	return m
}

func (m *recoveringNetworkManager) Attach(_ context.Context, request network.AttachRequest) (*network.Attachment, error) {
	if m.beforeAttach != nil {
		m.beforeAttach(request)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// Like the WireGuard manager, the attachment is recorded before any
	// step that can fail.
	m.attached[request.AllocationID] = true
	if m.attachErr != nil {
		return nil, m.attachErr
	}
	return &network.Attachment{AllocationID: request.AllocationID, Namespace: request.Namespace, Network: request.Network,
		NetworkNamespace: "/var/run/netns/" + request.AllocationID, Address: "10.42.0.2/24"}, nil
}

func (m *recoveringNetworkManager) UpdatePlan(context.Context, string, network.Plan) error {
	return nil
}

func (m *recoveringNetworkManager) Detach(_ context.Context, attachment *network.Attachment) error {
	if attachment == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.detached = append(m.detached, attachment.AllocationID)
	delete(m.attached, attachment.AllocationID)
	return nil
}

func (m *recoveringNetworkManager) DetachAllocation(ctx context.Context, id string) error {
	if m.beforeDetach != nil {
		if err := m.beforeDetach(ctx, id); err != nil {
			return err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.byID = append(m.byID, id)
	if m.detachErr != nil {
		return m.detachErr
	}
	delete(m.attached, id)
	return nil
}

func (m *recoveringNetworkManager) Attachments(context.Context) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var ids []string
	for id := range m.attached {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids, m.listErr
}

func (m *recoveringNetworkManager) isAttached(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.attached[id]
}

func wireGuardTestRequest() *nodeapi.AllocationRequest {
	request := operationTestRequest()
	request.Tasks = []spec.TaskSpec{{Name: "first", Image: "image", Networking: &spec.TaskNetworkingSpec{Mode: spec.TaskNetworkWireGuard}}}
	request.NetworkPlan = &network.Plan{}
	return request
}

func newDurableNetworkTestAgent(t *testing.T, rt runtime.ContainerRuntime, manager network.Manager) (*Agent, *storage.LocalStorage) {
	t.Helper()
	local := storage.NewLocalStorage(t.TempDir())
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	agent := newOperationTestAgent(t, rt)
	agent.SetNetworkManager(manager)
	agent.ConfigureDurability(local, "test")
	return agent, local
}

func TestRunAllocationRecordsNetworkIntentBeforeAttach(t *testing.T) {
	manager := newRecoveringNetworkManager()
	agent, local := newDurableNetworkTestAgent(t, &reconcilerRuntime{}, manager)
	id := "allocation-g2-first"
	var intent *network.AttachmentIntent
	manager.beforeAttach = func(network.AttachRequest) {
		var recorded Allocation
		if err := local.Get(allocationRecordKey(id), &recorded); err != nil {
			t.Errorf("read record before attach: %v", err)
			return
		}
		intent = recorded.NetworkIntent
	}

	if err := runGroup(context.Background(), agent, wireGuardTestRequest()); err != nil {
		t.Fatal(err)
	}
	want := network.AttachmentIntent{AllocationID: id, Namespace: "default", Network: "default"}
	if intent == nil || *intent != want {
		t.Fatalf("intent recorded before attach = %+v, want %+v", intent, want)
	}
	var recorded Allocation
	if err := local.Get(allocationRecordKey(id), &recorded); err != nil || recorded.Network == nil || recorded.NetworkIntent == nil {
		t.Fatalf("running record = %+v, error = %v, want attachment and intent", recorded, err)
	}
}

func TestFailedAttachDetachesByIntent(t *testing.T) {
	attachErr := errors.New("veth setup failed")
	manager := newRecoveringNetworkManager()
	manager.attachErr = attachErr
	agent, local := newDurableNetworkTestAgent(t, &reconcilerRuntime{}, manager)
	id := "allocation-g2-first"

	if err := runGroup(context.Background(), agent, wireGuardTestRequest()); !errors.Is(err, attachErr) {
		t.Fatalf("run error = %v, want attach failure", err)
	}
	if !slices.Equal(manager.byID, []string{id}) || manager.isAttached(id) {
		t.Fatalf("detached by ID = %v, attached = %v, want partial attachment removed", manager.byID, manager.isAttached(id))
	}
	if agent.allocations[id] != nil {
		t.Fatal("allocation retained after successful cleanup")
	}
	if err := local.Get(allocationRecordKey(id), &Allocation{}); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("record after cleanup: %v, want not found", err)
	}
}

func TestFailedIntentCleanupRetainsRecordUntilStopSucceeds(t *testing.T) {
	attachErr := errors.New("veth setup failed")
	detachErr := errors.New("netns busy")
	manager := newRecoveringNetworkManager()
	manager.attachErr, manager.detachErr = attachErr, detachErr
	agent, local := newDurableNetworkTestAgent(t, &reconcilerRuntime{}, manager)
	id := "allocation-g2-first"

	if err := runGroup(context.Background(), agent, wireGuardTestRequest()); !errors.Is(err, detachErr) {
		t.Fatalf("run error = %v, want detach failure", err)
	}
	var recorded Allocation
	if err := local.Get(allocationRecordKey(id), &recorded); err != nil || recorded.Status != "stopping" || recorded.Network != nil || recorded.NetworkIntent == nil {
		t.Fatalf("record = %+v, error = %v, want stopping with intent only", recorded, err)
	}

	manager.detachErr = nil
	if err := agent.StopAllocation(context.Background(), id); err != nil {
		t.Fatalf("stop retry: %v", err)
	}
	if manager.isAttached(id) || len(manager.byID) != 2 {
		t.Fatalf("detached by ID = %v, attached = %v, want retry to detach by ID", manager.byID, manager.isAttached(id))
	}
	if err := local.Get(allocationRecordKey(id), &recorded); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("record after stop: %v, want not found", err)
	}
}

func TestRecoverMissingContainerDetachesIntentOnlyRecord(t *testing.T) {
	// The agent stopped during Attach: the record holds the intent but not
	// the attachment, and no container was created.
	record := recoveryTestAllocation(0)
	record.Ports = nil
	record.Status = "starting"
	record.NetworkIntent = &network.AttachmentIntent{AllocationID: record.ID, Namespace: record.Namespace, Network: record.Namespace}
	rt := &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{}}
	agent, local := newRecoveryTestAgent(t, rt, record)
	manager := newRecoveringNetworkManager(record.ID)
	agent.SetNetworkManager(manager)

	if err := agent.recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(manager.byID, []string{record.ID}) || manager.isAttached(record.ID) {
		t.Fatalf("detached by ID = %v, want %s", manager.byID, record.ID)
	}
	if err := local.Get(allocationRecordKey(record.ID), &Allocation{}); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("record after recovery: %v, want not found", err)
	}
}

func TestRunAllocationRetryDetachesIntentOnlyRecord(t *testing.T) {
	record := recoveryTestAllocation(0)
	record.ID, record.ContainerID, record.AllocationID = "allocation-g2-first", "allocation-g2-first", "allocation"
	record.Generation, record.JobRevision, record.ExecutionHash = 2, 7, "execution-hash"
	record.Ports = nil
	record.Status = "stopping"
	record.NetworkIntent = &network.AttachmentIntent{AllocationID: record.ID, Namespace: "default", Network: "default"}
	manager := newRecoveringNetworkManager(record.ID)
	agent, _ := newDurableNetworkTestAgent(t, &reconcilerRuntime{}, manager)
	agent.allocations[record.ID] = record

	if err := runGroup(context.Background(), agent, wireGuardTestRequest()); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(manager.byID, []string{record.ID}) {
		t.Fatalf("detached by ID = %v, want leftover attachment removed before the retry attached", manager.byID)
	}
	if !manager.isAttached(record.ID) || agent.allocations[record.ID].Network == nil {
		t.Fatal("retry did not attach the allocation again")
	}
}

func TestRemoveOrphanedNetworkAttachmentsKeepsOwnedAttachments(t *testing.T) {
	manager := newRecoveringNetworkManager("recorded", "malformed", "in-memory", "orphan")
	agent, local := newDurableNetworkTestAgent(t, &reconcilerRuntime{}, manager)
	if err := agent.persistAllocation(&Allocation{ID: "recorded", ContainerID: "recorded", Status: "running"}); err != nil {
		t.Fatal(err)
	}
	if err := local.Put(allocationRecordKey("malformed"), "not an allocation"); err != nil {
		t.Fatal(err)
	}
	agent.allocations["in-memory"] = &Allocation{ID: "in-memory"}

	agent.removeOrphanedNetworkAttachments(context.Background())

	if !slices.Equal(manager.byID, []string{"orphan"}) {
		t.Fatalf("detached by ID = %v, want only the orphan", manager.byID)
	}
	for _, id := range []string{"recorded", "malformed", "in-memory"} {
		if !manager.isAttached(id) {
			t.Fatalf("owned attachment %s removed", id)
		}
	}
}

func TestRemoveOrphanedNetworkAttachmentsSweepsReadableRecordsDespiteListErrors(t *testing.T) {
	manager := newRecoveringNetworkManager("orphan")
	manager.listErr = errors.New("unreadable attachment record")
	agent, _ := newDurableNetworkTestAgent(t, &reconcilerRuntime{}, manager)

	agent.removeOrphanedNetworkAttachments(context.Background())

	if !slices.Equal(manager.byID, []string{"orphan"}) {
		t.Fatalf("detached by ID = %v, want the readable orphan", manager.byID)
	}
}

func TestRemoveOrphanedNetworkAttachmentsSkipsWithoutVerifiableOwnership(t *testing.T) {
	// Without durable records, ownership cannot be verified.
	manager := newRecoveringNetworkManager("orphan")
	agent := newOperationTestAgent(t, &reconcilerRuntime{})
	agent.SetNetworkManager(manager)
	agent.removeOrphanedNetworkAttachments(context.Background())
	if len(manager.byID) != 0 {
		t.Fatalf("detached %v without durable records", manager.byID)
	}

	// An unreadable record may belong to any allocation.
	root := t.TempDir()
	local := storage.NewLocalStorage(root)
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	agent = newOperationTestAgent(t, &reconcilerRuntime{})
	agent.SetNetworkManager(manager)
	agent.ConfigureDurability(local, "test")
	recordDir := filepath.Join(root, "agent", "allocations")
	if err := os.MkdirAll(recordDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "missing"), filepath.Join(recordDir, "unreadable")); err != nil {
		t.Fatal(err)
	}
	agent.removeOrphanedNetworkAttachments(context.Background())
	if len(manager.byID) != 0 {
		t.Fatalf("detached %v with unreadable records", manager.byID)
	}
}

func TestInitSweepsOrphanedNetworkAttachmentsOnlyAfterFullListing(t *testing.T) {
	for _, complete := range []bool{true, false} {
		name := "complete listing"
		if !complete {
			name = "incomplete listing"
		}
		t.Run(name, func(t *testing.T) {
			rt := &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{}}
			if !complete {
				// An unreadable container could belong to an allocation
				// whose record recovery has not seen.
				rt.containers = []runtime.ContainerInfo{{ID: "unreadable", Status: runtime.StatusUnknown}}
			}
			agent, _ := newRecoveryTestAgent(t, rt)
			agent.server = &client.ServerClient{}
			manager := newRecoveringNetworkManager("orphan")
			agent.SetNetworkManager(manager)
			ctx, cancel := context.WithCancel(context.Background())
			if !complete {
				// Stop the background recovery retry so the test drives it.
				cancel()
			}
			t.Cleanup(cancel)

			err := agent.Init(ctx)
			if complete {
				if err != nil {
					t.Fatal(err)
				}
				if !slices.Equal(manager.byID, []string{"orphan"}) {
					t.Fatalf("detached by ID = %v, want the orphan swept at startup", manager.byID)
				}
				return
			}
			if err == nil {
				t.Fatal("Init succeeded with an unreadable unrecorded container")
			}
			if len(manager.byID) != 0 {
				t.Fatalf("detached %v before recovery listed every container", manager.byID)
			}
		})
	}
}

func TestRecoveryRetrySweepsOrphanedNetworkAttachmentsAfterLateListing(t *testing.T) {
	rt := &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{}, listErr: errors.New("containerd unavailable")}
	agent, _ := newRecoveryTestAgent(t, rt)
	manager := newRecoveringNetworkManager("orphan")
	agent.SetNetworkManager(manager)
	if err := agent.recover(context.Background()); err != nil {
		t.Fatalf("recover with failed listing: %v", err)
	}
	if !agent.recoveryListPending {
		t.Fatal("listing failure did not leave recovery pending")
	}

	// A listing that still holds an unreadable unrecorded container keeps
	// the sweep deferred.
	rt.listErr = nil
	rt.containers = []runtime.ContainerInfo{{ID: "unreadable", Status: runtime.StatusUnknown}}
	agent.retryRecovery(context.Background())
	if len(manager.byID) != 0 {
		t.Fatalf("detached %v while listing was incomplete", manager.byID)
	}

	rt.containers = nil
	agent.retryRecovery(context.Background())
	if !slices.Equal(manager.byID, []string{"orphan"}) {
		t.Fatalf("detached by ID = %v, want the orphan swept once listing completed", manager.byID)
	}
}

func TestHungOrphanDetachDoesNotBlockAgentOrReleaseOwnershipOnCancellation(t *testing.T) {
	manager := newRecoveringNetworkManager("allocation-g2-first")
	agent, local := newDurableNetworkTestAgent(t, &reconcilerRuntime{}, manager)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	manager.beforeDetach = func(context.Context, string) error {
		close(entered)
		<-release // Deliberately ignore cancellation, like a hung external operation.
		return errors.New("detach failed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	swept := make(chan struct{})
	go func() {
		agent.removeOrphanedNetworkAttachments(ctx)
		close(swept)
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("detach did not enter")
	}

	// Exercise actual writers and persistence, not just TryLock. The same-ID
	// start is refused promptly, while stop/drain and other starts can proceed.
	progress := make(chan error, 1)
	go func() {
		request := operationTestRequest()
		if err := agent.StartGroup(context.Background(), request); err == nil {
			progress <- errors.New("same-ID start accepted during detach")
			return
		}
		if err := agent.StopGroup(context.Background(), &nodeapi.StopAllocationRequest{AllocationID: "allocation", Generation: 2, Epoch: 1}); err != nil {
			progress <- err
			return
		}
		if err := agent.DrainGroup(context.Background(), &nodeapi.DrainAllocationRequest{AllocationID: "allocation", Generation: 2, Epoch: 1}); err != nil {
			progress <- err
			return
		}
		request.AllocationID = "unrelated"
		if err := runGroup(context.Background(), agent, request); err != nil {
			progress <- err
			return
		}
		agent.OnReconciledStatus("unrelated-g2-first", "exited")
		_ = agent.allocationStatuses()
		var record Allocation
		if err := local.Get(allocationRecordKey("unrelated-g2-first"), &record); err != nil {
			progress <- err
			return
		}
		if record.Status != "exited" {
			progress <- errors.New("status writer did not persist")
			return
		}
		progress <- nil
	}()
	select {
	case err := <-progress:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("hung detach blocked agent progress")
	}
	cancel() // Agent shutdown must not wait for the noncooperative manager.
	select {
	case <-swept:
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown did not end the sweep")
	}
	// A second sweep cannot launch another worker or release the reservation.
	agent.detachOrphanedNetwork(context.Background(), manager, "allocation-g2-first")
	if err := agent.StartGroup(context.Background(), operationTestRequest()); err == nil {
		t.Fatal("cancelled wait released ownership before detach returned")
	}
	once.Do(func() { close(release) })
	deadline := time.Now().Add(3 * time.Second)
	for {
		agent.mu.RLock()
		pending := agent.orphanDetaches["allocation-g2-first"]
		agent.mu.RUnlock()
		if !pending {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("completed detach retained its reservation")
		}
		time.Sleep(time.Millisecond)
	}
	// A failed detach leaves the attachment journal for retry. Once it returns,
	// retry cleanup can succeed and the previously refused start can run.
	manager.beforeDetach = nil
	agent.removeOrphanedNetworkAttachments(context.Background())
	if manager.isAttached("allocation-g2-first") {
		t.Fatal("retry did not remove attachment")
	}
	request := operationTestRequest()
	request.Generation++ // The earlier explicit stop permanently fenced generation 2.
	if err := runGroup(context.Background(), agent, request); err != nil {
		t.Fatalf("start after cleanup: %v", err)
	}
}

func TestOrphanSweepRechecksOwnershipAfterSnapshot(t *testing.T) {
	for _, owner := range []string{"durable", "in-memory", "accepted-start"} {
		t.Run(owner, func(t *testing.T) {
			manager := newRecoveringNetworkManager("a-orphan", "allocation-g2-first")
			agent, local := newDurableNetworkTestAgent(t, &reconcilerRuntime{}, manager)
			manager.beforeDetach = func(_ context.Context, id string) error {
				if id != "a-orphan" {
					return nil
				}
				// The sweep already took its durable ownership snapshot.
				switch owner {
				case "durable":
					return local.Put(allocationRecordKey("allocation-g2-first"), "malformed but owned")
				case "in-memory":
					agent.mu.Lock()
					agent.allocations["allocation-g2-first"] = &Allocation{ID: "allocation-g2-first"}
					agent.mu.Unlock()
				case "accepted-start":
					agent.mu.Lock()
					agent.starts = map[string]*groupStart{"allocation": {generation: 2, tasks: []string{"first"}}}
					agent.mu.Unlock()
				}
				return nil
			}
			agent.removeOrphanedNetworkAttachments(context.Background())
			if !slices.Equal(manager.byID, []string{"a-orphan"}) || !manager.isAttached("allocation-g2-first") {
				t.Fatalf("new owner lost attachment: detached %v", manager.byID)
			}
		})
	}
}

func TestOrphanDetachCancellationAndRetry(t *testing.T) {
	manager := newRecoveringNetworkManager("orphan")
	agent, _ := newDurableNetworkTestAgent(t, &reconcilerRuntime{}, manager)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	agent.detachOrphanedNetwork(ctx, manager, "orphan")
	if len(manager.byID) != 0 {
		t.Fatal("cancelled detach touched network")
	}
	manager.detachErr = context.DeadlineExceeded
	agent.detachOrphanedNetwork(context.Background(), manager, "orphan")
	if !manager.isAttached("orphan") {
		t.Fatal("failed detach lost retry evidence")
	}
	manager.detachErr = nil
	agent.detachOrphanedNetwork(context.Background(), manager, "orphan")
	if manager.isAttached("orphan") || len(manager.byID) != 2 {
		t.Fatal("detach retry did not converge")
	}
}

func TestOrphanDetachTimeoutKeepsReservationUntilWorkerReturns(t *testing.T) {
	agent := newOperationTestAgent(t, &reconcilerRuntime{})
	manager := newRecoveringNetworkManager("orphan")
	synctest.Test(t, func(t *testing.T) {
		entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
		manager.beforeDetach = func(context.Context, string) error {
			close(entered)
			<-release
			return nil
		}
		started := time.Now()
		go func() {
			agent.detachOrphanedNetwork(context.Background(), manager, "orphan")
			close(done)
		}()
		<-entered
		<-done // Fake time advances to the production timeout, no wall-clock sleep.
		if elapsed := time.Since(started); elapsed != 30*time.Second {
			t.Fatalf("detach wait = %v, want 30s", elapsed)
		}
		agent.mu.RLock()
		pending := agent.orphanDetaches["orphan"]
		agent.mu.RUnlock()
		if !pending {
			t.Fatal("timeout released the live worker's reservation")
		}
		close(release)
		synctest.Wait()
		agent.mu.RLock()
		pending = agent.orphanDetaches["orphan"]
		agent.mu.RUnlock()
		if pending || manager.isAttached("orphan") {
			t.Fatal("late successful cleanup did not release reservation and attachment")
		}
	})
}
