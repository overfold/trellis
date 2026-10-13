package agent

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/internal/client"
	"github.com/overfold/trellis/orchestrator/internal/network"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	"github.com/overfold/trellis/orchestrator/internal/spec"
	"github.com/overfold/trellis/orchestrator/internal/storage"
)

func executionTestDocument(a *Agent, leader uuid.UUID) *nodeapi.ExecutionIntent {
	request := singleTaskRequest()
	request.Tasks[0].Resources = &spec.ResourcesSpec{CPU: 100, Memory: 1 << 20}
	request.Tasks[0].Networking = &spec.TaskNetworkingSpec{Mode: spec.TaskNetworkNone}
	return &nodeapi.ExecutionIntent{NodeID: a.nodeID, LeaderID: leader, Epoch: request.Epoch, Version: 1, Complete: true, Allocations: []nodeapi.AllocationIntent{{Mode: nodeapi.ExecutionRetain, Request: *request}}, NetworkPlans: map[string]network.Plan{}}
}

func TestExecutionRejectsWholeDocumentBeforePersistence(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*nodeapi.ExecutionIntent)
	}{
		{"wrong node", func(d *nodeapi.ExecutionIntent) { d.NodeID = uuid.New() }},
		{"unsupported compatibility level", func(d *nodeapi.ExecutionIntent) { d.Release = 1 }},
		{"wrong leader", func(d *nodeapi.ExecutionIntent) { d.LeaderID = uuid.New() }},
		{"partial", func(d *nodeapi.ExecutionIntent) { d.Complete = false }},
		{"missing allocations", func(d *nodeapi.ExecutionIntent) { d.Allocations = nil; d.CleanupAbsent = true }},
		{"missing network plans", func(d *nodeapi.ExecutionIntent) { d.NetworkPlans = nil }},
		{"mixed epoch", func(d *nodeapi.ExecutionIntent) { d.Allocations[0].Request.Epoch++ }},
		{"duplicate", func(d *nodeapi.ExecutionIntent) { d.Allocations = append(d.Allocations, d.Allocations[0]) }},
		{"multiple live generations", func(d *nodeapi.ExecutionIntent) {
			other := d.Allocations[0]
			other.Request.Generation++
			d.Allocations = append(d.Allocations, other)
		}},
		{"cleanup newer than live generation", func(d *nodeapi.ExecutionIntent) {
			other := d.Allocations[0]
			other.Mode = nodeapi.ExecutionCleanup
			other.Request.Generation++
			d.Allocations = append(d.Allocations, other)
		}},
		{"secrets", func(d *nodeapi.ExecutionIntent) {
			d.Allocations[0].Request.Secrets = []nodeapi.DeliveredSecret{{Value: []byte("plaintext")}}
		}},
		{"token", func(d *nodeapi.ExecutionIntent) {
			d.Allocations[0].Request.EnvOverrides = map[string]map[string]string{"task": {"TOKEN": "token"}}
		}},
		{"malformed task", func(d *nodeapi.ExecutionIntent) { d.Allocations[0].Request.Tasks[0].Resources = nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			a := newOperationTestAgent(t, &reconcilerRuntime{})
			a.nodeID = uuid.New()
			local := storage.NewLocalStorage(t.TempDir())
			if err := local.Init(); err != nil {
				t.Fatal(err)
			}
			a.ConfigureDurability(local, "test")
			leader := uuid.New()
			a.server = client.NewServerClient("", "", nil)
			a.server.SetControlPlane(nodeapi.ControlPlaneResponse{LeaderID: leader})
			doc := executionTestDocument(a, leader)
			test.mutate(doc)
			if err := a.AcceptExecution(doc); err == nil {
				t.Fatal("invalid document accepted")
			}
			var saved nodeapi.ExecutionIntent
			if err := local.Get("agent/execution-intent", &saved); err == nil {
				t.Fatal("invalid document persisted")
			}
			if a.epoch != 0 {
				t.Fatal("invalid document advanced epoch")
			}
		})
	}
}

func TestRetainDoesNotLaunchAndCleanupReportsCompletion(t *testing.T) {
	rt := &reconcilerRuntime{}
	a := newOperationTestAgent(t, rt)
	a.nodeID = uuid.New()
	leader := uuid.New()
	a.server = client.NewServerClient("", "", nil)
	a.server.SetControlPlane(nodeapi.ControlPlaneResponse{LeaderID: leader})
	doc := executionTestDocument(a, leader)
	if err := a.AcceptExecution(doc); err != nil {
		t.Fatal(err)
	}
	if err := a.reconcileExecution(context.Background(), doc); err != nil {
		t.Fatal(err)
	}
	if len(a.starts) != 0 || len(a.allocations) != 0 {
		t.Fatal("retain launched execution")
	}
	doc.Version++
	doc.Allocations[0].Mode = nodeapi.ExecutionCleanup
	if err := a.AcceptExecution(doc); err != nil {
		t.Fatal(err)
	}
	if err := a.reconcileExecution(context.Background(), doc); err != nil {
		t.Fatal(err)
	}
	statuses := a.allocationStatuses()
	if len(statuses) != 1 || !statuses[0].CleanupComplete {
		t.Fatalf("cleanup observation: %+v", statuses)
	}
	doc.Version--
	if err := a.AcceptExecution(doc); err == nil {
		t.Fatal("stale version accepted")
	}
}

func TestExecutionExactEmptyGenerationReceipts(t *testing.T) {
	a := newOperationTestAgent(t, &reconcilerRuntime{})
	local := storage.NewLocalStorage(t.TempDir())
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	a.ConfigureDurability(local, "test")
	a.nodeID = uuid.New()
	leader := uuid.New()
	a.server = client.NewServerClient("", "", nil)
	a.server.SetControlPlane(nodeapi.ControlPlaneResponse{LeaderID: leader})
	doc := executionTestDocument(a, leader)
	doc.Allocations[0].Mode = nodeapi.ExecutionCleanup
	doc.Allocations[0].Request.Tasks = nil
	older := doc.Allocations[0]
	doc.Allocations[0].Request.Generation++
	doc.Allocations = append(doc.Allocations, older)
	if err := a.AcceptExecution(doc); err != nil {
		t.Fatal(err)
	}
	if err := a.reconcileExecution(context.Background(), doc); err != nil {
		t.Fatal(err)
	}
	proofs := map[uint64]bool{}
	for _, status := range a.allocationStatuses() {
		if status.CleanupComplete {
			proofs[status.Generation] = true
		}
	}
	if len(proofs) != 2 || !proofs[older.Request.Generation] || !proofs[older.Request.Generation+1] {
		t.Fatalf("exact receipts missing: %v", proofs)
	}
	restarted := newOperationTestAgent(t, &reconcilerRuntime{})
	restarted.ConfigureDurability(local, "test")
	if err := restarted.recoverStartAdmissions(); err != nil {
		t.Fatal(err)
	}
	if len(restarted.allocationStatuses()) != 2 {
		t.Fatal("restart lost exact generation receipts")
	}
}

func TestExecutionCleanupOlderKnownGeneration(t *testing.T) {
	a := newOperationTestAgent(t, &reconcilerRuntime{})
	a.nodeID = uuid.New()
	leader := uuid.New()
	a.server = client.NewServerClient("", "", nil)
	a.server.SetControlPlane(nodeapi.ControlPlaneResponse{LeaderID: leader})
	old := recoveryTestAllocation(0)
	old.Status = "stopping"
	a.allocations[old.ID] = old
	newer := *old
	newer.Generation++
	newer.ID = taskRecordID(newer.AllocationID, newer.Generation, newer.TaskName)
	newer.ContainerID = newer.ID
	newer.Status = "running"
	a.allocations[newer.ID] = &newer
	doc := executionTestDocument(a, leader)
	doc.Allocations[0] = nodeapi.AllocationIntent{Mode: nodeapi.ExecutionCleanup, Request: nodeapi.AllocationRequest{AllocationID: old.AllocationID, Generation: old.Generation, Epoch: doc.Epoch}}
	if err := a.AcceptExecution(doc); err != nil {
		t.Fatal(err)
	}
	if err := a.reconcileExecution(context.Background(), doc); err != nil {
		t.Fatal(err)
	}
	if _, exists := a.snapshotAllocation(old.ID); exists {
		t.Fatal("old generation still exists")
	}
	if _, exists := a.snapshotAllocation(newer.ID); !exists {
		t.Fatal("cleanup removed newer generation")
	}
	for _, status := range a.allocationStatuses() {
		if status.CleanupComplete && status.Generation == old.Generation {
			return
		}
	}
	t.Fatal("old generation cleanup has no exact proof")
}

func TestExecutionStalledAllocationAndSupersededCleanup(t *testing.T) {
	a := newOperationTestAgent(t, &reconcilerRuntime{})
	a.nodeID = uuid.New()
	leader := uuid.New()
	a.server = client.NewServerClient("", "", nil)
	a.server.SetControlPlane(nodeapi.ControlPlaneResponse{LeaderID: leader})
	doc := executionTestDocument(a, leader)
	id := doc.Allocations[0].Request.AllocationID
	doc.Allocations[0].Mode = nodeapi.ExecutionCleanup
	other := doc.Allocations[0]
	other.Request.AllocationID = "unrelated"
	doc.Allocations = append(doc.Allocations, other)
	unlock, err := a.waitAllocationOperation(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if unlock != nil {
			unlock()
		}
	}()
	if err := a.AcceptExecution(doc); err != nil {
		t.Fatal(err)
	}
	a.dispatchExecution(a.intentContext, a.intent, true)
	deadline := time.Now().Add(time.Second)
	for {
		a.mu.RLock()
		_, done := a.cleanupComplete[completionKey("unrelated", other.Request.Generation)]
		a.mu.RUnlock()
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("stalled allocation blocked unrelated cleanup")
		}
		time.Sleep(time.Millisecond)
	}
	doc.Version++
	doc.Allocations[0].Mode = nodeapi.ExecutionRetain
	if err := a.AcceptExecution(doc); err != nil {
		t.Fatal(err)
	}
	unlock()
	unlock = nil
	deadline = time.Now().Add(time.Second)
	for {
		a.intentMu.Lock()
		busy := a.intentWorkers[id]
		a.intentMu.Unlock()
		if !busy {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("superseded worker did not return")
		}
		time.Sleep(time.Millisecond)
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.stoppedGenerations[id] != 0 {
		t.Fatal("superseded cleanup admitted a stop after retain persisted")
	}
}

func TestExecutionVersionFenceWithoutCancellation(t *testing.T) {
	a := newOperationTestAgent(t, &reconcilerRuntime{})
	a.nodeID = uuid.New()
	leader := uuid.New()
	a.server = client.NewServerClient("", "", nil)
	a.server.SetControlPlane(nodeapi.ControlPlaneResponse{LeaderID: leader})
	old := executionTestDocument(a, leader)
	old.Allocations[0].Mode = nodeapi.ExecutionCleanup
	if err := a.AcceptExecution(old); err != nil {
		t.Fatal(err)
	}
	next := executionTestDocument(a, leader)
	next.Version++
	if err := a.AcceptExecution(next); err != nil {
		t.Fatal(err)
	}
	// Simulate a worker that already passed its cancellation check. A live
	// context cannot bypass the persisted same-epoch version fence.
	if err := a.reconcileExecution(context.Background(), old); !errors.Is(err, context.Canceled) {
		t.Fatalf("stale cleanup admission returned %v", err)
	}
	if len(a.allocationStatuses()) != 0 {
		t.Fatal("stale cleanup produced a receipt")
	}
	r := old.Allocations[0].Request
	ctx := context.WithValue(context.Background(), executionFenceKey{}, [2]uint64{old.Epoch, old.Version})
	if err := a.DrainGroup(ctx, &nodeapi.DrainAllocationRequest{AllocationID: r.AllocationID, Generation: r.Generation, Epoch: r.Epoch, Sequence: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("stale drain admission returned %v", err)
	}
}

func TestExecutionNewEpochResetsVersionAndFencesOldWorkers(t *testing.T) {
	for _, version := range []uint64{1, 9} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			a := newOperationTestAgent(t, &reconcilerRuntime{})
			a.nodeID = uuid.New()
			leader := uuid.New()
			a.server = client.NewServerClient("", "", nil)
			a.server.SetControlPlane(nodeapi.ControlPlaneResponse{LeaderID: leader})
			old := executionTestDocument(a, leader)
			old.Version = 9
			old.Allocations[0].Mode = nodeapi.ExecutionCleanup
			if err := a.AcceptExecution(old); err != nil {
				t.Fatal(err)
			}
			oldWork := a.intentContext
			next := executionTestDocument(a, leader)
			next.Epoch++
			next.Version = version
			next.Allocations[0].Request.Epoch = next.Epoch
			if err := a.AcceptExecution(next); err != nil {
				t.Fatalf("new epoch with reset/reused version rejected: %v", err)
			}
			if !errors.Is(oldWork.Err(), context.Canceled) {
				t.Fatal("new epoch did not cancel old workers")
			}
			if err := a.reconcileExecution(context.Background(), old); !errors.Is(err, ErrStaleEpoch) {
				t.Fatalf("old epoch cleanup admitted: %v", err)
			}
			if len(a.allocationStatuses()) != 0 {
				t.Fatal("old epoch cleanup produced a receipt")
			}
			old.Version = 100
			if err := a.AcceptExecution(old); err == nil {
				t.Fatal("older epoch with higher version accepted")
			}
		})
	}
}

func TestExecutionRecoveryValidatesCachedContent(t *testing.T) {
	a := newOperationTestAgent(t, &reconcilerRuntime{})
	a.local = storage.NewLocalStorage(t.TempDir())
	if err := a.local.Init(); err != nil {
		t.Fatal(err)
	}
	a.nodeID = uuid.New()
	doc := executionTestDocument(a, uuid.New())
	doc.Allocations[0].Request.Tasks = nil
	if err := a.local.Put("agent/execution-intent", doc); err != nil {
		t.Fatal(err)
	}
	if err := a.recoverExecutionIntent(); err == nil {
		t.Fatal("invalid cached tasks accepted")
	}
	doc = executionTestDocument(a, uuid.New())
	if err := a.local.Put("agent/execution-intent", doc); err != nil {
		t.Fatal(err)
	}
	if err := a.recoverExecutionIntent(); err != nil {
		t.Fatal(err)
	}
	if a.intentFresh {
		t.Fatal("cached authority admitted fresh starts")
	}
	recovered := newOperationTestAgent(t, &reconcilerRuntime{})
	recovered.local, recovered.nodeID = a.local, a.nodeID
	recovered.epoch = doc.Epoch + 1
	if err := recovered.recoverExecutionIntent(); err != nil {
		t.Fatal(err)
	}
	if recovered.intent != nil {
		t.Fatal("cached document older than exec epoch was replayed")
	}
}
