package agent

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/internal/client"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	"github.com/overfold/trellis/orchestrator/internal/storage"
)

func TestExecutionAbsenceAcknowledgesDurableReceipt(t *testing.T) {
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
	if err := a.AcceptExecution(doc); err != nil {
		t.Fatal(err)
	}
	if err := a.reconcileExecution(context.Background(), doc); err != nil {
		t.Fatal(err)
	}
	id, generation := doc.Allocations[0].Request.AllocationID, doc.Allocations[0].Request.Generation
	doc.Version++
	doc.Allocations = []nodeapi.AllocationIntent{}
	if err := a.AcceptExecution(doc); err != nil {
		t.Fatal(err)
	}
	a.dispatchExecution(context.Background(), doc, true)
	if len(a.cleanupComplete) != 1 {
		t.Fatal("recovery grace discarded receipt")
	}
	doc.Version++
	doc.CleanupAbsent = true
	if err := a.AcceptExecution(doc); err != nil {
		t.Fatal(err)
	}
	a.dispatchExecution(context.Background(), doc, true)
	if len(a.cleanupComplete) != 0 || a.stoppedGenerations[id] != generation {
		t.Fatal("receipt retirement lost generation fence")
	}
	records, errs := local.ListRaw("agent/cleanup-complete")
	if len(records) != 0 || len(errs) != 0 {
		t.Fatalf("receipt persisted after acknowledgement: %d %v", len(records), errs)
	}
}
