package server

import (
	"testing"
	"time"

	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
)

func TestReconcilePublishesPromptlyDespiteDisconnectedAgent(t *testing.T) {
	s, node, allocation := executionFixture(t)
	node.Host, node.Port = "192.0.2.1", 1
	allocation.Phase = lifecycle.PhaseRunning
	done := make(chan error, 1)
	go func() { done <- s.StopAllocationByID(t.Context(), allocation.Namespace, allocation.ID) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("disconnected agent delayed committed cleanup publication")
	}
	doc, err := s.ExecutionIntent(t.Context(), node.ID, 0)
	if err != nil || doc == nil || doc.Allocations[0].Mode != nodeapi.ExecutionCleanup {
		t.Fatalf("cleanup intent=%+v err=%v", doc, err)
	}
	if allocation.CleanupComplete || allocation.Phase != lifecycle.PhaseStopping {
		t.Fatal("publication was incorrectly treated as cleanup proof")
	}
}
