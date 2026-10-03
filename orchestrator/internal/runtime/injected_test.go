package runtime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestInjectedRuntimeAmbiguousResultsAndPersistence(t *testing.T) {
	dir := t.TempDir()
	state, faults := filepath.Join(dir, "runtime.json"), filepath.Join(dir, "fault.json")
	r, err := NewInjectedRuntime(state, faults)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	id, err := r.Create(ctx, CreateOptions{ID: "allocation/web", Labels: map[string]string{"trellis.cluster": "test"}})
	if err != nil {
		t.Fatal(err)
	}
	writeFault(t, faults, InjectedFault{Operation: "start", Timing: "after", Count: 1})
	if err := r.Start(ctx, id); err == nil {
		t.Fatal("ambiguous start must report an error")
	}
	info, err := r.Inspect(ctx, id)
	if err != nil || info.Status != StatusRunning {
		t.Fatalf("start effect was not retained: info=%+v err=%v", info, err)
	}

	writeFault(t, faults, InjectedFault{Operation: "stop", Timing: "after", Count: 1})
	if err := r.Stop(ctx, id); err == nil {
		t.Fatal("ambiguous stop must report an error")
	}
	reopened, err := NewInjectedRuntime(state, faults)
	if err != nil {
		t.Fatal(err)
	}
	managed, err := reopened.ListManaged(ctx, "test")
	if err != nil || len(managed) != 1 || managed[0].Status != StatusStopped {
		t.Fatalf("durable inventory mismatch: %+v, %v", managed, err)
	}
	// The one-shot fault is durable too; retrying converges successfully.
	if err := reopened.Stop(ctx, id); err != nil {
		t.Fatalf("idempotent retry: %v", err)
	}
}

func writeFault(t *testing.T, path string, fault InjectedFault) {
	t.Helper()
	b, _ := json.Marshal(fault)
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestInjectedRuntimePausedContainerStaysListed(t *testing.T) {
	dir := t.TempDir()
	state := filepath.Join(dir, "runtime.json")
	r, err := NewInjectedRuntime(state, "")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	id, err := r.Create(ctx, CreateOptions{ID: "allocation/web", Labels: map[string]string{"trellis.cluster": "test"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Start(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := r.Pause(ctx, id); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewInjectedRuntime(state, "")
	if err != nil {
		t.Fatal(err)
	}
	managed, err := reopened.ListManaged(ctx, "test")
	if err != nil || len(managed) != 1 || managed[0].ID != id || managed[0].Status != StatusPaused {
		t.Fatalf("paused container listing = %+v, %v", managed, err)
	}
	if err := reopened.Stop(ctx, id); err != nil {
		t.Fatalf("stop paused container: %v", err)
	}
}
