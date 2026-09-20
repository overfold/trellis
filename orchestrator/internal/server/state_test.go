package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/lifecycle"
	"github.com/clofour/trellis/internal/spec"
	"github.com/clofour/trellis/internal/state"
)

type backupStore struct {
	snapshot *state.DesiredSnapshot
	data     memoryStore
}

func (b *backupStore) BackupDesired(string) (*state.DesiredSnapshot, error) { return b.snapshot, nil }
func (b *backupStore) RestoreDesired(_ string, snapshot *state.DesiredSnapshot) error {
	b.snapshot = snapshot
	return nil
}
func (b *backupStore) Get(ctx context.Context, key string) ([]byte, error) {
	return b.data.Get(ctx, key)
}
func (b *backupStore) List(ctx context.Context, prefix string) (map[string][]byte, error) {
	return b.data.List(ctx, prefix)
}
func (b *backupStore) Put(ctx context.Context, key string, value []byte) error {
	return b.data.Put(ctx, key, value)
}
func (b *backupStore) Delete(ctx context.Context, key string) error { return b.data.Delete(ctx, key) }

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
func (m memoryStore) Batch(_ context.Context, mutations []state.Mutation) error {
	for _, mutation := range mutations {
		if mutation.Key == "" {
			return fmt.Errorf("empty key")
		}
	}
	for _, mutation := range mutations {
		if mutation.Value == nil {
			delete(m, mutation.Key)
		} else {
			m[mutation.Key] = mutation.Value
		}
	}
	return nil
}

var _ state.Store = memoryStore{}
var _ state.AtomicStore = memoryStore{}

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
		Health:     lifecycle.HealthUnknown}
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

func TestBackupRestoreRoundTripsPersistedJob(t *testing.T) {
	ctx := context.Background()
	store := &backupStore{data: memoryStore{}, snapshot: &state.DesiredSnapshot{Jobs: map[string][]byte{}, JobRevisions: map[string][]byte{}, Secrets: map[string][]byte{}, VolumeRegistrations: map[string][]byte{}, NetworkPortRegistrations: map[string][]byte{}}}
	job := &Job{Spec: &spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "api", Count: 1, Tasks: []spec.TaskSpec{{Name: "app", Image: "app"}}}}}, Revision: 1}
	raw, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	store.snapshot.Jobs["default%00web"] = raw
	historical := &JobRevisionRecord{Revision: 1, Spec: &spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "api", Count: 2000, Tasks: []spec.TaskSpec{{Name: "app", Image: "app"}}}}}, CreatedAt: time.Now()}
	historicalRaw, err := json.Marshal(historical)
	if err != nil {
		t.Fatal(err)
	}
	store.snapshot.JobRevisions["default%00web/1"] = historicalRaw
	s := NewServer(slog.Default(), nil, newNopStateController(), store, "test", "")
	if err := s.Restore(ctx, mustBackup(t, s)); err != nil {
		t.Fatalf("restore backup containing persisted job: %v", err)
	}
	var restored Job
	if err := json.Unmarshal(store.snapshot.Jobs["default%00web"], &restored); err != nil {
		t.Fatal(err)
	}
	if restored.Spec == nil || restored.Spec.TaskGroups[0].Tasks[0].Resources == nil {
		t.Fatalf("restored job was not canonicalized: %#v", restored)
	}
	if string(store.snapshot.JobRevisions["default%00web/1"]) != string(historicalRaw) {
		t.Fatal("restore rewrote historical revision")
	}
}

func mustBackup(t *testing.T, s *Server) *api.BackupSnapshot {
	t.Helper()
	backup, err := s.Backup(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return backup
}
