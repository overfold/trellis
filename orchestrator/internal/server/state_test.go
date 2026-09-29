package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/overfold/trellis/internal/api"
	"github.com/overfold/trellis/internal/lifecycle"
	"github.com/overfold/trellis/internal/spec"
	"github.com/overfold/trellis/internal/state"
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
		if mutation.Key == "" && mutation.DeletePrefix == "" {
			return fmt.Errorf("empty key")
		}
	}
	for _, mutation := range mutations {
		if mutation.DeletePrefix != "" {
			for key := range m {
				if strings.HasPrefix(key, mutation.DeletePrefix) {
					delete(m, key)
				}
			}
			continue
		}
		if mutation.Value == nil {
			delete(m, mutation.Key)
		} else {
			m[mutation.Key] = mutation.Value
		}
	}
	return nil
}

func (m memoryStore) IteratePrefix(ctx context.Context, prefix string, visit func(key string, value []byte) error) error {
	keys := make([]string, 0)
	for key := range m {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(key, m[key]); err != nil {
			return err
		}
	}
	return nil
}

var _ state.Store = memoryStore{}
var _ state.AtomicStore = memoryStore{}
var _ state.PrefixIterator = memoryStore{}

func TestJobRevisionRetentionAndDeletion(t *testing.T) {
	ctx := context.Background()
	store := memoryStore{}
	controller := NewStateController(store, "test")
	identity := jobKey("default", "web")
	for revision := 1; revision <= jobRevisionRetention+3; revision++ {
		job := &Job{Spec: &spec.JobSpec{Namespace: "default", Name: "web"}, Revision: revision}
		record := &JobRevisionRecord{Revision: revision, Spec: job.Spec, CreatedAt: time.Unix(int64(revision), 0).UTC()}
		if err := controller.PutJobWithRevision(ctx, identity, job, record); err != nil {
			t.Fatal(err)
		}
	}
	revisions, err := controller.ListJobRevisions(ctx, identity)
	if err != nil {
		t.Fatal(err)
	}
	if len(revisions) != jobRevisionRetention || revisions[0].Revision != 4 || revisions[len(revisions)-1].Revision != 13 {
		t.Fatalf("retained revisions = %#v, want 4 through 13", revisions)
	}
	if err := controller.DeleteJob(ctx, identity); err != nil {
		t.Fatal(err)
	}
	if entries, err := store.List(ctx, "trellis/test/job-revisions/"); err != nil || len(entries) != 0 {
		t.Fatalf("job deletion retained revisions: entries=%#v err=%v", entries, err)
	}
}

func TestListJobRevisionsBoundsLegacyHistory(t *testing.T) {
	ctx := context.Background()
	store := memoryStore{}
	controller := NewStateController(store, "test")
	prefix := "trellis/test/job-revisions/" + url.QueryEscape(jobKey("default", "web")) + "/"
	for revision := 1; revision <= 25; revision++ {
		raw, err := json.Marshal(&JobRevisionRecord{Revision: revision, Spec: &spec.JobSpec{Namespace: "default", Name: "web"}, CreatedAt: time.Unix(int64(revision), 0).UTC()})
		if err != nil {
			t.Fatal(err)
		}
		store[prefix+fmt.Sprint(revision)] = raw
	}
	revisions, err := controller.ListJobRevisions(ctx, jobKey("default", "web"))
	if err != nil {
		t.Fatal(err)
	}
	if len(revisions) != jobRevisionRetention || revisions[0].Revision != 16 || revisions[9].Revision != 25 {
		t.Fatalf("bounded revisions = %#v, want 16 through 25", revisions)
	}
}

func TestCompactJobRevisionsBoundsLegacyHistoryAndRemovesOrphans(t *testing.T) {
	ctx := context.Background()
	store := memoryStore{}
	controller := NewStateController(store, "test")
	liveIdentity := jobKey("default", "web")
	for _, identity := range []string{liveIdentity, jobKey("default", "deleted")} {
		prefix := "trellis/test/job-revisions/" + url.QueryEscape(identity) + "/"
		for revision := 1; revision <= 12; revision++ {
			name := "web"
			if identity != liveIdentity {
				name = "deleted"
			}
			raw, err := json.Marshal(&JobRevisionRecord{Revision: revision, Spec: &spec.JobSpec{Namespace: "default", Name: name}, CreatedAt: time.Unix(int64(revision), 0).UTC()})
			if err != nil {
				t.Fatal(err)
			}
			store[prefix+fmt.Sprint(revision)] = raw
		}
	}
	if err := controller.CompactJobRevisions(ctx, map[string]*Job{liveIdentity: {Spec: &spec.JobSpec{Namespace: "default", Name: "web"}, Revision: 12}}); err != nil {
		t.Fatal(err)
	}
	entries, err := store.List(ctx, "trellis/test/job-revisions/")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != jobRevisionRetention {
		t.Fatalf("compacted revision count = %d, want %d", len(entries), jobRevisionRetention)
	}
	revisions, err := controller.ListJobRevisions(ctx, liveIdentity)
	if err != nil {
		t.Fatal(err)
	}
	if revisions[0].Revision != 3 || revisions[9].Revision != 12 {
		t.Fatalf("compacted revisions = %#v, want 3 through 12", revisions)
	}
}

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

func TestRetainedJobRevisionEntriesBoundsHistoryAndDropsOrphans(t *testing.T) {
	jobs := map[string][]byte{url.QueryEscape(jobKey("default", "web")): []byte(`{}`)}
	revisions := make(map[string][]byte)
	for _, name := range []string{"web", "deleted"} {
		identity := jobKey("default", name)
		for revision := 1; revision <= jobRevisionRetention+2; revision++ {
			raw, err := json.Marshal(&JobRevisionRecord{
				Revision:  revision,
				Spec:      &spec.JobSpec{Namespace: "default", Name: name},
				CreatedAt: time.Unix(int64(revision), 0).UTC(),
			})
			if err != nil {
				t.Fatal(err)
			}
			revisions[url.QueryEscape(identity)+"/"+fmt.Sprint(revision)] = raw
		}
	}
	retained, err := retainedJobRevisionEntries(jobs, revisions)
	if err != nil {
		t.Fatal(err)
	}
	if len(retained) != jobRevisionRetention {
		t.Fatalf("retained revision count = %d, want %d", len(retained), jobRevisionRetention)
	}
	if retained["default%00web/1"] != nil || retained["default%00web/2"] != nil || retained["default%00web/3"] == nil || retained["default%00deleted/12"] != nil {
		t.Fatalf("unexpected retained revisions: %#v", retained)
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
