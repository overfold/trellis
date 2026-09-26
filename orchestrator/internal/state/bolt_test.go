package state

import (
	"context"
	"encoding/json"
	"net/url"
	"path/filepath"
	"testing"

	"github.com/clofour/trellis/internal/spec"
)

func validSnapshotJob(t *testing.T, namespace, name string, revision int) (string, []byte) {
	t.Helper()
	value, err := json.Marshal(persistedJob{Spec: &spec.JobSpec{
		Namespace:  namespace,
		Name:       name,
		TaskGroups: []spec.TaskGroupSpec{{Name: "group", Count: 1, Tasks: []spec.TaskSpec{{Name: "task", Image: "example/image:1"}}}},
	}, Revision: revision})
	if err != nil {
		t.Fatal(err)
	}
	return url.QueryEscape(namespace + "\x00" + name), value
}

func TestBoltStoreRoundTrip(t *testing.T) {
	store, err := NewBoltStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	ctx := context.Background()

	val, err := store.Get(ctx, "missing")
	if err != nil {
		t.Fatal(err)
	}
	if val != nil {
		t.Fatalf("expected nil for missing key, got %q", val)
	}

	if err := store.Put(ctx, "key1", []byte("value1")); err != nil {
		t.Fatal(err)
	}
	val, err = store.Get(ctx, "key1")
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "value1" {
		t.Fatalf("expected value1, got %q", val)
	}
}

func TestBoltStoreListPrefix(t *testing.T) {
	store, err := NewBoltStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	ctx := context.Background()

	_ = store.Put(ctx, "trellis/default/jobs/web", []byte("web"))
	_ = store.Put(ctx, "trellis/default/jobs/api", []byte("api"))
	_ = store.Put(ctx, "trellis/default/nodes/n1", []byte("node1"))
	if err := store.Put(ctx, "trellis/other/jobs/x", []byte("x")); err != nil {
		t.Fatal(err)
	}

	result, err := store.List(ctx, "trellis/default/jobs/")
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 results, got %d: %v", len(result), result)
	}
	if string(result["trellis/default/jobs/web"]) != "web" {
		t.Fatal("missing web entry")
	}
	if string(result["trellis/default/jobs/api"]) != "api" {
		t.Fatal("missing api entry")
	}
}

func TestBoltStoreDelete(t *testing.T) {
	store, err := NewBoltStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	ctx := context.Background()

	if err := store.Put(ctx, "key", []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, "key"); err != nil {
		t.Fatal(err)
	}
	val, err := store.Get(ctx, "key")
	if err != nil {
		t.Fatal(err)
	}
	if val != nil {
		t.Fatalf("expected nil after delete, got %q", val)
	}

	if err := store.Delete(ctx, "nonexistent"); err != nil {
		t.Fatal("delete of missing key should not error")
	}
}

func TestRestoreDesiredKeepsRuntimeStateAndRequiresFreshTarget(t *testing.T) {
	store, err := NewBoltStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	ctx := context.Background()
	if err := store.Put(ctx, "trellis/new/nodes/local", []byte(`{"id":"local"}`)); err != nil {
		t.Fatal(err)
	}
	jobKey, jobValue := validSnapshotJob(t, "default", "web", 3)
	snapshot := &DesiredSnapshot{
		Jobs:                     map[string][]byte{jobKey: jobValue},
		NetworkPortRegistrations: map[string][]byte{"acme": []byte(`{"namespace":"acme","slot":7}`)},
	}
	if err := store.RestoreDesired("new", snapshot); err != nil {
		t.Fatal(err)
	}
	all, err := store.List(ctx, "trellis/new/")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 ||
		all["trellis/new/nodes/local"] == nil ||
		all["trellis/new/jobs/"+jobKey] == nil ||
		all["trellis/new/network-port-registrations/acme"] == nil {
		t.Fatalf("unexpected restored state: %#v", all)
	}
	if err := store.RestoreDesired("new", snapshot); err == nil {
		t.Fatal("expected restore into non-fresh desired state to fail")
	}
}

func TestRestoreDesiredRejectsInvalidSnapshotWithoutWriting(t *testing.T) {
	store, err := NewBoltStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	key, value := validSnapshotJob(t, "default", "web", 2)
	var job persistedJob
	if err := json.Unmarshal(value, &job); err != nil {
		t.Fatal(err)
	}
	job.Spec.Name = "other"
	value, _ = json.Marshal(job)
	snapshot := &DesiredSnapshot{
		Jobs:                     map[string][]byte{key: value},
		NetworkPortRegistrations: map[string][]byte{"bad": []byte(`{"namespace":"bad","slot":-1}`)},
	}
	if err := store.RestoreDesired("new", snapshot); err == nil {
		t.Fatal("expected semantic validation failure")
	}
	entries, err := store.List(context.Background(), "trellis/new/")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("invalid restore wrote state: %#v", entries)
	}
}

func TestBoltStoreBackupRestoreVolumeRegistration(t *testing.T) {
	source, err := NewBoltStore(filepath.Join(t.TempDir(), "source.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = source.Close() }()
	ctx := context.Background()
	key := url.QueryEscape("acme/database")
	value := []byte(`{"namespace":"acme","name":"database","node_id":"2a193e44-b975-4b8b-83f0-93189885e38d"}`)
	if err := source.Put(ctx, "trellis/old/volume-registrations/"+key, value); err != nil {
		t.Fatal(err)
	}
	snapshot, err := source.DesiredSnapshot("old")
	if err != nil {
		t.Fatal(err)
	}

	target, err := NewBoltStore(filepath.Join(t.TempDir(), "target.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = target.Close() }()
	if err := target.RestoreDesired("new", snapshot); err != nil {
		t.Fatal(err)
	}
	restored, err := target.Get(ctx, "trellis/new/volume-registrations/"+key)
	if err != nil {
		t.Fatal(err)
	}
	if string(restored) != string(value) {
		t.Fatalf("restored volume registration = %q, want %q", restored, value)
	}
}

func TestBoltBatchAndDesiredSnapshot(t *testing.T) {
	store, err := NewBoltStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	ctx := context.Background()
	jobKey, jobValue := validSnapshotJob(t, "default", "web", 1)
	mutations := []Mutation{
		{Key: "trellis/c/jobs/" + jobKey, Value: jobValue},
		{Key: "trellis/c/job-revisions/" + jobKey + "/1", Value: []byte(`{"revision":1}`)},
	}
	if err := store.Batch(ctx, mutations); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.DesiredSnapshot("c")
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Jobs) != 1 || len(snapshot.JobRevisions) != 1 {
		t.Fatalf("snapshot omitted atomic job state: %#v", snapshot)
	}
	if err := store.Batch(ctx, []Mutation{{Key: "trellis/c/jobs/other", Value: []byte("bad")}, {Key: "", Value: []byte("fail")}}); err == nil {
		t.Fatal("expected invalid batch to fail")
	}
	if value, err := store.Get(ctx, "trellis/c/jobs/other"); err != nil || value != nil {
		t.Fatalf("failed batch was not atomic: value=%q err=%v", value, err)
	}
}

var _ Store = (*BoltStore)(nil)
var _ AtomicStore = (*BoltStore)(nil)
