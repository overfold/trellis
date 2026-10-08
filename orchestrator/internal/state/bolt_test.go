package state

import (
	"bytes"
	"context"
	"encoding/json"
	"net/url"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/overfold/trellis/orchestrator/internal/spec"
	bolt "go.etcd.io/bbolt"
)

func validSnapshotJob(t *testing.T, namespace, name string, revision int) (string, []byte) {
	t.Helper()
	job := &spec.JobSpec{
		Namespace:  namespace,
		Name:       name,
		TaskGroups: []spec.TaskGroupSpec{{Name: "group", Count: 1, Tasks: []spec.TaskSpec{{Name: "task", Image: "example/image:1"}}}},
	}
	if err := spec.Canonicalize(job, spec.DefaultLimits()); err != nil {
		t.Fatal(err)
	}
	value, _ := json.Marshal(persistedJob{Spec: job, Revision: revision, Version: revision})
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

func TestBoltSnapshotStreamsDeterministicVersionedJSON(t *testing.T) {
	store, err := NewBoltStore(filepath.Join(t.TempDir(), "source.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	ctx := context.Background()
	for key, value := range map[string]string{"z/key": "last", "a/key": "first", "m/key": "middle"} {
		if err := store.Put(ctx, key, []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := store.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var encoded bytes.Buffer
	if err := snapshot.persistTo(&encoded); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	want, _ := json.Marshal(map[string][]byte{"z/key": []byte("last"), "a/key": []byte("first"), "m/key": []byte("middle")})
	want = append(append([]byte("[2,"), want...), ']')
	if !bytes.Equal(encoded.Bytes(), want) {
		t.Fatalf("streamed snapshot = %s, want %s", encoded.Bytes(), want)
	}

	target, err := NewBoltStore(filepath.Join(t.TempDir(), "target.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = target.Close() }()
	if err := target.RestoreReader(bytes.NewReader(encoded.Bytes())); err != nil {
		t.Fatal(err)
	}
	entries, err := target.List(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(entries, map[string][]byte{"a/key": []byte("first"), "m/key": []byte("middle"), "z/key": []byte("last")}) {
		t.Fatalf("restored streamed snapshot = %#v", entries)
	}
}

func TestBoltRestoreReaderLegacy(t *testing.T) {
	store, err := NewBoltStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	if err := store.Put(context.Background(), "old", []byte("discard")); err != nil {
		t.Fatal(err)
	}
	// Keys resembling envelope fields remain ordinary legacy keys. Opaque
	// binary and empty values must survive without record reinterpretation.
	if err := store.RestoreReader(strings.NewReader(`{"version":"AP8=","data":"","z":"bGFzdA=="}`)); err != nil {
		t.Fatal(err)
	}
	got, err := store.List(context.Background(), "")
	want := map[string][]byte{"version": {0, 255}, "data": {}, "z": []byte("last")}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("legacy restore = %#v, %v; want %#v", got, err, want)
	}
}

func TestBoltRestoreReaderIsAtomicOnInvalidSnapshot(t *testing.T) {
	store, err := NewBoltStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	ctx := context.Background()
	if err := store.Put(ctx, "existing", []byte("value")); err != nil {
		t.Fatal(err)
	}
	invalid := []string{
		``, `null`, `[]`, `[2]`, `[2,null]`, `[2,[]]`, `["2",{}]`,
		`[2.5,{}]`, `[null,{}]`, `[1,{}]`, `[0,{}]`, `[-1,{}]`,
		`[3,`, `[999,{}]`, `[2,{},{}]`, `[2,{}`, `[2,{}] {}`,
	}
	// Exercise the same strict payload contract in both supported formats,
	// including errors discovered only after a valid entry was written.
	for _, payload := range []string{
		`{"new":"bmV3","bad":"!"}`, `{"new":"bmV3","bad":null}`,
		`{"new":"bmV3","bad":4}`, `{"new":"bmV3","bad":{}}`,
		`{"new":"bmV3","":"YQ=="}`, `{"new":"","new":"YQ=="}`,
		`{"new":"bmV3","new":"YQ=="}`, `{"new":"bmV3"`,
		`{"` + strings.Repeat("k", bolt.MaxKeySize+1) + `":""}`,
	} {
		invalid = append(invalid, payload, "[2,"+payload+"]")
	}
	invalid = append(invalid, `{} {}`)
	for i, input := range invalid {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			if err := store.RestoreReader(strings.NewReader(input)); err == nil {
				t.Fatal("expected invalid snapshot to fail")
			}
			got, err := store.List(ctx, "")
			if err != nil || !reflect.DeepEqual(got, map[string][]byte{"existing": []byte("value")}) {
				t.Fatalf("failed restore changed state: entries=%#v err=%v", got, err)
			}
		})
	}
}

func TestBoltRestoreReaderUnknownVersionBeforeTransaction(t *testing.T) {
	store, err := NewBoltStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	// Even with storage unavailable and no payload, reject the version rather
	// than attempting a transaction or interpreting unknown data.
	err = store.RestoreReader(strings.NewReader(`[3,`))
	if err == nil || !strings.Contains(err.Error(), "unsupported application snapshot version 3") {
		t.Fatalf("expected version rejection before storage access, got %v", err)
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
		NetworkSubnetRegistrations: map[string][]byte{
			"acme/11111111-1111-1111-1111-111111111111": []byte(`{"namespace":"acme","node_id":"11111111-1111-1111-1111-111111111111","index":3}`),
		},
	}
	if err := store.RestoreDesired("new", snapshot); err != nil {
		t.Fatal(err)
	}
	all, err := store.List(ctx, "trellis/new/")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 4 ||
		all["trellis/new/nodes/local"] == nil ||
		all["trellis/new/jobs/"+jobKey] == nil ||
		all["trellis/new/network-port-registrations/acme"] == nil ||
		all["trellis/new/network-subnet-registrations/acme/11111111-1111-1111-1111-111111111111"] == nil {
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

func TestRestoreDottedDiscoveryIdentityIsAtomic(t *testing.T) {
	for _, field := range []string{"namespace", "job", "group", "volume namespace", "port namespace", "subnet namespace", "secret namespace"} {
		t.Run(field, func(t *testing.T) {
			store, err := NewBoltStore(filepath.Join(t.TempDir(), "test.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = store.Close() }()
			_, raw := validSnapshotJob(t, "default", "web", 1)
			var job persistedJob
			if err := json.Unmarshal(raw, &job); err != nil {
				t.Fatal(err)
			}
			snapshot := &DesiredSnapshot{}
			switch field {
			case "namespace":
				job.Spec.Namespace = "team.prod"
			case "job":
				job.Spec.Name = "web.v1"
			case "group":
				job.Spec.TaskGroups[0].Name = "api.v1"
			case "volume namespace":
				snapshot.VolumeRegistrations = map[string][]byte{url.QueryEscape("team.prod/data.v1"): []byte(`{"namespace":"team.prod","name":"data.v1","node_id":"11111111-1111-1111-1111-111111111111"}`)}
			case "port namespace":
				snapshot.NetworkPortRegistrations = map[string][]byte{"team.prod": []byte(`{"namespace":"team.prod","slot":0}`)}
			case "subnet namespace":
				snapshot.NetworkSubnetRegistrations = map[string][]byte{"team.prod/11111111-1111-1111-1111-111111111111": []byte(`{"namespace":"team.prod","node_id":"11111111-1111-1111-1111-111111111111","index":0}`)}
			case "secret namespace":
				snapshot.Secrets = map[string][]byte{"team.prod/db.password": []byte(`{"namespace":"team.prod","name":"db.password","version":1,"record_id":"r","key_id":"k","ciphertext_size":1,"nonce":"YQ","ciphertext":"YQ","wrap_nonce":"YQ","wrapped_dek":"YQ"}`)}
			}
			key := url.QueryEscape(job.Spec.Namespace + "\x00" + job.Spec.Name)
			raw, err = json.Marshal(job)
			if err != nil {
				t.Fatal(err)
			}
			snapshot.Jobs = map[string][]byte{key: raw}
			if err := store.RestoreDesired("test", snapshot); err == nil {
				t.Fatal("restore accepted dotted identity")
			}
			entries, err := store.List(t.Context(), "trellis/test/")
			if err != nil || len(entries) != 0 {
				t.Fatalf("invalid restore wrote state: %v, %v", entries, err)
			}
			// Namespace restrictions must not narrow ordinary resource names.
			safe := &DesiredSnapshot{
				VolumeRegistrations: map[string][]byte{url.QueryEscape("default/data.v1"): []byte(`{"namespace":"default","name":"data.v1","node_id":"11111111-1111-1111-1111-111111111111"}`)},
				Secrets:             map[string][]byte{"default/db.password": []byte(`{"namespace":"default","name":"db.password","version":1,"record_id":"r","key_id":"k","ciphertext_size":1,"nonce":"YQ","ciphertext":"YQ","wrap_nonce":"YQ","wrapped_dek":"YQ"}`)},
			}
			if err := ValidateDesiredSnapshot(safe, nil); err != nil {
				t.Fatalf("dotted ordinary names rejected: %v", err)
			}
		})
	}
}

func TestValidateDesiredSnapshotRejectsInvalidNetworkSubnetRegistrations(t *testing.T) {
	node := "11111111-1111-1111-1111-111111111111"
	other := "22222222-2222-2222-2222-222222222222"
	for name, registrations := range map[string]map[string][]byte{
		"negative index": {"acme/" + node: []byte(`{"namespace":"acme","node_id":"` + node + `","index":-1}`)},
		"key mismatch":   {"acme/" + other: []byte(`{"namespace":"acme","node_id":"` + node + `","index":0}`)},
		"missing node":   {"acme/00000000-0000-0000-0000-000000000000": []byte(`{"namespace":"acme","index":0}`)},
		"shared index": {
			"acme/" + node:  []byte(`{"namespace":"acme","node_id":"` + node + `","index":2}`),
			"acme/" + other: []byte(`{"namespace":"acme","node_id":"` + other + `","index":2}`),
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := ValidateDesiredSnapshot(&DesiredSnapshot{NetworkSubnetRegistrations: registrations}, nil); err == nil {
				t.Fatal("expected invalid network subnet registration to be rejected")
			}
		})
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

func TestBoltBatchDeletesPrefixAtomically(t *testing.T) {
	store, err := NewBoltStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	ctx := context.Background()
	for _, key := range []string{"revisions/job/1", "revisions/job/2", "revisions/other/1"} {
		if err := store.Put(ctx, key, []byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Batch(ctx, []Mutation{{DeletePrefix: "revisions/job/"}, {Key: "revisions/job/3", Value: []byte("new")}}); err != nil {
		t.Fatal(err)
	}
	entries, err := store.List(ctx, "revisions/")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || string(entries["revisions/job/3"]) != "new" || entries["revisions/other/1"] == nil {
		t.Fatalf("entries after prefix replacement = %#v", entries)
	}
	if err := store.Batch(ctx, []Mutation{{DeletePrefix: "revisions/"}, {Key: "", Value: []byte("invalid")}}); err == nil {
		t.Fatal("expected invalid prefix-deletion batch to fail")
	}
	entries, err = store.List(ctx, "revisions/")
	if err != nil || len(entries) != 2 {
		t.Fatalf("failed batch changed prefix: entries=%#v err=%v", entries, err)
	}
}

var _ Store = (*BoltStore)(nil)
var _ AtomicStore = (*BoltStore)(nil)
var _ PrefixIterator = (*BoltStore)(nil)

func TestDesiredSnapshotCarriesClusterRecord(t *testing.T) {
	store, err := NewBoltStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	ctx := context.Background()
	if err := store.Put(ctx, "trellis/old/meta", []byte(`{"settings":{"a":1}}`)); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.DesiredSnapshot("old")
	if err != nil {
		t.Fatal(err)
	}
	if string(snapshot.Cluster) != `{"settings":{"a":1}}` {
		t.Fatalf("snapshot cluster = %q", snapshot.Cluster)
	}

	snapshot.Cluster = []byte(`{"settings":{"a":2}}`)
	if err := store.RestoreDesired("new", snapshot); err != nil {
		t.Fatal(err)
	}
	restored, err := store.Get(ctx, "trellis/new/meta")
	if err != nil || string(restored) != `{"settings":{"a":2}}` {
		t.Fatalf("restored cluster record = %q, %v", restored, err)
	}
	if err := store.RestoreDesired("other", &DesiredSnapshot{Cluster: []byte(`{`)}); err == nil {
		t.Fatal("restored an invalid cluster record")
	}
}
