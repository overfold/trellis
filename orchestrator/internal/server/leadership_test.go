package server

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/overfold/trellis/orchestrator/internal/spec"
	"github.com/overfold/trellis/orchestrator/internal/state"
)

func TestAcquireLeadershipAdvancesDurableEpoch(t *testing.T) {
	ctx := context.Background()
	store, err := state.NewBoltStore(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	stateCtl := NewStateController(store, "test-cluster")
	if err := stateCtl.PutCluster(ctx, &Cluster{AdministratorPublicKey: "key", ControlEpoch: 3}); err != nil {
		t.Fatal(err)
	}

	// Model a long-lived follower that initialized while epoch 1 was current,
	// then observed two leadership terms without restarting. Leadership must be
	// acquired from the durable epoch, not this stale in-memory snapshot.
	s := &Server{
		state:   stateCtl,
		cluster: &Cluster{AdministratorPublicKey: "key", ControlEpoch: 1},
		now:     time.Now,
	}
	if err := s.AcquireLeadership(ctx); err != nil {
		t.Fatal(err)
	}

	persisted, err := stateCtl.GetCluster(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if persisted == nil || persisted.ControlEpoch != 4 {
		t.Fatalf("durable control epoch = %v, want 4", persisted)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.controlEpoch != 4 {
		t.Fatalf("active control epoch = %d, want 4", s.controlEpoch)
	}
	if s.cluster.ControlEpoch != 4 {
		t.Fatalf("cached cluster control epoch = %d, want 4", s.cluster.ControlEpoch)
	}
}

func TestAcquireLeadershipUsesDurableJobsAndAuthority(t *testing.T) {
	store, err := state.NewBoltStore(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	controller := NewStateController(store, "test")
	cluster := &Cluster{AdministratorPublicKey: "current-key", ControlEpoch: 40, Settings: DefaultClusterSettings()}
	if err := controller.PutCluster(t.Context(), cluster); err != nil {
		t.Fatal(err)
	}
	job := &Job{Spec: canonicalTestSpec(&spec.JobSpec{Namespace: "default", Name: "durable", TaskGroups: []spec.TaskGroupSpec{{Name: "web", Count: 1, Tasks: []spec.TaskSpec{{Name: "app", Image: "example/app:1"}}}}}), Revision: 12, Version: 12}
	identity := jobKey("default", "durable")
	for version := 1; version <= 12; version++ {
		job.Revision, job.Version = version, version
		record := &JobRevisionRecord{Spec: job.Spec, Revision: version, Version: version, CreatedAt: time.Unix(int64(version), 0)}
		if err := controller.PutJobWithRevision(t.Context(), identity, job, record); err != nil {
			t.Fatal(err)
		}
	}
	// The follower cache omits a job accepted after its previous reload.
	s := &Server{state: controller, cluster: &Cluster{AdministratorPublicKey: "stale-key", ControlEpoch: 2}, jobs: map[string]*Job{}, now: time.Now}
	for expectedEpoch := uint64(41); expectedEpoch <= 42; expectedEpoch++ {
		if err := s.AcquireLeadership(t.Context()); err != nil {
			t.Fatal(err)
		}
		persisted, err := controller.GetCluster(t.Context())
		if err != nil || persisted.ControlEpoch != expectedEpoch || persisted.AdministratorPublicKey != "current-key" || s.controlEpoch != expectedEpoch || s.jobs[identity] == nil {
			t.Fatalf("activation lost durable authority/jobs: cluster=%+v error=%v", persisted, err)
		}
		records, err := controller.ListJobRevisions(t.Context(), identity)
		if err != nil || len(records) != 10 || records[0].Version != 3 || records[9].Version != 12 {
			t.Fatalf("stale cache removed durable history: %+v, %v", records, err)
		}
	}
	cluster.ControlEpoch = ^uint64(0)
	if err := controller.PutCluster(t.Context(), cluster); err != nil {
		t.Fatal(err)
	}
	if err := s.AcquireLeadership(t.Context()); err == nil {
		t.Fatal("exhausted control epoch wrapped and was reused")
	}
	persisted, err := controller.GetCluster(t.Context())
	if err != nil || persisted.ControlEpoch != ^uint64(0) || s.controlEpoch != 42 {
		t.Fatalf("exhaustion changed durable or active epoch: %+v active=%d error=%v", persisted, s.controlEpoch, err)
	}
}

func TestAcquireLeadershipEpochUpdateFailureLeavesEpochAndMemoryUnchanged(t *testing.T) {
	ctx := context.Background()
	store := &failingBatchStore{memoryStore: memoryStore{}}
	stateCtl := NewStateController(store, "test-cluster")
	if err := stateCtl.PutCluster(ctx, &Cluster{AdministratorPublicKey: "key", ControlEpoch: 3}); err != nil {
		t.Fatal(err)
	}
	identity := jobKey("default", "web")
	job := &Job{Spec: canonicalTestSpec(&spec.JobSpec{Namespace: "default", Name: "web"}), Revision: 12}
	s := &Server{
		state:        stateCtl,
		cluster:      &Cluster{AdministratorPublicKey: "key", ControlEpoch: 3},
		controlEpoch: 3,
		jobs:         map[string]*Job{identity: job},
		now:          time.Now,
	}

	err := s.AcquireLeadership(ctx)
	if err == nil || !strings.Contains(err.Error(), "storage unavailable") {
		t.Fatalf("AcquireLeadership error = %v, want storage failure", err)
	}
	persisted, getErr := stateCtl.GetCluster(ctx)
	if getErr != nil {
		t.Fatal(getErr)
	}
	if persisted.ControlEpoch != 3 || s.cluster.ControlEpoch != 3 || s.controlEpoch != 3 {
		t.Fatalf("epoch after failed activation: durable=%d cluster=%d active=%d; want 3", persisted.ControlEpoch, s.cluster.ControlEpoch, s.controlEpoch)
	}
	if s.jobs[identity] != job {
		t.Fatal("failed activation changed cached jobs")
	}
	if len(store.batches) != 1 {
		t.Fatalf("batch count = %d, want one epoch update batch", len(store.batches))
	}
	if len(store.batches[0]) != 1 || store.batches[0][0].Key != "trellis/test-cluster/meta" {
		t.Fatalf("leadership batch = %#v, want only cluster update", store.batches[0])
	}
}
