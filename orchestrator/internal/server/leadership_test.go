package server

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/network"
	"github.com/clofour/trellis/internal/state"
	"github.com/google/uuid"
)

func TestAcquireLeadershipAdvancesDurableEpoch(t *testing.T) {
	ctx := context.Background()
	store, err := state.NewBoltStore(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	stateCtl := NewStateController(store, "test-cluster")
	if err := stateCtl.PutCluster(ctx, &Cluster{Hash: "hash", ControlEpoch: 3}); err != nil {
		t.Fatal(err)
	}

	// Model a long-lived follower that initialized while epoch 1 was current,
	// then observed two leadership terms without restarting. Leadership must be
	// acquired from the durable epoch, not this stale in-memory snapshot.
	s := &Server{
		state:   stateCtl,
		cluster: &Cluster{Hash: "hash", ControlEpoch: 1},
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

func TestAcquireLeadershipInvalidatesAndFencesCachedNetworkPlans(t *testing.T) {
	ctx := context.Background()
	store, err := state.NewBoltStore(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	stateCtl := NewStateController(store, "test-cluster")
	if err := stateCtl.PutCluster(ctx, &Cluster{Hash: "hash", ControlEpoch: 3}); err != nil {
		t.Fatal(err)
	}

	now := time.Date(2026, 9, 24, 17, 0, 0, 0, time.UTC)
	nodeID := uuid.New()
	key := networkPlanKey{nodeID: nodeID, namespace: "acme"}
	stale := networkPlanTarget{
		key:       key,
		namespace: "acme",
		address:   "node-a:8127",
		nodeID:    nodeID,
		plan: &network.Plan{
			ListenPort: 51820,
			Peers:      []network.PeerPlan{{PublicKey: "old-peer"}},
		},
		hash:  "old",
		epoch: 3,
	}
	s := &Server{
		state:              stateCtl,
		cluster:            &Cluster{Hash: "hash", ControlEpoch: 3},
		controlEpoch:       3,
		now:                func() time.Time { return now },
		networkPlans:       map[networkPlanKey]*networkPlanState{key: {target: stale, appliedHash: "old", appliedAt: now.Add(-networkPlanRepairInterval - time.Second)}},
		networkPlanWorkers: make(map[uuid.UUID]uint64),
		networkPlanWake:    make(chan struct{}, 1),
	}
	if err := s.AcquireLeadership(ctx); err != nil {
		t.Fatal(err)
	}

	s.networkPlanMu.Lock()
	if len(s.networkPlans) != 0 {
		t.Fatalf("cached plans survived leadership change: %#v", s.networkPlans)
	}
	s.networkPlanMu.Unlock()

	// Even if an old-term target races back into the cache, it must not be sent
	// using the new term's epoch.
	s.setDesiredNetworkPlans([]networkPlanTarget{stale})
	emitted := make(chan *api.NetworkPlanRequest, 1)
	s.dispatchPendingNetworkPlans(ctx, func(_ context.Context, _ string, request *api.NetworkPlanRequest) error {
		emitted <- request
		return nil
	})
	select {
	case request := <-emitted:
		t.Fatalf("stale plan emitted after leadership change: %#v", request)
	case <-time.After(30 * time.Millisecond):
	}

	fresh := stale
	fresh.epoch = 4
	fresh.plan = &network.Plan{ListenPort: 51820, Peers: []network.PeerPlan{{PublicKey: "new-peer"}}}
	s.setDesiredNetworkPlans([]networkPlanTarget{fresh})
	s.dispatchPendingNetworkPlans(ctx, func(_ context.Context, _ string, request *api.NetworkPlanRequest) error {
		emitted <- request
		return nil
	})
	select {
	case request := <-emitted:
		if request.Epoch != 4 || len(request.Plan.Peers) != 1 || request.Plan.Peers[0].PublicKey != "new-peer" {
			t.Fatalf("fresh request = %#v, want epoch 4 new-peer plan", request)
		}
	case <-time.After(time.Second):
		t.Fatal("fresh plan was not emitted")
	}
}
