package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"testing"
	"time"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/network"
	"github.com/google/uuid"
)

func TestNamespaceNodeSubnetIsStableAndNamespaceScoped(t *testing.T) {
	pool := netip.MustParsePrefix("10.64.0.0/10")
	node := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	first := namespaceNodeSubnet(pool, "acme", node)
	if first != namespaceNodeSubnet(pool, "acme", node) {
		t.Fatal("subnet allocation is not stable")
	}
	if !pool.Contains(first.Addr()) || first.Bits() != 24 {
		t.Fatalf("subnet %s is outside pool %s", first, pool)
	}
	if first == namespaceNodeSubnet(pool, "globex", node) {
		t.Fatal("different namespaces received the same deterministic subnet")
	}
}

func TestNetworkPlanUsesRegisteredPeerIdentity(t *testing.T) {
	targetID, peerID := uuid.New(), uuid.New()
	s := &Server{
		networkPool:  netip.MustParsePrefix("10.64.0.0/10"),
		networkPorts: map[string]int{"acme": 3},
		nodes:        map[uuid.UUID]*Node{},
	}
	target := &Node{ID: targetID, WireGuardPortBase: 51820, WireGuardPortCount: 256}
	s.nodes[targetID] = target
	s.nodes[peerID] = &Node{
		ID: peerID, WireGuardPublicKey: "peer-key", WireGuardEndpoint: "node-b:51820",
		WireGuardPortBase: 51820, WireGuardPortCount: 256,
	}
	plan, err := s.networkPlan("acme", target)
	if err != nil {
		t.Fatal(err)
	}
	if plan.ListenPort != 51823 {
		t.Fatalf("listen port = %d, want 51823", plan.ListenPort)
	}
	if len(plan.Peers) != 1 || plan.Peers[0].PublicKey != "peer-key" || plan.Peers[0].Endpoint != "node-b:51823" {
		t.Fatalf("unexpected plan: %#v", plan)
	}
}

func TestNetworkPlanUsesDifferentPortsForDifferentNamespaces(t *testing.T) {
	nodeID := uuid.New()
	node := &Node{ID: nodeID, WireGuardPortBase: 51820, WireGuardPortCount: 256}
	s := &Server{
		networkPool:  netip.MustParsePrefix("10.64.0.0/10"),
		networkPorts: map[string]int{"acme": 3, "globex": 11},
		nodes:        map[uuid.UUID]*Node{nodeID: node},
	}
	acme, err := s.networkPlan("acme", node)
	if err != nil {
		t.Fatal(err)
	}
	globex, err := s.networkPlan("globex", node)
	if err != nil {
		t.Fatal(err)
	}
	if acme.ListenPort != 51823 || globex.ListenPort != 51831 || acme.ListenPort == globex.ListenPort {
		t.Fatalf("unexpected namespace ports: acme=%d globex=%d", acme.ListenPort, globex.ListenPort)
	}
}

func TestNetworkPlanOperationTimeoutScalesWithWorkAndRetry(t *testing.T) {
	if got := networkPlanOperationTimeout(&network.Plan{}, 0); got != networkPlanBaseTimeout {
		t.Fatalf("empty plan timeout = %s, want %s", got, networkPlanBaseTimeout)
	}

	plan := &network.Plan{Peers: make([]network.PeerPlan, 400)}
	for i := range plan.Peers {
		plan.Peers[i] = network.PeerPlan{
			PublicKey:  fmt.Sprintf("peer-%03d", i),
			AllowedIPs: []string{fmt.Sprintf("10.%d.%d.0/24", 50+i/256, i%256)},
		}
	}
	first := networkPlanOperationTimeout(plan, 0)
	if first <= networkPlanBaseTimeout {
		t.Fatalf("large plan timeout = %s, want > %s", first, networkPlanBaseTimeout)
	}
	second := networkPlanOperationTimeout(plan, 1)
	if second != 2*first {
		t.Fatalf("retry timeout = %s, want %s", second, 2*first)
	}
}

func TestSendNetworkPlanTargetUsesScaledDeadline(t *testing.T) {
	s := &Server{
		log:          slog.Default(),
		now:          time.Now,
		controlEpoch: 7,
	}
	plan := &network.Plan{Peers: make([]network.PeerPlan, 400)}
	for i := range plan.Peers {
		plan.Peers[i] = network.PeerPlan{
			PublicKey:  fmt.Sprintf("peer-%03d", i),
			AllowedIPs: []string{fmt.Sprintf("10.%d.%d.0/24", 50+i/256, i%256)},
		}
	}
	target := networkPlanTarget{
		key:       networkPlanKey{nodeID: uuid.New(), namespace: "acme"},
		namespace: "acme",
		address:   "node-a:8127",
		nodeID:    uuid.New(),
		plan:      plan,
		epoch:     7,
		attempt:   1,
	}

	started := time.Now()
	called := false
	s.sendNetworkPlanTarget(context.Background(), target, func(ctx context.Context, _ uuid.UUID, _ string, _ *api.NetworkPlanRequest) error {
		called = true
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatal("network plan request has no deadline")
		}
		want := networkPlanOperationTimeout(plan, 1)
		if got := deadline.Sub(started); got < want-time.Second {
			t.Fatalf("network plan deadline budget = %s, want approximately %s", got, want)
		}
		return nil
	})
	if !called {
		t.Fatal("network plan update was not called")
	}
}

func TestNetworkPlanStateCoalescesUnchangedPlansAndRepairsPeriodically(t *testing.T) {
	now := time.Date(2026, 9, 23, 18, 0, 0, 0, time.UTC)
	s := &Server{
		log:                slog.Default(),
		now:                func() time.Time { return now },
		controlEpoch:       7,
		networkPlans:       make(map[networkPlanKey]*networkPlanState),
		networkPlanWorkers: make(map[uuid.UUID]uint64),
	}
	nodeID := uuid.New()
	target := networkPlanTarget{
		key:       networkPlanKey{nodeID: nodeID, namespace: "acme"},
		namespace: "acme",
		address:   "node-a:8127",
		nodeID:    nodeID,
		plan:      &network.Plan{ListenPort: 51820},
		epoch:     7,
	}

	s.setDesiredNetworkPlans([]networkPlanTarget{target})
	pending := s.claimPendingNetworkPlans(now, 7)
	if len(pending) != 1 {
		t.Fatalf("initial pending plans = %d, want 1", len(pending))
	}
	s.finishNetworkPlanAttempt(pending[0], nil)
	s.releaseNetworkPlanWorker(nodeID, 7)

	s.setDesiredNetworkPlans([]networkPlanTarget{target})
	if pending := s.claimPendingNetworkPlans(now.Add(time.Minute), 7); len(pending) != 0 {
		t.Fatalf("unchanged plan was requeued: %#v", pending)
	}

	changed := target
	changed.plan = &network.Plan{
		ListenPort: 51820,
		Peers:      []network.PeerPlan{{PublicKey: "new-peer", AllowedIPs: []string{"10.64.1.0/24"}}},
	}
	s.setDesiredNetworkPlans([]networkPlanTarget{changed})
	pending = s.claimPendingNetworkPlans(now.Add(time.Minute), 7)
	if len(pending) != 1 {
		t.Fatalf("changed pending plans = %d, want 1", len(pending))
	}
	s.finishNetworkPlanAttempt(pending[0], nil)
	s.releaseNetworkPlanWorker(nodeID, 7)

	identityChanged := changed
	identityChanged.wireGuardPublicKey = "rotated-key"
	s.setDesiredNetworkPlans([]networkPlanTarget{identityChanged})
	pending = s.claimPendingNetworkPlans(now.Add(2*time.Minute), 7)
	if len(pending) != 1 {
		t.Fatalf("identity change pending plans = %d, want 1", len(pending))
	}
	s.finishNetworkPlanAttempt(pending[0], nil)
	s.releaseNetworkPlanWorker(nodeID, 7)

	if pending := s.claimPendingNetworkPlans(now.Add(networkPlanRepairInterval+time.Second), 7); len(pending) != 1 {
		t.Fatalf("periodic repair pending plans = %d, want 1", len(pending))
	}
}

func TestNetworkPlanDispatcherIsolatesBlockedNodeAndRotatesNamespaces(t *testing.T) {
	s := &Server{
		log:                slog.Default(),
		now:                time.Now,
		controlEpoch:       7,
		networkPlans:       make(map[networkPlanKey]*networkPlanState),
		networkPlanWorkers: make(map[uuid.UUID]uint64),
		networkPlanWake:    make(chan struct{}, 1),
	}
	slowNode, healthyNode, laterNode := uuid.New(), uuid.New(), uuid.New()
	target := func(nodeID uuid.UUID, namespace, address string) networkPlanTarget {
		return networkPlanTarget{
			key:       networkPlanKey{nodeID: nodeID, namespace: namespace},
			namespace: namespace,
			address:   address,
			nodeID:    nodeID,
			plan:      &network.Plan{},
			epoch:     7,
		}
	}
	targets := []networkPlanTarget{
		target(slowNode, "one", "slow"),
		target(slowNode, "two", "slow"),
		target(healthyNode, "default", "healthy"),
	}
	s.setDesiredNetworkPlans(targets)

	slowCalls := make(chan string, 2)
	slowRelease := make(chan struct{}, 2)
	healthyCalls := make(chan string, 2)
	update := func(_ context.Context, _ uuid.UUID, address string, request *api.NetworkPlanRequest) error {
		if request.Epoch != 7 {
			t.Errorf("request epoch = %d, want 7", request.Epoch)
		}
		if address == "slow" {
			slowCalls <- request.Namespace
			<-slowRelease
			return errors.New("agent unavailable")
		}
		healthyCalls <- request.Namespace
		return nil
	}

	s.dispatchPendingNetworkPlans(context.Background(), update)
	select {
	case got := <-slowCalls:
		if got != "one" {
			t.Fatalf("first slow namespace = %q, want one", got)
		}
	case <-time.After(time.Second):
		t.Fatal("slow node worker did not start")
	}
	select {
	case got := <-healthyCalls:
		if got != "default" {
			t.Fatalf("healthy namespace = %q, want default", got)
		}
	case <-time.After(time.Second):
		t.Fatal("healthy node was blocked by slow node")
	}

	// A topology change on another node must dispatch while the first node is
	// still blocked in its request.
	targets = append(targets, target(laterNode, "later", "later"))
	s.setDesiredNetworkPlans(targets)
	s.dispatchPendingNetworkPlans(context.Background(), update)
	select {
	case got := <-healthyCalls:
		if got != "later" {
			t.Fatalf("later namespace = %q, want later", got)
		}
	case <-time.After(time.Second):
		t.Fatal("new healthy-node work was blocked by slow node")
	}

	slowRelease <- struct{}{}
	deadline := time.Now().Add(time.Second)
	for {
		s.networkPlanMu.Lock()
		busy := s.networkPlanWorkers[slowNode] == 7
		s.networkPlanMu.Unlock()
		if !busy {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("slow node worker did not release")
		}
		time.Sleep(time.Millisecond)
	}

	// The failed namespace is backed off, so the next claim for this node must
	// advance to the other dirty namespace instead of restarting at "one".
	s.dispatchPendingNetworkPlans(context.Background(), update)
	select {
	case got := <-slowCalls:
		if got != "two" {
			t.Fatalf("second slow namespace = %q, want two", got)
		}
	case <-time.After(time.Second):
		t.Fatal("second namespace was starved after first failure")
	}
	slowRelease <- struct{}{}
}
