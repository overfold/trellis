package server

import (
	"context"
	"errors"
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

func TestNetworkPlanStateCoalescesUnchangedPlansAndRepairsPeriodically(t *testing.T) {
	now := time.Date(2026, 9, 23, 18, 0, 0, 0, time.UTC)
	s := &Server{
		log:          slog.Default(),
		now:          func() time.Time { return now },
		networkPlans: make(map[networkPlanKey]*networkPlanState),
	}
	nodeID := uuid.New()
	target := networkPlanTarget{
		key:       networkPlanKey{nodeID: nodeID, namespace: "acme"},
		namespace: "acme",
		address:   "node-a:8127",
		nodeID:    nodeID,
		plan:      &network.Plan{ListenPort: 51820},
	}

	s.setDesiredNetworkPlans([]networkPlanTarget{target})
	pending := s.pendingNetworkPlans(now)
	if len(pending) != 1 {
		t.Fatalf("initial pending plans = %d, want 1", len(pending))
	}
	s.finishNetworkPlanAttempt(pending[0], nil)

	s.setDesiredNetworkPlans([]networkPlanTarget{target})
	if pending := s.pendingNetworkPlans(now.Add(time.Minute)); len(pending) != 0 {
		t.Fatalf("unchanged plan was requeued: %#v", pending)
	}

	changed := target
	changed.plan = &network.Plan{
		ListenPort: 51820,
		Peers:      []network.PeerPlan{{PublicKey: "new-peer", AllowedIPs: []string{"10.64.1.0/24"}}},
	}
	s.setDesiredNetworkPlans([]networkPlanTarget{changed})
	pending = s.pendingNetworkPlans(now.Add(time.Minute))
	if len(pending) != 1 {
		t.Fatalf("changed pending plans = %d, want 1", len(pending))
	}
	s.finishNetworkPlanAttempt(pending[0], nil)

	if pending := s.pendingNetworkPlans(now.Add(networkPlanRepairInterval + time.Second)); len(pending) != 1 {
		t.Fatalf("periodic repair pending plans = %d, want 1", len(pending))
	}
}

func TestSendNetworkPlansFailureDoesNotStarveLaterNamespaces(t *testing.T) {
	s := &Server{
		log:          slog.Default(),
		now:          time.Now,
		networkPlans: make(map[networkPlanKey]*networkPlanState),
	}
	nodeID := uuid.New()
	var targets []networkPlanTarget
	for _, namespace := range []string{"one", "two", "three"} {
		targets = append(targets, networkPlanTarget{
			key:       networkPlanKey{nodeID: nodeID, namespace: namespace},
			namespace: namespace,
			address:   "node-a",
			nodeID:    nodeID,
			plan:      &network.Plan{},
		})
	}
	var called []string
	update := func(_ context.Context, _ string, request *api.NetworkPlanRequest) error {
		called = append(called, request.Namespace)
		if request.Namespace == "one" {
			return errors.New("first namespace failed")
		}
		return nil
	}

	s.sendNetworkPlans(context.Background(), targets, 7, update)
	if len(called) != 3 {
		t.Fatalf("agent received %d updates, want all 3: %v", len(called), called)
	}
	for i, want := range []string{"one", "two", "three"} {
		if called[i] != want {
			t.Fatalf("update %d = %q, want %q", i, called[i], want)
		}
	}
}
