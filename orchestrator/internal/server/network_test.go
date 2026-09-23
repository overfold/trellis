package server

import (
	"context"
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

func TestSendNetworkPlansBoundsSlowAgentAndUpdatesHealthyAgent(t *testing.T) {
	s := &Server{log: slog.Default()}
	targets := []networkPlanTarget{
		{namespace: "default", address: "slow", plan: &network.Plan{}},
		{namespace: "default", address: "healthy", plan: &network.Plan{}},
	}
	healthyCalled := make(chan struct{}, 1)
	update := func(ctx context.Context, address string, request *api.NetworkPlanRequest) error {
		if request.Epoch != 7 || request.Namespace != "default" {
			t.Errorf("unexpected network plan request: %#v", request)
		}
		if address == "healthy" {
			healthyCalled <- struct{}{}
			return nil
		}
		<-ctx.Done()
		return ctx.Err()
	}
	start := time.Now()
	s.sendNetworkPlans(context.Background(), targets, 7, update)
	if elapsed := time.Since(start); elapsed > networkPlanTimeout+time.Second {
		t.Fatalf("network plan reconciliation took %s, exceeds deadline", elapsed)
	}
	select {
	case <-healthyCalled:
	default:
		t.Fatal("healthy agent did not receive its network plan")
	}
}
