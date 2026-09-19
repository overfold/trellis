package server

import (
	"net/netip"
	"testing"

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
