package server

import (
	"context"
	"net/netip"
	"testing"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	"github.com/overfold/trellis/orchestrator/internal/spec"
)

func TestNetworkPlanUsesRegisteredPeerIdentity(t *testing.T) {
	targetID, peerID := uuid.New(), uuid.New()
	s := &Server{
		networkPool:  netip.MustParsePrefix("10.64.0.0/10"),
		networkPorts: map[string]int{"acme": 3},
		networkSubnets: map[networkSubnetKey]int{
			{namespace: "acme", node: targetID}: 0,
			{namespace: "acme", node: peerID}:   1,
		},
		nodes: map[uuid.UUID]*Node{},
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
		networkSubnets: map[networkSubnetKey]int{
			{namespace: "acme", node: nodeID}:   0,
			{namespace: "globex", node: nodeID}: 1,
		},
		nodes: map[uuid.UUID]*Node{nodeID: node},
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

func TestStartExecutionHashIgnoresNetworkPeerChanges(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	s.networkPool = netip.MustParsePrefix("10.64.0.0/10")
	s.networkPorts = map[string]int{"default": 0}
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy, WireGuardPortBase: 51820, WireGuardPortCount: 256}
	s.nodes[node.ID] = node
	s.networkSubnets = map[networkSubnetKey]int{{namespace: "default", node: node.ID}: 0}
	task := spec.TaskSpec{Name: "app", Image: "app", Networking: &spec.TaskNetworkingSpec{Mode: spec.TaskNetworkWireGuard}}
	s.jobs[jobKey("default", "web")] = &Job{Spec: canonicalTestSpec(&spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "app", Tasks: []spec.TaskSpec{task}}}}), Revision: 1}
	alloc := &Allocation{ID: "allocation", Namespace: "default", JobName: "web", TaskGroupName: "app", Tasks: []spec.TaskSpec{task}, Node: node, Generation: 1, JobRevision: 1, Phase: lifecycle.PhasePlaced}
	s.allocations = []*Allocation{alloc}
	start := &Action{Type: ActionStart, Allocation: alloc}
	if err := s.Execute(context.Background(), start); err != nil {
		t.Fatal(err)
	}
	first := committedExecutionRequest(t, s, node.ID, alloc.ID, alloc.Generation)
	peerID := uuid.New()
	s.nodes[peerID] = &Node{ID: peerID, WireGuardPublicKey: "peer-key", WireGuardEndpoint: "peer:51820", WireGuardPortBase: 51820, WireGuardPortCount: 256}
	s.networkSubnets[networkSubnetKey{namespace: "default", node: peerID}] = 1
	if err := s.Execute(context.Background(), start); err != nil {
		t.Fatal(err)
	}
	second := committedExecutionRequest(t, s, node.ID, alloc.ID, alloc.Generation)
	if first.ExecutionHash == "" || first.ExecutionHash != second.ExecutionHash {
		t.Fatalf("execution hashes changed with peers: %q, %q", first.ExecutionHash, second.ExecutionHash)
	}
	if first.NetworkPlan == nil || second.NetworkPlan == nil || len(first.NetworkPlan.Peers) != 0 || len(second.NetworkPlan.Peers) != 1 {
		t.Fatalf("start plans did not reflect peer change: first=%+v second=%+v", first.NetworkPlan, second.NetworkPlan)
	}
}

func TestStartRequestCarriesDrainOutsideExecutionHash(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy}
	s.nodes[node.ID] = node
	task := spec.TaskSpec{Name: "app", Image: "app"}
	s.jobs[jobKey("default", "web")] = &Job{Spec: canonicalTestSpec(&spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "app", Tasks: []spec.TaskSpec{task}}}}), Revision: 1}
	alloc := &Allocation{ID: "allocation", Namespace: "default", JobName: "web", TaskGroupName: "app", Tasks: []spec.TaskSpec{task}, Node: node, Generation: 1, JobRevision: 1, Phase: lifecycle.PhasePlaced}
	s.allocations = []*Allocation{alloc}
	start := &Action{Type: ActionStart, Allocation: alloc}
	if err := s.Execute(context.Background(), start); err != nil {
		t.Fatal(err)
	}
	first := committedExecutionRequest(t, s, node.ID, alloc.ID, alloc.Generation)
	alloc.Draining, alloc.DrainSequence = true, 3
	if err := s.Execute(context.Background(), start); err != nil {
		t.Fatal(err)
	}
	second := committedExecutionRequest(t, s, node.ID, alloc.ID, alloc.Generation)
	if first.Draining || first.DrainSequence != 0 || !second.Draining || second.DrainSequence != 3 {
		t.Fatalf("start drain state first=(%t,%d) second=(%t,%d), want (false,0) then (true,3)", first.Draining, first.DrainSequence, second.Draining, second.DrainSequence)
	}
	if first.ExecutionHash == "" || first.ExecutionHash != second.ExecutionHash {
		t.Fatalf("execution hash changed with drain state: %q, %q", first.ExecutionHash, second.ExecutionHash)
	}
}

func TestStartExecutionHashChangesWithNetworkPool(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	s.networkPool = netip.MustParsePrefix("10.64.0.0/10")
	s.networkPorts = map[string]int{"default": 0}
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy, WireGuardPortBase: 51820, WireGuardPortCount: 256}
	s.nodes[node.ID] = node
	s.networkSubnets = map[networkSubnetKey]int{{namespace: "default", node: node.ID}: 0}
	task := spec.TaskSpec{Name: "app", Image: "app", Networking: &spec.TaskNetworkingSpec{Mode: spec.TaskNetworkWireGuard}}
	s.jobs[jobKey("default", "web")] = &Job{Spec: canonicalTestSpec(&spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "app", Tasks: []spec.TaskSpec{task}}}}), Revision: 1}
	alloc := &Allocation{ID: "allocation", Namespace: "default", JobName: "web", TaskGroupName: "app", Tasks: []spec.TaskSpec{task}, Node: node, Generation: 1, JobRevision: 1, Phase: lifecycle.PhasePlaced}
	s.allocations = []*Allocation{alloc}
	start := &Action{Type: ActionStart, Allocation: alloc}
	if err := s.Execute(context.Background(), start); err != nil {
		t.Fatal(err)
	}
	first := committedExecutionRequest(t, s, node.ID, alloc.ID, alloc.Generation)
	// Network identity changes require a new generation, never mutation of
	// the already committed execution identity.
	alloc.Generation++
	s.networkPool = netip.MustParsePrefix("10.128.0.0/10")
	if err := s.Execute(context.Background(), start); err != nil {
		t.Fatal(err)
	}
	second := committedExecutionRequest(t, s, node.ID, alloc.ID, alloc.Generation)
	if first.NetworkPlan == nil || second.NetworkPlan == nil || first.NetworkPlan.CIDR == second.NetworkPlan.CIDR || first.NetworkPlan.Gateway == second.NetworkPlan.Gateway {
		t.Fatalf("network pool change did not change subnet and gateway: first=%+v second=%+v", first.NetworkPlan, second.NetworkPlan)
	}
	if first.ExecutionHash == "" || first.ExecutionHash == second.ExecutionHash {
		t.Fatalf("execution hash did not change with subnet and gateway: %q, %q", first.ExecutionHash, second.ExecutionHash)
	}
}
