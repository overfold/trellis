package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	"github.com/overfold/trellis/orchestrator/internal/network"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
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
	start := &Action{Type: ActionStart, Allocation: alloc}
	if err := s.Execute(context.Background(), start); err != nil {
		t.Fatal(err)
	}
	peerID := uuid.New()
	s.nodes[peerID] = &Node{ID: peerID, WireGuardPublicKey: "peer-key", WireGuardEndpoint: "peer:51820", WireGuardPortBase: 51820, WireGuardPortCount: 256}
	s.networkSubnets[networkSubnetKey{namespace: "default", node: peerID}] = 1
	if err := s.Execute(context.Background(), start); err != nil {
		t.Fatal(err)
	}
	calls := agent.recordedCalls()
	if len(calls) != 2 {
		t.Fatalf("start calls = %d, want 2", len(calls))
	}
	var first, second nodeapi.AllocationRequest
	if err := json.Unmarshal(calls[0].body, &first); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(calls[1].body, &second); err != nil {
		t.Fatal(err)
	}
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
	start := &Action{Type: ActionStart, Allocation: alloc}
	if err := s.Execute(context.Background(), start); err != nil {
		t.Fatal(err)
	}
	alloc.Draining, alloc.DrainSequence = true, 3
	if err := s.Execute(context.Background(), start); err != nil {
		t.Fatal(err)
	}
	calls := agent.recordedCalls()
	if len(calls) != 2 {
		t.Fatalf("start calls = %d, want 2", len(calls))
	}
	var first, second nodeapi.AllocationRequest
	if err := json.Unmarshal(calls[0].body, &first); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(calls[1].body, &second); err != nil {
		t.Fatal(err)
	}
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
	start := &Action{Type: ActionStart, Allocation: alloc}
	if err := s.Execute(context.Background(), start); err != nil {
		t.Fatal(err)
	}
	s.networkPool = netip.MustParsePrefix("10.128.0.0/10")
	if err := s.Execute(context.Background(), start); err != nil {
		t.Fatal(err)
	}
	calls := agent.recordedCalls()
	if len(calls) != 2 {
		t.Fatalf("start calls = %d, want 2", len(calls))
	}
	var first, second nodeapi.AllocationRequest
	if err := json.Unmarshal(calls[0].body, &first); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(calls[1].body, &second); err != nil {
		t.Fatal(err)
	}
	if first.NetworkPlan == nil || second.NetworkPlan == nil || first.NetworkPlan.CIDR == second.NetworkPlan.CIDR || first.NetworkPlan.Gateway == second.NetworkPlan.Gateway {
		t.Fatalf("network pool change did not change subnet and gateway: first=%+v second=%+v", first.NetworkPlan, second.NetworkPlan)
	}
	if first.ExecutionHash == "" || first.ExecutionHash == second.ExecutionHash {
		t.Fatalf("execution hash did not change with subnet and gateway: %q, %q", first.ExecutionHash, second.ExecutionHash)
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
	s.sendNetworkPlanTarget(context.Background(), target, func(ctx context.Context, _ uuid.UUID, _ string, _ *nodeapi.NetworkPlanRequest) error {
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
	update := func(_ context.Context, _ uuid.UUID, address string, request *nodeapi.NetworkPlanRequest) error {
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

func TestNetworkPlanOperationTimeoutIsCappedAcrossFailures(t *testing.T) {
	limit := networkPlanOperationTimeout(&network.Plan{}, networkPlanMaxBackoffDoublings)
	if limit != networkPlanBaseTimeout<<networkPlanMaxBackoffDoublings {
		t.Fatalf("capped timeout = %s", limit)
	}
	for _, attempt := range []int{networkPlanMaxBackoffDoublings + 1, 20, 1000, 1 << 30} {
		if got := networkPlanOperationTimeout(&network.Plan{}, attempt); got != limit {
			t.Fatalf("attempt %d timeout = %s, want %s", attempt, got, limit)
		}
	}
}
