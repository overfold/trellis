package server

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"net/netip"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/internal/lifecycle"
	"github.com/overfold/trellis/internal/spec"
)

var (
	subnetNodeA = uuid.MustParse("11111111-1111-1111-1111-111111111111")
	subnetNodeB = uuid.MustParse("22222222-2222-2222-2222-222222222222")
	subnetNodeC = uuid.MustParse("33333333-3333-3333-3333-333333333333")
)

func TestPlanNetworkSubnetsAssignsLowestFreeIndexesDeterministically(t *testing.T) {
	pool := netip.MustParsePrefix("10.64.0.0/10")
	current := map[networkSubnetKey]int{}
	namespaces := []string{"globex", "acme"}
	nodes := []uuid.UUID{subnetNodeB, subnetNodeA}

	plan, err := planNetworkSubnetRegistrations(pool, current, namespaces, nodes)
	if err != nil {
		t.Fatal(err)
	}
	want := map[networkSubnetKey]int{
		{namespace: "acme", node: subnetNodeA}:   0,
		{namespace: "acme", node: subnetNodeB}:   1,
		{namespace: "globex", node: subnetNodeA}: 2,
		{namespace: "globex", node: subnetNodeB}: 3,
	}
	if !reflect.DeepEqual(plan.Subnets, want) {
		t.Fatalf("subnets = %v, want %v", plan.Subnets, want)
	}
	if len(plan.Registrations) != 4 || plan.Registrations[0].Namespace != "acme" || plan.Registrations[0].NodeID != subnetNodeA || plan.Registrations[3].Index != 3 {
		t.Fatalf("registrations are not in deterministic order: %+v", plan.Registrations)
	}
	if !plan.Ready["acme"] || !plan.Ready["globex"] {
		t.Fatalf("ready = %v, want both namespaces", plan.Ready)
	}
	if len(current) != 0 || !reflect.DeepEqual(namespaces, []string{"globex", "acme"}) || !reflect.DeepEqual(nodes, []uuid.UUID{subnetNodeB, subnetNodeA}) {
		t.Fatal("planning modified its inputs")
	}

	again, err := planNetworkSubnetRegistrations(pool, current, []string{"acme", "globex"}, []uuid.UUID{subnetNodeA, subnetNodeB})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(again, plan) {
		t.Fatalf("planning depends on input order:\n%+v\n%+v", again, plan)
	}

	seenSubnets := map[netip.Prefix]bool{}
	seenLinks := map[string]bool{}
	for _, index := range plan.Subnets {
		subnet, link := networkSubnet(pool, index), networkLinkAddress(index)
		if seenSubnets[subnet] || seenLinks[link] || !pool.Contains(subnet.Addr()) {
			t.Fatalf("index %d maps to duplicate or out-of-pool subnet %s / link %s", index, subnet, link)
		}
		seenSubnets[subnet], seenLinks[link] = true, true
	}
}

func TestPlanNetworkSubnetsKeepsExistingAndReusesReleasedIndexes(t *testing.T) {
	pool := netip.MustParsePrefix("10.64.0.0/10")
	current := map[networkSubnetKey]int{
		{namespace: "acme", node: subnetNodeA}:    4,
		{namespace: "acme", node: subnetNodeB}:    0,
		{namespace: "retired", node: subnetNodeA}: 1,
		{namespace: "acme", node: subnetNodeC}:    2,
	}
	before := maps.Clone(current)

	// "retired" has no network-using jobs and node C was removed.
	plan, err := planNetworkSubnetRegistrations(pool, current, []string{"acme", "initech"}, []uuid.UUID{subnetNodeA, subnetNodeB})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(current, before) {
		t.Fatal("planning modified current registrations")
	}
	wantDeletions := []*NetworkSubnetRegistration{
		{Namespace: "acme", NodeID: subnetNodeC, Index: 2},
		{Namespace: "retired", NodeID: subnetNodeA, Index: 1},
	}
	if !reflect.DeepEqual(plan.Deletions, wantDeletions) {
		t.Fatalf("deletions = %+v, want %+v", plan.Deletions, wantDeletions)
	}
	want := map[networkSubnetKey]int{
		{namespace: "acme", node: subnetNodeA}:    4,
		{namespace: "acme", node: subnetNodeB}:    0,
		{namespace: "initech", node: subnetNodeA}: 1,
		{namespace: "initech", node: subnetNodeB}: 2,
	}
	if !reflect.DeepEqual(plan.Subnets, want) {
		t.Fatalf("subnets = %v, want existing kept and released indexes reused: %v", plan.Subnets, want)
	}
}

func TestPlanNetworkSubnetsReportsExhaustionWithoutPartialAssignment(t *testing.T) {
	// A /22 holds four /24 node subnets.
	pool := netip.MustParsePrefix("10.64.0.0/22")
	current := map[networkSubnetKey]int{{namespace: "acme", node: subnetNodeA}: 0}

	plan, err := planNetworkSubnetRegistrations(pool, current, []string{"acme", "globex", "initech"}, []uuid.UUID{subnetNodeA, subnetNodeB})
	if !errors.Is(err, errNetworkSubnetExhausted) {
		t.Fatalf("error = %v, want exhaustion", err)
	}
	if !plan.Ready["acme"] || !plan.Ready["globex"] || plan.Ready["initech"] {
		t.Fatalf("ready = %v, want acme and globex only", plan.Ready)
	}
	for key := range plan.Subnets {
		if key.namespace == "initech" {
			t.Fatalf("exhausted namespace received a partial assignment: %v", plan.Subnets)
		}
	}
	if len(plan.Registrations) != 3 {
		t.Fatalf("registrations = %+v, want the three that fit", plan.Registrations)
	}

	// Adding a node to a full pool leaves existing addressing intact.
	full := plan.Subnets
	plan, err = planNetworkSubnetRegistrations(pool, full, []string{"acme", "globex"}, []uuid.UUID{subnetNodeA, subnetNodeB, subnetNodeC})
	if !errors.Is(err, errNetworkSubnetExhausted) {
		t.Fatalf("error = %v, want exhaustion", err)
	}
	if !reflect.DeepEqual(plan.Subnets, full) || len(plan.Registrations) != 0 || len(plan.Deletions) != 0 || len(plan.Ready) != 0 {
		t.Fatalf("exhaustion changed existing addressing: %+v", plan)
	}
}

func TestPlanNetworkSubnetsRejectsDuplicateIndexes(t *testing.T) {
	current := map[networkSubnetKey]int{
		{namespace: "acme", node: subnetNodeA}:   3,
		{namespace: "globex", node: subnetNodeA}: 3,
	}
	if _, err := planNetworkSubnetRegistrations(netip.MustParsePrefix("10.64.0.0/10"), current, []string{"acme", "globex"}, []uuid.UUID{subnetNodeA}); err == nil || errors.Is(err, errNetworkSubnetExhausted) {
		t.Fatalf("error = %v, want duplicate-index error", err)
	}
}

func TestNetworkSubnetIndexAddressing(t *testing.T) {
	pool := netip.MustParsePrefix("10.64.0.0/10")
	if got := networkSubnet(pool, 0).String(); got != "10.64.0.0/24" {
		t.Fatalf("subnet 0 = %s", got)
	}
	if got := networkSubnet(pool, networkSubnetCapacity(pool)-1).String(); got != "10.127.255.0/24" {
		t.Fatalf("last subnet = %s", got)
	}
	if got := networkSubnetCapacity(netip.MustParsePrefix("10.0.0.0/8")); got != maxNetworkSubnets {
		t.Fatalf("/8 capacity = %d, want link-address bound %d", got, maxNetworkSubnets)
	}
	if first, last := networkLinkAddress(0), networkLinkAddress(maxNetworkSubnets-1); first != "169.254.1.0/32" || last != "169.254.254.255/32" {
		t.Fatalf("link address range = %s..%s", first, last)
	}
}

func TestReconcileWithholdsPlacementWhenSubnetPoolIsExhausted(t *testing.T) {
	ctx := context.Background()
	store := memoryStore{}
	controller := NewStateController(store, "test")
	s := NewServer(slog.Default(), nil, controller, store, "test", "")
	s.client = newTestAgentClient()
	s.wireGuardPortCount = 8
	// A /23 addresses two namespaces on one node.
	s.networkPool = netip.MustParsePrefix("10.64.0.0/23")
	node := &Node{ID: subnetNodeA, Status: NodeStatusHealthy, WireGuardPortBase: 51820, WireGuardPortCount: 8, CPUCapacity: 4000, MemoryCapacity: 4 << 30, CPUAllocatable: 4000, MemoryAllocatable: 4 << 30, Capabilities: []spec.NodeCapability{spec.CapabilityNamespaceNetworking}}
	addTestNode(s, node, time.Now())
	for _, namespace := range []string{"acme", "globex", "initech"} {
		s.jobs[jobKey(namespace, "web")] = &Job{
			Spec: canonicalTestSpec(&spec.JobSpec{
				Namespace: namespace,
				Name:      "web",
				TaskGroups: []spec.TaskGroupSpec{{
					Name:  "api",
					Count: 1,
					Tasks: []spec.TaskSpec{{Name: "server", Image: "app", Networking: &spec.TaskNetworkingSpec{Mode: spec.TaskNetworkWireGuard}}},
				}},
			}),
			Revision: 1,
		}
	}

	s.Reconcile(ctx)

	placed := map[string]bool{}
	for _, allocation := range s.allocations {
		placed[allocation.Namespace] = true
	}
	if !placed["acme"] || !placed["globex"] || placed["initech"] {
		t.Fatalf("placed namespaces = %v, want acme and globex only", placed)
	}
	persisted, err := controller.listNetworkSubnetRegistrations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := map[networkSubnetKey]int{
		{namespace: "acme", node: subnetNodeA}:   0,
		{namespace: "globex", node: subnetNodeA}: 1,
	}
	if !reflect.DeepEqual(persisted, want) || !reflect.DeepEqual(s.networkSubnets, want) {
		t.Fatalf("subnet registrations persisted=%v memory=%v, want %v", persisted, s.networkSubnets, want)
	}
	acme, err := s.networkPlan("acme", node)
	if err != nil {
		t.Fatal(err)
	}
	globex, err := s.networkPlan("globex", node)
	if err != nil {
		t.Fatal(err)
	}
	if acme.CIDR != "10.64.0.0/24" || globex.CIDR != "10.64.1.0/24" || acme.WireGuardAddress == globex.WireGuardAddress {
		t.Fatalf("namespace plans collide: acme=%+v globex=%+v", acme, globex)
	}
	if _, err := s.networkPlan("initech", node); err == nil {
		t.Fatal("exhausted namespace received a network plan")
	}

	// Removing a namespace's jobs releases its subnet for the waiting one.
	delete(s.jobs, jobKey("acme", "web"))
	for _, allocation := range s.allocations {
		if allocation.Namespace == "acme" {
			allocation.Phase = lifecycle.PhaseStopped
		}
	}
	s.Reconcile(ctx)
	persisted, err = controller.listNetworkSubnetRegistrations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want = map[networkSubnetKey]int{
		{namespace: "globex", node: subnetNodeA}:  1,
		{namespace: "initech", node: subnetNodeA}: 0,
	}
	if !reflect.DeepEqual(persisted, want) {
		t.Fatalf("subnet registrations after release = %v, want %v", persisted, want)
	}
}
