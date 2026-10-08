package network

import (
	"context"
	"crypto/sha256"
	"os"
	"slices"
	"strings"
	"testing"
)

func TestNamespaceResourceNamesSeparateOldCollision(t *testing.T) {
	first, second := "audit-420639", "audit-1362137"
	a, b := sha256.Sum256([]byte(first+"\x00"+first)), sha256.Sum256([]byte(second+"\x00"+second))
	if string(a[:5]) != string(b[:5]) {
		t.Fatal("fixture must collide under the old 40-bit scheme")
	}
	for _, prefix := range []string{"tb", "tw", ""} {
		left, right := short(prefix, first+"\x00"+first), short(prefix, second+"\x00"+second)
		if left == right || prefix != "" && len(left) > 15 {
			t.Fatalf("resource names %q, %q are not distinct valid names", left, right)
		}
	}
	manager, _ := newRecoveryTestManager(t)
	plan := recoveryTestPlan()
	one, err := manager.Attach(t.Context(), AttachRequest{Namespace: first, Network: first, AllocationID: "one", Plan: plan})
	if err != nil {
		t.Fatal(err)
	}
	plan.CIDR, plan.Gateway = "10.42.2.0/24", "10.42.2.1"
	two, err := manager.Attach(t.Context(), AttachRequest{Namespace: second, Network: second, AllocationID: "two", Plan: plan})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Detach(t.Context(), one); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(two.LeasePath); err != nil {
		t.Fatalf("colliding namespace lost its lease: %v", err)
	}
	assertAttachments(t, manager, "two")
}

func TestNetworkRefusesUnownedResourcesAndAmbiguousCreate(t *testing.T) {
	manager, runner := newRecoveryTestManager(t)
	if err := manager.ensureLink(t.Context(), "lo", "not-trellis", "type", "bridge"); err == nil {
		t.Fatal("adopted the host loopback interface")
	}
	if len(runner.commands) != 0 {
		t.Fatal("mutated an unowned link")
	}
	if err := os.WriteFile(manager.netnsPath("foreign"), []byte("foreign"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Attach(t.Context(), AttachRequest{Namespace: "acme", Network: "acme", AllocationID: "foreign", Plan: recoveryTestPlan()}); err == nil {
		t.Fatal("adopted an existing network namespace")
	}
	if len(runner.commands) != 0 {
		t.Fatal("refused attach changed host resources")
	}
	runner.fail = []string{"ip link add"}
	if err := manager.ensureLink(t.Context(), "tb-audit", "owner", "type", "bridge"); err == nil {
		t.Fatal("ambiguous creation was treated as adoption")
	}
	if len(runner.commands) != 1 {
		t.Fatalf("ambiguous creation ran follow-up mutations: %v", runner.commands)
	}
}

func TestDetachUsesJournalNotCallerNames(t *testing.T) {
	manager, runner := newRecoveryTestManager(t)
	attachment := attachForRecovery(t, manager, "one")
	attachment.HostVeth, attachment.Bridge, attachment.WireGuardInterface = "lo", "eth0", "foreign"
	runner.commands = nil
	if err := manager.Detach(t.Context(), attachment); err != nil {
		t.Fatal(err)
	}
	for _, command := range runner.commands {
		if strings.Contains(command, " lo") || strings.Contains(command, " eth0") || strings.Contains(command, " foreign") {
			t.Fatalf("caller redirected teardown: %s", command)
		}
	}
}

func TestPartialAttachmentProtectsSharedPath(t *testing.T) {
	manager, runner := newRecoveryTestManager(t)
	one := attachForRecovery(t, manager, "one")
	if err := manager.recordAttachment(attachmentRecord{AllocationID: "pending", Namespace: "acme", Network: "acme", CIDR: recoveryTestPlan().CIDR}); err != nil {
		t.Fatal(err)
	}
	runner.commands = nil
	if err := manager.Detach(t.Context(), one); err != nil {
		t.Fatal(err)
	}
	for _, command := range runner.commands {
		if command == "ip link del "+one.Bridge || command == "ip link del "+one.WireGuardInterface {
			t.Fatalf("removed a shared path owned by an interrupted attachment: %s", command)
		}
	}
}

func TestIPv6DisabledBeforeHostLinksComeUp(t *testing.T) {
	manager, runner := newRecoveryTestManager(t)
	a := attachForRecovery(t, manager, "one")
	for _, name := range []string{a.Bridge, a.WireGuardInterface, a.HostVeth} {
		disable := slices.Index(runner.commands, "sysctl -w net.ipv6.conf."+name+".disable_ipv6=1")
		up := slices.Index(runner.commands, "ip link set "+name+" up")
		if disable < 0 || up < disable {
			t.Fatalf("IPv6 not disabled before %s came up: %v", name, runner.commands)
		}
	}
}

func TestWireGuardIngressHostInputVerdicts(t *testing.T) {
	model := twoNamespaceModel(t, true)
	model.chains["INPUT"] = append(model.chains["INPUT"], "-j ACCEPT")
	for _, state := range []string{"NEW", "ESTABLISHED"} {
		for _, port := range []int{22, 53, 8128} {
			want := "DROP"
			if state == "ESTABLISHED" {
				want = "ACCEPT" // Host/host-network initiated connections remain usable.
			}
			if got := model.verdict(t, "INPUT", packet{in: "tw-acme", src: "10.42.9.2", dst: "10.42.1.1", proto: "tcp", dport: port, states: []string{state}}); got != want {
				t.Fatalf("remote ingress port %d state %s = %s, want %s", port, state, got, want)
			}
		}
	}
}

func TestFirewallRepairRestoresAPIGrantsFromJournals(t *testing.T) {
	manager, _ := newRecoveryTestManager(t)
	model := newIPTablesModel()
	manager.run = model
	a := attachForRecovery(t, manager, "api")
	plan := recoveryTestPlan()
	plan.APIPort = 0 // Peer updates intentionally have no per-group API grant.
	quiet, err := manager.Attach(t.Context(), AttachRequest{Namespace: "acme", Network: "acme", AllocationID: "quiet", Plan: plan})
	if err != nil {
		t.Fatal(err)
	}
	model.chains[inputChain] = nil // External firewall reload.
	if err := manager.UpdatePlan(context.Background(), "acme", plan); err != nil {
		t.Fatal(err)
	}
	p := packet{in: a.Bridge, src: "10.42.1.2", dst: plan.Gateway, proto: "tcp", dport: 8126, states: []string{"NEW"}}
	if got := model.verdict(t, "INPUT", p); got != "ACCEPT" {
		t.Fatalf("API unreachable after firewall repair: %s", got)
	}
	if err := manager.Detach(t.Context(), a); err != nil {
		t.Fatal(err)
	}
	if got := model.verdict(t, "INPUT", p); got != "DROP" {
		t.Fatalf("retired API grant still allowed: %s", got)
	}
	if err := manager.Detach(t.Context(), quiet); err != nil {
		t.Fatal(err)
	}
}

func TestOldResourceJournalIsNotSilentlyReinterpreted(t *testing.T) {
	manager, runner := newRecoveryTestManager(t)
	attachForRecovery(t, manager, "old")
	raw := []byte(`{"allocation_id":"old","namespace":"acme","network":"acme","cidr":"10.42.1.0/24"}`)
	if err := os.WriteFile(manager.journalPath("old"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	runner.commands = nil
	if err := manager.DetachAllocation(t.Context(), "old"); err == nil || !strings.Contains(err.Error(), "creating binary") {
		t.Fatalf("old journal was accepted without upgrade preparation: %v", err)
	}
	if len(runner.commands) != 0 {
		t.Fatal("old journal ran teardown using the new name scheme")
	}
	if retained, err := os.ReadFile(manager.journalPath("old")); err != nil || string(retained) != string(raw) {
		t.Fatalf("old ownership evidence was discarded: %v", err)
	}
}
