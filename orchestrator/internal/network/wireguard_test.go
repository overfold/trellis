package network

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type recordingRunner struct{ commands []string }

func (r *recordingRunner) Run(_ context.Context, name string, args ...string) error {
	r.commands = append(r.commands, name+" "+strings.Join(args, " "))
	return nil
}

type failingRunner struct {
	recordingRunner
	failCommand string
}

func (r *failingRunner) Run(_ context.Context, name string, args ...string) error {
	command := name + " " + strings.Join(args, " ")
	r.commands = append(r.commands, command)
	if strings.Contains(command, r.failCommand) {
		return errors.New("command failed")
	}
	return nil
}

type blockingRunner struct {
	blockCommand string
	entered      chan struct{}
	release      chan struct{}
}

func (r *blockingRunner) Run(_ context.Context, name string, args ...string) error {
	if name+" "+strings.Join(args, " ") == r.blockCommand {
		close(r.entered)
		<-r.release
	}
	return nil
}

func TestWireGuardAttachBuildsIsolatedNamespace(t *testing.T) {
	dir := t.TempDir()
	key := filepath.Join(dir, "private.key")
	if err := os.WriteFile(key, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := Config{CIDR: "10.42.1.0/24", Gateway: "10.42.1.1", WireGuardAddress: "10.42.255.1/32", PrivateKeyFile: key, ListenPort: 51820,
		Peers: []Peer{{PublicKey: "peer", Endpoint: "192.0.2.2:51820", AllowedIPs: []string{"10.42.2.0/24"}}}}
	raw, _ := json.Marshal(cfg)
	if err := os.WriteFile(filepath.Join(dir, "blue.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	runner := &recordingRunner{}
	manager := NewWireGuardManager(dir)
	manager.run = runner
	manager.stateDir = t.TempDir()
	if err := manager.ConfigureWorkloadDNS(context.Background(), WorkloadDNSAddress); err != nil {
		t.Fatal(err)
	}
	a, err := manager.Attach(context.Background(), AttachRequest{Namespace: "acme", Network: "blue", AllocationID: "alloc-1"})
	if err != nil {
		t.Fatalf("Attach() error = %v", err)
	}
	if a.Namespace != "acme" || a.NetworkNamespace != "/var/run/netns/alloc-1" || !strings.HasPrefix(a.Address, "10.42.1.") {
		t.Fatalf("unexpected attachment: %#v", a)
	}
	joined := strings.Join(runner.commands, "\n")
	for _, want := range []string{"type wireguard", "wg set", "ip netns add alloc-1", "netns alloc-1", "iptables -C FORWARD", "ip addr replace 198.18.0.53/32 dev lo", "iptables -C INPUT -i tb"} {
		if !strings.Contains(joined, want) {
			t.Errorf("commands do not contain %q:\n%s", want, joined)
		}
	}
}

func TestWireGuardRejectsUntrustedNetworkName(t *testing.T) {
	m := NewWireGuardManager(t.TempDir())
	if _, err := m.Attach(context.Background(), AttachRequest{Namespace: "namespace", Network: "../escape", AllocationID: "alloc"}); err == nil {
		t.Fatal("Attach accepted path traversal")
	}
}

func TestAutomatedIdentityPersists(t *testing.T) {
	dir := t.TempDir()
	first, err := NewAutomatedWireGuardManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	public1, err := first.Identity()
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewAutomatedWireGuardManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	public2, err := second.Identity()
	if err != nil {
		t.Fatal(err)
	}
	if public1 == "" || public1 != public2 {
		t.Fatalf("identity was not stable: %q != %q", public1, public2)
	}
	info, err := os.Stat(filepath.Join(dir, "identity.key"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("private key mode = %o", info.Mode().Perm())
	}
}

func TestConfigureWorkloadDNSRejectsIPv6(t *testing.T) {
	manager := NewWireGuardManager(t.TempDir())
	manager.run = &recordingRunner{}
	if err := manager.ConfigureWorkloadDNS(context.Background(), "fd00::53"); err == nil {
		t.Fatal("expected IPv6 workload DNS address to be rejected")
	}
}

func TestAutomatedWireGuardUsesPlanListenPort(t *testing.T) {
	manager, err := NewAutomatedWireGuardManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	runner := &recordingRunner{}
	manager.run = runner
	_, err = manager.Attach(context.Background(), AttachRequest{
		Namespace:    "acme",
		Network:      "acme",
		AllocationID: "alloc-port",
		Plan: Plan{
			CIDR:             "10.42.1.0/24",
			Gateway:          "10.42.1.1",
			WireGuardAddress: "169.254.1.1/32",
			ListenPort:       51917,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(runner.commands, "\n")
	if !strings.Contains(joined, "listen-port 51917") {
		t.Fatalf("namespace listen port was not applied:\n%s", joined)
	}
}

func TestWireGuardDetachRemovesNamespacePathAfterLastAllocation(t *testing.T) {
	manager, err := NewAutomatedWireGuardManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	runner := &recordingRunner{}
	manager.run = runner
	plan := Plan{
		CIDR:             "10.42.1.0/24",
		Gateway:          "10.42.1.1",
		WireGuardAddress: "169.254.1.1/32",
		ListenPort:       51917,
	}
	first, err := manager.Attach(context.Background(), AttachRequest{
		Namespace: "acme", Network: "acme", AllocationID: "alloc-one", Plan: plan,
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := manager.Attach(context.Background(), AttachRequest{
		Namespace: "acme", Network: "acme", AllocationID: "alloc-two", Plan: plan,
	})
	if err != nil {
		t.Fatal(err)
	}

	runner.commands = nil
	if err := manager.Detach(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(runner.commands, "\n")
	if strings.Contains(joined, "ip link del "+first.WireGuardInterface) || strings.Contains(joined, "ip link del "+first.Bridge) {
		t.Fatalf("namespace path removed while another allocation still used it:\n%s", joined)
	}

	runner.commands = nil
	if err := manager.Detach(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	joined = strings.Join(runner.commands, "\n")
	for _, want := range []string{
		"ip link del " + second.WireGuardInterface,
		"ip link del " + second.Bridge,
		"iptables -D FORWARD",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("last detach did not tear down %q:\n%s", want, joined)
		}
	}
	if _, err := os.Stat(filepath.Join(manager.stateDir, "acme")); !os.IsNotExist(err) {
		t.Fatalf("namespace lease directory still exists after last detach: %v", err)
	}
}

func TestReserveAddressResolvesCollisionAndPersistsChoice(t *testing.T) {
	dir := t.TempDir()
	var first, second string
	addresses := map[string]string{}
	for i := 0; ; i++ {
		candidate := fmt.Sprintf("alloc-%d", i)
		address, err := allocationAddress("10.42.1.0/29", candidate)
		if err != nil {
			t.Fatal(err)
		}
		if owner, exists := addresses[address]; exists {
			first, second = owner, candidate
			break
		}
		addresses[address] = candidate
	}
	firstAddress, _, err := reserveAddress(dir, "10.42.1.0/29", first)
	if err != nil {
		t.Fatal(err)
	}
	secondAddress, secondLease, err := reserveAddress(dir, "10.42.1.0/29", second)
	if err != nil {
		t.Fatal(err)
	}
	if secondAddress == firstAddress {
		t.Fatalf("colliding allocations both received %s", firstAddress)
	}
	again, lease, err := reserveAddress(dir, "10.42.1.0/29", second)
	if err != nil {
		t.Fatal(err)
	}
	if again != secondAddress || lease != secondLease {
		t.Fatalf("persisted lease changed: (%s, %s) != (%s, %s)", again, lease, secondAddress, secondLease)
	}
}

func TestAuthoritativePlanRemovesStalePeersAndRoutes(t *testing.T) {
	manager, err := NewAutomatedWireGuardManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	runner := &recordingRunner{}
	manager.run = runner
	plan := Plan{CIDR: "10.42.1.0/24", Gateway: "10.42.1.1", WireGuardAddress: "169.254.1.1/32", ListenPort: 51917,
		Peers: []PeerPlan{{PublicKey: "old-peer", AllowedIPs: []string{"10.42.2.0/24"}}}}
	first, err := manager.Attach(context.Background(), AttachRequest{Namespace: "acme", Network: "acme", AllocationID: "alloc-old", Plan: plan})
	if err != nil {
		t.Fatal(err)
	}
	plan.Peers = []PeerPlan{{PublicKey: "new-peer", AllowedIPs: []string{"10.42.3.0/24"}}}
	runner.commands = nil
	if _, err := manager.Attach(context.Background(), AttachRequest{Namespace: "acme", Network: "acme", AllocationID: "alloc-new", Plan: plan}); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(runner.commands, "\n")
	for _, want := range []string{"peer old-peer remove", "ip route del 10.42.2.0/24 dev " + first.WireGuardInterface} {
		if !strings.Contains(joined, want) {
			t.Fatalf("commands do not contain %q:\n%s", want, joined)
		}
	}
}

func TestUpdatePlanAddsPeerAndRouteWithoutNewAllocation(t *testing.T) {
	manager, err := NewAutomatedWireGuardManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	runner := &recordingRunner{}
	manager.run = runner
	plan := Plan{CIDR: "10.42.1.0/24", Gateway: "10.42.1.1", WireGuardAddress: "169.254.1.1/32", ListenPort: 51917}
	attachment, err := manager.Attach(context.Background(), AttachRequest{Namespace: "acme", Network: "acme", AllocationID: "alloc-old", Plan: plan})
	if err != nil {
		t.Fatal(err)
	}
	runner.commands = nil
	plan.Peers = []PeerPlan{{PublicKey: "new-peer", Endpoint: "node-b:51917", AllowedIPs: []string{"10.42.2.0/24"}}}
	if err := manager.UpdatePlan(context.Background(), "acme", plan); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(runner.commands, "\n")
	for _, want := range []string{
		"ip addr replace 169.254.1.1/32 dev " + attachment.WireGuardInterface,
		"wg set " + attachment.WireGuardInterface + " private-key " + filepath.Join(manager.stateDir, "identity.key") + " listen-port 51917",
		"wg set " + attachment.WireGuardInterface + " peer new-peer allowed-ips 10.42.2.0/24 endpoint node-b:51917",
		"ip route replace 10.42.2.0/24 dev " + attachment.WireGuardInterface,
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("commands do not contain %q:\n%s", want, joined)
		}
	}
	runner.commands = nil
	plan.Peers = nil
	if err := manager.UpdatePlan(context.Background(), "acme", plan); err != nil {
		t.Fatal(err)
	}
	joined = strings.Join(runner.commands, "\n")
	for _, want := range []string{"peer new-peer remove", "ip route del 10.42.2.0/24 dev " + attachment.WireGuardInterface} {
		if !strings.Contains(joined, want) {
			t.Fatalf("commands do not contain %q:\n%s", want, joined)
		}
	}
}

func TestUpdatePlanWaitingForFinalDetachDoesNotRestorePlan(t *testing.T) {
	manager, err := NewAutomatedWireGuardManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager.run = &recordingRunner{}
	plan := Plan{CIDR: "10.42.1.0/24", Gateway: "10.42.1.1", WireGuardAddress: "169.254.1.1/32", ListenPort: 51917,
		Peers: []PeerPlan{{PublicKey: "old-peer", AllowedIPs: []string{"10.42.2.0/24"}}}}
	attachment, err := manager.Attach(context.Background(), AttachRequest{Namespace: "acme", Network: "acme", AllocationID: "alloc-old", Plan: plan})
	if err != nil {
		t.Fatal(err)
	}

	runner := &blockingRunner{
		blockCommand: "ip link del " + attachment.WireGuardInterface,
		entered:      make(chan struct{}),
		release:      make(chan struct{}),
	}
	manager.run = runner
	detached := make(chan error, 1)
	go func() { detached <- manager.Detach(context.Background(), attachment) }()
	<-runner.entered

	plan.Peers = []PeerPlan{{PublicKey: "intermediate-peer", AllowedIPs: []string{"10.42.3.0/24"}}}
	updated := make(chan error, 1)
	go func() { updated <- manager.UpdatePlan(context.Background(), "acme", plan) }()
	close(runner.release)
	if err := <-detached; err != nil {
		t.Fatal(err)
	}
	if err := <-updated; err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(manager.planPath("acme", "acme")); !os.IsNotExist(err) {
		t.Fatalf("plan restored after final detach: %v", err)
	}

	manager.run = &failingRunner{failCommand: "ip route del"}
	plan.Peers = []PeerPlan{{PublicKey: "new-peer", AllowedIPs: []string{"10.42.4.0/24"}}}
	if _, err := manager.Attach(context.Background(), AttachRequest{Namespace: "acme", Network: "acme", AllocationID: "alloc-new", Plan: plan}); err != nil {
		t.Fatalf("Attach() after final detach: %v", err)
	}
}

func TestAttachReturnsSetupErrors(t *testing.T) {
	manager, err := NewAutomatedWireGuardManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	runner := &failingRunner{failCommand: "ip addr replace 10.42.1.1/24"}
	manager.run = runner
	_, err = manager.Attach(context.Background(), AttachRequest{Namespace: "acme", Network: "acme", AllocationID: "alloc-error", Plan: Plan{
		CIDR: "10.42.1.0/24", Gateway: "10.42.1.1", WireGuardAddress: "169.254.1.1/32", ListenPort: 51917,
	}})
	if err == nil {
		t.Fatal("Attach reported success after bridge address setup failed")
	}
}
