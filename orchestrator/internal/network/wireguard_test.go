package network

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
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

type cleanupRunner struct {
	links         map[string]bool
	namespaces    map[string]bool
	firewallRules map[string]bool
	failCommand   string
	failed        bool
}

func (r *cleanupRunner) Run(_ context.Context, name string, args ...string) error {
	command := name + " " + strings.Join(args, " ")
	if command == r.failCommand && !r.failed {
		r.failed = true
		return errors.New("injected cleanup failure")
	}
	if name == "ip" && len(args) >= 3 && args[0] == "link" {
		switch args[1] {
		case "del":
			if !r.links[args[2]] {
				return errors.New("device does not exist")
			}
			delete(r.links, args[2])
		case "show":
			if len(args) < 4 || !r.links[args[3]] {
				return errors.New("device does not exist")
			}
		}
	}
	if name == "ip" && len(args) >= 3 && args[0] == "netns" {
		switch args[1] {
		case "del":
			if !r.namespaces[args[2]] {
				return errors.New("No such file or directory")
			}
			delete(r.namespaces, args[2])
		case "pids":
			if !r.namespaces[args[2]] {
				return errors.New("No such file or directory")
			}
		}
	}
	if name == "iptables" && len(args) >= 2 {
		rule := strings.Join(args[1:], " ")
		switch args[0] {
		case "-D":
			if !r.firewallRules[rule] {
				return errors.New("Bad rule")
			}
			delete(r.firewallRules, rule)
		case "-C":
			if !r.firewallRules[rule] {
				return errors.New("Bad rule")
			}
		}
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

type slowRecordingRunner struct {
	recordingRunner
	delay time.Duration
}

type cancelledPlanRunner struct {
	recordingRunner
	command string
	entered chan struct{}
	once    sync.Once
}

func (r *cancelledPlanRunner) Run(ctx context.Context, name string, args ...string) error {
	if name == r.command {
		r.once.Do(func() { close(r.entered) })
		<-ctx.Done()
		return ctx.Err()
	}
	return r.recordingRunner.Run(ctx, name, args...)
}

func TestUpdatePlanCancelsSlowCommandsAndRetries(t *testing.T) {
	for _, command := range []string{"ip", "wg", "iptables"} {
		t.Run(command, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				manager, err := NewAutomatedWireGuardManager(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				manager.netnsDir = t.TempDir()
				manager.run = &recordingRunner{}
				plan := Plan{CIDR: "10.42.1.0/24", Gateway: "10.42.1.1", WireGuardAddress: "169.254.1.1/32", ListenPort: 51917}
				if _, err := manager.Attach(context.Background(), AttachRequest{Namespace: "acme", Network: "acme", AllocationID: "alloc", Plan: plan}); err != nil {
					t.Fatal(err)
				}
				runner := &cancelledPlanRunner{command: command, entered: make(chan struct{})}
				manager.run = runner
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- manager.UpdatePlan(ctx, "acme", plan) }()
				<-runner.entered
				cancel()
				if err := <-done; !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled %s error = %v", command, err)
				}
				manager.run = &recordingRunner{}
				if err := manager.UpdatePlan(context.Background(), "acme", plan); err != nil {
					t.Fatalf("retry after %s cancellation: %v", command, err)
				}
			})
		})
	}
}

func (r *slowRecordingRunner) Run(ctx context.Context, name string, args ...string) error {
	select {
	case <-time.After(r.delay):
	case <-ctx.Done():
		return ctx.Err()
	}
	return r.recordingRunner.Run(ctx, name, args...)
}

type partialRouteRunner struct {
	recordingRunner
	routes      map[string]bool
	failReplace string
	failed      bool
}

func (r *partialRouteRunner) Run(_ context.Context, name string, args ...string) error {
	command := name + " " + strings.Join(args, " ")
	r.commands = append(r.commands, command)
	if name != "ip" || len(args) < 3 || args[0] != "route" {
		return nil
	}
	switch args[1] {
	case "flush":
		if len(args) >= 4 && args[2] == "exact" {
			delete(r.routes, args[3])
		}
	case "replace":
		route := args[2]
		if route == r.failReplace && !r.failed {
			r.failed = true
			return errors.New("injected route replace failure")
		}
		r.routes[route] = true
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
	manager.netnsDir = t.TempDir()
	if err := manager.ConfigureWorkloadDNS(context.Background(), WorkloadDNSAddress); err != nil {
		t.Fatal(err)
	}
	a, err := manager.Attach(context.Background(), AttachRequest{Namespace: "acme", Network: "blue", AllocationID: "alloc-1"})
	if err != nil {
		t.Fatalf("Attach() error = %v", err)
	}
	if a.Namespace != "acme" || a.NetworkNamespace != manager.netnsPath("alloc-1") || !strings.HasPrefix(a.Address, "10.42.1.") {
		t.Fatalf("unexpected attachment: %#v", a)
	}
	joined := strings.Join(runner.commands, "\n")
	for _, want := range []string{"type wireguard", "wg set", "unshare --net -- mount --bind /proc/self/ns/net", "netns alloc-1", "iptables -I FORWARD 1 -j TRELLIS-FORWARD", "ip addr replace 198.18.0.53/32 dev lo", "iptables -I INPUT 1 -j TRELLIS-INPUT", "iptables -C TRELLIS-INPUT -i tb"} {
		if !strings.Contains(joined, want) {
			t.Errorf("commands do not contain %q:\n%s", want, joined)
		}
	}
}

func TestNamespaceFirewallAcceptsDNSOnlyFromNamespaceCIDR(t *testing.T) {
	runner := &recordingRunner{}
	manager := NewWireGuardManager(t.TempDir())
	manager.run = runner
	manager.dnsAddress = WorkloadDNSAddress

	if err := manager.reconcileFirewall(context.Background(), "tb-acme", "tw-acme", "10.42.1.0/24", "10.42.1.1", 8128); err != nil {
		t.Fatal(err)
	}

	commands := strings.Join(runner.commands, "\n")
	for _, protocol := range []string{"udp", "tcp"} {
		valid := "iptables -C TRELLIS-INPUT -i tb-acme -s 10.42.1.0/24 -d 198.18.0.53 -p " + protocol + " --dport 53 -j ACCEPT"
		if !strings.Contains(commands, valid) {
			t.Errorf("namespace-source DNS traffic is not accepted for %s:\n%s", protocol, commands)
		}
	}
	if want := "iptables -C TRELLIS-FORWARD -i tb-acme ! -s 10.42.1.0/24 -j DROP"; !strings.Contains(commands, want) {
		t.Errorf("spoofed forwarded traffic is not dropped:\n%s", commands)
	}
	for _, want := range []string{
		"iptables -I FORWARD 1 -j TRELLIS-FORWARD",
		"iptables -C TRELLIS-FORWARD -i tb-acme -o tb+ -m conntrack ! --ctstate DNAT -j DROP",
		"iptables -C TRELLIS-FORWARD -i tb-acme -o tw+ -j DROP",
		"iptables -C TRELLIS-FORWARD -o tb-acme -j DROP",
	} {
		if !strings.Contains(commands, want) {
			t.Errorf("Trellis forwarding isolation does not contain %q:\n%s", want, commands)
		}
	}
	for _, shared := range []string{"iptables -A FORWARD", "iptables -I FORWARD -", "iptables -A INPUT", "iptables -I INPUT -"} {
		if strings.Contains(commands, shared) {
			t.Errorf("isolation rule was added to a shared built-in chain:\n%s", commands)
		}
	}
	if want := "iptables -C TRELLIS-INPUT -i tb-acme -s 10.42.1.0/24 -d 10.42.1.1 -p tcp --dport 8128 -j ACCEPT"; !strings.Contains(commands, want) {
		t.Errorf("namespace-source API traffic is not accepted:\n%s", commands)
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
	manager.netnsDir = t.TempDir()
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
	manager.netnsDir = t.TempDir()
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
		"iptables -D TRELLIS-FORWARD",
		"iptables -D FORWARD -j TRELLIS-FORWARD",
		"iptables -X TRELLIS-FORWARD",
		"iptables -D TRELLIS-INPUT -i " + second.Bridge + " -j DROP",
		"iptables -D INPUT -j TRELLIS-INPUT",
		"iptables -X TRELLIS-INPUT",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("last detach did not tear down %q:\n%s", want, joined)
		}
	}
	if _, err := os.Stat(filepath.Join(manager.stateDir, "acme")); !os.IsNotExist(err) {
		t.Fatalf("namespace lease directory still exists after last detach: %v", err)
	}
}

func TestWireGuardDetachKeepsSharedFirewallChainForOtherNamespace(t *testing.T) {
	manager, err := NewAutomatedWireGuardManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager.netnsDir = t.TempDir()
	runner := &recordingRunner{}
	manager.run = runner
	first, err := manager.Attach(context.Background(), AttachRequest{
		Namespace: "acme", Network: "acme", AllocationID: "alloc-acme",
		Plan: Plan{CIDR: "10.42.1.0/24", Gateway: "10.42.1.1", WireGuardAddress: "169.254.1.1/32", ListenPort: 51917},
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := manager.Attach(context.Background(), AttachRequest{
		Namespace: "other", Network: "other", AllocationID: "alloc-other",
		Plan: Plan{CIDR: "10.42.2.0/24", Gateway: "10.42.2.1", WireGuardAddress: "169.254.2.1/32", ListenPort: 51918},
	})
	if err != nil {
		t.Fatal(err)
	}

	runner.commands = nil
	if err := manager.Detach(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(runner.commands, "\n")
	if strings.Contains(joined, "iptables -D FORWARD -j TRELLIS-FORWARD") || strings.Contains(joined, "iptables -X TRELLIS-FORWARD") ||
		strings.Contains(joined, "iptables -D INPUT -j TRELLIS-INPUT") || strings.Contains(joined, "iptables -X TRELLIS-INPUT") {
		t.Fatalf("shared firewall chains removed while another namespace used it:\n%s", joined)
	}

	runner.commands = nil
	if err := manager.Detach(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	joined = strings.Join(runner.commands, "\n")
	if !strings.Contains(joined, "iptables -D FORWARD -j TRELLIS-FORWARD") || !strings.Contains(joined, "iptables -X TRELLIS-FORWARD") ||
		!strings.Contains(joined, "iptables -D INPUT -j TRELLIS-INPUT") || !strings.Contains(joined, "iptables -X TRELLIS-INPUT") {
		t.Fatalf("shared firewall chains survived final namespace detach:\n%s", joined)
	}
}

func TestWireGuardDetachPreservesLeaseAndConvergesAfterCleanupFailure(t *testing.T) {
	tests := []struct {
		name        string
		failCommand func(*Attachment) string
		wantError   string
	}{
		{
			name:        "host veth",
			failCommand: func(a *Attachment) string { return "ip link del " + a.HostVeth },
			wantError:   "delete allocation veth",
		},
		{
			name:        "network namespace",
			failCommand: func(a *Attachment) string { return "ip netns del " + a.AllocationID },
			wantError:   "remove allocation network namespace",
		},
		{
			name: "firewall rule",
			failCommand: func(a *Attachment) string {
				return "iptables -D TRELLIS-FORWARD -i " + a.Bridge + " -o " + a.WireGuardInterface + " -j ACCEPT"
			},
			wantError: "delete firewall rule",
		},
		{
			name:        "WireGuard interface",
			failCommand: func(a *Attachment) string { return "ip link del " + a.WireGuardInterface },
			wantError:   "delete WireGuard interface",
		},
		{
			name:        "bridge",
			failCommand: func(a *Attachment) string { return "ip link del " + a.Bridge },
			wantError:   "delete bridge",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager, err := NewAutomatedWireGuardManager(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			manager.netnsDir = t.TempDir()
			manager.run = &recordingRunner{}
			attachment, err := manager.Attach(context.Background(), AttachRequest{
				Namespace: "acme", Network: "acme", AllocationID: "alloc-one", Plan: Plan{
					CIDR: "10.42.1.0/24", Gateway: "10.42.1.1", WireGuardAddress: "169.254.1.1/32", ListenPort: 51917,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			runner := &cleanupRunner{
				links: map[string]bool{
					attachment.HostVeth:           true,
					attachment.WireGuardInterface: true,
					attachment.Bridge:             true,
				},
				namespaces: map[string]bool{attachment.AllocationID: true},
				firewallRules: map[string]bool{
					"TRELLIS-FORWARD -i " + attachment.Bridge + " ! -s 10.42.1.0/24 -j DROP":                          true,
					"TRELLIS-FORWARD -i " + attachment.Bridge + " -o " + attachment.WireGuardInterface + " -j ACCEPT": true,
					"TRELLIS-FORWARD -o " + attachment.Bridge + " -j DROP":                                            true,
					"FORWARD -j TRELLIS-FORWARD":                         true,
					"TRELLIS-INPUT -i " + attachment.Bridge + " -j DROP": true,
					"INPUT -j TRELLIS-INPUT":                             true,
				},
				failCommand: tt.failCommand(attachment),
			}
			manager.run = runner
			if tt.name == "network namespace" {
				if err := os.WriteFile(manager.netnsPath(attachment.AllocationID), nil, 0o600); err != nil {
					t.Fatal(err)
				}
				recordTestNetnsInode(t, manager, attachment.AllocationID)
			}

			err = manager.Detach(context.Background(), attachment)
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("Detach() error = %v, want context %q", err, tt.wantError)
			}
			if _, err := os.Stat(attachment.LeasePath); err != nil {
				t.Fatalf("address lease was not preserved after cleanup failure: %v", err)
			}
			assertAttachments(t, manager, attachment.AllocationID)

			if tt.name == "network namespace" {
				if err := os.Remove(manager.netnsPath(attachment.AllocationID)); err != nil {
					t.Fatal(err)
				}
			}
			if err := manager.Detach(context.Background(), attachment); err != nil {
				t.Fatalf("Detach() retry error = %v", err)
			}
			if _, err := os.Stat(attachment.LeasePath); !os.IsNotExist(err) {
				t.Fatalf("address lease still exists after successful retry: %v", err)
			}
			assertAttachments(t, manager)
			if len(runner.links) != 0 || len(runner.namespaces) != 0 || len(runner.firewallRules) != 0 {
				t.Fatalf("kernel resources remain after successful retry: links=%v namespaces=%v firewall=%v", runner.links, runner.namespaces, runner.firewallRules)
			}
		})
	}
}

func TestWireGuardDetachDoesNotTreatInspectionFailureAsAbsence(t *testing.T) {
	manager, err := NewAutomatedWireGuardManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager.netnsDir = t.TempDir()
	manager.run = &recordingRunner{}
	attachment, err := manager.Attach(context.Background(), AttachRequest{
		Namespace: "acme", Network: "acme", AllocationID: "alloc-one", Plan: Plan{
			CIDR: "10.42.1.0/24", Gateway: "10.42.1.1", WireGuardAddress: "169.254.1.1/32", ListenPort: 51917,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	manager.run = &failingRunner{failCommand: "ip link"}

	err = manager.Detach(context.Background(), attachment)
	if err == nil || !strings.Contains(err.Error(), "verify absence") {
		t.Fatalf("Detach() error = %v, want failed absence verification", err)
	}
	if _, err := os.Stat(attachment.LeasePath); err != nil {
		t.Fatalf("address lease was not preserved after ambiguous cleanup failure: %v", err)
	}
	assertAttachments(t, manager, attachment.AllocationID)
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
	manager.netnsDir = t.TempDir()
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
	for _, want := range []string{"peer old-peer remove", "ip route flush exact 10.42.2.0/24 dev " + first.WireGuardInterface} {
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
	manager.netnsDir = t.TempDir()
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
		"wg set " + attachment.WireGuardInterface + " private-key " + filepath.Join(manager.stateDir, "identity.key") + " listen-port 51917 peer new-peer allowed-ips 10.42.2.0/24 endpoint node-b:51917",
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
	for _, want := range []string{"peer new-peer remove", "ip route flush exact 10.42.2.0/24 dev " + attachment.WireGuardInterface} {
		if !strings.Contains(joined, want) {
			t.Fatalf("commands do not contain %q:\n%s", want, joined)
		}
	}
}

func TestUpdatePlanFailurePersistsConservativePeerPlan(t *testing.T) {
	manager, err := NewAutomatedWireGuardManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager.netnsDir = t.TempDir()
	manager.run = &recordingRunner{}
	oldPlan := Plan{
		CIDR:             "10.42.1.0/24",
		Gateway:          "10.42.1.1",
		WireGuardAddress: "169.254.1.1/32",
		ListenPort:       51917,
		Peers:            []PeerPlan{{PublicKey: "old-peer", AllowedIPs: []string{"10.42.2.0/24"}}},
	}
	attachment, err := manager.Attach(context.Background(), AttachRequest{Namespace: "acme", Network: "acme", AllocationID: "alloc-old", Plan: oldPlan})
	if err != nil {
		t.Fatal(err)
	}

	newPlan := oldPlan
	newPlan.Peers = []PeerPlan{{PublicKey: "new-peer", AllowedIPs: []string{"10.42.3.0/24"}}}
	manager.run = &failingRunner{failCommand: "wg set"}
	if err := manager.UpdatePlan(context.Background(), "acme", newPlan); err == nil {
		t.Fatal("UpdatePlan reported success after batched WireGuard apply failed")
	}

	raw, err := os.ReadFile(manager.planPath("acme", "acme"))
	if err != nil {
		t.Fatal(err)
	}
	var persisted []Peer
	if err := json.Unmarshal(raw, &persisted); err != nil {
		t.Fatal(err)
	}
	if len(persisted) != 2 {
		t.Fatalf("failed apply did not retain conservative peer set: %#v", persisted)
	}
	keys := map[string]bool{}
	for _, peer := range persisted {
		keys[peer.PublicKey] = true
	}
	if !keys["old-peer"] || !keys["new-peer"] {
		t.Fatalf("conservative plan = %#v, want old-peer and new-peer", persisted)
	}

	runner := &recordingRunner{}
	manager.run = runner
	if err := manager.UpdatePlan(context.Background(), "acme", newPlan); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(runner.commands, "\n")
	want := "wg set " + attachment.WireGuardInterface
	if !strings.Contains(joined, want) || !strings.Contains(joined, "peer old-peer remove") || !strings.Contains(joined, "peer new-peer allowed-ips 10.42.3.0/24") {
		t.Fatalf("retry did not reconcile stale and desired peers in one batch:\n%s", joined)
	}
}

func TestUpdatePlanRetryConvergesAfterPartialRouteApply(t *testing.T) {
	manager, err := NewAutomatedWireGuardManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager.netnsDir = t.TempDir()
	manager.run = &recordingRunner{}
	oldPlan := Plan{
		CIDR:             "10.42.1.0/24",
		Gateway:          "10.42.1.1",
		WireGuardAddress: "169.254.1.1/32",
		ListenPort:       51917,
		Peers:            []PeerPlan{{PublicKey: "old-peer", AllowedIPs: []string{"10.42.2.0/24"}}},
	}
	if _, err := manager.Attach(context.Background(), AttachRequest{Namespace: "acme", Network: "acme", AllocationID: "alloc-old", Plan: oldPlan}); err != nil {
		t.Fatal(err)
	}

	newPlan := oldPlan
	newPlan.Peers = []PeerPlan{{PublicKey: "new-peer", AllowedIPs: []string{"10.42.3.0/24"}}}
	runner := &partialRouteRunner{
		routes:      map[string]bool{"10.42.2.0/24": true},
		failReplace: "10.42.3.0/24",
	}
	manager.run = runner
	if err := manager.UpdatePlan(context.Background(), "acme", newPlan); err == nil {
		t.Fatal("first UpdatePlan unexpectedly succeeded")
	}
	if runner.routes["10.42.2.0/24"] {
		t.Fatal("stale route was not removed before injected failure")
	}
	if runner.routes["10.42.3.0/24"] {
		t.Fatal("desired route was installed despite injected failure")
	}

	runner.commands = nil
	if err := manager.UpdatePlan(context.Background(), "acme", newPlan); err != nil {
		t.Fatalf("retry did not converge after partial route apply: %v", err)
	}
	if !runner.routes["10.42.3.0/24"] {
		t.Fatal("desired route was not installed on retry")
	}
	joined := strings.Join(runner.commands, "\n")
	if !strings.Contains(joined, "ip route flush exact 10.42.2.0/24 dev ") {
		t.Fatalf("retry did not idempotently re-remove stale route:\n%s", joined)
	}

	raw, err := os.ReadFile(manager.planPath("acme", "acme"))
	if err != nil {
		t.Fatal(err)
	}
	var persisted []Peer
	if err := json.Unmarshal(raw, &persisted); err != nil {
		t.Fatal(err)
	}
	if len(persisted) != 1 || persisted[0].PublicKey != "new-peer" {
		t.Fatalf("retry did not persist converged plan: %#v", persisted)
	}
}

func TestUpdatePlanSupersessionCleansPartiallyAppliedPeersAndRoutes(t *testing.T) {
	manager, err := NewAutomatedWireGuardManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager.netnsDir = t.TempDir()
	manager.run = &recordingRunner{}
	p1 := Plan{
		CIDR:             "10.42.1.0/24",
		Gateway:          "10.42.1.1",
		WireGuardAddress: "169.254.1.1/32",
		ListenPort:       51917,
		Peers:            []PeerPlan{{PublicKey: "peer-a", AllowedIPs: []string{"10.42.2.0/24"}}},
	}
	if _, err := manager.Attach(context.Background(), AttachRequest{Namespace: "acme", Network: "acme", AllocationID: "alloc-old", Plan: p1}); err != nil {
		t.Fatal(err)
	}

	p2 := p1
	p2.Peers = []PeerPlan{
		{PublicKey: "peer-a", AllowedIPs: []string{"10.42.2.0/24"}},
		{PublicKey: "peer-b", AllowedIPs: []string{"10.42.3.0/24"}},
		{PublicKey: "peer-c", AllowedIPs: []string{"10.42.4.0/24"}},
	}
	manager.run = &failingRunner{failCommand: "ip route replace 10.42.4.0/24"}
	if err := manager.UpdatePlan(context.Background(), "acme", p2); err == nil {
		t.Fatal("partially applied P2 unexpectedly succeeded")
	}

	raw, err := os.ReadFile(manager.planPath("acme", "acme"))
	if err != nil {
		t.Fatal(err)
	}
	var conservative []Peer
	if err := json.Unmarshal(raw, &conservative); err != nil {
		t.Fatal(err)
	}
	keys := map[string]bool{}
	for _, peer := range conservative {
		keys[peer.PublicKey] = true
	}
	if !keys["peer-a"] || !keys["peer-b"] || !keys["peer-c"] {
		t.Fatalf("partial P2 was not conservatively persisted: %#v", conservative)
	}

	runner := &recordingRunner{}
	manager.run = runner
	p3 := p1
	if err := manager.UpdatePlan(context.Background(), "acme", p3); err != nil {
		t.Fatalf("superseding P3 did not converge: %v", err)
	}
	joined := strings.Join(runner.commands, "\n")
	for _, want := range []string{
		"peer peer-b remove",
		"peer peer-c remove",
		"ip route flush exact 10.42.3.0/24 dev ",
		"ip route flush exact 10.42.4.0/24 dev ",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("superseding plan did not clean %q:\n%s", want, joined)
		}
	}

	raw, err = os.ReadFile(manager.planPath("acme", "acme"))
	if err != nil {
		t.Fatal(err)
	}
	var exact []Peer
	if err := json.Unmarshal(raw, &exact); err != nil {
		t.Fatal(err)
	}
	if len(exact) != 1 || exact[0].PublicKey != "peer-a" {
		t.Fatalf("converged P3 state = %#v, want only peer-a", exact)
	}
}

func TestAtomicNetworkPlanWritePreservesPreviousFileOnCommitFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "plan.json")
	old := []byte(`[{"public_key":"old-peer"}]`)
	next := []byte(`[{"public_key":"new-peer"}]`)
	if err := os.WriteFile(path, old, 0o600); err != nil {
		t.Fatal(err)
	}

	err := writeAtomicFileWithRename(path, next, 0o600, func(string, string) error {
		return errors.New("injected rename failure")
	})
	if err == nil {
		t.Fatal("atomic write unexpectedly succeeded")
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(old) {
		t.Fatalf("failed atomic commit changed previous file: %q", got)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "plan.json" {
		t.Fatalf("temporary network plan was not cleaned up: %#v", entries)
	}

	if err := writeAtomicFile(path, next); err != nil {
		t.Fatal(err)
	}
	got, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(next) {
		t.Fatalf("atomic replacement = %q, want %q", got, next)
	}

	err = writeAtomicFileWithOps(path, old, 0o600, os.Rename, func(string) (*os.File, error) {
		return nil, errors.New("injected directory open failure")
	})
	if err == nil || !strings.Contains(err.Error(), "open network plan directory") {
		t.Fatalf("directory open failure was not reported: %v", err)
	}
	got, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(old) {
		t.Fatalf("rename before directory sync failure did not install data: %q", got)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("network plan mode = %o, want 600", info.Mode().Perm())
	}
}

func TestUpdatePlanBatchesLargePeerSetWithinDeadline(t *testing.T) {
	manager, err := NewAutomatedWireGuardManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager.netnsDir = t.TempDir()
	manager.run = &recordingRunner{}
	plan := Plan{CIDR: "10.42.1.0/24", Gateway: "10.42.1.1", WireGuardAddress: "169.254.1.1/32", ListenPort: 51917}
	attachment, err := manager.Attach(context.Background(), AttachRequest{Namespace: "acme", Network: "acme", AllocationID: "alloc-old", Plan: plan})
	if err != nil {
		t.Fatal(err)
	}

	const peerCount = 400
	plan.Peers = make([]PeerPlan, 0, peerCount)
	for i := range peerCount {
		plan.Peers = append(plan.Peers, PeerPlan{
			PublicKey:  fmt.Sprintf("peer-%03d", i),
			Endpoint:   fmt.Sprintf("node-%03d.example.test:51917", i),
			AllowedIPs: []string{fmt.Sprintf("10.%d.%d.0/24", 50+i/256, i%256)},
		})
	}
	runner := &slowRecordingRunner{delay: time.Millisecond}
	manager.run = runner
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := manager.UpdatePlan(ctx, "acme", plan); err != nil {
		t.Fatalf("large UpdatePlan() did not converge within legacy deadline: %v", err)
	}

	wgSets := 0
	seen := make(map[string]bool, peerCount)
	for _, command := range runner.commands {
		if !strings.HasPrefix(command, "wg set "+attachment.WireGuardInterface+" ") {
			continue
		}
		wgSets++
		if len(command) > wireGuardCommandArgBudget {
			t.Fatalf("WireGuard command exceeded argument budget: %d > %d", len(command), wireGuardCommandArgBudget)
		}
		for i := range peerCount {
			key := fmt.Sprintf("peer-%03d", i)
			if strings.Contains(command, "peer "+key+" ") {
				seen[key] = true
			}
		}
	}
	if wgSets < 2 {
		t.Fatalf("large WireGuard plan used %d wg set command, want bounded batches", wgSets)
	}
	if len(seen) != peerCount {
		t.Fatalf("batched WireGuard plan covered %d/%d peers", len(seen), peerCount)
	}
}

func TestUpdatePlanWaitingForFinalDetachDoesNotRestorePlan(t *testing.T) {
	manager, err := NewAutomatedWireGuardManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager.netnsDir = t.TempDir()
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

	manager.run = &failingRunner{failCommand: "ip route flush"}
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

func TestPlanJSONPreservesExecutionHashInput(t *testing.T) {
	plan := Plan{
		CIDR: "10.42.1.0/24", Gateway: "10.42.1.1", WireGuardAddress: "169.254.1.1/32",
		ListenPort: 51917, APIPort: 8080,
		Peers: []PeerPlan{{PublicKey: "peer", Endpoint: "192.0.2.1:51918", AllowedIPs: []string{"10.42.2.0/24"}}},
	}
	raw, _ := json.Marshal(plan)
	const want = `{"CIDR":"10.42.1.0/24","Gateway":"10.42.1.1","WireGuardAddress":"169.254.1.1/32","ListenPort":51917,"APIPort":8080,"Peers":[{"PublicKey":"peer","Endpoint":"192.0.2.1:51918","AllowedIPs":["10.42.2.0/24"]}]}`
	if string(raw) != want {
		t.Fatalf("network plan encoding changed:\ngot  %s\nwant %s", raw, want)
	}
}
