package network

import (
	"context"
	"encoding/json"
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
	first, err := NewAutomatedWireGuardManager(dir, 51820)
	if err != nil {
		t.Fatal(err)
	}
	public1, err := first.Identity()
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewAutomatedWireGuardManager(dir, 51820)
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
