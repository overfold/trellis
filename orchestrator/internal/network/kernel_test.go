package network

import (
	"context"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Opt-in: requires root, network/mount namespaces, iptables and WireGuard.
// The child isolates both kernel tables and /var/run/netns from the host.
func TestKernelNamespaceAudit(t *testing.T) {
	if os.Getenv("TRELLIS_NETWORK_E2E") != "1" {
		t.Skip("set TRELLIS_NETWORK_E2E=1 on a privileged Linux host")
	}
	if os.Getenv("TRELLIS_NETWORK_E2E_CHILD") != "1" {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, "unshare", "-nm", os.Args[0], "-test.run=^TestKernelNamespaceAudit$", "-test.v")
		cmd.Env = append(os.Environ(), "TRELLIS_NETWORK_E2E_CHILD=1")
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("isolated kernel regressions: %v\n%s", err, output)
		}
		t.Log(string(output))
		return
	}
	run := func(name string, args ...string) {
		t.Helper()
		if err := (execRunner{}).Run(t.Context(), name, args...); err != nil {
			t.Fatal(err)
		}
	}
	run("mount", "--make-rprivate", "/")
	if err := os.MkdirAll(defaultNetnsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	run("mount", "-t", "tmpfs", "tmpfs", defaultNetnsDir)
	// sysfs must reflect this child's network namespace, not the parent's.
	run("mount", "-t", "sysfs", "sysfs", "/sys")
	run("ip", "link", "set", "lo", "up")
	manager, err := NewAutomatedWireGuardManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.ConfigureWorkloadDNS(t.Context(), WorkloadDNSAddress); err != nil {
		t.Fatal(err)
	}
	plan := recoveryTestPlan()
	one, err := manager.Attach(t.Context(), AttachRequest{Namespace: "audit-420639", Network: "audit-420639", AllocationID: "one", Plan: plan})
	if err != nil {
		t.Fatal(err)
	}
	otherPlan := plan
	otherPlan.CIDR, otherPlan.Gateway, otherPlan.WireGuardAddress, otherPlan.ListenPort = "10.42.2.0/24", "10.42.2.1", "169.254.1.2/32", 51918
	two, err := manager.Attach(t.Context(), AttachRequest{Namespace: "audit-1362137", Network: "audit-1362137", AllocationID: "two", Plan: otherPlan})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", ":8126")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{ReadHeaderTimeout: time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })}
	defer func() { _ = server.Close() }()
	go func() { _ = server.Serve(listener) }()
	connect := func(namespace, address string, want bool) {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, "ip", "netns", "exec", namespace, "curl", "--noproxy", "*", "-g", "--connect-timeout", "0.5", "--max-time", "1", "--fail", "--silent", "--show-error", "http://"+address+":8126/")
		output, err := cmd.CombinedOutput()
		if want && (err != nil || string(output) != "ok") || !want && err == nil {
			t.Fatalf("%s -> %s reachable=%v, want %v: %v %s", namespace, address, err == nil, want, err, output)
		}
	}
	connect("one", plan.Gateway, true)
	connect("two", otherPlan.Gateway, true)
	connect("one", otherPlan.Gateway, false) // Cross-namespace host input.

	// Prove IPv6 link-local can reach a dual-stack host listener without the
	// fix, then restore IPv4-only host links and exercise the same packet path.
	for _, name := range []string{one.Bridge, one.HostVeth} {
		run("sysctl", "-w", "net.ipv6.conf."+name+".disable_ipv6=0")
	}
	run("ip", "-6", "addr", "replace", "fe80::1/64", "dev", one.Bridge, "nodad")
	run("ip", "-n", "one", "-6", "addr", "replace", "fe80::2/64", "dev", "eth0", "nodad")
	connect("one", "[fe80::1%25eth0]", true)
	for _, name := range []string{one.Bridge, one.HostVeth} {
		if err := manager.disableIPv6(t.Context(), name); err != nil {
			t.Fatal(err)
		}
	}
	connect("one", "[fe80::1%25eth0]", false)

	// A real remote WireGuard peer reaches allocations, not host services.
	run("ip", "netns", "add", "remote")
	run("ip", "link", "add", "underlay", "type", "veth", "peer", "name", "remote-underlay")
	run("ip", "link", "set", "remote-underlay", "netns", "remote")
	run("ip", "addr", "add", "192.0.2.1/24", "dev", "underlay")
	run("ip", "link", "set", "underlay", "up")
	run("ip", "-n", "remote", "addr", "add", "192.0.2.2/24", "dev", "remote-underlay")
	run("ip", "-n", "remote", "link", "set", "remote-underlay", "up")
	run("ip", "-n", "remote", "link", "add", "wg0", "type", "wireguard")
	remoteIdentity, err := NewAutomatedWireGuardManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	remoteKey, err := remoteIdentity.Identity()
	if err != nil {
		t.Fatal(err)
	}
	localKey, err := manager.Identity()
	if err != nil {
		t.Fatal(err)
	}
	plan.APIPort = 0
	plan.Peers = []PeerPlan{{PublicKey: remoteKey, Endpoint: "192.0.2.2:52000", AllowedIPs: []string{"10.42.9.0/24"}}}
	if err := manager.UpdatePlan(t.Context(), one.Namespace, plan); err != nil {
		t.Fatal(err)
	}
	run("ip", "netns", "exec", "remote", "wg", "set", "wg0", "private-key", filepath.Join(remoteIdentity.stateDir, "identity.key"), "listen-port", "52000", "peer", localKey, "endpoint", "192.0.2.1:51917", "allowed-ips", "10.42.1.0/24", "persistent-keepalive", "1")
	run("ip", "-n", "remote", "addr", "add", "10.42.9.2/24", "dev", "wg0")
	run("ip", "-n", "remote", "link", "set", "wg0", "up")
	run("ip", "-n", "remote", "route", "add", "10.42.1.0/24", "dev", "wg0")
	fixtureCtx, stopFixture := context.WithCancel(t.Context())
	defer stopFixture()
	fixture := exec.CommandContext(fixtureCtx, "ip", "netns", "exec", "one", os.Args[0], "-test.run=^TestKernelHTTPFixture$")
	fixture.Env = append(os.Environ(), "TRELLIS_NETWORK_HTTP_FIXTURE=1")
	if err := fixture.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { stopFixture(); _ = fixture.Wait() }()
	allocationIP, _, _ := strings.Cut(one.Address, "/")
	httpClient := &http.Client{Timeout: time.Second, Transport: &http.Transport{Proxy: nil}}
	defer httpClient.CloseIdleConnections()
	deadline := time.Now().Add(3 * time.Second)
	for {
		response, err := httpClient.Get("http://" + allocationIP + ":8126/")
		if err == nil {
			_ = response.Body.Close()
			break // Node/host-network initiated traffic must still work.
		}
		if time.Now().After(deadline) {
			t.Fatalf("node could not reach its namespace workload: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	connect("remote", allocationIP, true) // Remote namespace peers reach workloads.
	// Baseline proves WireGuard transport and the host listener work.
	run("iptables", "-I", "INPUT", "1", "-i", one.WireGuardInterface, "-j", "ACCEPT")
	connect("remote", plan.Gateway, true)
	run("iptables", "-D", "INPUT", "-i", one.WireGuardInterface, "-j", "ACCEPT")
	connect("remote", plan.Gateway, false)

	// Flush the owned input chain, then repair using a grant-free peer update.
	run("iptables", "-F", inputChain)
	if err := manager.UpdatePlan(t.Context(), one.Namespace, plan); err != nil {
		t.Fatal(err)
	}
	connect("one", plan.Gateway, true)
	connect("remote", plan.Gateway, false)

	// Refuse foreign aliases before any teardown and ignore forged names.
	run("ip", "link", "set", "dev", one.Bridge, "alias", "foreign")
	if err := manager.DetachAllocation(t.Context(), "one"); err == nil {
		t.Fatal("teardown accepted foreign ownership")
	}
	connect("one", plan.Gateway, true)
	run("ip", "link", "set", "dev", one.Bridge, "alias", pathOwner(one.Namespace, one.Network)+":"+one.Bridge)
	one.HostVeth = "underlay"
	if err := manager.Detach(t.Context(), one); err != nil {
		t.Fatal(err)
	}
	if _, err := (execRunner{}).Output(t.Context(), "ip", "link", "show", "underlay"); err != nil {
		t.Fatalf("caller-supplied link was deleted: %v", err)
	}
	if exists, err := checkLinkOwner(two.Bridge, pathOwner(two.Namespace, two.Network)); !exists || err != nil {
		t.Fatalf("old hash collision damaged other namespace: %v", err)
	}
	if err := manager.UpdatePlan(t.Context(), two.Namespace, otherPlan); err != nil {
		t.Fatal(err)
	}
	connect("two", otherPlan.Gateway, true)
	if err := manager.Detach(t.Context(), two); err != nil {
		t.Fatal(err)
	}
	output, err := (execRunner{}).Output(t.Context(), "iptables", "-S")
	if err != nil || strings.Contains(output, "TRELLIS") {
		t.Fatalf("owned chains survived final teardown: %v %s", err, output)
	}
	t.Log("kernel verified collision isolation, IPv6 link-local denial, WireGuard host-input denial, API firewall repair, and ownership-safe teardown")
}

func TestKernelHTTPFixture(t *testing.T) {
	if os.Getenv("TRELLIS_NETWORK_HTTP_FIXTURE") != "1" {
		t.Skip("subprocess-only workload listener")
	}
	listener, err := net.Listen("tcp", ":8126")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{ReadHeaderTimeout: time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })}
	if err := server.Serve(listener); err != nil {
		t.Fatal(err)
	}
}
