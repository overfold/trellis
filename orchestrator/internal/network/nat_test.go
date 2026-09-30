package network

import (
	"context"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// packet is what the model's rule evaluator matches against.
type packet struct {
	in, out, src, dst, proto string
	dport                    int
	states                   []string
}

// verdict evaluates chain for p the way iptables does for the options the
// manager uses, following jumps to Trellis chains. It returns the first
// terminal target with its arguments, or "" when the chain falls through.
func (m *iptablesModel) verdict(t *testing.T, chain string, p packet) string {
	t.Helper()
	for _, rule := range m.chains[chain] {
		target, ok := matchRule(t, rule, p)
		if !ok {
			continue
		}
		if target == "RETURN" {
			return ""
		}
		name, _, _ := strings.Cut(target, " ")
		prefix, _, _ := strings.Cut(chain, "/")
		nested := name
		if strings.Contains(chain, "/") {
			nested = prefix + "/" + name
		}
		if _, isChain := m.chains[nested]; isChain {
			if result := m.verdict(t, nested, p); result != "" {
				return result
			}
			continue
		}
		return target
	}
	return ""
}

func matchRule(t *testing.T, rule string, p packet) (string, bool) {
	t.Helper()
	fields := strings.Fields(rule)
	negate := false
	for i := 0; i < len(fields); i++ {
		field := fields[i]
		if field == "!" {
			negate = true
			continue
		}
		if field == "-j" {
			return strings.Join(fields[i+1:], " "), true
		}
		i++
		value := fields[i]
		var matched bool
		switch field {
		case "-i", "-o":
			iface := p.in
			if field == "-o" {
				iface = p.out
			}
			if prefix, wildcard := strings.CutSuffix(value, "+"); wildcard {
				matched = strings.HasPrefix(iface, prefix)
			} else {
				matched = iface == value
			}
		case "-s", "-d":
			address := p.src
			if field == "-d" {
				address = p.dst
			}
			prefix, err := netip.ParsePrefix(value)
			if err != nil {
				prefix = netip.PrefixFrom(netip.MustParseAddr(value), 32)
			}
			matched = address != "" && prefix.Contains(netip.MustParseAddr(address))
		case "-p":
			matched = p.proto == value
		case "--dport":
			port, err := strconv.Atoi(value)
			if err != nil {
				t.Fatalf("rule %q has invalid port", rule)
			}
			matched = p.dport == port
		case "--ctstate":
			for _, state := range strings.Split(value, ",") {
				matched = matched || slices.Contains(p.states, state)
			}
		case "-m", "--dst-type":
			// Modules load matchers; address types are asserted separately.
			matched = true
		default:
			t.Fatalf("rule %q has unsupported option %s", rule, field)
		}
		if matched == negate {
			return "", false
		}
		negate = false
	}
	t.Fatalf("rule %q has no target", rule)
	return "", false
}

func twoNamespaceModel(t *testing.T, acmeFirst bool) *iptablesModel {
	t.Helper()
	model := newIPTablesModel()
	manager := NewWireGuardManager(t.TempDir())
	manager.run = model
	manager.dnsAddress = WorkloadDNSAddress
	paths := [][]string{
		{"tb-acme", "tw-acme", "10.42.1.0/24", "10.42.1.1"},
		{"tb-other", "tw-other", "10.42.2.0/24", "10.42.2.1"},
	}
	if !acmeFirst {
		slices.Reverse(paths)
	}
	for _, path := range paths {
		if err := manager.reconcileFirewall(context.Background(), path[0], path[1], path[2], path[3], 8128); err != nil {
			t.Fatal(err)
		}
	}
	return model
}

func TestNamespaceForwardingVerdicts(t *testing.T) {
	newConn := []string{"NEW"}
	published := []string{"NEW", "DNAT"}
	reply := []string{"ESTABLISHED"}
	tests := []struct {
		name string
		p    packet
		want string
	}{
		{"egress leaves the node", packet{in: "tb-acme", out: "eth0", src: "10.42.1.5", states: newConn}, "ACCEPT"},
		{"egress replies return", packet{in: "eth0", out: "tb-acme", dst: "10.42.1.5", states: reply}, "ACCEPT"},
		{"namespace peers over WireGuard", packet{in: "tb-acme", out: "tw-acme", src: "10.42.1.5", states: newConn}, "ACCEPT"},
		{"WireGuard peers enter the bridge", packet{in: "tw-acme", out: "tb-acme", src: "10.42.9.5", dst: "10.42.1.5", states: newConn}, "ACCEPT"},
		{"same bridge", packet{in: "tb-acme", out: "tb-acme", src: "10.42.1.5", dst: "10.42.1.6", states: newConn}, "ACCEPT"},
		{"spoofed source", packet{in: "tb-acme", out: "eth0", src: "10.42.2.5", states: newConn}, "DROP"},
		{"another namespace bridge", packet{in: "tb-acme", out: "tb-other", src: "10.42.1.5", dst: "10.42.2.5", states: newConn}, "DROP"},
		{"another namespace WireGuard", packet{in: "tb-acme", out: "tw-other", src: "10.42.1.5", states: newConn}, "DROP"},
		{"another namespace WireGuard into the bridge", packet{in: "tw-other", out: "tb-acme", dst: "10.42.1.5", states: newConn}, "DROP"},
		{"unpublished inbound", packet{in: "eth0", out: "tb-acme", src: "203.0.113.9", dst: "10.42.1.5", states: newConn}, "DROP"},
		{"published inbound", packet{in: "eth0", out: "tb-acme", src: "203.0.113.9", dst: "10.42.1.5", states: published}, "ACCEPT"},
		{"published port of another namespace", packet{in: "tb-other", out: "tb-acme", src: "10.42.2.5", dst: "10.42.1.5", states: published}, "ACCEPT"},
		{"reply to another namespace client", packet{in: "tb-acme", out: "tb-other", src: "10.42.1.5", dst: "10.42.2.5", states: []string{"ESTABLISHED", "DNAT"}}, "ACCEPT"},
		{"hairpin on the same bridge", packet{in: "tb-acme", out: "tb-acme", src: "10.42.1.5", dst: "10.42.1.6", states: published}, "ACCEPT"},
	}
	for _, acmeFirst := range []bool{true, false} {
		model := twoNamespaceModel(t, acmeFirst)
		for _, test := range tests {
			t.Run(test.name+" acme first "+strconv.FormatBool(acmeFirst), func(t *testing.T) {
				if got := model.verdict(t, forwardChain, test.p); got != test.want {
					t.Fatalf("FORWARD verdict = %q, want %q\n%s", got, test.want, strings.Join(model.chains[forwardChain], "\n"))
				}
			})
		}
	}
}

func TestNamespaceInputVerdicts(t *testing.T) {
	model := twoNamespaceModel(t, true)
	for _, test := range []struct {
		name string
		p    packet
		want string
	}{
		{"workload DNS", packet{in: "tb-acme", src: "10.42.1.5", dst: WorkloadDNSAddress, proto: "udp", dport: 53, states: []string{"NEW"}}, "ACCEPT"},
		{"API proxy", packet{in: "tb-acme", src: "10.42.1.5", dst: "10.42.1.1", proto: "tcp", dport: 8128, states: []string{"NEW"}}, "ACCEPT"},
		{"host service", packet{in: "tb-acme", src: "10.42.1.5", dst: "192.0.2.10", proto: "tcp", dport: 22, states: []string{"NEW"}}, "DROP"},
		{"reply to the node", packet{in: "tb-acme", src: "10.42.1.5", dst: "192.0.2.10", proto: "tcp", dport: 40000, states: []string{"ESTABLISHED"}}, "ACCEPT"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := model.verdict(t, inputChain, test.p); got != test.want {
				t.Fatalf("INPUT verdict = %q, want %q", got, test.want)
			}
		})
	}
}

func TestNamespaceMasqueradeVerdicts(t *testing.T) {
	model := twoNamespaceModel(t, true)
	for _, test := range []struct {
		name string
		p    packet
		want string
	}{
		{"egress", packet{src: "10.42.1.5", out: "eth0", states: []string{"NEW"}}, "MASQUERADE"},
		{"namespace peers keep their source", packet{src: "10.42.1.5", out: "tw-acme", states: []string{"NEW"}}, ""},
		{"same bridge keeps its source", packet{src: "10.42.1.5", out: "tb-acme", states: []string{"NEW"}}, ""},
		{"hairpin", packet{src: "10.42.1.5", out: "tb-acme", states: []string{"NEW", "DNAT"}}, "MASQUERADE"},
		{"published port keeps the client source", packet{src: "203.0.113.9", out: "tb-acme", states: []string{"NEW", "DNAT"}}, ""},
		{"node traffic", packet{src: "192.0.2.10", out: "eth0", states: []string{"NEW"}}, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := model.verdict(t, "nat/"+postroutingChain, test.p); got != test.want {
				t.Fatalf("POSTROUTING verdict = %q, want %q", got, test.want)
			}
		})
	}
}

func TestNamespaceNATChainsJumpFirst(t *testing.T) {
	model := newIPTablesModel()
	model.chains["nat/PREROUTING"] = []string{"-m addrtype --dst-type LOCAL -j DOCKER"}
	model.chains["nat/POSTROUTING"] = []string{"-s 172.17.0.0/16 ! -o docker0 -j MASQUERADE"}
	manager := NewWireGuardManager(t.TempDir())
	manager.run = model
	for range 2 {
		if err := manager.reconcileFirewall(context.Background(), "tb-acme", "tw-acme", "10.42.1.0/24", "10.42.1.1", 0); err != nil {
			t.Fatal(err)
		}
	}
	for chain, want := range map[string][]string{
		"nat/PREROUTING":  {"-m addrtype --dst-type LOCAL -j TRELLIS-PORTS", "-m addrtype --dst-type LOCAL -j DOCKER"},
		"nat/OUTPUT":      {"-m addrtype --dst-type LOCAL ! -d 127.0.0.0/8 -j TRELLIS-PORTS"},
		"nat/POSTROUTING": {"-j TRELLIS-POSTROUTING", "-s 172.17.0.0/16 ! -o docker0 -j MASQUERADE"},
	} {
		if !slices.Equal(model.chains[chain], want) {
			t.Fatalf("%s = %q, want %q", chain, model.chains[chain], want)
		}
	}
}

func TestPublishedPortsForwardToAllocation(t *testing.T) {
	model := newIPTablesModel()
	manager, err := NewAutomatedWireGuardManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager.netnsDir = t.TempDir()
	manager.run = model
	plan := Plan{CIDR: "10.42.1.0/24", Gateway: "10.42.1.1", WireGuardAddress: "169.254.0.1/32", ListenPort: 51917}
	web, err := manager.Attach(context.Background(), AttachRequest{
		Namespace: "acme", Network: "acme", AllocationID: "alloc-web", Plan: plan,
		Ports: []PortMapping{{HostPort: 80, ContainerPort: 8080}, {HostPort: 443, ContainerPort: 8443}},
	})
	if err != nil {
		t.Fatal(err)
	}
	quiet, err := manager.Attach(context.Background(), AttachRequest{Namespace: "acme", Network: "acme", AllocationID: "alloc-quiet", Plan: plan})
	if err != nil {
		t.Fatal(err)
	}
	if len(quiet.Ports) != 0 {
		t.Fatalf("attachment without ports records %v", quiet.Ports)
	}
	address := strings.Split(web.Address, "/")[0]
	for _, test := range []struct {
		proto  string
		dport  int
		target string
	}{
		{"tcp", 80, "DNAT --to-destination " + address + ":8080"},
		{"udp", 80, "DNAT --to-destination " + address + ":8080"},
		{"tcp", 443, "DNAT --to-destination " + address + ":8443"},
		{"tcp", 8080, ""},
	} {
		if got := model.verdict(t, "nat/PREROUTING", packet{proto: test.proto, dport: test.dport}); got != test.target {
			t.Fatalf("PREROUTING %s/%d = %q, want %q", test.proto, test.dport, got, test.target)
		}
	}
	chain := "nat/" + allocationPortsChain("alloc-web")
	if got := len(model.chains[chain]); got != 4 {
		t.Fatalf("%s has %d rules, want tcp and udp for two ports:\n%s", chain, got, strings.Join(model.chains[chain], "\n"))
	}

	// Stopping the allocation stops forwarding before its address is freed,
	// and leaves the namespace path of the remaining allocation intact.
	if err := manager.Detach(context.Background(), web); err != nil {
		t.Fatal(err)
	}
	if _, exists := model.chains[chain]; exists {
		t.Fatalf("%s survived detach", chain)
	}
	if got := model.verdict(t, "nat/PREROUTING", packet{proto: "tcp", dport: 80}); got != "" {
		t.Fatalf("port 80 still forwards after detach: %q", got)
	}
	if len(model.chains["nat/"+portsChain]) != 0 {
		t.Fatalf("%s kept jumps: %q", portsChain, model.chains["nat/"+portsChain])
	}
	if err := manager.Detach(context.Background(), quiet); err != nil {
		t.Fatal(err)
	}
	for chain, rules := range model.chains {
		if strings.Contains(chain, "TRELLIS") {
			t.Fatalf("%s survived the last namespace path: %q", chain, rules)
		}
	}
	for _, parent := range []string{"nat/PREROUTING", "nat/OUTPUT", "nat/POSTROUTING", "INPUT", "FORWARD"} {
		if len(model.chains[parent]) != 0 {
			t.Fatalf("%s kept Trellis jumps: %q", parent, model.chains[parent])
		}
	}
}

func TestPublishedPortsAreRemovedFromRecordAfterCrash(t *testing.T) {
	model := newIPTablesModel()
	manager, err := NewAutomatedWireGuardManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager.netnsDir = t.TempDir()
	manager.run = model
	if _, err := manager.Attach(context.Background(), AttachRequest{
		Namespace: "acme", Network: "acme", AllocationID: "alloc-web",
		Plan:  Plan{CIDR: "10.42.1.0/24", Gateway: "10.42.1.1", WireGuardAddress: "169.254.0.1/32", ListenPort: 51917},
		Ports: []PortMapping{{HostPort: 80, ContainerPort: 8080}},
	}); err != nil {
		t.Fatal(err)
	}
	// A restarted agent that never recorded the attachment detaches by ID.
	if err := manager.DetachAllocation(context.Background(), "alloc-web"); err != nil {
		t.Fatal(err)
	}
	if _, exists := model.chains["nat/"+allocationPortsChain("alloc-web")]; exists {
		t.Fatal("published ports survived detach from the attachment record")
	}
	// Detach is idempotent once everything is gone.
	if err := manager.DetachAllocation(context.Background(), "alloc-web"); err != nil {
		t.Fatal(err)
	}
}

func TestPublishPortsRetryConverges(t *testing.T) {
	model := newIPTablesModel()
	manager := NewWireGuardManager(t.TempDir())
	manager.run = model
	if err := manager.reconcileFirewall(context.Background(), "tb-acme", "tw-acme", "10.42.1.0/24", "10.42.1.1", 0); err != nil {
		t.Fatal(err)
	}
	ports := []PortMapping{{HostPort: 80, ContainerPort: 8080}}
	for range 2 {
		if err := manager.publishPorts(context.Background(), "alloc-web", "10.42.1.5/24", ports); err != nil {
			t.Fatal(err)
		}
	}
	if got := model.chains["nat/"+allocationPortsChain("alloc-web")]; len(got) != 2 {
		t.Fatalf("retried publish left %d rules, want 2: %q", len(got), got)
	}
	if got := model.chains["nat/"+portsChain]; len(got) != 1 {
		t.Fatalf("retried publish left %d jumps, want 1: %q", len(got), got)
	}
}

func TestAttachRejectsInvalidPortMappings(t *testing.T) {
	manager, err := NewAutomatedWireGuardManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	runner := &recordingRunner{}
	manager.run = runner
	for _, ports := range [][]PortMapping{{{HostPort: 0, ContainerPort: 80}}, {{HostPort: 80, ContainerPort: 70000}}} {
		_, err := manager.Attach(context.Background(), AttachRequest{
			Namespace: "acme", Network: "acme", AllocationID: "alloc-web",
			Plan:  Plan{CIDR: "10.42.1.0/24", Gateway: "10.42.1.1", WireGuardAddress: "169.254.0.1/32", ListenPort: 51917},
			Ports: ports,
		})
		if err == nil || !strings.Contains(err.Error(), "out of range") {
			t.Fatalf("Attach(%v) error = %v, want out of range", ports, err)
		}
	}
	if len(runner.commands) != 0 {
		t.Fatalf("invalid ports reached the host: %q", runner.commands)
	}
}
