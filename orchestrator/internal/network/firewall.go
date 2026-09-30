package network

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"os/exec"
	"strings"
)

// Trellis-owned iptables chains. Each built-in chain jumps to its Trellis
// chain first, so host rules cannot bypass namespace isolation.
const (
	forwardChain     = "TRELLIS-FORWARD"
	inputChain       = "TRELLIS-INPUT"
	portsChain       = "TRELLIS-PORTS"
	postroutingChain = "TRELLIS-POSTROUTING"
)

// Interface-name wildcards for every namespace bridge and WireGuard
// interface on the node; they match the prefixes of short("tb"|"tw", ...).
const (
	anyBridge    = "tb+"
	anyWireGuard = "tw+"
)

// chainJump is a built-in chain's jump to a Trellis-owned chain.
type chainJump struct {
	table, parent, chain string
	match                []string
}

// chainJumps lists every jump namespace networking installs. Published ports
// are translated for traffic addressed to any local address, whether it
// arrives from the network or a namespace bridge (PREROUTING) or originates
// on the node itself (OUTPUT). Loopback destinations are excluded: routing a
// 127.0.0.0/8 source to a namespace would need route_localnet on every node.
var chainJumps = []chainJump{
	{parent: "FORWARD", chain: forwardChain},
	{parent: "INPUT", chain: inputChain},
	{table: "nat", parent: "PREROUTING", chain: portsChain, match: []string{"-m", "addrtype", "--dst-type", "LOCAL"}},
	{table: "nat", parent: "OUTPUT", chain: portsChain, match: []string{"-m", "addrtype", "--dst-type", "LOCAL", "!", "-d", "127.0.0.0/8"}},
	{table: "nat", parent: "POSTROUTING", chain: postroutingChain},
}

// firewallRule is one rule in a Trellis-owned chain. Rules are appended in
// order unless insert places them at the head of their chain.
type firewallRule struct {
	table  string
	insert bool
	args   []string // chain, then the rule specification
}

// namespaceRules returns the firewall and NAT rules of one namespace path:
// its bridge, WireGuard interface, and node-local CIDR. Every rule names the
// path's own bridge or CIDR, so rules of different namespaces can interleave
// in the shared chains without affecting each other. When cidr is empty, only
// rules that do not need it are returned; removal uses that when the CIDR of
// a partial attachment is unknown.
//
// Forwarding: allocations reach their namespace peers over WireGuard and
// everything else beyond the node through NAT, but never another namespace's
// bridge or WireGuard interface directly. Only namespace peers, replies, and
// published ports (DNAT) may enter the bridge. Host input from the bridge is
// limited to workload DNS, the API proxy, and replies to connections the node
// opened. Namespace traffic leaving the node other than over WireGuard is
// masqueraded; so is a published-port connection from the same bridge, so the
// reply returns through the node instead of directly between allocations.
func namespaceRules(bridge, wg, cidr, gateway, dnsAddress string, apiPort int) []firewallRule {
	var rules []firewallRule
	add := func(table string, insert bool, args ...string) {
		rules = append(rules, firewallRule{table: table, insert: insert, args: args})
	}
	if cidr != "" {
		add("", false, forwardChain, "-i", bridge, "!", "-s", cidr, "-j", "DROP")
	}
	add("", false, forwardChain, "-i", bridge, "-o", bridge, "-j", "ACCEPT")
	add("", false, forwardChain, "-o", bridge, "-m", "conntrack", "--ctstate", "RELATED,ESTABLISHED", "-j", "ACCEPT")
	add("", false, forwardChain, "-o", bridge, "-m", "conntrack", "--ctstate", "DNAT", "-j", "ACCEPT")
	add("", false, forwardChain, "-i", wg, "-o", bridge, "-j", "ACCEPT")
	add("", false, forwardChain, "-o", bridge, "-j", "DROP")
	add("", false, forwardChain, "-i", bridge, "-m", "conntrack", "--ctstate", "RELATED,ESTABLISHED", "-j", "ACCEPT")
	add("", false, forwardChain, "-i", bridge, "-o", wg, "-j", "ACCEPT")
	add("", false, forwardChain, "-i", bridge, "-o", anyBridge, "-m", "conntrack", "!", "--ctstate", "DNAT", "-j", "DROP")
	add("", false, forwardChain, "-i", bridge, "-o", anyWireGuard, "-j", "DROP")
	add("", false, forwardChain, "-i", bridge, "-j", "ACCEPT")

	if cidr != "" && dnsAddress != "" {
		for _, protocol := range []string{"udp", "tcp"} {
			add("", true, inputChain, "-i", bridge, "-s", cidr, "-d", dnsAddress, "-p", protocol, "--dport", "53", "-j", "ACCEPT")
		}
	}
	if cidr != "" && apiPort > 0 {
		add("", true, inputChain, "-i", bridge, "-s", cidr, "-d", gateway, "-p", "tcp", "--dport", fmt.Sprint(apiPort), "-j", "ACCEPT")
	}
	add("", true, inputChain, "-i", bridge, "-m", "conntrack", "--ctstate", "RELATED,ESTABLISHED", "-j", "ACCEPT")
	add("", false, inputChain, "-i", bridge, "-j", "DROP")

	if cidr != "" {
		add("nat", false, postroutingChain, "-s", cidr, "-o", wg, "-j", "RETURN")
		add("nat", false, postroutingChain, "-s", cidr, "-o", bridge, "-m", "conntrack", "--ctstate", "DNAT", "-j", "MASQUERADE")
		add("nat", false, postroutingChain, "-s", cidr, "-o", bridge, "-j", "RETURN")
		add("nat", false, postroutingChain, "-s", cidr, "-j", "MASQUERADE")
	}
	return rules
}

func (m *WireGuardManager) reconcileFirewall(ctx context.Context, bridge, wg, cidr, gateway string, apiPort int) error {
	prefix, err := netip.ParsePrefix(cidr)
	if err != nil || !prefix.Addr().Is4() {
		return fmt.Errorf("namespace firewall CIDR must be IPv4: %q", cidr)
	}
	prefix = prefix.Masked()
	gatewayAddress, err := netip.ParseAddr(gateway)
	if err != nil || !prefix.Contains(gatewayAddress) {
		return fmt.Errorf("namespace firewall gateway %q must be within %s", gateway, prefix)
	}
	for _, jump := range chainJumps {
		if err := m.ensureJumpChain(ctx, jump); err != nil {
			return err
		}
	}
	for _, rule := range namespaceRules(bridge, wg, prefix.String(), gateway, m.dnsAddress, apiPort) {
		flag := "-A"
		if rule.insert {
			flag = "-I"
		}
		if err := m.ensureRule(ctx, rule.table, flag, rule.args...); err != nil {
			return err
		}
	}
	return nil
}

// iptables runs iptables against table, where "" is the filter table.
func (m *WireGuardManager) iptables(ctx context.Context, table string, args ...string) error {
	if table != "" {
		args = append([]string{"-t", table}, args...)
	}
	return m.run.Run(ctx, "iptables", args...)
}

// ensureRule adds rule with the given insertion flag (-A or -I) when an
// identical rule is not already present.
func (m *WireGuardManager) ensureRule(ctx context.Context, table, flag string, rule ...string) error {
	if m.iptables(ctx, table, append([]string{"-C"}, rule...)...) == nil {
		return nil
	}
	return m.iptables(ctx, table, append([]string{flag}, rule...)...)
}

// ensureChain creates a Trellis-owned chain unless it already exists.
func (m *WireGuardManager) ensureChain(ctx context.Context, table, chain string) error {
	if m.iptables(ctx, table, "-L", chain, "-n") == nil {
		return nil
	}
	if err := m.iptables(ctx, table, "-N", chain); err != nil {
		return fmt.Errorf("create Trellis chain %s: %w", chain, err)
	}
	return nil
}

// ensureJumpChain creates a Trellis-owned chain and makes its jump the first
// rule of a built-in chain, so host rules that accept traffic earlier cannot
// bypass Trellis isolation.
func (m *WireGuardManager) ensureJumpChain(ctx context.Context, jump chainJump) error {
	if err := m.ensureChain(ctx, jump.table, jump.chain); err != nil {
		return err
	}
	rule := append(append([]string{jump.parent}, jump.match...), "-j", jump.chain)
	if m.iptables(ctx, jump.table, append([]string{"-C"}, rule...)...) == nil {
		if err := m.iptables(ctx, jump.table, append([]string{"-D"}, rule...)...); err != nil {
			return fmt.Errorf("reposition Trellis %s chain: %w", jump.parent, err)
		}
	}
	insert := append(append([]string{"-I", jump.parent, "1"}, jump.match...), "-j", jump.chain)
	if err := m.iptables(ctx, jump.table, insert...); err != nil {
		return fmt.Errorf("install Trellis %s chain: %w", jump.parent, err)
	}
	return nil
}

// removeChains removes every built-in chain's jump to a Trellis-owned chain
// and then the chains themselves; every step tolerates prior removal. It runs
// only after the last namespace path on the node is gone, so each chain is
// flushed first in case a rule from an earlier plan (such as a changed API
// port) was left behind.
func (m *WireGuardManager) removeChains(ctx context.Context) error {
	for _, jump := range chainJumps {
		rule := append(append([]string{jump.parent}, jump.match...), "-j", jump.chain)
		if err := m.deleteRule(ctx, jump.table, rule...); err != nil {
			return err
		}
	}
	removed := make(map[string]bool)
	for _, jump := range chainJumps {
		if removed[jump.table+"/"+jump.chain] {
			continue
		}
		removed[jump.table+"/"+jump.chain] = true
		if err := m.removeChain(ctx, jump.table, jump.chain); err != nil {
			return err
		}
	}
	return nil
}

const noChain = "No chain/target/match by that name"

// removeChain flushes and deletes a Trellis-owned chain that nothing jumps to
// any longer, tolerating a chain that is already gone.
func (m *WireGuardManager) removeChain(ctx context.Context, table, chain string) error {
	if err := m.iptables(ctx, table, "-F", chain); err != nil {
		inspectErr := m.iptables(ctx, table, "-L", chain, "-n")
		absent := explicitAbsence(err, noChain) || explicitAbsence(inspectErr, noChain)
		if ctx.Err() != nil || !absent {
			return fmt.Errorf("flush Trellis chain %s: %w", chain, err)
		}
	}
	if err := m.iptables(ctx, table, "-X", chain); err != nil {
		inspectErr := m.iptables(ctx, table, "-L", chain, "-n")
		absent := explicitAbsence(err, noChain) || explicitAbsence(inspectErr, noChain)
		if ctx.Err() != nil || !absent {
			if inspectErr != nil {
				return fmt.Errorf("delete Trellis chain %s: %w (verify absence: %v)", chain, err, inspectErr)
			}
			return fmt.Errorf("delete Trellis chain %s: %w", chain, err)
		}
	}
	return nil
}

// deleteRule deletes a rule, tolerating a rule or chain that is already gone.
func (m *WireGuardManager) deleteRule(ctx context.Context, table string, args ...string) error {
	if err := m.iptables(ctx, table, append([]string{"-D"}, args...)...); err != nil {
		inspectErr := m.iptables(ctx, table, append([]string{"-C"}, args...)...)
		var exitErr *exec.ExitError
		absent := explicitAbsence(err, "Bad rule", "does a matching rule exist", noChain) ||
			explicitAbsence(inspectErr, "Bad rule", "does a matching rule exist", noChain) ||
			errors.As(inspectErr, &exitErr) && exitErr.ExitCode() == 1
		if ctx.Err() == nil && absent {
			return nil
		}
		if inspectErr != nil {
			return fmt.Errorf("delete firewall rule %s: %w (verify absence: %v)", strings.Join(args, " "), err, inspectErr)
		}
		return fmt.Errorf("delete firewall rule %s: %w", strings.Join(args, " "), err)
	}
	return nil
}

// allocationPortsChain names the NAT chain holding one allocation's published
// ports. It is derived from the allocation ID alone, so a detach can remove
// it without knowing the allocation's address.
func allocationPortsChain(allocationID string) string {
	return short("TRELLIS-P-", allocationID)
}

// publishedPortProtocols are the protocols each published port forwards;
// a port declares no protocol, so both are published.
var publishedPortProtocols = []string{"tcp", "udp"}

// publishPorts forwards each mapping's node port to the allocation address.
// The destination is rewritten but not the source, so the task sees the
// client's address. The allocation's chain is rebuilt from scratch, so a
// retry after a partial publish converges.
func (m *WireGuardManager) publishPorts(ctx context.Context, allocationID, address string, ports []PortMapping) error {
	if len(ports) == 0 {
		return nil
	}
	prefix, err := netip.ParsePrefix(address)
	if err != nil || !prefix.Addr().Is4() {
		return fmt.Errorf("published port destination must be IPv4: %q", address)
	}
	chain := allocationPortsChain(allocationID)
	if err := m.ensureChain(ctx, "nat", chain); err != nil {
		return err
	}
	if err := m.iptables(ctx, "nat", "-F", chain); err != nil {
		return fmt.Errorf("reset published ports: %w", err)
	}
	for _, port := range ports {
		destination := fmt.Sprintf("%s:%d", prefix.Addr(), port.ContainerPort)
		for _, protocol := range publishedPortProtocols {
			if err := m.iptables(ctx, "nat", "-A", chain, "-p", protocol, "--dport", fmt.Sprint(port.HostPort), "-j", "DNAT", "--to-destination", destination); err != nil {
				return fmt.Errorf("publish port %d: %w", port.HostPort, err)
			}
		}
	}
	return m.ensureRule(ctx, "nat", "-A", portsChain, "-j", chain)
}

// unpublishPorts removes an allocation's published ports, tolerating any of
// them already being gone.
func (m *WireGuardManager) unpublishPorts(ctx context.Context, allocationID string) error {
	chain := allocationPortsChain(allocationID)
	if err := m.deleteRule(ctx, "nat", portsChain, "-j", chain); err != nil {
		return err
	}
	return m.removeChain(ctx, "nat", chain)
}

func validPortMappings(ports []PortMapping) error {
	for _, port := range ports {
		if port.HostPort < 1 || port.HostPort > 65535 || port.ContainerPort < 1 || port.ContainerPort > 65535 {
			return fmt.Errorf("published port %d->%d is out of range", port.HostPort, port.ContainerPort)
		}
	}
	return nil
}
