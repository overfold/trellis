package network

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// iptablesModel is an ordered, in-memory model of the iptables filter and
// nat tables. It supports the subset of commands the manager uses so tests can
// assert chain placement and rule order rather than only the issued commands.
// Filter chains are keyed by name and nat chains by "nat/" and their name.
type iptablesModel struct {
	chains   map[string][]string
	commands []string
	outputs  int
}

func newIPTablesModel() *iptablesModel {
	return &iptablesModel{chains: map[string][]string{"INPUT": nil, "FORWARD": nil, "nat/PREROUTING": nil, "nat/OUTPUT": nil, "nat/POSTROUTING": nil}}
}

// Output supports "iptables [-t TABLE] -S CHAIN N", printing the rule the way
// iptables does.
func (m *iptablesModel) Output(_ context.Context, name string, args ...string) (string, error) {
	m.outputs++
	table := ""
	if len(args) >= 2 && args[0] == "-t" {
		table, args = args[1]+"/", args[2:]
	}
	if name != "iptables" || len(args) != 3 || args[0] != "-S" {
		return "", fmt.Errorf("unsupported output command %s %v", name, args)
	}
	rules, exists := m.chains[table+args[1]]
	if !exists {
		return "", errors.New("iptables: No chain/target/match by that name")
	}
	index, err := strconv.Atoi(args[2])
	if err != nil || index < 1 || index > len(rules) {
		return "", errors.New("iptables: Index of deletion too big")
	}
	return "-A " + args[1] + " " + rules[index-1] + "\n", nil
}

func (m *iptablesModel) Run(_ context.Context, name string, args ...string) error {
	m.commands = append(m.commands, name+" "+strings.Join(args, " "))
	if name != "iptables" {
		return nil
	}
	table := ""
	if len(args) >= 2 && args[0] == "-t" {
		table, args = args[1]+"/", args[2:]
	}
	if len(args) < 2 {
		return fmt.Errorf("unsupported iptables command %v", args)
	}
	flag, chain := args[0], table+args[1]
	rules, exists := m.chains[chain]
	if !exists && flag != "-N" {
		return errors.New("iptables: No chain/target/match by that name")
	}
	rule := strings.Join(args[2:], " ")
	// Like iptables, refuse a rule that jumps to a Trellis chain that does
	// not exist, even when only checking or deleting it.
	if _, target, ok := strings.Cut(" "+rule, " -j "); ok && strings.HasPrefix(target, "TRELLIS") {
		name, _, _ := strings.Cut(target, " ")
		if _, targetExists := m.chains[table+name]; !targetExists {
			return fmt.Errorf("iptables v1.8.9 (nf_tables): Couldn't load target `%s':No such file or directory", name)
		}
	}
	switch flag {
	case "-N":
		if exists {
			return errors.New("iptables: chain already exists")
		}
		m.chains[chain] = nil
	case "-L":
	case "-C":
		if !slices.Contains(rules, rule) {
			return errors.New("iptables: Bad rule (does a matching rule exist in that chain?)")
		}
	case "-A":
		m.chains[chain] = append(rules, rule)
	case "-I":
		position := 1
		if len(args) > 2 {
			if parsed, err := strconv.Atoi(args[2]); err == nil {
				position = parsed
				rule = strings.Join(args[3:], " ")
			}
		}
		m.chains[chain] = slices.Insert(rules, position-1, rule)
	case "-D":
		index := slices.Index(rules, rule)
		if index < 0 {
			return errors.New("iptables: Bad rule (does a matching rule exist in that chain?)")
		}
		m.chains[chain] = slices.Delete(rules, index, index+1)
	case "-F":
		m.chains[chain] = nil
	case "-X":
		if len(rules) != 0 {
			return errors.New("iptables: directory not empty")
		}
		for _, other := range m.chains {
			for _, rule := range other {
				if rule == "-j "+args[1] || strings.HasSuffix(rule, " -j "+args[1]) {
					return errors.New("iptables: too many links")
				}
			}
		}
		delete(m.chains, chain)
	default:
		return fmt.Errorf("unsupported iptables flag %s", flag)
	}
	return nil
}

func TestNamespaceFirewallFiltersHostTrafficBeforeHostAcceptRules(t *testing.T) {
	model := newIPTablesModel()
	// A host firewall such as ufw or firewalld already accepts traffic.
	model.chains["INPUT"] = []string{"-j ACCEPT"}
	model.chains["FORWARD"] = []string{"-j ACCEPT"}
	manager := NewWireGuardManager(t.TempDir())
	manager.run = model
	manager.dnsAddress = WorkloadDNSAddress

	for range 2 {
		if err := manager.reconcileFirewall(context.Background(), "tb-acme", "tw-acme", "10.42.1.0/24", "10.42.1.1", 8128); err != nil {
			t.Fatal(err)
		}
		if err := manager.reconcileFirewall(context.Background(), "tb-other", "tw-other", "10.42.2.0/24", "10.42.2.1", 8128); err != nil {
			t.Fatal(err)
		}
	}

	if want := []string{"-j TRELLIS-INPUT", "-j ACCEPT"}; !slices.Equal(model.chains["INPUT"], want) {
		t.Fatalf("INPUT = %q, want %q", model.chains["INPUT"], want)
	}
	if want := []string{"-j TRELLIS-FORWARD", "-j ACCEPT"}; !slices.Equal(model.chains["FORWARD"], want) {
		t.Fatalf("FORWARD = %q, want %q", model.chains["FORWARD"], want)
	}
	input := model.chains[inputChain]
	if len(input) != 14 {
		t.Fatalf("%s has %d rules after repeated reconciliation, want 14:\n%s", inputChain, len(input), strings.Join(input, "\n"))
	}
	for _, bridge := range []string{"tb-acme", "tb-other"} {
		drop := slices.Index(input, "-i "+bridge+" -j DROP")
		if drop < 0 {
			t.Fatalf("%s has no DROP for %s:\n%s", inputChain, bridge, strings.Join(input, "\n"))
		}
		accepts := 0
		for index, rule := range input {
			if strings.HasPrefix(rule, "-i "+bridge+" ") && strings.HasSuffix(rule, "-j ACCEPT") {
				accepts++
				if index > drop {
					t.Fatalf("%s ACCEPT %q follows its DROP:\n%s", bridge, rule, strings.Join(input, "\n"))
				}
			}
		}
		if accepts != 4 {
			t.Fatalf("%s has %d ACCEPT rules for %s, want DNS udp/tcp, API, and replies:\n%s", inputChain, accepts, bridge, strings.Join(input, "\n"))
		}
	}
}

func TestNamespaceFirewallRepositionsChainsAfterHostInsertsRules(t *testing.T) {
	model := newIPTablesModel()
	manager := NewWireGuardManager(t.TempDir())
	manager.run = model
	if err := manager.reconcileFirewall(context.Background(), "tb-acme", "tw-acme", "10.42.1.0/24", "10.42.1.1", 0); err != nil {
		t.Fatal(err)
	}
	// A host firewall reload inserts its own rules ahead of Trellis.
	model.chains["INPUT"] = slices.Insert(model.chains["INPUT"], 0, "-j ACCEPT")

	if err := manager.reconcileFirewall(context.Background(), "tb-acme", "tw-acme", "10.42.1.0/24", "10.42.1.1", 0); err != nil {
		t.Fatal(err)
	}
	if want := []string{"-j TRELLIS-INPUT", "-j ACCEPT"}; !slices.Equal(model.chains["INPUT"], want) {
		t.Fatalf("INPUT = %q, want %q", model.chains["INPUT"], want)
	}
}

func TestNamespacePathRemovalDeletesTrellisChainsAfterLastPath(t *testing.T) {
	model := newIPTablesModel()
	model.chains["INPUT"] = []string{"-j ACCEPT"}
	manager, err := NewAutomatedWireGuardManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager.run = model
	manager.dnsAddress = WorkloadDNSAddress
	first, err := manager.Attach(context.Background(), AttachRequest{
		Namespace: "acme", Network: "acme", AllocationID: "alloc-acme",
		Plan: Plan{CIDR: "10.42.1.0/24", Gateway: "10.42.1.1", WireGuardAddress: "169.254.0.1/32", ListenPort: 51917, APIPort: 8126},
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := manager.Attach(context.Background(), AttachRequest{
		Namespace: "other", Network: "other", AllocationID: "alloc-other",
		Plan: Plan{CIDR: "10.42.2.0/24", Gateway: "10.42.2.1", WireGuardAddress: "169.254.0.2/32", ListenPort: 51918, APIPort: 8126},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := manager.Detach(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	for _, rule := range model.chains[inputChain] {
		if strings.Contains(rule, first.Bridge) {
			t.Fatalf("%s kept rule for detached bridge: %q", inputChain, rule)
		}
	}
	if len(model.chains[inputChain]) != 7 || model.chains["INPUT"][0] != "-j "+inputChain {
		t.Fatalf("remaining namespace lost its input filtering: INPUT=%q %s=%q", model.chains["INPUT"], inputChain, model.chains[inputChain])
	}

	if err := manager.Detach(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	if _, exists := model.chains[inputChain]; exists {
		t.Fatalf("%s survived the last namespace path", inputChain)
	}
	if _, exists := model.chains[forwardChain]; exists {
		t.Fatalf("%s survived the last namespace path", forwardChain)
	}
	if want := []string{"-j ACCEPT"}; !slices.Equal(model.chains["INPUT"], want) {
		t.Fatalf("INPUT = %q, want host rules only %q", model.chains["INPUT"], want)
	}
}
