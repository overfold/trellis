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

type crashRunner struct {
	commandRunner
	match func(string) bool
}

func (r crashRunner) Run(ctx context.Context, name string, args ...string) error {
	if err := r.commandRunner.Run(ctx, name, args...); err != nil {
		return err
	}
	if r.match(name + " " + strings.Join(args, " ")) {
		panic("simulated process death")
	}
	return nil
}

func expectNetworkCrash(t *testing.T, attach func()) {
	t.Helper()
	defer func() {
		if value := recover(); value != "simulated process death" {
			t.Fatalf("expected simulated process death, got %v", value)
		}
	}()
	attach()
}

func restartedManager(m *WireGuardManager, run commandRunner) *WireGuardManager {
	return &WireGuardManager{stateDir: m.stateDir, netnsDir: m.netnsDir, run: run, dnsAddress: WorkloadDNSAddress}
}

func TestAttachmentCrashWindowsConvergeAfterRestart(t *testing.T) {
	for _, step := range []string{"bridge", "wireguard", "netns", "veth", "published-netns", "ports"} {
		t.Run(step, func(t *testing.T) {
			m, _ := newRecoveryTestManager(t)
			model := newIPTablesModel()
			m.run = crashRunner{commandRunner: model, match: func(command string) bool {
				switch step {
				case "bridge":
					return strings.HasPrefix(command, "ip link add tb")
				case "wireguard":
					return strings.HasPrefix(command, "ip link add tw")
				case "netns":
					return strings.HasPrefix(command, "unshare --net")
				case "veth":
					return strings.HasPrefix(command, "ip link add vh")
				case "published-netns":
					return strings.Contains(command, "netns crashed")
				case "ports":
					return strings.Contains(command, "--to-destination") && strings.Contains(command, " -A ")
				default:
					return false
				}
			}}
			expectNetworkCrash(t, func() {
				_, _ = m.Attach(t.Context(), AttachRequest{Namespace: "plans", Network: "plans", AllocationID: "crashed", Plan: recoveryTestPlan(), Ports: []PortMapping{{HostPort: 18080, ContainerPort: 8126}}})
			})
			fresh := restartedManager(m, model)
			if err := fresh.CleanupAttachments(t.Context(), WorkloadDNSAddress); err != nil {
				t.Fatal(err)
			}
			assertAttachments(t, fresh)
			for _, path := range []string{m.netnsPath("crashed"), m.netnsStage("crashed"), filepath.Join(m.stateDir, leaseStorageDir, "plans")} {
				if _, err := os.Lstat(path); !os.IsNotExist(err) {
					t.Fatalf("crash cleanup retained %s: %v", path, err)
				}
			}
			if _, err := fresh.Attach(t.Context(), AttachRequest{Namespace: "plans", Network: "plans", AllocationID: "crashed", Plan: recoveryTestPlan()}); err != nil {
				t.Fatalf("resource reuse after crash cleanup: %v", err)
			}
		})
	}
}

func TestAttachmentJournalFailuresNeverPublishUnrecordedNetns(t *testing.T) {
	for _, stage := range []string{"intent", "address", "netns", "ports"} {
		for _, installed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/rename=%v", stage, installed), func(t *testing.T) {
				m, _ := newRecoveryTestManager(t)
				failed := false
				m.journalWrite = func(path string, raw []byte) error {
					var record attachmentRecord
					if err := json.Unmarshal(raw, &record); err != nil {
						return err
					}
					selected := stage == "intent" && record.Address == "" || stage == "address" && record.Address != "" && !record.NetnsCreated || stage == "netns" && record.NetnsCreated && !record.PortsReady || stage == "ports" && record.PortsReady
					if !failed && selected {
						failed = true
						if installed {
							if err := writeAtomicFile(path, raw); err != nil {
								return err
							}
						}
						return errors.New("injected journal failure")
					}
					if failed {
						return errors.New("journal storage remains unavailable")
					}
					return writeAtomicFile(path, raw)
				}
				if _, err := m.Attach(t.Context(), AttachRequest{Namespace: "plans", Network: "plans", AllocationID: "failed", Plan: recoveryTestPlan()}); err == nil || !failed {
					t.Fatalf("journal failure not returned: %v", err)
				}
				fresh := restartedManager(m, &recordingRunner{})
				if err := fresh.CleanupAttachments(t.Context(), WorkloadDNSAddress); err != nil {
					t.Fatal(err)
				}
				assertAttachments(t, fresh)
				if _, err := os.Lstat(m.netnsPath("failed")); !os.IsNotExist(err) {
					t.Fatalf("failed journal left a public netns: %v", err)
				}
			})
		}
	}
}

func TestNetnsPublicationRefusesRacingForeignName(t *testing.T) {
	m, _ := newRecoveryTestManager(t)
	foreign := m.netnsPath("raced")
	injected := false
	m.journalWrite = func(path string, raw []byte) error {
		if err := writeAtomicFile(path, raw); err != nil {
			return err
		}
		var record attachmentRecord
		if err := json.Unmarshal(raw, &record); err != nil {
			return err
		}
		if record.NetnsCreated && !injected {
			injected = true
			return os.WriteFile(foreign, []byte("foreign"), 0o600)
		}
		return nil
	}
	if _, err := m.Attach(t.Context(), AttachRequest{Namespace: "plans", Network: "plans", AllocationID: "raced", Plan: recoveryTestPlan()}); err == nil || !strings.Contains(err.Error(), "refuse unowned") {
		t.Fatalf("publication race did not fail closed: %v", err)
	}
	if raw, err := os.ReadFile(foreign); err != nil || string(raw) != "foreign" {
		t.Fatalf("publication touched foreign namespace: %s %v", raw, err)
	}
	fresh := restartedManager(m, &recordingRunner{})
	assertAttachments(t, fresh, "raced")
	if err := os.Remove(foreign); err != nil {
		t.Fatal(err)
	}
	if err := fresh.CleanupAttachments(t.Context(), WorkloadDNSAddress); err != nil {
		t.Fatal(err)
	}
	assertAttachments(t, fresh)
	if err := os.Mkdir(m.netnsStage("orphan"), 0o700); err != nil {
		t.Fatal(err)
	}
	foreign = filepath.Join(m.netnsStage("orphan"), "namespace")
	if err := os.WriteFile(foreign, []byte("foreign"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := fresh.Attach(t.Context(), AttachRequest{Namespace: "plans", Network: "plans", AllocationID: "orphan", Plan: recoveryTestPlan()}); err == nil {
		t.Fatal("adopted an unjournaled private namespace stage")
	}
	if raw, err := os.ReadFile(foreign); err != nil || string(raw) != "foreign" {
		t.Fatalf("removed foreign staging state: %s %v", raw, err)
	}
	assertAttachments(t, fresh)
}

func TestLegacyPlansLeaseMigrationIsRetryableAndPreservesPeerPlans(t *testing.T) {
	for _, duplicate := range []string{"none", "interrupted", "conflict", "symlink"} {
		t.Run(duplicate, func(t *testing.T) {
			m, _ := newRecoveryTestManager(t)
			legacy := filepath.Join(m.stateDir, "plans")
			if err := os.MkdirAll(legacy, 0o700); err != nil {
				t.Fatal(err)
			}
			plan := m.planPath("other", "other")
			if err := os.WriteFile(plan, []byte("[]"), 0o600); err != nil {
				t.Fatal(err)
			}
			source := filepath.Join(legacy, "10.42.1.9_24")
			if err := os.WriteFile(source, []byte("legacy"), 0o600); err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(m.stateDir, leaseStorageDir, "plans", filepath.Base(source))
			if duplicate != "none" {
				if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
					t.Fatal(err)
				}
				if duplicate == "interrupted" {
					if err := os.Link(source, target); err != nil {
						t.Fatal(err)
					}
				} else if duplicate == "symlink" {
					if err := os.Symlink(source, target); err != nil {
						t.Fatal(err)
					}
				} else if err := os.WriteFile(target, []byte("foreign"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			_, err := m.leaseDirectory("plans")
			if duplicate == "conflict" || duplicate == "symlink" {
				if err == nil {
					t.Fatal("overwrote conflicting migration state")
				}
				if raw, err := os.ReadFile(source); err != nil || string(raw) != "legacy" {
					t.Fatalf("conflict lost source: %s %v", raw, err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if _, err := m.leaseDirectory("plans"); err != nil {
					t.Fatal(err)
				}
				if raw, err := os.ReadFile(target); err != nil || string(raw) != "legacy" {
					t.Fatalf("migration lost owner: %s %v", raw, err)
				}
				if err := m.validateAttachmentCIDR("other", "other", "10.42.1.0/24"); err == nil {
					t.Fatal("orphan plans lease did not reserve its subnet")
				}
			}
			if raw, err := os.ReadFile(plan); err != nil || string(raw) != "[]" {
				t.Fatalf("migration modified peer plan: %s %v", raw, err)
			}
		})
	}
}

func TestTopologyRepairRestoresPublishedPortsAfterRestart(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy=%v", legacy), func(t *testing.T) {
			m, _ := newRecoveryTestManager(t)
			model := newIPTablesModel()
			m.run = model
			request := AttachRequest{Namespace: "plans", Network: "plans", AllocationID: "web", Plan: recoveryTestPlan(), Ports: []PortMapping{{HostPort: 18080, ContainerPort: 8126}, {HostPort: 18443, ContainerPort: 9000}}}
			// Occupy the hash-selected IP so legacy recovery must use the lease,
			// not recompute the allocation hash.
			dir := filepath.Join(m.stateDir, leaseStorageDir, "plans")
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			first, _, err := reserveAddress(dir, request.Plan.CIDR, "web")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, strings.ReplaceAll(first, "/", "_")), []byte("blocker"), 0o600); err != nil {
				t.Fatal(err)
			}
			attachment, err := m.Attach(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			if attachment.Address == first {
				t.Fatal("fixture did not force address probing")
			}
			if legacy {
				record, err := m.readAttachmentRecord("web")
				if err != nil {
					t.Fatal(err)
				}
				record.Version, record.Address, record.PortsReady = 2, "", false
				if err := m.saveAttachment(record); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(m.netnsPath("web")); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(m.netnsPath("web"), nil, 0o600); err != nil {
					t.Fatal(err)
				}
				recordTestNetnsInode(t, m, "web")
				entries, err := os.ReadDir(dir)
				if err != nil {
					t.Fatal(err)
				}
				for _, entry := range entries {
					if err := os.Rename(filepath.Join(dir, entry.Name()), filepath.Join(m.stateDir, "plans", entry.Name())); err != nil {
						t.Fatal(err)
					}
				}
			}
			// Lose all tables/chains, not just the per-allocation rules.
			model = newIPTablesModel()
			fresh := restartedManager(m, model)
			for range 2 {
				if err := fresh.UpdatePlan(t.Context(), "plans", request.Plan); err != nil {
					t.Fatal(err)
				}
			}
			ip, _, _ := strings.Cut(attachment.Address, "/")
			for _, parent := range []string{"nat/PREROUTING", "nat/OUTPUT"} {
				for _, proto := range []string{"tcp", "udp"} {
					for _, port := range request.Ports {
						want := fmt.Sprintf("DNAT --to-destination %s:%d", ip, port.ContainerPort)
						if got := model.verdict(t, parent, packet{proto: proto, dport: port.HostPort}); got != want {
							t.Fatalf("%s %s/%d = %q, want %q", parent, proto, port.HostPort, got, want)
						}
					}
				}
			}
			if len(model.chains["nat/"+allocationPortsChain("web")]) != 4 || len(model.chains["nat/"+portsChain]) != 1 {
				t.Fatal("repair duplicated rules/jumps")
			}
			// A failed stop is still stopping after restart, not a port publisher.
			fresh.run = &failingModel{iptablesModel: model, fail: "ip link del " + attachment.HostVeth}
			if err := fresh.DetachAllocation(t.Context(), "web"); err == nil {
				t.Fatal("fixture did not interrupt detach")
			}
			fresh = restartedManager(fresh, model)
			if err := fresh.UpdatePlan(t.Context(), "plans", request.Plan); err != nil {
				t.Fatal(err)
			}
			if got := model.verdict(t, "nat/PREROUTING", packet{proto: "tcp", dport: 18080}); got != "" {
				t.Fatalf("repair republished a stopping attachment: %s", got)
			}
		})
	}
}
