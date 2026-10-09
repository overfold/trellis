package network

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
)

// absentRunner fails commands matching any fail substring, as ip does for a
// link or network namespace that does not exist.
type absentRunner struct {
	recordingRunner
	fail []string
	hook func(command string)
}

func (r *absentRunner) Run(_ context.Context, name string, args ...string) error {
	command := name + " " + strings.Join(args, " ")
	r.commands = append(r.commands, command)
	if r.hook != nil {
		r.hook(command)
	}
	for _, fail := range r.fail {
		if strings.Contains(command, fail) {
			if strings.Contains(command, "ip link show") {
				return errors.New("device does not exist")
			}
			if strings.Contains(command, "iptables -D") || strings.Contains(command, "iptables -C") {
				return errors.New("Bad rule")
			}
			return errors.New("command failed")
		}
	}
	return nil
}

func newRecoveryTestManager(t *testing.T) (*WireGuardManager, *absentRunner) {
	t.Helper()
	manager, err := NewAutomatedWireGuardManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	runner := &absentRunner{}
	manager.run = runner
	manager.netnsDir = t.TempDir()
	manager.dnsAddress = WorkloadDNSAddress
	return manager, runner
}

func recoveryTestPlan() Plan {
	return Plan{CIDR: "10.42.1.0/24", Gateway: "10.42.1.1", WireGuardAddress: "169.254.1.1/32", ListenPort: 51917, APIPort: 8126}
}

func recordTestNetnsInode(t *testing.T, manager *WireGuardManager, id string) {
	t.Helper()
	info, err := os.Stat(manager.netnsPath(id))
	if err != nil {
		t.Fatal(err)
	}
	record, err := manager.readAttachmentRecord(id)
	if err != nil {
		t.Fatal(err)
	}
	record.NetnsCreated, record.NetnsInode = true, info.Sys().(*syscall.Stat_t).Ino
	raw, _ := json.Marshal(record)
	if err := writeAtomicFile(manager.journalPath(id), raw); err != nil {
		t.Fatal(err)
	}
}

func attachForRecovery(t *testing.T, manager *WireGuardManager, allocation string) *Attachment {
	t.Helper()
	attachment, err := manager.Attach(context.Background(), AttachRequest{Namespace: "acme", Network: "acme", AllocationID: allocation, Plan: recoveryTestPlan()})
	if err != nil {
		t.Fatal(err)
	}
	return attachment
}

func assertAttachments(t *testing.T, manager *WireGuardManager, want ...string) {
	t.Helper()
	got, err := manager.Attachments(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("attachments = %v, want %v", got, want)
	}
}

type cancelledDetachRunner struct {
	recordingRunner
	entered chan struct{}
}

func (r *cancelledDetachRunner) Run(ctx context.Context, name string, args ...string) error {
	_ = r.recordingRunner.Run(ctx, name, args...)
	if len(r.commands) == 1 {
		close(r.entered)
		<-ctx.Done()
	}
	return ctx.Err()
}

func TestDetachAllocationCancelledCommandRetainsJournalAndLeaseForRetry(t *testing.T) {
	manager, runner := newRecoveryTestManager(t)
	attachment := attachForRecovery(t, manager, "alloc-cancel")
	blocked := &cancelledDetachRunner{entered: make(chan struct{})}
	manager.run = blocked
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { done <- manager.DetachAllocation(ctx, attachment.AllocationID) }()
	select {
	case <-blocked.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("detach did not enter external command")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("detach error = %v, want cancellation", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("detach command did not honor cancellation")
	}
	assertAttachments(t, manager, "alloc-cancel")
	leaseDir := filepath.Dir(attachment.LeasePath)
	if leases, err := os.ReadDir(leaseDir); err != nil || len(leases) == 0 {
		t.Fatalf("cancelled detach lost address reservation: %v, %v", leases, err)
	}
	manager.run = runner
	if err := manager.DetachAllocation(context.Background(), attachment.AllocationID); err != nil {
		t.Fatalf("retry detach: %v", err)
	}
	assertAttachments(t, manager)
	if _, err := os.Stat(leaseDir); !os.IsNotExist(err) {
		t.Fatalf("retry retained lease directory: %v", err)
	}
	if err := manager.DetachAllocation(context.Background(), attachment.AllocationID); err != nil {
		t.Fatalf("idempotent retry: %v", err)
	}
}

func TestAttachRecordsAttachmentBeforeCreatingResources(t *testing.T) {
	manager, runner := newRecoveryTestManager(t)
	recorded := false
	runner.hook = func(string) {
		if _, err := os.Stat(manager.journalPath("alloc-one")); err == nil {
			recorded = true
		} else if !recorded {
			t.Errorf("command ran before the attachment was recorded")
		}
	}
	attachForRecovery(t, manager, "alloc-one")
	assertAttachments(t, manager, "alloc-one")
}

func TestNamespaceForIPUsesJournaledNamespaceCIDR(t *testing.T) {
	manager, _ := newRecoveryTestManager(t)
	attachForRecovery(t, manager, "alloc-one")

	if namespace, ok := manager.NamespaceForIP(netip.MustParseAddr("10.42.1.23")); !ok || namespace != "acme" {
		t.Fatalf("NamespaceForIP(local) = %q, %v; want acme, true", namespace, ok)
	}
	if namespace, ok := manager.NamespaceForIP(netip.MustParseAddr("10.42.2.23")); ok || namespace != "" {
		t.Fatalf("NamespaceForIP(other) = %q, %v; want empty, false", namespace, ok)
	}
}

func TestAttachRejectsOverlappingNetworksUntilFinalDetach(t *testing.T) {
	for _, tc := range []struct{ namespace, network, cidr string }{
		{"other", "other", "10.42.1.9/24"},
		{"acme", "other", "10.42.1.128/25"},
		{"other", "acme", "10.42.0.0/23"},
	} {
		t.Run(tc.namespace+"/"+tc.network+"/"+tc.cidr, func(t *testing.T) {
			manager, runner := newRecoveryTestManager(t)
			first := attachForRecovery(t, manager, "alloc-one")
			second := attachForRecovery(t, manager, "alloc-two")
			plan := recoveryTestPlan()
			plan.CIDR = tc.cidr
			request := AttachRequest{Namespace: tc.namespace, Network: tc.network, AllocationID: "replacement", Plan: plan}
			before := len(runner.commands)
			if _, err := manager.Attach(context.Background(), request); err == nil || !strings.Contains(err.Error(), "overlaps") {
				t.Fatalf("overlapping attach error = %v", err)
			}
			if len(runner.commands) != before {
				t.Fatal("rejected attach ran network commands")
			}
			assertAttachments(t, manager, "alloc-one", "alloc-two")
			// Warm attribution, then remove only one of two owners.
			address := netip.MustParseAddr("10.42.1.23")
			if ns, ok := manager.NamespaceForIP(address); !ok || ns != "acme" {
				t.Fatalf("initial attribution = %q, %v", ns, ok)
			}
			if err := manager.Detach(context.Background(), first); err != nil {
				t.Fatal(err)
			}
			if ns, ok := manager.NamespaceForIP(address); !ok || ns != "acme" {
				t.Fatalf("remaining attachment attribution = %q, %v", ns, ok)
			}
			if _, err := manager.Attach(context.Background(), request); err == nil {
				t.Fatal("subnet reused before final detach")
			}
			if err := manager.Detach(context.Background(), second); err != nil {
				t.Fatal(err)
			}
			if _, ok := manager.NamespaceForIP(address); ok {
				t.Fatal("final detach left attribution cached")
			}
			// Use a valid gateway for the reclaimed CIDR.
			request.Plan = recoveryTestPlan()
			if _, err := manager.Attach(context.Background(), request); err != nil {
				t.Fatalf("attach after final detach: %v", err)
			}
			if ns, ok := manager.NamespaceForIP(address); !ok || ns != tc.namespace {
				t.Fatalf("reclaimed attribution = %q, %v", ns, ok)
			}
		})
	}
}

func TestAttachWithoutExistingStateDirectory(t *testing.T) {
	manager, _ := newRecoveryTestManager(t)
	manager.stateDir = filepath.Join(t.TempDir(), "network")
	attachForRecovery(t, manager, "first")
	assertAttachments(t, manager, "first")
}

func TestAttachRejectsOrphanedAddressLeases(t *testing.T) {
	manager, runner := newRecoveryTestManager(t)
	attachment := attachForRecovery(t, manager, "orphan")
	if err := os.Remove(manager.journalPath(attachment.AllocationID)); err != nil {
		t.Fatal(err)
	}
	before := len(runner.commands)
	_, err := manager.Attach(context.Background(), AttachRequest{Namespace: "other", Network: "other", AllocationID: "replacement", Plan: recoveryTestPlan()})
	if err == nil || !strings.Contains(err.Error(), "overlaps leases") || len(runner.commands) != before {
		t.Fatalf("orphan lease attach: error=%v commands=%d", err, len(runner.commands)-before)
	}
}

func TestFailedDetachKeepsSubnetReserved(t *testing.T) {
	manager, runner := newRecoveryTestManager(t)
	attachment := attachForRecovery(t, manager, "orphan")
	runner.fail = []string{"ip netns del orphan"}
	if err := os.WriteFile(manager.netnsPath("orphan"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := manager.Detach(context.Background(), attachment); err == nil {
		t.Fatal("expected detach failure")
	}
	before := len(runner.commands)
	if _, err := manager.Attach(context.Background(), AttachRequest{Namespace: "other", Network: "other", AllocationID: "replacement", Plan: recoveryTestPlan()}); err == nil || !strings.Contains(err.Error(), "overlaps") {
		t.Fatalf("attach after failed detach = %v", err)
	}
	if len(runner.commands) != before {
		t.Fatal("overlapping attach ran commands after failed detach")
	}
	assertAttachments(t, manager, "orphan")
}

func TestNamespaceForIPAmbiguityColdAndWarm(t *testing.T) {
	for _, warm := range []bool{false, true} {
		for _, cidr := range []string{"10.42.1.9/24", "10.42.1.128/25", "10.42.0.0/23"} {
			t.Run(fmt.Sprintf("warm=%v/%s", warm, cidr), func(t *testing.T) {
				manager, _ := newRecoveryTestManager(t)
				attachForRecovery(t, manager, "original")
				address := netip.MustParseAddr("10.42.1.229")
				if warm {
					if ns, ok := manager.NamespaceForIP(address); !ok || ns != "acme" {
						t.Fatalf("warm attribution = %q, %v", ns, ok)
					}
				}
				// Simulate conflicting state left by an older manager; Attach now
				// rejects it, but attribution must also handle existing journals.
				if err := manager.recordAttachment(attachmentRecord{AllocationID: "conflict", Namespace: "other", Network: "other", CIDR: cidr}); err != nil {
					t.Fatal(err)
				}
				for range 2 {
					if ns, ok := manager.NamespaceForIP(address); ok || ns != "" {
						t.Fatalf("ambiguous attribution = %q, %v", ns, ok)
					}
				}
				if err := manager.DetachAllocation(context.Background(), "conflict"); err != nil {
					t.Fatal(err)
				}
				if ns, ok := manager.NamespaceForIP(address); !ok || ns != "acme" {
					t.Fatalf("attribution after conflict detach = %q, %v", ns, ok)
				}
			})
		}
	}
}

func TestNamespaceForIPRecoversAfterJournalRepair(t *testing.T) {
	for _, fault := range []string{"corrupt", "conflicting", "unreadable-directory"} {
		t.Run(fault, func(t *testing.T) {
			manager, _ := newRecoveryTestManager(t)
			attachForRecovery(t, manager, "alloc-one")
			path := manager.journalPath("alloc-one")
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			switch fault {
			case "corrupt":
				if err := os.WriteFile(path, []byte("{"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "conflicting":
				if err := manager.recordAttachment(attachmentRecord{AllocationID: "alloc-two", Namespace: "other", Network: "other", CIDR: "10.42.1.9/24"}); err != nil {
					t.Fatal(err)
				}
			case "unreadable-directory":
				dir := filepath.Dir(path)
				if err := os.Rename(dir, dir+"-saved"); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(dir, nil, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			address := netip.MustParseAddr("10.42.1.23")
			if namespace, ok := manager.NamespaceForIP(address); ok || namespace != "" || manager.namespaceCIDRsLoaded {
				t.Fatalf("bad journal did not fail closed: %q, %v", namespace, ok)
			}
			// New attachments must not publish a partial cache while loading failed.
			if fault != "unreadable-directory" {
				err := manager.recordAttachment(attachmentRecord{AllocationID: "alloc-three", Namespace: "third", Network: "third", CIDR: "10.42.3.0/24"})
				if fault == "corrupt" && err == nil {
					t.Fatal("created ownership state alongside an unreadable journal")
				} else if fault != "corrupt" && err != nil {
					t.Fatal(err)
				}
				if _, ok := manager.NamespaceForIP(netip.MustParseAddr("10.42.3.9")); ok {
					t.Fatal("partial cache became visible")
				}
			}
			switch fault {
			case "corrupt":
				if err := os.WriteFile(path, raw, 0o600); err != nil {
					t.Fatal(err)
				}
			case "conflicting":
				if err := os.Remove(manager.journalPath("alloc-two")); err != nil {
					t.Fatal(err)
				}
			case "unreadable-directory":
				dir := filepath.Dir(path)
				if err := os.Remove(dir); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(dir+"-saved", dir); err != nil {
					t.Fatal(err)
				}
			}
			manager.namespaceCIDRsRetryAt = time.Now().Add(time.Hour)
			if _, ok := manager.NamespaceForIP(address); ok {
				t.Fatal("load retry ignored backoff")
			}
			manager.namespaceCIDRsRetryAt = time.Time{}
			if namespace, ok := manager.NamespaceForIP(address); !ok || namespace != "acme" {
				t.Fatalf("repaired journal did not recover: %q, %v", namespace, ok)
			}
		})
	}
}

func TestDetachAllocationRemovesLeftoverAttachmentByID(t *testing.T) {
	manager, runner := newRecoveryTestManager(t)
	// The agent stopped before it recorded the attachment Attach returned.
	attachment := attachForRecovery(t, manager, "alloc-one")

	runner.commands = nil
	if err := manager.DetachAllocation(context.Background(), "alloc-one"); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(runner.commands, "\n")
	for _, want := range []string{
		"ip link del " + attachment.HostVeth,
		"ip netns del alloc-one",
		"iptables -D TRELLIS-FORWARD -i " + attachment.Bridge + " ! -s 10.42.1.0/24 -j DROP",
		"iptables -D FORWARD -j TRELLIS-FORWARD",
		"iptables -X TRELLIS-FORWARD",
		"iptables -D TRELLIS-INPUT -i " + attachment.Bridge + " -s 10.42.1.0/24 -d 10.42.1.1 -p tcp --dport 8126 -j ACCEPT",
		"iptables -D TRELLIS-INPUT -i " + attachment.Bridge + " -s 10.42.1.0/24 -d " + WorkloadDNSAddress + " -p udp --dport 53 -j ACCEPT",
		"iptables -D TRELLIS-INPUT -i " + attachment.Bridge + " -j DROP",
		"iptables -D INPUT -j TRELLIS-INPUT",
		"iptables -X TRELLIS-INPUT",
		"ip link del " + attachment.WireGuardInterface,
		"ip link del " + attachment.Bridge,
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("detach by ID did not run %q:\n%s", want, joined)
		}
	}
	if _, err := os.Stat(attachment.LeasePath); !os.IsNotExist(err) {
		t.Fatalf("address lease survived detach by ID: %v", err)
	}
	if _, err := os.Stat(filepath.Join(manager.stateDir, "acme")); !os.IsNotExist(err) {
		t.Fatalf("lease directory survived the last detach: %v", err)
	}
	assertAttachments(t, manager)

	// Detaching again has nothing left to do.
	runner.commands = nil
	if err := manager.DetachAllocation(context.Background(), "alloc-one"); err != nil {
		t.Fatal(err)
	}
	if len(runner.commands) != 0 {
		t.Fatalf("repeated detach ran commands:\n%s", strings.Join(runner.commands, "\n"))
	}
}

func TestDetachAllocationKeepsOtherAllocationsAndNamespacePath(t *testing.T) {
	manager, runner := newRecoveryTestManager(t)
	first := attachForRecovery(t, manager, "alloc-one")
	second := attachForRecovery(t, manager, "alloc-two")

	runner.commands = nil
	if err := manager.DetachAllocation(context.Background(), "alloc-one"); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(runner.commands, "\n")
	if strings.Contains(joined, second.HostVeth) || strings.Contains(joined, "alloc-two") || strings.Contains(joined, "ip link del "+first.Bridge) {
		t.Fatalf("detach by ID touched another allocation or the shared path:\n%s", joined)
	}
	if _, err := os.Stat(first.LeasePath); !os.IsNotExist(err) {
		t.Fatalf("detached allocation's lease survived: %v", err)
	}
	if _, err := os.Stat(second.LeasePath); err != nil {
		t.Fatalf("other allocation's lease removed: %v", err)
	}
	assertAttachments(t, manager, "alloc-two")
}

func TestCleanupAttachmentsTearsDownAllJournaledResourcesAndIsIdempotent(t *testing.T) {
	manager, runner := newRecoveryTestManager(t)
	first := attachForRecovery(t, manager, "alloc-one")
	second := attachForRecovery(t, manager, "alloc-two")
	runner.commands = nil

	if err := manager.CleanupAttachments(context.Background(), WorkloadDNSAddress); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(runner.commands, "\n")
	for _, want := range []string{
		"ip link del " + first.HostVeth,
		"ip netns del alloc-one",
		"ip link del " + second.HostVeth,
		"ip netns del alloc-two",
		"iptables -D TRELLIS-INPUT -i " + first.Bridge + " -s 10.42.1.0/24 -d " + WorkloadDNSAddress + " -p udp --dport 53 -j ACCEPT",
		"ip link del " + first.WireGuardInterface,
		"ip link del " + first.Bridge,
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("cleanup did not run %q:\n%s", want, joined)
		}
	}
	assertAttachments(t, manager)

	runner.commands = nil
	if err := manager.CleanupAttachments(context.Background(), WorkloadDNSAddress); err != nil {
		t.Fatalf("repeated cleanup: %v", err)
	}
	if len(runner.commands) != 0 {
		t.Fatalf("repeated cleanup ran commands:\n%s", strings.Join(runner.commands, "\n"))
	}
}

func TestCleanupAttachmentsFailsClosedOnUnreadableJournal(t *testing.T) {
	manager, runner := newRecoveryTestManager(t)
	attachForRecovery(t, manager, "alloc-one")
	dir := filepath.Join(manager.stateDir, attachmentJournalDir)
	if err := os.WriteFile(filepath.Join(dir, "alloc-bad.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	runner.commands = nil

	if err := manager.CleanupAttachments(context.Background(), WorkloadDNSAddress); err == nil {
		t.Fatal("cleanup succeeded with an unreadable journal")
	}
	if len(runner.commands) != 0 {
		t.Fatalf("failed-closed cleanup ran commands:\n%s", strings.Join(runner.commands, "\n"))
	}
	if _, err := os.Stat(manager.journalPath("alloc-one")); err != nil {
		t.Fatalf("readable journal was removed: %v", err)
	}
}

func TestDetachAllocationSucceedsWhenResourcesAreAlreadyGone(t *testing.T) {
	manager, runner := newRecoveryTestManager(t)
	attachment := attachForRecovery(t, manager, "alloc-one")
	if err := os.Remove(attachment.LeasePath); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(manager.netnsPath("alloc-one")); err != nil {
		t.Fatal(err)
	}
	// ip fails for a link or namespace that does not exist, and no named
	// network namespace remains.
	runner.fail = []string{"ip link del", "ip link show", "ip netns del", "iptables -D"}

	if err := manager.DetachAllocation(context.Background(), "alloc-one"); err != nil {
		t.Fatalf("detach of absent resources: %v", err)
	}
	assertAttachments(t, manager)
}

func TestDetachAllocationKeepsRecordUntilRemovalSucceeds(t *testing.T) {
	manager, runner := newRecoveryTestManager(t)
	attachment := attachForRecovery(t, manager, "alloc-one")

	// The veth still exists but cannot be removed.
	runner.fail = []string{"ip link del " + attachment.HostVeth}
	if err := manager.DetachAllocation(context.Background(), "alloc-one"); err == nil {
		t.Fatal("detach reported success while the veth remained")
	}
	assertAttachments(t, manager, "alloc-one")

	// The network namespace still exists but cannot be removed.
	if err := os.WriteFile(manager.netnsPath("alloc-one"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	recordTestNetnsInode(t, manager, "alloc-one")
	runner.fail = []string{"ip netns del"}
	if err := manager.DetachAllocation(context.Background(), "alloc-one"); err == nil {
		t.Fatal("detach reported success while the network namespace remained")
	}
	assertAttachments(t, manager, "alloc-one")
	if _, err := os.Stat(attachment.LeasePath); err != nil {
		t.Fatalf("lease released before the namespace was removed: %v", err)
	}

	runner.fail = nil
	if err := manager.DetachAllocation(context.Background(), "alloc-one"); err != nil {
		t.Fatal(err)
	}
	assertAttachments(t, manager)
}

func TestDetachRemovesAttachmentRecord(t *testing.T) {
	manager, _ := newRecoveryTestManager(t)
	attachment := attachForRecovery(t, manager, "alloc-one")
	if err := manager.Detach(context.Background(), attachment); err != nil {
		t.Fatal(err)
	}
	assertAttachments(t, manager)
}

func TestAttachFailureRollsBackAndRemovesRecord(t *testing.T) {
	manager, runner := newRecoveryTestManager(t)
	other := attachForRecovery(t, manager, "alloc-other")
	runner.fail = []string{"ip -n alloc-one addr add"}
	_, err := manager.Attach(context.Background(), AttachRequest{Namespace: "acme", Network: "acme", AllocationID: "alloc-one", Plan: recoveryTestPlan()})
	if err == nil {
		t.Fatal("Attach reported success after address setup failed")
	}
	joined := strings.Join(runner.commands, "\n")
	for _, want := range []string{"ip link del " + short("vh", "alloc-one"), "ip netns del alloc-one"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("rollback did not run %q:\n%s", want, joined)
		}
	}
	entries, err := os.ReadDir(filepath.Dir(other.LeasePath))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || filepath.Join(filepath.Dir(other.LeasePath), entries[0].Name()) != other.LeasePath {
		t.Fatalf("leases after rollback = %v, want only %s", entries, other.LeasePath)
	}
	assertAttachments(t, manager, "alloc-other")
}

func TestAttachFailedRollbackKeepsRecordForLaterDetach(t *testing.T) {
	manager, runner := newRecoveryTestManager(t)
	runner.hook = func(command string) {
		if command == "ip netns add alloc-one" {
			if err := os.WriteFile(manager.netnsPath("alloc-one"), nil, 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	runner.fail = []string{"ip -n alloc-one addr add", "ip netns del"}
	if _, err := manager.Attach(context.Background(), AttachRequest{Namespace: "acme", Network: "acme", AllocationID: "alloc-one", Plan: recoveryTestPlan()}); err == nil {
		t.Fatal("Attach reported success after address setup failed")
	}
	assertAttachments(t, manager, "alloc-one")

	runner.fail = nil
	if err := manager.DetachAllocation(context.Background(), "alloc-one"); err != nil {
		t.Fatal(err)
	}
	assertAttachments(t, manager)
}

func TestAttachRefusesExistingAttachmentRecord(t *testing.T) {
	manager, runner := newRecoveryTestManager(t)
	first := attachForRecovery(t, manager, "alloc-one")
	runner.commands = nil
	if _, err := manager.Attach(context.Background(), AttachRequest{Namespace: "acme", Network: "acme", AllocationID: "alloc-one", Plan: recoveryTestPlan()}); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("second Attach error = %v, want existing attachment", err)
	}
	if len(runner.commands) != 0 {
		t.Fatalf("refused Attach ran commands:\n%s", strings.Join(runner.commands, "\n"))
	}
	if _, err := os.Stat(first.LeasePath); err != nil {
		t.Fatalf("refused Attach removed the existing lease: %v", err)
	}
	assertAttachments(t, manager, "alloc-one")
}

func TestAttachmentsReportsUnreadableRecordsWithReadableOnes(t *testing.T) {
	manager, _ := newRecoveryTestManager(t)
	attachForRecovery(t, manager, "alloc-one")
	dir := filepath.Join(manager.stateDir, attachmentJournalDir)
	if err := os.WriteFile(filepath.Join(dir, "alloc-bad.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".network-plan-123"), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	ids, err := manager.Attachments(context.Background())
	if err == nil || !strings.Contains(err.Error(), "alloc-bad") {
		t.Fatalf("attachments error = %v, want unreadable record reported", err)
	}
	if !slices.Equal(ids, []string{"alloc-one"}) {
		t.Fatalf("attachments = %v, want readable record", ids)
	}
	if err := manager.DetachAllocation(context.Background(), "alloc-bad"); err == nil {
		t.Fatal("detached an allocation from an unreadable record")
	}
}

func TestDetachAllocationRejectsUnsafeIdentifiers(t *testing.T) {
	manager, runner := newRecoveryTestManager(t)
	for _, id := range []string{"", "../escape", "/abs", ".hidden"} {
		if err := manager.DetachAllocation(context.Background(), id); err == nil {
			t.Fatalf("DetachAllocation(%q) succeeded", id)
		}
	}
	if len(runner.commands) != 0 {
		t.Fatalf("unsafe detach ran commands:\n%s", strings.Join(runner.commands, "\n"))
	}
}

// TestDetachAllocationRemovesRealNetworkNamespace exercises detach by ID
// against a real veth and named network namespace.
func TestDetachAllocationRemovesRealNetworkNamespace(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("network namespaces require root")
	}
	if _, err := exec.LookPath("ip"); err != nil {
		t.Skip("network namespaces require iproute2")
	}
	manager, err := NewAutomatedWireGuardManager(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	allocation := "trellis-detach-test"
	hostVeth, peerVeth := short("vh", allocation), short("vc", allocation)
	if err := manager.recordAttachment(attachmentRecord{AllocationID: allocation, Namespace: "acme", Network: "acme", Gateway: "10.42.1.1"}); err != nil {
		t.Fatal(err)
	}
	// Leave the attachment as a crash between Attach steps would.
	run := execRunner{}
	ctx := context.Background()
	if err := run.Run(ctx, "ip", "netns", "add", allocation); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = run.Run(ctx, "ip", "netns", "del", allocation) })
	recordTestNetnsInode(t, manager, allocation)
	if err := run.Run(ctx, "ip", "link", "add", hostVeth, "type", "veth", "peer", "name", peerVeth); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = run.Run(ctx, "ip", "link", "del", hostVeth) })
	if err := run.Run(ctx, "ip", "link", "set", "dev", hostVeth, "alias", allocationOwner(allocation)+":"+hostVeth); err != nil {
		t.Fatal(err)
	}
	if err := run.Run(ctx, "ip", "link", "set", peerVeth, "netns", allocation); err != nil {
		t.Fatal(err)
	}

	if err := manager.DetachAllocation(ctx, allocation); err != nil {
		t.Fatal(err)
	}
	if run.Run(ctx, "ip", "link", "show", "dev", hostVeth) == nil {
		t.Fatal("host veth survived detach by ID")
	}
	if _, err := os.Lstat(manager.netnsPath(allocation)); !os.IsNotExist(err) {
		t.Fatalf("network namespace survived detach by ID: %v", err)
	}
	if err := manager.DetachAllocation(ctx, allocation); err != nil {
		t.Fatalf("repeated detach: %v", err)
	}
}
