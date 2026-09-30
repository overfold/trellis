package network

import (
	"context"
	"errors"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
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

func TestDetachAllocationSucceedsWhenResourcesAreAlreadyGone(t *testing.T) {
	manager, runner := newRecoveryTestManager(t)
	attachment := attachForRecovery(t, manager, "alloc-one")
	if err := os.Remove(attachment.LeasePath); err != nil {
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
	entries, err := os.ReadDir(filepath.Join(manager.stateDir, "acme"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || filepath.Join(manager.stateDir, "acme", entries[0].Name()) != other.LeasePath {
		t.Fatalf("leases after rollback = %v, want only %s", entries, other.LeasePath)
	}
	assertAttachments(t, manager, "alloc-other")
}

func TestAttachFailedRollbackKeepsRecordForLaterDetach(t *testing.T) {
	manager, runner := newRecoveryTestManager(t)
	if err := os.WriteFile(manager.netnsPath("alloc-one"), nil, 0o600); err != nil {
		t.Fatal(err)
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
	if err := run.Run(ctx, "ip", "link", "add", hostVeth, "type", "veth", "peer", "name", peerVeth); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = run.Run(ctx, "ip", "link", "del", hostVeth) })
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
