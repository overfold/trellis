package network

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// attachmentJournalDir holds one record per attachment below the manager's
// state directory. The leading dot keeps it apart from per-network lease
// directories, whose names must be safe identifiers.
const attachmentJournalDir = ".attachments"

const defaultNetnsDir = "/var/run/netns"

// CleanupJournaledAttachments removes only resources described by attachment
// journals below stateDir. Unlike the normal manager constructor, it does not
// initialize or create network state when there is nothing to clean up.
func CleanupJournaledAttachments(ctx context.Context, stateDir, dnsAddress string) error {
	manager := &WireGuardManager{stateDir: stateDir, run: execRunner{}}
	return manager.CleanupAttachments(ctx, dnsAddress)
}

// attachmentRecord is journaled before Attach creates anything, and removed
// only after every resource it names is gone. It holds what Detach needs that
// cannot be derived from the allocation ID.
type attachmentRecord struct {
	AllocationID string `json:"allocation_id"`
	Namespace    string `json:"namespace"`
	Network      string `json:"network"`
	CIDR         string `json:"cidr,omitempty"`
	Gateway      string `json:"gateway"`
	APIPort      int    `json:"api_port"`
	// Ports are published node ports whose NAT rules detach removes.
	Ports []PortMapping `json:"ports,omitempty"`
}

func (m *WireGuardManager) journalPath(allocationID string) string {
	return filepath.Join(m.stateDir, attachmentJournalDir, allocationID+".json")
}

func (m *WireGuardManager) netnsPath(allocationID string) string {
	dir := m.netnsDir
	if dir == "" {
		dir = defaultNetnsDir
	}
	return filepath.Join(dir, allocationID)
}

// recordAttachment durably journals an attachment before any of its
// resources exist. It refuses an allocation that already has a record, so an
// attach never adopts, and a failed attach never removes, another attempt's
// resources.
func (m *WireGuardManager) recordAttachment(record attachmentRecord) error {
	path := m.journalPath(record.AllocationID)
	if _, err := os.Lstat(path); err == nil {
		return fmt.Errorf("network attachment for %s already exists; detach it first", record.AllocationID)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("inspect network attachment record: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create network attachment records: %w", err)
	}
	raw, _ := json.Marshal(record)
	if err := writeAtomicFile(path, raw, 0o600); err != nil {
		return fmt.Errorf("record network attachment: %w", err)
	}
	if m.namespaceCIDRsLoaded {
		if prefix, err := netip.ParsePrefix(record.CIDR); err == nil {
			m.namespaceCIDRs[prefix.Masked()] = record.Namespace
		}
	}
	return nil
}

func (m *WireGuardManager) readAttachmentRecord(allocationID string) (*attachmentRecord, error) {
	raw, err := os.ReadFile(m.journalPath(allocationID))
	if err != nil {
		return nil, err
	}
	var record attachmentRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return nil, fmt.Errorf("parse network attachment record for %s: %w", allocationID, err)
	}
	if record.AllocationID != allocationID || !safeName.MatchString(record.Namespace) || !safeName.MatchString(record.Network) {
		return nil, fmt.Errorf("network attachment record for %s is inconsistent", allocationID)
	}
	return &record, nil
}

// DetachAllocation removes whatever Attach created for an allocation, found
// from the attachment record Attach journaled before creating anything. It
// is idempotent: resources that are already gone are skipped, and an
// allocation without a record has nothing left to remove.
func (m *WireGuardManager) DetachAllocation(ctx context.Context, allocationID string) error {
	if !safeAllocation.MatchString(allocationID) {
		return fmt.Errorf("allocation must be a safe identifier")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	record, err := m.readAttachmentRecord(allocationID)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return m.detachLocked(ctx, Attachment{
		AllocationID: record.AllocationID,
		Namespace:    record.Namespace,
		Network:      record.Network,
		Address:      record.CIDR,
		Gateway:      record.Gateway,
		APIPort:      record.APIPort,
		Ports:        record.Ports,
	})
}

// Attachments lists the allocation IDs that have an attachment record, so
// their resources may still exist. Records that cannot be read are reported
// in the error alongside every readable ID.
func (m *WireGuardManager) Attachments(context.Context) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entries, err := os.ReadDir(filepath.Join(m.stateDir, attachmentJournalDir))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list network attachment records: %w", err)
	}
	var ids []string
	var errs []error
	for _, entry := range entries {
		name := entry.Name()
		// Skip temporary files an interrupted atomic write left behind.
		if strings.HasPrefix(name, ".") {
			continue
		}
		id, ok := strings.CutSuffix(name, ".json")
		if !ok || !safeAllocation.MatchString(id) {
			errs = append(errs, fmt.Errorf("unexpected network attachment record %q", name))
			continue
		}
		if _, err := m.readAttachmentRecord(id); err != nil {
			errs = append(errs, err)
			continue
		}
		ids = append(ids, id)
	}
	return ids, errors.Join(errs...)
}

// CleanupAttachments removes every attachment described by the durable
// journals in this manager's state directory. It is intended for offline node
// removal, after workloads have stopped. An unreadable journal prevents any
// teardown so uninstall cannot discard the only record of owned resources.
func (m *WireGuardManager) CleanupAttachments(ctx context.Context, dnsAddress string) error {
	ip, err := netip.ParseAddr(dnsAddress)
	if err != nil || !ip.Is4() {
		return fmt.Errorf("workload DNS address must be IPv4: %s", dnsAddress)
	}
	m.dnsAddress = dnsAddress
	ids, err := m.Attachments(ctx)
	if err != nil {
		return fmt.Errorf("inspect network attachments: %w", err)
	}
	for _, id := range ids {
		if err := m.DetachAllocation(ctx, id); err != nil {
			return fmt.Errorf("detach network attachment %s: %w", id, err)
		}
	}
	return nil
}

// NamespaceForIP returns the namespace whose locally attached network contains
// address. Unreadable or inconsistent attachment state fails closed.
func (m *WireGuardManager) NamespaceForIP(address netip.Addr) (string, bool) {
	if !address.Is4() {
		return "", false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.namespaceCIDRsLoaded && !time.Now().Before(m.namespaceCIDRsRetryAt) {
		m.loadNamespaceCIDRsLocked()
	}
	for prefix, namespace := range m.namespaceCIDRs {
		if prefix.Contains(address) {
			return namespace, true
		}
	}
	return "", false
}

func (m *WireGuardManager) loadNamespaceCIDRsLocked() {
	m.namespaceCIDRs = make(map[netip.Prefix]string)
	m.namespaceCIDRsRetryAt = time.Now().Add(time.Second)
	cidrs := make(map[netip.Prefix]string)
	entries, err := os.ReadDir(filepath.Join(m.stateDir, attachmentJournalDir))
	if errors.Is(err, fs.ErrNotExist) {
		m.namespaceCIDRsLoaded = true
		return
	}
	if err != nil {
		return
	}
	for _, entry := range entries {
		id, ok := strings.CutSuffix(entry.Name(), ".json")
		if !ok || !safeAllocation.MatchString(id) {
			continue
		}
		record, err := m.readAttachmentRecord(id)
		if err != nil {
			return
		}
		prefix, err := netip.ParsePrefix(record.CIDR)
		if err != nil {
			return
		}
		prefix = prefix.Masked()
		if namespace, exists := cidrs[prefix]; exists && namespace != record.Namespace {
			return
		}
		cidrs[prefix] = record.Namespace
	}
	m.namespaceCIDRs = cidrs
	m.namespaceCIDRsLoaded = true
}

// detachLocked removes an allocation's published ports, veth, and network
// namespace. When its
// lease is the last one, it removes the shared namespace path before releasing
// that lease, then removes the attachment record. Each step tolerates a
// resource that is already gone, so a retry after a partial attach, detach, or
// crash converges. Every name is derived from the Trellis allocation,
// namespace, and network, so state Trellis does not own is never touched.
func (m *WireGuardManager) detachLocked(ctx context.Context, a Attachment) error {
	if !safeAllocation.MatchString(a.AllocationID) || !safeName.MatchString(a.Namespace) || !safeName.MatchString(a.Network) {
		return fmt.Errorf("network attachment has unsafe identifiers")
	}
	// Stop publishing ports before the address they forward to is released.
	if len(a.Ports) > 0 {
		if err := m.unpublishPorts(ctx, a.AllocationID); err != nil {
			return err
		}
	}
	hostVeth := a.HostVeth
	if hostVeth == "" {
		hostVeth = short("vh", a.AllocationID)
	}
	// Deleting the host end also deletes its peer, wherever the peer is.
	if err := m.deleteLink(ctx, hostVeth, "allocation veth"); err != nil {
		return err
	}
	if err := m.run.Run(ctx, "ip", "netns", "del", a.AllocationID); err != nil {
		if _, statErr := os.Lstat(m.netnsPath(a.AllocationID)); !errors.Is(statErr, fs.ErrNotExist) {
			return fmt.Errorf("remove allocation network namespace: %w", err)
		}
	}
	leaseDir := filepath.Join(m.stateDir, a.Network)
	otherLeases, err := hasOtherAllocationLeases(leaseDir, a.AllocationID)
	if err != nil {
		return err
	}
	if !otherLeases {
		if err := m.removeNamespacePathLocked(ctx, a); err != nil {
			return err
		}
	}
	if err := removeAllocationLeases(leaseDir, a.AllocationID); err != nil {
		return err
	}
	if !otherLeases {
		if err := os.Remove(leaseDir); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove empty network lease directory: %w", err)
		}
	}
	if err := os.Remove(m.journalPath(a.AllocationID)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove network attachment record: %w", err)
	}
	return nil
}

func hasOtherAllocationLeases(leaseDir, allocationID string) (bool, error) {
	entries, err := os.ReadDir(leaseDir)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read network leases: %w", err)
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			return false, fmt.Errorf("unexpected address lease entry %s", entry.Name())
		}
		owner, err := os.ReadFile(filepath.Join(leaseDir, entry.Name()))
		if err != nil {
			return false, fmt.Errorf("read address lease %s: %w", entry.Name(), err)
		}
		if string(owner) != allocationID {
			return true, nil
		}
	}
	return false, nil
}

// removeAllocationLeases removes the address leases an allocation holds. A
// lease file names its address and contains the owning allocation ID.
func removeAllocationLeases(leaseDir, allocationID string) error {
	entries, err := os.ReadDir(leaseDir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read network leases: %w", err)
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		path := filepath.Join(leaseDir, entry.Name())
		owner, err := os.ReadFile(path)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("read address lease %s: %w", entry.Name(), err)
		}
		if string(owner) != allocationID {
			continue
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove address lease %s: %w", entry.Name(), err)
		}
	}
	return nil
}

// removeNamespacePathLocked tears down a namespace's bridge, WireGuard
// interface, firewall rules, applied plan, and lease directory once its last
// local allocation is gone.
func (m *WireGuardManager) removeNamespacePathLocked(ctx context.Context, a Attachment) error {
	bridge := a.Bridge
	if bridge == "" {
		bridge = short("tb", a.Namespace+"\x00"+a.Network)
	}
	wg := a.WireGuardInterface
	if wg == "" {
		wg = short("tw", a.Namespace+"\x00"+a.Network)
	}
	cidr := ""
	if address, err := netip.ParsePrefix(a.Address); err == nil {
		cidr = address.Masked().String()
	}
	for _, rule := range namespaceRules(bridge, wg, cidr, a.Gateway, m.dnsAddress, a.APIPort) {
		if err := m.deleteRule(ctx, rule.table, rule.args...); err != nil {
			return err
		}
	}
	otherPath, err := m.hasOtherNamespacePath(a.Namespace, a.Network)
	if err != nil {
		return err
	}
	if !otherPath {
		if err := m.removeChains(ctx); err != nil {
			return err
		}
	}
	if err := m.deleteLink(ctx, wg, "WireGuard interface"); err != nil {
		return err
	}
	if err := m.deleteLink(ctx, bridge, "bridge"); err != nil {
		return err
	}
	if err := os.Remove(m.planPath(a.Namespace, a.Network)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove applied network plan: %w", err)
	}
	if prefix, err := netip.ParsePrefix(a.Address); err == nil && m.namespaceCIDRsLoaded {
		delete(m.namespaceCIDRs, prefix.Masked())
	}
	return nil
}

func (m *WireGuardManager) hasOtherNamespacePath(namespace, network string) (bool, error) {
	entries, err := os.ReadDir(filepath.Join(m.stateDir, attachmentJournalDir))
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("list network attachment records: %w", err)
	}
	for _, entry := range entries {
		id, ok := strings.CutSuffix(entry.Name(), ".json")
		if !ok || !safeAllocation.MatchString(id) {
			continue
		}
		record, err := m.readAttachmentRecord(id)
		if err != nil {
			return false, err
		}
		if record.Namespace != namespace || record.Network != network {
			return true, nil
		}
	}
	return false, nil
}

func explicitAbsence(err error, messages ...string) bool {
	if err == nil {
		return false
	}
	for _, message := range messages {
		if strings.Contains(err.Error(), message) {
			return true
		}
	}
	return false
}

func (m *WireGuardManager) deleteLink(ctx context.Context, name, resource string) error {
	if err := m.run.Run(ctx, "ip", "link", "del", name); err != nil {
		inspectErr := m.run.Run(ctx, "ip", "link", "show", "dev", name)
		absent := explicitAbsence(err, "does not exist", "Cannot find device") ||
			explicitAbsence(inspectErr, "does not exist", "Cannot find device")
		if ctx.Err() == nil && absent {
			return nil
		}
		if inspectErr != nil {
			return fmt.Errorf("delete %s %s: %w (verify absence: %w)", resource, name, err, inspectErr)
		}
		return fmt.Errorf("delete %s %s: %w", resource, name, err)
	}
	return nil
}
