package network

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
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
	stateDir, err := filepath.Abs(stateDir)
	if err != nil {
		return err
	}
	manager := &WireGuardManager{stateDir: stateDir, run: execRunner{}}
	return manager.CleanupAttachments(ctx, dnsAddress)
}

// attachmentRecord is journaled before Attach creates anything, and removed
// only after every resource it names is gone. It holds what Detach needs that
// cannot be derived from the allocation ID.
type attachmentRecord struct {
	Version      int    `json:"version"`
	AllocationID string `json:"allocation_id"`
	Namespace    string `json:"namespace"`
	Network      string `json:"network"`
	CIDR         string `json:"cidr,omitempty"`
	Gateway      string `json:"gateway"`
	APIPort      int    `json:"api_port"`
	NetnsCreated bool   `json:"netns_created"`
	NetnsInode   uint64 `json:"netns_inode"`
	StageInode   uint64 `json:"stage_inode,omitempty"`
	Address      string `json:"address,omitempty"`
	PortsReady   bool   `json:"ports_ready,omitempty"`
	Detaching    bool   `json:"detaching,omitempty"`
	PathGroup    uint32 `json:"path_group,omitempty"`
	VethGroup    uint32 `json:"veth_group,omitempty"`
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

func pathOwner(namespace, network string) string {
	return "trellis:path:" + namespace + ":" + network
}

func allocationOwner(allocation string) string { return "trellis:allocation:" + short("", allocation) }

// Kernel aliases retain the full identity: truncated names are never ownership.
func checkLinkOwner(name, owner string) (bool, error) {
	return checkLinkMarker(name, owner, 0)
}

func checkLinkMarker(name, owner string, group uint32) (bool, error) {
	raw, err := os.ReadFile(filepath.Join("/sys/class/net", name, "ifalias"))
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect link %s ownership: %w", name, err)
	}
	if strings.TrimSpace(string(raw)) != owner+":"+name {
		if strings.TrimSpace(string(raw)) == "" && group != 0 {
			rawGroup, err := os.ReadFile(filepath.Join("/sys/class/net", name, "netdev_group"))
			if err == nil && strings.TrimSpace(string(rawGroup)) == strconv.FormatUint(uint64(group), 10) {
				return true, nil
			}
		}
		return true, fmt.Errorf("refuse unowned link %s", name)
	}
	return true, nil
}

func (m *WireGuardManager) ownerGroup(owner string) (uint32, error) {
	ids, err := m.attachmentIDsLocked()
	if err != nil {
		return 0, err
	}
	for _, id := range ids {
		record, err := m.readAttachmentRecord(id)
		if err != nil {
			return 0, err
		}
		if owner == pathOwner(record.Namespace, record.Network) && record.PathGroup != 0 {
			return record.PathGroup, nil
		}
		if owner == allocationOwner(id) {
			return record.VethGroup, nil
		}
	}
	return 0, nil
}

func (m *WireGuardManager) checkLinkOwner(name, owner string) (bool, error) {
	group, err := m.ownerGroup(owner)
	if err != nil {
		return false, err
	}
	return checkLinkMarker(name, owner, group)
}

func (m *WireGuardManager) checkResourceOwnership(namespace, network, allocation string) error {
	if _, err := os.Lstat(m.journalPath(allocation)); err == nil {
		return fmt.Errorf("network attachment for %s already exists; detach it first", allocation)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	key := namespace + "\x00" + network
	existingPath := false
	for _, prefix := range []string{"tb", "tw"} {
		exists, err := m.checkLinkOwner(short(prefix, key), pathOwner(namespace, network))
		if err != nil {
			return err
		}
		existingPath = existingPath || exists
	}
	for _, prefix := range []string{"vh", "vc"} {
		if exists, err := m.checkLinkOwner(short(prefix, allocation), allocationOwner(allocation)); err != nil {
			return err
		} else if exists {
			return fmt.Errorf("allocation link already exists; detach first")
		}
	}
	for _, path := range []string{m.netnsPath(allocation), m.netnsStage(allocation)} {
		if _, err := os.Lstat(path); !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("refuse existing or unreadable allocation network namespace %s", allocation)
		}
	}
	ids, err := m.attachmentIDsLocked()
	if err != nil {
		return err
	}
	journaledPath := false
	for _, id := range ids {
		record, err := m.readAttachmentRecord(id)
		if err != nil {
			return err
		}
		other := record.Namespace + "\x00" + record.Network
		journaledPath = journaledPath || key == other
		if key != other && short("tb", key) == short("tb", other) || allocation != id && short("vh", allocation) == short("vh", id) || allocation != id && allocationPortsChain(allocation) == allocationPortsChain(id) {
			return fmt.Errorf("network resource name collision with attachment %s", id)
		}
	}
	if existingPath && !journaledPath {
		return fmt.Errorf("refuse namespace path without an attachment journal")
	}
	return nil
}

func (m *WireGuardManager) attachmentIDsLocked() ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(m.stateDir, attachmentJournalDir))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		id, ok := strings.CutSuffix(entry.Name(), ".json")
		if !ok || !safeAllocation.MatchString(id) {
			return nil, fmt.Errorf("unexpected attachment record %q", entry.Name())
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// validateAttachmentCIDR checks journals (including interrupted attaches) and
// leases before creating anything. The caller holds m.mu through reservation.
func (m *WireGuardManager) validateAttachmentCIDR(namespace, network, cidr string) error {
	prefix, err := netip.ParsePrefix(cidr)
	if err != nil || !prefix.Addr().Is4() || prefix.Bits() > 29 {
		return fmt.Errorf("CIDR %q has no IPv4 allocation space", cidr)
	}
	entries, err := os.ReadDir(filepath.Join(m.stateDir, attachmentJournalDir))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("inspect attachment CIDRs: %w", err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		id, ok := strings.CutSuffix(entry.Name(), ".json")
		if !ok || !safeAllocation.MatchString(id) {
			return fmt.Errorf("unexpected network attachment record %q", entry.Name())
		}
		record, err := m.readAttachmentRecord(id)
		if err != nil {
			return err
		}
		existing, err := netip.ParsePrefix(record.CIDR)
		if err != nil {
			return fmt.Errorf("invalid attachment CIDR for %s: %w", id, err)
		}
		if prefix.Overlaps(existing) && (namespace != record.Namespace || network != record.Network) {
			return fmt.Errorf("CIDR %s overlaps attachment %s in namespace %q network %q", cidr, id, record.Namespace, record.Network)
		}
	}
	// Leases can outlive a journal after interrupted or manual recovery. Their
	// filenames retain the subnet even when no allocation record is readable.
	// Migrate all legacy directories before checking the separate lease tree.
	legacy, err := os.ReadDir(m.stateDir)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	for _, entry := range legacy {
		if entry.IsDir() && safeName.MatchString(entry.Name()) {
			if _, err := m.leaseDirectory(entry.Name()); err != nil {
				return err
			}
		}
	}
	networks, err := os.ReadDir(filepath.Join(m.stateDir, leaseStorageDir))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect network leases: %w", err)
	}
	for _, entry := range networks {
		if !entry.IsDir() || !safeName.MatchString(entry.Name()) || entry.Name() == network {
			continue
		}
		leases, err := os.ReadDir(filepath.Join(m.stateDir, leaseStorageDir, entry.Name()))
		if err != nil {
			return fmt.Errorf("inspect network %q leases: %w", entry.Name(), err)
		}
		for _, lease := range leases {
			existing, err := netip.ParsePrefix(strings.ReplaceAll(lease.Name(), "_", "/"))
			if err != nil {
				return fmt.Errorf("invalid address lease %q in network %q", lease.Name(), entry.Name())
			}
			if prefix.Overlaps(existing) {
				return fmt.Errorf("CIDR %s overlaps leases in network %q", cidr, entry.Name())
			}
		}
	}
	return nil
}

// recordAttachment durably journals an attachment before any of its
// resources exist. It refuses an allocation that already has a record, so an
// attach never adopts, and a failed attach never removes, another attempt's
// resources.
func (m *WireGuardManager) recordAttachment(record attachmentRecord) error {
	record.Version = 3 // Private netns staging and durable published-port destination.
	ids, err := m.attachmentIDsLocked()
	if err != nil {
		return err
	}
	used := map[uint32]bool{0: true}
	for _, id := range ids {
		other, err := m.readAttachmentRecord(id)
		if err != nil {
			return err
		}
		used[other.PathGroup], used[other.VethGroup] = true, true
		if other.Namespace == record.Namespace && other.Network == record.Network && other.PathGroup != 0 {
			record.PathGroup = other.PathGroup
		}
	}
	for _, group := range []*uint32{&record.PathGroup, &record.VethGroup} {
		for *group == 0 {
			var raw [4]byte
			if _, err := rand.Read(raw[:]); err != nil {
				return err
			}
			candidate := binary.BigEndian.Uint32(raw[:]) & 0x7fffffff
			if !used[candidate] {
				*group = candidate
				used[candidate] = true
			}
		}
	}
	path := m.journalPath(record.AllocationID)
	if _, err := os.Lstat(path); err == nil {
		return fmt.Errorf("network attachment for %s already exists; detach it first", record.AllocationID)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("inspect network attachment record: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create network attachment records: %w", err)
	}
	if err := syncDirectory(m.stateDir); err != nil {
		return err
	}
	if err := m.saveAttachment(&record); err != nil {
		return fmt.Errorf("record network attachment: %w", err)
	}
	m.namespaceCIDRsLoaded = false
	m.namespaceCIDRsRetryAt = time.Time{}
	clear(m.namespaceCIDRs)
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
	if record.Version != 2 && record.Version != 3 {
		return nil, fmt.Errorf("network attachment %s has unsupported resource version %d; drain and clean up with the creating binary before upgrading", allocationID, record.Version)
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
	namespace := ""
	for prefix, owner := range m.namespaceCIDRs {
		if prefix.Contains(address) {
			if namespace != "" && namespace != owner {
				return "", false
			}
			namespace = owner
		}
	}
	return namespace, namespace != ""
}

func (m *WireGuardManager) loadNamespaceCIDRsLocked() {
	m.namespaceCIDRsLoaded = false
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
		for existing, namespace := range cidrs {
			if existing.Overlaps(prefix) && namespace != record.Namespace {
				return
			}
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
	record, err := m.readAttachmentRecord(a.AllocationID)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	// The durable journal, not caller-supplied device names, owns teardown.
	a = Attachment{AllocationID: record.AllocationID, Namespace: record.Namespace, Network: record.Network,
		Address: record.CIDR, Gateway: record.Gateway, APIPort: record.APIPort, Ports: record.Ports}
	for name, owner := range map[string]string{
		short("vh", a.AllocationID):               allocationOwner(a.AllocationID),
		short("vc", a.AllocationID):               allocationOwner(a.AllocationID),
		short("tb", a.Namespace+"\x00"+a.Network): pathOwner(a.Namespace, a.Network),
		short("tw", a.Namespace+"\x00"+a.Network): pathOwner(a.Namespace, a.Network),
	} {
		if record.Version == 2 && name == short("vc", a.AllocationID) {
			continue // Legacy peers were not marked; the owned host end removes them.
		}
		if _, err := m.checkLinkOwner(name, owner); err != nil {
			return err
		}
	}
	if info, err := os.Lstat(m.netnsPath(a.AllocationID)); err == nil {
		owned := false
		if record.Version == 3 && info.Mode()&os.ModeSymlink != 0 {
			target, err := os.Readlink(m.netnsPath(a.AllocationID))
			owned = err == nil && target == filepath.Join(m.netnsStage(a.AllocationID), "namespace")
			if owned {
				info, err := os.Stat(target)
				owned = errors.Is(err, fs.ErrNotExist)
				if err == nil {
					inode := info.Sys().(*syscall.Stat_t).Ino
					owned = inode == record.NetnsInode || record.StageInode != 0 && inode == record.StageInode
				}
			}
		} else {
			owned = info.Sys().(*syscall.Stat_t).Ino == record.NetnsInode
		}
		if !record.NetnsCreated || !owned {
			return fmt.Errorf("refuse unowned network namespace %s", a.AllocationID)
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	// Stop publishing ports before the address they forward to is released.
	record.Detaching = true
	if err := m.saveAttachment(record); err != nil {
		return err
	}
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
	if record.Version == 3 {
		if err := m.deleteLink(ctx, short("vc", a.AllocationID), "allocation veth peer"); err != nil {
			return err
		}
	}
	if record.NetnsCreated {
		if err := m.run.Run(ctx, "ip", "netns", "del", a.AllocationID); err != nil {
			if _, statErr := os.Lstat(m.netnsPath(a.AllocationID)); !errors.Is(statErr, fs.ErrNotExist) {
				return fmt.Errorf("remove allocation network namespace: %w", err)
			}
		}
		if err := os.Remove(m.netnsPath(a.AllocationID)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	if record.Version == 3 {
		if err := m.removeNetnsStage(ctx, a.AllocationID); err != nil {
			return err
		}
	}
	leaseDir, err := m.leaseDirectory(a.Network)
	if err != nil {
		return err
	}
	otherLeases, err := hasOtherAllocationLeases(leaseDir, a.AllocationID)
	if err != nil {
		return err
	}
	ids, err := m.attachmentIDsLocked()
	if err != nil {
		return err
	}
	otherAPIGrant := false
	for _, id := range ids {
		if id == a.AllocationID {
			continue
		}
		other, err := m.readAttachmentRecord(id)
		if err != nil {
			return err
		}
		if other.Namespace == a.Namespace && other.Network == a.Network {
			// Interrupted attaches may own shared links without a lease yet.
			otherLeases = true
			otherAPIGrant = otherAPIGrant || other.APIPort == a.APIPort
		}
	}
	if a.APIPort > 0 && !otherAPIGrant {
		if err := m.deleteRule(ctx, "", inputChain, "-i", short("tb", a.Namespace+"\x00"+a.Network), "-s", record.CIDR, "-d", a.Gateway, "-p", "tcp", "--dport", fmt.Sprint(a.APIPort), "-j", "ACCEPT"); err != nil {
			return err
		}
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
		if _, err := os.Stat(filepath.Dir(leaseDir)); err == nil {
			if err := syncDirectory(filepath.Dir(leaseDir)); err != nil {
				return err
			}
		} else if !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	if err := os.Remove(m.journalPath(a.AllocationID)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove network attachment record: %w", err)
	}
	if err := syncDirectory(filepath.Dir(m.journalPath(a.AllocationID))); err != nil {
		return err
	}
	m.namespaceCIDRsLoaded = false
	m.namespaceCIDRsRetryAt = time.Time{}
	clear(m.namespaceCIDRs)
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
	return syncDirectory(leaseDir)
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
