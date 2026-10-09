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
	"syscall"
)

func (m *WireGuardManager) saveAttachment(record *attachmentRecord) error {
	raw, _ := json.Marshal(record)
	if m.journalWrite != nil {
		return m.journalWrite(m.journalPath(record.AllocationID), raw)
	}
	return writeAtomicFile(m.journalPath(record.AllocationID), raw)
}

func (m *WireGuardManager) netnsStage(id string) string {
	return filepath.Join(m.stateDir, attachmentJournalDir, ".netns-"+id)
}

// Only private, journal-owned paths exist until the namespace inode is
// durable. A symlink publishes it without overwriting a foreign name, even
// when state and /run/netns are on different filesystems.
func (m *WireGuardManager) createNetns(ctx context.Context, record *attachmentRecord) error {
	dir := m.netnsStage(record.AllocationID)
	if err := os.Mkdir(dir, 0o700); err != nil {
		return err
	}
	stage := filepath.Join(dir, "namespace")
	if err := os.WriteFile(stage, nil, 0o600); err != nil {
		return err
	}
	for _, path := range []string{stage, dir, filepath.Dir(dir)} {
		if err := syncDirectory(path); err != nil {
			return err
		}
	}
	info, err := os.Stat(stage)
	if err != nil {
		return err
	}
	record.StageInode = info.Sys().(*syscall.Stat_t).Ino
	if err := m.saveAttachment(record); err != nil {
		return err
	}
	if err := m.run.Run(ctx, "unshare", "--net", "--", "mount", "--bind", "/proc/self/ns/net", stage); err != nil {
		return err
	}
	info, err = os.Stat(stage)
	if err != nil {
		return err
	}
	record.NetnsInode = info.Sys().(*syscall.Stat_t).Ino
	record.NetnsCreated = true
	if err := m.saveAttachment(record); err != nil {
		return err // Nothing has been published, even if rename succeeded.
	}
	public := m.netnsPath(record.AllocationID)
	if err := os.MkdirAll(filepath.Dir(public), 0o755); err != nil {
		return err
	}
	return os.Symlink(stage, public)
}

func (m *WireGuardManager) removeNetnsStage(ctx context.Context, id string) error {
	dir := m.netnsStage(id)
	stage := filepath.Join(dir, "namespace")
	if _, err := os.Lstat(stage); err == nil {
		if err := m.run.Run(ctx, "umount", stage); err != nil && !explicitAbsence(err, "not mounted", "Invalid argument") {
			return err
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return os.RemoveAll(dir)
}

const leaseStorageDir = ".leases"

// Migrate individual legacy lease files, not the directory: "plans" also
// contains peer-plan JSON. Hard linking refuses conflicting destination state
// and makes interruption between publication and removal safely retryable.
func (m *WireGuardManager) leaseDirectory(network string) (string, error) {
	dest := filepath.Join(m.stateDir, leaseStorageDir, network)
	legacy := filepath.Join(m.stateDir, network)
	entries, err := os.ReadDir(legacy)
	if errors.Is(err, fs.ErrNotExist) {
		return dest, nil
	}
	if err != nil {
		return "", err
	}
	for _, entry := range entries {
		if network == "plans" && (strings.HasSuffix(entry.Name(), ".json") || strings.HasPrefix(entry.Name(), ".network-plan-")) {
			continue
		}
		prefix, err := netip.ParsePrefix(strings.ReplaceAll(entry.Name(), "_", "/"))
		if err != nil || !prefix.Addr().Is4() || !entry.Type().IsRegular() {
			return "", fmt.Errorf("unexpected legacy lease %s/%s", network, entry.Name())
		}
		source, target := filepath.Join(legacy, entry.Name()), filepath.Join(dest, entry.Name())
		owner, err := os.ReadFile(source)
		if err != nil {
			return "", fmt.Errorf("read legacy lease owner %s: %w", source, err)
		}
		if !safeAllocation.Match(owner) {
			return "", fmt.Errorf("invalid legacy lease owner %s", source)
		}
		if err := os.MkdirAll(dest, 0o700); err != nil {
			return "", err
		}
		if err := os.Link(source, target); err != nil {
			if !errors.Is(err, fs.ErrExist) {
				return "", err
			}
			a, err := os.Stat(source)
			if err != nil {
				return "", err
			}
			b, err := os.Lstat(target)
			if err != nil || !os.SameFile(a, b) {
				return "", fmt.Errorf("conflicting migrated lease %s", target)
			}
		}
		if err := syncDirectory(dest); err != nil {
			return "", err
		}
		for _, dir := range []string{filepath.Dir(dest), m.stateDir} {
			if err := syncDirectory(dir); err != nil {
				return "", err
			}
		}
		if err := os.Remove(source); err != nil {
			return "", err
		}
		if err := syncDirectory(legacy); err != nil {
			return "", err
		}
	}
	if network != "plans" {
		if err := os.Remove(legacy); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return "", err
		}
	}
	return dest, nil
}

func syncDirectory(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return f.Sync()
}

// v2 journals omitted the probed address. Never recompute its hash: an IPAM
// collision may have assigned a different address. Require one owned lease.
func (m *WireGuardManager) publishedAddress(record *attachmentRecord) (string, error) {
	if record.Version == 3 {
		return record.Address, nil
	}
	dir, err := m.leaseDirectory(record.Network)
	if err != nil {
		return "", err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}
	address := ""
	for _, entry := range entries {
		owner, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return "", err
		}
		if string(owner) == record.AllocationID {
			if address != "" {
				return "", fmt.Errorf("ambiguous published address for %s", record.AllocationID)
			}
			address = strings.ReplaceAll(entry.Name(), "_", "/")
		}
	}
	if address == "" {
		return "", fmt.Errorf("missing published address lease for %s", record.AllocationID)
	}
	return address, nil
}
