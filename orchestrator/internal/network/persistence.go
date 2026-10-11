package network

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
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

func (m *WireGuardManager) leaseDirectory(network string) string {
	return filepath.Join(m.stateDir, leaseStorageDir, network)
}

func syncDirectory(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return f.Sync()
}
