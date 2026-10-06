package runtime

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
)

// rotatedLogSuffix names the file that holds a task log's previous output
// after rotation, beside the active "<container>.log".
const rotatedLogSuffix = ".1"

// maxLineAlignment bounds how far rotation looks for a line boundary at
// which to start the rotated file.
const maxLineAlignment = 64 << 10

// logRotations coordinates rotation of each runtime task log with its readers
// and its removal. The containerd shim appends task output to the active log
// through its own O_APPEND descriptor, so rotation copies the newest output
// aside and truncates the active log in place; the shim's next write lands at
// the new end of the file.
type logRotations struct {
	mu    sync.Mutex
	paths map[string]*logRotation
}

// logRotation is the rotation state of one active log path. Readers hold mu
// while they open the log files or check generation, so they never observe a
// rotation half done.
type logRotation struct {
	mu sync.Mutex
	// generation counts rotations of the path since the runtime started.
	generation uint64
	// discarded is how many bytes at the head of the previous active log the
	// latest rotation left out of the rotated file.
	discarded int64
}

func (r *logRotations) get(path string) *logRotation {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.paths == nil {
		r.paths = make(map[string]*logRotation)
	}
	rotation := r.paths[path]
	if rotation == nil {
		rotation = &logRotation{}
		r.paths[path] = rotation
	}
	return rotation
}

func (r *logRotations) forget(path string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.paths, path)
}

// EnforceLogLimit bounds every task log in the runtime log directory, running
// or retained, to about limit bytes. An active log larger than half the limit
// is rotated: its newest half-limit bytes replace the rotated file and the
// active log is truncated. Output a task writes between checks can briefly
// take a log past the limit, and output written while a rotation finishes
// copying is lost.
func (c *ContainerdRuntime) EnforceLogLimit(limit int64) error {
	if limit < 2 {
		return fmt.Errorf("task log limit must be at least 2 bytes")
	}
	entries, err := os.ReadDir(c.logDir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("list task logs in %s: %w", c.logDir, err)
	}
	var errs []error
	for _, entry := range entries {
		if !entry.Type().IsRegular() || !strings.HasSuffix(entry.Name(), ".log") {
			continue
		}
		path := filepath.Join(c.logDir, entry.Name())
		if err := c.rotateLog(path, limit/2); err != nil {
			errs = append(errs, fmt.Errorf("rotate task log %s: %w", path, err))
		}
	}
	return errors.Join(errs...)
}

func (c *ContainerdRuntime) rotateLog(path string, keep int64) error {
	rotation := c.rotations.get(path)
	rotation.mu.Lock()
	defer rotation.mu.Unlock()
	active, err := os.OpenFile(path, os.O_RDWR|syscall.O_NOFOLLOW, 0)
	if os.IsNotExist(err) {
		// Removed since the directory was read.
		c.rotations.forget(path)
		return nil
	}
	if err != nil {
		return err
	}
	defer func() { _ = active.Close() }()
	info, err := active.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() <= keep {
		return nil
	}
	// Look for a line boundary only in the older half of what is kept, so
	// alignment never discards most of the newest output.
	discarded, err := lineStart(active, info.Size()-keep, min(keep/2, maxLineAlignment))
	if err != nil {
		return err
	}
	if err := copyRotatedLog(active, path, discarded, keep); err != nil {
		if !errors.Is(err, syscall.ENOSPC) {
			return err
		}
		// With the filesystem full, the newest output cannot be copied
		// aside. Free the space anyway: discard this log's retained output
		// rather than let one task keep the whole node's disk full.
		if removeErr := os.Remove(path + rotatedLogSuffix); removeErr != nil && !os.IsNotExist(removeErr) {
			return errors.Join(err, removeErr)
		}
		if truncateErr := active.Truncate(0); truncateErr != nil {
			return errors.Join(err, truncateErr)
		}
		rotation.generation++
		rotation.discarded = info.Size()
		return fmt.Errorf("discarded retained output to free space: %w", err)
	}
	if err := active.Truncate(0); err != nil {
		return err
	}
	rotation.generation++
	rotation.discarded = discarded
	return nil
}

// copyRotatedLog replaces the rotated log with the active log's output from
// offset discarded.
func copyRotatedLog(active *os.File, path string, discarded, keep int64) error {
	temporary := path + rotatedLogSuffix + ".tmp"
	target, err := os.OpenFile(temporary, os.O_CREATE|os.O_TRUNC|os.O_WRONLY|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return err
	}
	// Copy the newest keep bytes and whatever the task appends meanwhile, but
	// stop at twice that so a task writing faster than the copy cannot stall
	// rotation or grow the rotated file without bound.
	_, copyErr := io.Copy(target, io.NewSectionReader(active, discarded, 2*keep))
	closeErr := target.Close()
	if copyErr != nil || closeErr != nil {
		_ = os.Remove(temporary)
		return errors.Join(copyErr, closeErr)
	}
	if err := os.Rename(temporary, path+rotatedLogSuffix); err != nil {
		_ = os.Remove(temporary)
		return err
	}
	return nil
}

// lineStart returns the start of the first line that begins at or after
// offset within window bytes, so the rotated file does not begin with a
// partial line. Without a line break in the window it returns offset.
func lineStart(r io.ReaderAt, offset, window int64) (int64, error) {
	if offset == 0 {
		return 0, nil
	}
	// The byte before offset tells whether offset already starts a line.
	buffer := make([]byte, window+1)
	n, err := r.ReadAt(buffer, offset-1)
	if err != nil && err != io.EOF {
		return 0, err
	}
	if i := bytes.IndexByte(buffer[:n], '\n'); i >= 0 {
		return offset + int64(i), nil
	}
	return offset, nil
}
