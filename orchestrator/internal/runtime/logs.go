package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

const logPollInterval = 200 * time.Millisecond

// logReader streams a task log. For a runtime log it reads the rotated file
// before the active one and, across later rotations, resumes in the rotated
// file where it left off in the active log. A reader that falls more than one
// rotation behind skips the output rotation discarded in between.
type logReader struct {
	ctx    context.Context
	follow bool
	// path and rotation are empty for a legacy log, which never rotates.
	path     string
	rotation *logRotation

	// mu serializes Read with Close. Read releases it while waiting for
	// output.
	mu     sync.Mutex
	closed bool
	done   chan struct{}
	active *os.File
	// generation is the rotation generation of the active log content and
	// offset the read position in it.
	generation uint64
	offset     int64
	// rotated is the unread rest of the rotated log, read before the active
	// log.
	rotated     *io.SectionReader
	rotatedFile *os.File
}

// openRuntimeLog opens a runtime task log and positions it tail lines from
// the end of its retained output, which spans the rotated and active files.
func openRuntimeLog(ctx context.Context, path string, rotation *logRotation, follow bool, tail int) (*logReader, error) {
	rotation.mu.Lock()
	defer rotation.mu.Unlock()
	active, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	r := &logReader{ctx: ctx, follow: follow, path: path, rotation: rotation, done: make(chan struct{}), active: active, generation: rotation.generation}
	activeInfo, err := active.Stat()
	if err != nil {
		_ = r.Close()
		return nil, err
	}
	rotated, err := os.Open(path + rotatedLogSuffix)
	if err != nil && !os.IsNotExist(err) {
		_ = r.Close()
		return nil, err
	}
	var rotatedSize int64
	if rotated != nil {
		r.rotatedFile = rotated
		info, err := rotated.Stat()
		if err != nil {
			_ = r.Close()
			return nil, err
		}
		rotatedSize = info.Size()
	}
	start, err := tailOffset(joinedLog{rotated: rotated, rotatedSize: rotatedSize, active: active}, rotatedSize+activeInfo.Size(), tail)
	if err != nil {
		_ = r.Close()
		return nil, err
	}
	if start < rotatedSize {
		r.rotated = io.NewSectionReader(rotated, start, rotatedSize-start)
	} else {
		r.closeRotated()
		r.offset = start - rotatedSize
	}
	return r, nil
}

// newLogReader streams a legacy log file, which Trellis does not rotate.
func newLogReader(ctx context.Context, file *os.File, follow bool, tail int) (io.ReadCloser, error) {
	r := &logReader{ctx: ctx, follow: follow, done: make(chan struct{}), active: file}
	info, err := file.Stat()
	if err != nil {
		_ = r.Close()
		return nil, err
	}
	if r.offset, err = tailOffset(file, info.Size(), tail); err != nil {
		_ = r.Close()
		return nil, err
	}
	return r, nil
}

func (r *logReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for {
		if r.closed {
			return 0, os.ErrClosed
		}
		if r.rotated != nil {
			n, err := r.rotated.Read(p)
			if n > 0 {
				return n, nil
			}
			if err != nil && err != io.EOF {
				return 0, err
			}
			r.closeRotated()
			continue
		}
		n, err := r.active.ReadAt(p, r.offset)
		if err != nil && err != io.EOF {
			return 0, err
		}
		if r.rotation != nil {
			rotated, err := r.followRotation()
			if err != nil {
				return 0, err
			}
			if rotated {
				// The bytes just read may already be the next generation's;
				// the rotated file holds this generation's unread rest.
				continue
			}
		}
		if n > 0 {
			r.offset += int64(n)
			return n, nil
		}
		if !r.follow {
			return 0, io.EOF
		}
		if !r.wait() {
			return 0, io.EOF
		}
	}
}

// followRotation switches the reader to the rotated file when the active log
// was rotated since the reader last checked. The caller holds r.mu.
func (r *logReader) followRotation() (bool, error) {
	r.rotation.mu.Lock()
	defer r.rotation.mu.Unlock()
	if r.rotation.generation == r.generation {
		return false, nil
	}
	var begin int64
	if r.rotation.generation == r.generation+1 {
		begin = max(r.offset-r.rotation.discarded, 0)
	}
	r.generation, r.offset = r.rotation.generation, 0
	file, err := os.Open(r.path + rotatedLogSuffix)
	if os.IsNotExist(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return false, err
	}
	if begin >= info.Size() {
		_ = file.Close()
		return true, nil
	}
	r.rotatedFile = file
	r.rotated = io.NewSectionReader(file, begin, info.Size()-begin)
	return true, nil
}

// wait pauses a follower until more output may be available. It reports false
// once the request is cancelled or the reader is closed. The caller holds r.mu.
func (r *logReader) wait() bool {
	r.mu.Unlock()
	defer r.mu.Lock()
	timer := time.NewTimer(logPollInterval)
	defer timer.Stop()
	select {
	case <-r.ctx.Done():
		return false
	case <-r.done:
		return false
	case <-timer.C:
		return true
	}
}

func (r *logReader) closeRotated() {
	if r.rotatedFile != nil {
		_ = r.rotatedFile.Close()
	}
	r.rotated, r.rotatedFile = nil, nil
}

func (r *logReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	close(r.done)
	r.closeRotated()
	return r.active.Close()
}

// joinedLog reads a rotated log followed by its active log as one stream.
type joinedLog struct {
	rotated     io.ReaderAt
	rotatedSize int64
	active      io.ReaderAt
}

func (j joinedLog) ReadAt(p []byte, offset int64) (int, error) {
	n := 0
	if offset < j.rotatedSize {
		want := min(int64(len(p)), j.rotatedSize-offset)
		read, err := j.rotated.ReadAt(p[:want], offset)
		n += read
		if int64(read) < want {
			if err == nil {
				err = io.EOF
			}
			return n, err
		}
		if n == len(p) {
			return n, nil
		}
		offset += int64(read)
	}
	read, err := j.active.ReadAt(p[n:], offset-j.rotatedSize)
	return n + read, err
}

// tailOffset returns the offset of the line that starts tail lines before the
// end of the first size bytes of r, or zero when there are fewer lines.
func tailOffset(r io.ReaderAt, size int64, tail int) (int64, error) {
	if tail <= 0 {
		return 0, nil
	}
	position := size
	lines := 0
	buffer := make([]byte, 32*1024)
	for position > 0 {
		start := max(int64(0), position-int64(len(buffer)))
		n, err := r.ReadAt(buffer[:position-start], start)
		if err != nil && err != io.EOF {
			return 0, err
		}
		for i := n - 1; i >= 0; i-- {
			if buffer[i] == '\n' && start+int64(i) < size-1 {
				lines++
				if lines == tail {
					return start + int64(i) + 1, nil
				}
			}
		}
		position = start
	}
	return 0, nil
}

// LogUsage reports the total size of the task logs in the runtime log
// directory and the space on its filesystem. Before the first task creates
// the directory, it reports the filesystem of the nearest existing ancestor.
// Logs still written to the legacy temporary directory are not counted.
func (c *ContainerdRuntime) LogUsage() (LogUsage, error) {
	usage, err := logDirUsage(c.logDir)
	if err != nil {
		return LogUsage{}, fmt.Errorf("measure task logs in %s: %w", c.logDir, err)
	}
	return usage, nil
}

func logDirUsage(dir string) (LogUsage, error) {
	var usage LogUsage
	entries, err := os.ReadDir(dir)
	if err != nil && !os.IsNotExist(err) {
		return LogUsage{}, err
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() || !taskLogFile(entry.Name()) {
			continue
		}
		info, err := entry.Info()
		if os.IsNotExist(err) {
			continue // Removed since the directory was read.
		}
		if err != nil {
			return LogUsage{}, err
		}
		usage.Bytes += info.Size()
	}
	var stat syscall.Statfs_t
	for path := dir; ; path = filepath.Dir(path) {
		err := syscall.Statfs(path, &stat)
		if err == nil {
			break
		}
		if !errors.Is(err, syscall.ENOENT) || filepath.Dir(path) == path {
			return LogUsage{}, err
		}
	}
	blockSize := int64(stat.Bsize)                             //nolint:unconvert // Bsize is a small positive block size; its type differs by architecture.
	usage.FilesystemAvailable = int64(stat.Bavail) * blockSize //nolint:gosec // Block counts of a real filesystem fit in int64.
	usage.FilesystemCapacity = int64(stat.Blocks) * blockSize  //nolint:gosec // Block counts of a real filesystem fit in int64.
	return usage, nil
}

// taskLogFile reports whether name is an active, rotated, or partially
// rotated task log in the runtime log directory.
func taskLogFile(name string) bool {
	for _, suffix := range []string{".log", ".log" + rotatedLogSuffix, ".log" + rotatedLogSuffix + ".tmp"} {
		if strings.HasSuffix(name, suffix) {
			return true
		}
	}
	return false
}
