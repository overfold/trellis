package runtime

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func newLogReader(ctx context.Context, file *os.File, follow bool, tail int) (io.ReadCloser, error) {
	if tail > 0 {
		start, err := tailOffset(file, tail)
		if err != nil {
			_ = file.Close()
			return nil, err
		}
		if _, err := file.Seek(start, io.SeekStart); err != nil {
			_ = file.Close()
			return nil, err
		}
	}
	if !follow {
		return file, nil
	}
	reader, writer := io.Pipe()
	go func() {
		defer func() { _ = file.Close() }()
		defer func() { _ = writer.Close() }()
		buf := bufio.NewReader(file)
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			chunk, err := buf.ReadBytes('\n')
			if len(chunk) > 0 {
				if _, werr := writer.Write(chunk); werr != nil {
					return
				}
			}
			if err == nil {
				continue
			}
			if err != io.EOF {
				_ = writer.CloseWithError(err)
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return reader, nil
}

func tailOffset(file *os.File, tail int) (int64, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	position := info.Size()
	lines := 0
	buffer := make([]byte, 32*1024)
	for position > 0 {
		start := max(int64(0), position-int64(len(buffer)))
		n, err := file.ReadAt(buffer[:position-start], start)
		if err != nil && err != io.EOF {
			return 0, err
		}
		for i := n - 1; i >= 0; i-- {
			if buffer[i] == '\n' && start+int64(i) < info.Size()-1 {
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
		if !entry.Type().IsRegular() || !strings.HasSuffix(entry.Name(), ".log") {
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
