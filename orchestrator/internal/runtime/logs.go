package runtime

import (
	"context"
	"errors"
	"io"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/containerd/containerd/v2/pkg/cio"
)

const (
	logSegmentBytes = 8 << 20
	logSegmentCount = 4
	maxLogFollowers = 32
	logPollInterval = 200 * time.Millisecond
)

// ErrTooManyLogFollowers indicates that the node's bounded set of follow
// streams is full. Non-following log reads do not consume a slot.
var ErrTooManyLogFollowers = errors.New("too many concurrent log followers")

// rotatingLogWriter keeps the active file and a fixed number of older files.
// Writes from stdout and stderr may be concurrent, so rotation and writes share
// one lock.
type rotatingLogWriter struct {
	mu          sync.Mutex
	path        string
	segmentSize int64
	segments    int
	file        *os.File
	size        int64
}

func newRotatingLogWriter(path string, segmentSize int64, segments int) (*rotatingLogWriter, error) {
	if segmentSize <= 0 || segments <= 0 {
		return nil, errors.New("log retention must have positive segment size and count")
	}
	w := &rotatingLogWriter{path: path, segmentSize: segmentSize, segments: segments}
	if err := w.open(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *rotatingLogWriter) open() error {
	file, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return err
	}
	w.file = file
	w.size = info.Size()
	if w.size > w.segmentSize {
		return w.trimOversizedActive()
	}
	return nil
}

func (w *rotatingLogWriter) trimOversizedActive() error {
	if err := w.file.Close(); err != nil {
		return err
	}
	w.file = nil
	source, err := os.Open(w.path)
	if err != nil {
		return err
	}
	defer func() { _ = source.Close() }()
	if _, err := source.Seek(w.size-w.segmentSize, io.SeekStart); err != nil {
		return err
	}
	temporary := w.path + ".trim"
	target, err := os.OpenFile(temporary, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, copyErr := io.CopyN(target, source, w.segmentSize)
	closeErr := target.Close()
	if copyErr != nil || closeErr != nil {
		_ = os.Remove(temporary)
		return errors.Join(copyErr, closeErr)
	}
	if err := os.Rename(temporary, w.path); err != nil {
		_ = os.Remove(temporary)
		return err
	}
	w.size = 0
	return w.open()
}

func (w *rotatingLogWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return 0, os.ErrClosed
	}
	written := 0
	for len(p) > 0 {
		if w.size == w.segmentSize {
			if err := w.rotate(); err != nil {
				return written, err
			}
		}
		remaining := w.segmentSize - w.size
		chunk := p
		if int64(len(chunk)) > remaining {
			chunk = chunk[:remaining]
		}
		n, err := w.file.Write(chunk)
		written += n
		w.size += int64(n)
		p = p[n:]
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}

func (w *rotatingLogWriter) rotate() error {
	if err := w.file.Close(); err != nil {
		return err
	}
	w.file = nil
	if w.segments == 1 {
		if err := os.Remove(w.path); err != nil && !os.IsNotExist(err) {
			return err
		}
	} else {
		oldest := logSegmentPath(w.path, w.segments-1)
		if err := os.Remove(oldest); err != nil && !os.IsNotExist(err) {
			return err
		}
		for generation := w.segments - 2; generation >= 0; generation-- {
			from := logSegmentPath(w.path, generation)
			to := logSegmentPath(w.path, generation+1)
			if err := os.Rename(from, to); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}
	file, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	w.file = file
	w.size = 0
	return nil
}

func (w *rotatingLogWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

func logSegmentPath(path string, generation int) string {
	if generation == 0 {
		return path
	}
	return path + "." + strconv.Itoa(generation)
}

func rotatingLogCreator(binary, path string) cio.Creator {
	uri, err := cio.LogURIGenerator("binary-v2", binary, map[string]string{"log-sink": path})
	if err != nil {
		return func(string) (cio.IO, error) { return nil, err }
	}
	return cio.LogURI(uri)
}

// RunLogSink runs the shim-owned logging process. containerd supplies task
// stdout, stderr, and the binary-v2 readiness pipe as descriptors 3, 4, and 5.
// The process belongs to the containerd shim, so task output remains consumed
// while the Trellis daemon is restarting.
func RunLogSink(path string) error {
	stdout := os.NewFile(3, "task-stdout")
	stderr := os.NewFile(4, "task-stderr")
	ready := os.NewFile(5, "container-wait")
	if stdout == nil || stderr == nil || ready == nil {
		return errors.New("containerd log descriptors are unavailable")
	}
	defer func() { _ = stdout.Close() }()
	defer func() { _ = stderr.Close() }()
	defer func() { _ = ready.Close() }()
	return runLogSink(path, stdout, stderr, ready)
}

func runLogSink(path string, stdout, stderr io.Reader, ready io.WriteCloser) error {
	writer, err := newRotatingLogWriter(path, logSegmentBytes, logSegmentCount)
	if err != nil {
		return err
	}
	if _, err := ready.Write([]byte{1}); err != nil {
		_ = writer.Close()
		return err
	}
	if err := ready.Close(); err != nil {
		_ = writer.Close()
		return err
	}

	errs := make(chan error, 2)
	copyStream := func(stream io.Reader) {
		_, err := io.CopyBuffer(writer, stream, make([]byte, 32*1024))
		errs <- err
	}
	go copyStream(stdout)
	go copyStream(stderr)
	return errors.Join(<-errs, <-errs, writer.Close())
}

type logSegment struct {
	file   *os.File
	start  int64
	length int64
}

func openLogSegments(path string, count int) ([]logSegment, error) {
	segments := make([]logSegment, 0, count)
	var offset int64
	for generation := count - 1; generation >= 0; generation-- {
		file, err := os.Open(logSegmentPath(path, generation))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			closeLogSegments(segments)
			return nil, err
		}
		info, err := file.Stat()
		if err != nil {
			_ = file.Close()
			closeLogSegments(segments)
			return nil, err
		}
		segments = append(segments, logSegment{file: file, start: offset, length: info.Size()})
		offset += info.Size()
	}
	if len(segments) == 0 {
		return nil, os.ErrNotExist
	}
	return segments, nil
}

func closeLogSegments(segments []logSegment) {
	for _, segment := range segments {
		_ = segment.file.Close()
	}
}

func tailPosition(segments []logSegment, tail int) (int64, error) {
	if tail <= 0 {
		return 0, nil
	}
	total := segments[len(segments)-1].start + segments[len(segments)-1].length
	position := total
	lines := 0
	buffer := make([]byte, 32*1024)
	for position > 0 {
		start := max(int64(0), position-int64(len(buffer)))
		n, err := readSegmentsAt(segments, buffer[:position-start], start)
		if err != nil && err != io.EOF {
			return 0, err
		}
		for i := n - 1; i >= 0; i-- {
			if buffer[i] == '\n' && start+int64(i) < total-1 {
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

func readSegmentsAt(segments []logSegment, p []byte, offset int64) (int, error) {
	written := 0
	for _, segment := range segments {
		if offset >= segment.start+segment.length {
			continue
		}
		within := max(int64(0), offset-segment.start)
		n, err := segment.file.ReadAt(p[written:], within)
		written += n
		offset += int64(n)
		if written == len(p) {
			return written, nil
		}
		if err != nil && err != io.EOF {
			return written, err
		}
	}
	return written, io.EOF
}

type segmentedLogReader struct {
	ctx            context.Context
	path           string
	segments       []logSegment
	index          int
	follow         bool
	active         bool
	detectRotation bool
	release        func()
	closeOnce      sync.Once
}

func newSegmentedLogReader(ctx context.Context, path string, segmentCount int, follow bool, tail int, release func()) (io.ReadCloser, error) {
	segments, err := openLogSegments(path, segmentCount)
	if err != nil {
		if release != nil {
			release()
		}
		return nil, err
	}
	start, err := tailPosition(segments, tail)
	if err != nil {
		closeLogSegments(segments)
		if release != nil {
			release()
		}
		return nil, err
	}
	reader := &segmentedLogReader{ctx: ctx, path: path, segments: segments, follow: follow, detectRotation: true, release: release}
	for reader.index < len(segments)-1 && start >= segments[reader.index].start+segments[reader.index].length {
		reader.index++
	}
	if reader.index < len(segments) {
		segment := segments[reader.index]
		if _, err := segment.file.Seek(start-segment.start, io.SeekStart); err != nil {
			_ = reader.Close()
			return nil, err
		}
		reader.active = reader.index == len(segments)-1
	}
	return reader, nil
}

func (r *segmentedLogReader) Read(p []byte) (int, error) {
	for {
		if r.index >= len(r.segments) {
			if !r.follow {
				return 0, io.EOF
			}
			if err := r.wait(); err != nil {
				return 0, err
			}
			continue
		}
		segment := &r.segments[r.index]
		n, err := segment.file.Read(p)
		if n > 0 {
			return n, nil
		}
		if err != nil && err != io.EOF {
			return 0, err
		}
		if !r.active {
			_ = segment.file.Close()
			r.index++
			if r.index < len(r.segments) {
				r.active = r.index == len(r.segments)-1
			}
			continue
		}
		if !r.follow {
			return 0, io.EOF
		}
		rotated, err := r.activeRotated(segment.file)
		if err != nil {
			return 0, err
		}
		if rotated {
			file, err := os.Open(r.path)
			if err != nil {
				if os.IsNotExist(err) {
					if err := r.wait(); err != nil {
						return 0, err
					}
					continue
				}
				return 0, err
			}
			_ = segment.file.Close()
			// Discard closed segment metadata as well as file descriptors so a
			// long-lived follower stays bounded across unlimited rotations.
			r.segments = []logSegment{{file: file}}
			r.index = 0
			continue
		}
		if err := r.wait(); err != nil {
			return 0, err
		}
	}
}

func (r *segmentedLogReader) activeRotated(file *os.File) (bool, error) {
	if !r.detectRotation {
		return false, nil
	}
	opened, err := file.Stat()
	if err != nil {
		return false, err
	}
	current, err := os.Stat(r.path)
	if os.IsNotExist(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return !os.SameFile(opened, current), nil
}

func (r *segmentedLogReader) wait() error {
	timer := time.NewTimer(logPollInterval)
	defer timer.Stop()
	select {
	case <-r.ctx.Done():
		return r.ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (r *segmentedLogReader) Close() error {
	var closeErr error
	r.closeOnce.Do(func() {
		closeLogSegments(r.segments)
		if r.release != nil {
			r.release()
		}
		closeErr = nil
	})
	return closeErr
}

func newLogReader(ctx context.Context, file *os.File, follow bool, tail int) (io.ReadCloser, error) {
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	segments := []logSegment{{file: file, length: info.Size()}}
	start, err := tailPosition(segments, tail)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if _, err := file.Seek(start, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, err
	}
	return &segmentedLogReader{ctx: ctx, segments: segments, follow: follow, active: true}, nil
}
