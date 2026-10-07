package runtime

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func newRotationTestRuntime(t *testing.T) *ContainerdRuntime {
	t.Helper()
	dir := t.TempDir()
	return &ContainerdRuntime{logDir: dir, legacyLogDir: filepath.Join(dir, "absent")}
}

// openTaskLog opens the active log of container "task" the way the containerd shim does.
func openTaskLog(t *testing.T, r *ContainerdRuntime) *os.File {
	t.Helper()
	file, err := os.OpenFile(r.logPath("task"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	return file
}

func write(t *testing.T, file *os.File, data string) {
	t.Helper()
	if _, err := file.WriteString(data); err != nil {
		t.Fatal(err)
	}
}

func readLog(t *testing.T, r *ContainerdRuntime, tail int) string {
	t.Helper()
	stream, err := r.Logs(context.Background(), "task", false, tail)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.Close() }()
	data, err := io.ReadAll(stream)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func readN(t *testing.T, stream io.Reader, n int) string {
	t.Helper()
	data := make([]byte, n)
	done := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(stream, data)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out reading %d bytes", n)
	}
	return string(data)
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}

func TestEnforceLogLimitKeepsNewestOutputAndShimKeepsAppending(t *testing.T) {
	r := newRotationTestRuntime(t)
	shim := openTaskLog(t, r)
	write(t, shim, "0123456789abcdefghij")
	if err := r.EnforceLogLimit(8); err != nil {
		t.Fatal(err)
	}
	if got := fileSize(t, r.logPath("task")); got != 0 {
		t.Fatalf("active log size after rotation = %d", got)
	}
	if got, err := os.ReadFile(r.logPath("task") + rotatedLogSuffix); err != nil || string(got) != "ghij" {
		t.Fatalf("rotated log = %q, %v", got, err)
	}
	// The shim's O_APPEND descriptor writes at the new end of the active log.
	write(t, shim, "KL")
	if got := readLog(t, r, 0); got != "ghijKL" {
		t.Fatalf("retained output = %q", got)
	}
	if got := fileSize(t, r.logPath("task")); got != 2 {
		t.Fatalf("active log size after append = %d", got)
	}
}

func TestEnforceLogLimitLeavesSmallLogsAndOtherFiles(t *testing.T) {
	r := newRotationTestRuntime(t)
	write(t, openTaskLog(t, r), "1234")
	if err := os.WriteFile(filepath.Join(r.logDir, "task-hosts"), make([]byte, 100), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := r.EnforceLogLimit(8); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(r.logPath("task") + rotatedLogSuffix); !os.IsNotExist(err) {
		t.Fatalf("log at half the limit was rotated: %v", err)
	}
	if got := fileSize(t, filepath.Join(r.logDir, "task-hosts")); got != 100 {
		t.Fatalf("non-log file size = %d", got)
	}
}

func TestEnforceLogLimitWithoutLogDirectory(t *testing.T) {
	r := &ContainerdRuntime{logDir: filepath.Join(t.TempDir(), "missing")}
	if err := r.EnforceLogLimit(8); err != nil {
		t.Fatal(err)
	}
}

func TestLogTailSpansRotatedAndActiveLogs(t *testing.T) {
	r := newRotationTestRuntime(t)
	shim := openTaskLog(t, r)
	write(t, shim, "one\ntwo\nthree\n")
	if err := r.EnforceLogLimit(20); err != nil {
		t.Fatal(err)
	}
	write(t, shim, "four\n")
	for tail, want := range map[int]string{
		1:  "four\n",
		2:  "three\nfour\n",
		3:  "two\nthree\nfour\n",
		10: "two\nthree\nfour\n",
		0:  "two\nthree\nfour\n",
	} {
		if got := readLog(t, r, tail); got != want {
			t.Fatalf("tail %d = %q, want %q", tail, got, want)
		}
	}
}

func TestLogFollowContinuesAcrossRotation(t *testing.T) {
	r := newRotationTestRuntime(t)
	shim := openTaskLog(t, r)
	write(t, shim, "abcdef")
	stream, err := r.Logs(t.Context(), "task", true, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.Close() }()
	if got := readN(t, stream, 6); got != "abcdef" {
		t.Fatalf("initial output = %q", got)
	}
	write(t, shim, "ghij")
	if err := r.EnforceLogLimit(8); err != nil {
		t.Fatal(err)
	}
	write(t, shim, "KLM")
	// The follower had read through "f": it resumes in the rotated file at
	// "g" and continues in the truncated active log, without repeating.
	if got := readN(t, stream, 7); got != "ghijKLM" {
		t.Fatalf("output across rotation = %q", got)
	}
}

func TestLogFollowerMoreThanOneRotationBehindSkipsDiscardedOutput(t *testing.T) {
	r := newRotationTestRuntime(t)
	shim := openTaskLog(t, r)
	write(t, shim, "ab")
	stream, err := r.Logs(context.Background(), "task", true, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.Close() }()
	if got := readN(t, stream, 2); got != "ab" {
		t.Fatalf("initial output = %q", got)
	}
	write(t, shim, "cdefgh")
	if err := r.EnforceLogLimit(4); err != nil {
		t.Fatal(err)
	}
	write(t, shim, "ijkl")
	if err := r.EnforceLogLimit(4); err != nil {
		t.Fatal(err)
	}
	write(t, shim, "m")
	if got := readN(t, stream, 3); got != "klm" {
		t.Fatalf("output after two rotations = %q", got)
	}
}

func TestRemoveRetainedLogsRemovesRotatedFiles(t *testing.T) {
	r := newRotationTestRuntime(t)
	write(t, openTaskLog(t, r), "0123456789")
	if err := r.EnforceLogLimit(4); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(r.logPath("task")+rotatedLogSuffix+".tmp", []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	usage, err := r.LogUsage()
	if err != nil {
		t.Fatal(err)
	}
	if usage.Bytes != 2+7 {
		t.Fatalf("log usage = %d, want rotated and temporary bytes", usage.Bytes)
	}
	if err := r.RemoveRetainedLogs("task"); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(r.logDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("files left after removal: %v", entries)
	}
}

func TestLogFollowStopsOnCloseAndCancellation(t *testing.T) {
	r := newRotationTestRuntime(t)
	openTaskLog(t, r)
	ctx, cancel := context.WithCancel(context.Background())
	cancelled, err := r.Logs(ctx, "task", true, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cancelled.Close() }()
	closed, err := r.Logs(context.Background(), "task", true, 0)
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 2)
	for _, stream := range []io.Reader{cancelled, closed} {
		go func() {
			_, err := stream.Read(make([]byte, 1))
			results <- err
		}()
	}
	cancel()
	_ = closed.Close()
	for range 2 {
		select {
		case err := <-results:
			if !errors.Is(err, io.EOF) && !errors.Is(err, os.ErrClosed) {
				t.Fatalf("read error = %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("follow read did not stop")
		}
	}
}

// TestLogFollowUnderConcurrentRotation writes numbered lines while rotating
// the log and checks that a follower never repeats or reorders output. The
// rotations here come far faster than the follower polls, so it falls more
// than one rotation behind and gaps are expected.
func TestLogFollowUnderConcurrentRotation(t *testing.T) {
	r := newRotationTestRuntime(t)
	shim := openTaskLog(t, r)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := r.Logs(ctx, "task", true, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.Close() }()

	const lines = 20000
	var wg sync.WaitGroup
	wg.Add(2)
	writerDone := make(chan struct{})
	go func() {
		defer wg.Done()
		defer close(writerDone)
		for i := range lines {
			if _, err := fmt.Fprintf(shim, "%08d\n", i); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-writerDone:
				return
			default:
			}
			if err := r.EnforceLogLimit(4096); err != nil {
				t.Error(err)
				return
			}
		}
	}()

	go func() {
		// Rotation can lose output written while it copies, so mark the end
		// only once rotation has stopped.
		wg.Wait()
		_, _ = shim.WriteString("end\n")
	}()

	scanner := bufio.NewScanner(stream)
	last := -1
	deadline := time.AfterFunc(30*time.Second, cancel)
	defer deadline.Stop()
	for scanner.Scan() {
		line := scanner.Text()
		if line == "end" {
			break
		}
		value, err := strconv.Atoi(line)
		if err != nil || len(line) != 8 {
			// A line cut by output lost during rotation.
			continue
		}
		if value <= last {
			t.Fatalf("line %d after %d: output repeated or reordered", value, last)
		}
		last = value
	}
	if scanner.Text() != "end" {
		t.Fatalf("follower stopped after line %d (scan error %v)", last, scanner.Err())
	}
	if size := fileSize(t, r.logPath("task")) + fileSize(t, r.logPath("task")+rotatedLogSuffix); size > 4*4096 {
		t.Fatalf("retained %d bytes for a 4096-byte limit", size)
	}
	if !strings.HasSuffix(readLog(t, r, 1), "end\n") {
		t.Fatal("retained output does not end with the newest line")
	}
}

func TestEnforceLogLimitStartsRotatedLogAtLineBoundary(t *testing.T) {
	r := newRotationTestRuntime(t)
	shim := openTaskLog(t, r)
	write(t, shim, "first line\nsecond\nthird\n")
	// Keeping the newest 10 bytes would start mid-way through "second".
	if err := r.EnforceLogLimit(20); err != nil {
		t.Fatal(err)
	}
	if got := readLog(t, r, 0); got != "third\n" {
		t.Fatalf("retained output = %q", got)
	}
	write(t, shim, strings.Repeat("x", 30))
	// Without a line break near the cut, rotation keeps the newest bytes.
	if err := r.EnforceLogLimit(20); err != nil {
		t.Fatal(err)
	}
	if got := readLog(t, r, 0); got != strings.Repeat("x", 10) {
		t.Fatalf("retained output without line breaks = %q", got)
	}
}

func TestEnforceLogLimitKeepsNewestOutputWhenOnlyLineBreakIsLast(t *testing.T) {
	r := newRotationTestRuntime(t)
	write(t, openTaskLog(t, r), strings.Repeat("x", 29)+"\n")
	if err := r.EnforceLogLimit(20); err != nil {
		t.Fatal(err)
	}
	if got := readLog(t, r, 0); got != strings.Repeat("x", 9)+"\n" {
		t.Fatalf("retained output = %q", got)
	}
}

// TestEnforceLogLimitFreesSpaceWhenFilesystemIsFull needs root to mount a
// small tmpfs and is skipped otherwise.
func TestEnforceLogLimitFreesSpaceWhenFilesystemIsFull(t *testing.T) {
	dir := t.TempDir()
	if err := syscall.Mount("tmpfs", dir, "tmpfs", 0, "size=64k"); err != nil {
		t.Skipf("mount tmpfs: %v", err)
	}
	t.Cleanup(func() { _ = syscall.Unmount(dir, 0) })
	r := &ContainerdRuntime{logDir: dir, legacyLogDir: filepath.Join(dir, "absent")}
	shim := openTaskLog(t, r)
	write(t, shim, "old\n")
	follower, err := r.Logs(t.Context(), "task", true, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = follower.Close() }()
	if got := readN(t, follower, 4); got != "old\n" {
		t.Fatalf("initial output = %q", got)
	}
	// Fill the filesystem so the rotated copy cannot be written.
	chunk := []byte(strings.Repeat("x", 1023) + "\n")
	for {
		if _, err := shim.Write(chunk); err != nil {
			if !errors.Is(err, syscall.ENOSPC) {
				t.Fatal(err)
			}
			break
		}
	}
	err = r.EnforceLogLimit(32 << 10)
	if !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("enforce on a full filesystem = %v, want ENOSPC reported", err)
	}
	if got := fileSize(t, r.logPath("task")); got != 0 {
		t.Fatalf("active log size = %d, want truncated to free space", got)
	}
	if _, err := os.Stat(r.logPath("task") + rotatedLogSuffix); !os.IsNotExist(err) {
		t.Fatalf("rotated log after discarding: %v", err)
	}
	// The task can write again, and the follower resumes with new output
	// instead of replaying discarded output.
	write(t, shim, "new\n")
	if got := readN(t, follower, 4); got != "new\n" {
		t.Fatalf("output after freeing space = %q", got)
	}
}
