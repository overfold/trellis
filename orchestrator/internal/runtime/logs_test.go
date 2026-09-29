package runtime

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type bufferWriteCloser struct {
	bytes.Buffer
	closed bool
}

func (w *bufferWriteCloser) Close() error {
	w.closed = true
	return nil
}

func TestLogReaderTailsLines(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "log")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("one\ntwo\nthree\n"); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	reader, err := newLogReader(context.Background(), file, false, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(got)) != "two\nthree" {
		t.Fatalf("got %q", got)
	}
}

func TestRotatingLogWriterBoundsRetentionAtSegmentBoundary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "task.log")
	writer, err := newRotatingLogWriter(path, 4, 3)
	if err != nil {
		t.Fatal(err)
	}
	input := []byte("0123456789ABC")
	if n, err := writer.Write(input); err != nil || n != len(input) {
		t.Fatalf("write = %d, %v", n, err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := newSegmentedLogReader(context.Background(), path, 3, false, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	// Opening the fourth segment discards the oldest complete segment; the
	// retained window is therefore two full segments plus the new active one.
	if want := input[4:]; !bytes.Equal(got, want) {
		t.Fatalf("retained logs = %q, want %q", got, want)
	}
	for generation := 0; generation < 3; generation++ {
		info, err := os.Stat(logSegmentPath(path, generation))
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() > 4 {
			t.Fatalf("segment %d is %d bytes", generation, info.Size())
		}
	}
	if _, err := os.Stat(logSegmentPath(path, 3)); !os.IsNotExist(err) {
		t.Fatalf("unexpected fourth retained segment: %v", err)
	}
}

func TestRunLogSinkSignalsReadyAndCombinesStreams(t *testing.T) {
	path := filepath.Join(t.TempDir(), "task.log")
	ready := &bufferWriteCloser{}
	if err := runLogSink(path, strings.NewReader("stdout"), strings.NewReader("stderr"), ready); err != nil {
		t.Fatal(err)
	}
	if ready.String() != "\x01" || !ready.closed {
		t.Fatalf("readiness = %q, closed=%v", ready.String(), ready.closed)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len("stdoutstderr") || !bytes.Contains(got, []byte("stdout")) || !bytes.Contains(got, []byte("stderr")) {
		t.Fatalf("combined log = %q", got)
	}
}

func TestRotatingLogCreatorUsesShimOwnedBinaryLogger(t *testing.T) {
	created, err := rotatingLogCreator("/usr/local/bin/trellis", "/var/lib/trellis/runtime/task.log")("task")
	if err != nil {
		t.Fatal(err)
	}
	config := created.Config()
	if config.Stdout != config.Stderr || !strings.HasPrefix(config.Stdout, "binary-v2:///usr/local/bin/trellis?") ||
		!strings.Contains(config.Stdout, "log-sink=%2Fvar%2Flib%2Ftrellis%2Fruntime%2Ftask.log") {
		t.Fatalf("logger config = %#v", config)
	}
}

func TestSegmentedLogReaderTailsAcrossRotation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "task.log")
	writer, err := newRotatingLogWriter(path, 5, 4)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("one\ntwo\nthree\nfour\n")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := newSegmentedLogReader(context.Background(), path, 4, false, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "three\nfour\n" {
		t.Fatalf("tail = %q", got)
	}
}

func TestLogFollowReadsAcrossRotation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "task.log")
	writer, err := newRotatingLogWriter(path, 4, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = writer.Close() }()
	if _, err := writer.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader, err := newSegmentedLogReader(ctx, path, 3, true, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	first := make([]byte, 3)
	if _, err := io.ReadFull(reader, first); err != nil || string(first) != "abc" {
		t.Fatalf("initial read = %q, %v", first, err)
	}
	if _, err := writer.Write([]byte("def")); err != nil {
		t.Fatal(err)
	}
	next := make([]byte, 3)
	if _, err := io.ReadFull(reader, next); err != nil || string(next) != "def" {
		t.Fatalf("read after rotation = %q, %v", next, err)
	}
}

func TestLogFollowDoesNotAccumulateRotatedSegments(t *testing.T) {
	path := filepath.Join(t.TempDir(), "task.log")
	writer, err := newRotatingLogWriter(path, 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = writer.Close() }()
	if _, err := writer.Write([]byte("a")); err != nil {
		t.Fatal(err)
	}
	reader, err := newSegmentedLogReader(context.Background(), path, 2, true, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	segmented := reader.(*segmentedLogReader)
	initial := make([]byte, 1)
	if _, err := io.ReadFull(reader, initial); err != nil || string(initial) != "a" {
		t.Fatalf("initial read = %q, %v", initial, err)
	}
	for _, chunk := range []string{"bc", "de", "fg", "hi"} {
		if _, err := writer.Write([]byte(chunk)); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, len(chunk))
		if _, err := io.ReadFull(reader, got); err != nil {
			t.Fatal(err)
		}
		if string(got) != chunk {
			t.Fatalf("follow read = %q, want %q", got, chunk)
		}
	}
	if len(segmented.segments) != 1 {
		t.Fatalf("follower retained %d segment records after rotation", len(segmented.segments))
	}
}

func TestLogFollowCancellationInterruptsRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "task.log")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	reader, err := newSegmentedLogReader(ctx, path, 1, true, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := reader.Read(make([]byte, 1))
		done <- err
	}()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("read error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("follow read did not stop after cancellation")
	}
	_ = reader.Close()
}

func TestLogReaderStreamsHugeNewlineFreeRecordInCallerSizedChunks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "task.log")
	if err := os.WriteFile(path, bytes.Repeat([]byte("x"), 2<<20), 0o600); err != nil {
		t.Fatal(err)
	}
	reader, err := newSegmentedLogReader(context.Background(), path, 1, false, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	buffer := make([]byte, 1024)
	if n, err := reader.Read(buffer); err != nil || n != len(buffer) {
		t.Fatalf("first bounded read = %d, %v", n, err)
	}
}
