package runtime

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"
)

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
	if _, err := file.WriteAt([]byte("later\n"), int64(len("one\ntwo\nthree\n"))); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(got)) != "two\nthree" {
		t.Fatalf("got %q", got)
	}
}
