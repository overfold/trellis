package runtime

import (
	"context"
	"io"
	"strings"
	"testing"
)

func TestLogReaderTailsLines(t *testing.T) {
	r := newRotationTestRuntime(t)
	file := openTaskLog(t, r)
	write(t, file, "one\ntwo\nthree\n")
	reader, err := r.Logs(context.Background(), "task", false, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	write(t, file, "later\n")
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(got)) != "two\nthree" {
		t.Fatalf("got %q", got)
	}
}
