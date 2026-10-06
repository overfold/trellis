package transport

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestStreamLimiterBoundsGlobalAndPerKey(t *testing.T) {
	limiter := NewStreamLimiter(3, 2)
	releaseA1, ok := limiter.Acquire("a")
	if !ok {
		t.Fatal("first stream rejected")
	}
	if _, ok := limiter.Acquire("a"); !ok {
		t.Fatal("second stream for one key rejected")
	}
	if _, ok := limiter.Acquire("a"); ok {
		t.Fatal("per-key limit not enforced")
	}
	if _, ok := limiter.Acquire("b"); !ok {
		t.Fatal("another key rejected below the global limit")
	}
	if _, ok := limiter.Acquire("c"); ok {
		t.Fatal("global limit not enforced")
	}
	releaseA1()
	releaseA1() // release is idempotent and must not free a second slot
	if _, ok := limiter.Acquire("c"); !ok {
		t.Fatal("released slot not reusable")
	}
	if _, ok := limiter.Acquire("c"); ok {
		t.Fatal("double release freed an extra slot")
	}
}

func TestNilStreamLimiterAdmitsEverything(t *testing.T) {
	release, ok := (*StreamLimiter)(nil).Acquire("a")
	if !ok {
		t.Fatal("nil limiter rejected a stream")
	}
	release()
}

func TestReleaseOnCloseReleasesOnce(t *testing.T) {
	released := 0
	rc := ReleaseOnClose(io.NopCloser(strings.NewReader("")), func() { released++ })
	_ = rc.Close()
	if released != 1 {
		t.Fatalf("released %d times, want 1", released)
	}
}

func TestCopyStreamCopiesToEOF(t *testing.T) {
	recorder := httptest.NewRecorder()
	if err := CopyStream(recorder, strings.NewReader("hello\nworld\n"), time.Second); err != nil {
		t.Fatal(err)
	}
	if got := recorder.Body.String(); got != "hello\nworld\n" {
		t.Fatalf("copied %q", got)
	}
}

// TestCopyStreamEndsWhenClientStopsReading shows a stalled client no longer
// pins the stream: the write deadline fails the copy.
func TestCopyStreamEndsWhenClientStopsReading(t *testing.T) {
	result := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		chunk := strings.Repeat("x", 1<<20)
		result <- CopyStream(w, endlessReader{chunk}, 100*time.Millisecond)
	}))
	defer server.Close()
	response, err := http.Get(server.URL) //nolint:noctx // test client; the body is never read
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("copy to a stalled client succeeded")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("stalled client pinned the stream")
	}
}

type endlessReader struct{ chunk string }

func (r endlessReader) Read(p []byte) (int, error) { return copy(p, r.chunk), nil }
