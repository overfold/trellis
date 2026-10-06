package transport

import (
	"errors"
	"io"
	"net/http"
	"sync"
	"time"
)

// StreamLimiter bounds concurrent long-lived streams globally and per key.
// A nil limiter admits every stream.
type StreamLimiter struct {
	mu     sync.Mutex
	global int
	perKey int
	total  int
	byKey  map[string]int
}

// NewStreamLimiter returns a limiter admitting at most global streams, and at
// most perKey for any one key.
func NewStreamLimiter(global, perKey int) *StreamLimiter {
	return &StreamLimiter{global: global, perKey: perKey, byKey: make(map[string]int)}
}

// Acquire claims a slot for key. The returned release func is idempotent and
// must be called when the stream ends. ok is false when a limit is reached.
func (l *StreamLimiter) Acquire(key string) (release func(), ok bool) {
	if l == nil {
		return func() {}, true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.total >= l.global || l.byKey[key] >= l.perKey {
		return nil, false
	}
	l.total++
	l.byKey[key]++
	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			defer l.mu.Unlock()
			l.total--
			if l.byKey[key]--; l.byKey[key] == 0 {
				delete(l.byKey, key)
			}
		})
	}, true
}

// ReleaseOnClose wraps rc so that closing it also calls release.
func ReleaseOnClose(rc io.ReadCloser, release func()) io.ReadCloser {
	return &releasingReadCloser{ReadCloser: rc, release: release}
}

type releasingReadCloser struct {
	io.ReadCloser
	release func()
}

func (r *releasingReadCloser) Close() error {
	defer r.release()
	return r.ReadCloser.Close()
}

// CopyStream copies r to w, flushing after each chunk. Every write must
// complete within writeTimeout, so a client that stops reading ends the
// stream instead of pinning it indefinitely. Waiting for r has no deadline:
// a quiet source is not a stalled client.
func CopyStream(w http.ResponseWriter, r io.Reader, writeTimeout time.Duration) error {
	rc := http.NewResponseController(w)
	buf := make([]byte, 32<<10)
	for {
		n, readErr := r.Read(buf)
		if n > 0 {
			// Deadline errors mean the writer cannot enforce one; the write
			// proceeds without it.
			_ = rc.SetWriteDeadline(time.Now().Add(writeTimeout))
			if _, err := w.Write(buf[:n]); err != nil {
				return err
			}
			if err := rc.Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
				return err
			}
			// Do not let an expired deadline outlive a quiet period.
			_ = rc.SetWriteDeadline(time.Time{})
		}
		if readErr == io.EOF {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}
