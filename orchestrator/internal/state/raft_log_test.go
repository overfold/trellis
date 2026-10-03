package state

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
)

type fakeLimiterClock struct {
	now     time.Time
	pending []func()
}

func newTestLimiter(burst, maxKeys int) (*logLimiter, *fakeLimiterClock, *[]logSummary) {
	clock := &fakeLimiterClock{now: time.Unix(1_700_000_000, 0)}
	var got []logSummary
	l := newLogLimiter(time.Minute, burst, maxKeys, func(s []logSummary) { got = append(got, s...) })
	l.now = func() time.Time { return clock.now }
	l.after = func(_ time.Duration, f func()) { clock.pending = append(clock.pending, f) }
	return l, clock, &got
}

func (c *fakeLimiterClock) fire() {
	pending := c.pending
	c.pending = nil
	for _, f := range pending {
		f()
	}
}

func TestLogLimiterSuppressesAndSummarizes(t *testing.T) {
	l, clock, got := newTestLimiter(3, 16)
	meta := logSummary{name: "raft.transport", level: hclog.Error, msg: "failed to decode incoming command"}

	allowed := 0
	for range 100 {
		if l.allow("k", meta) {
			allowed++
		}
	}
	if allowed != 3 {
		t.Fatalf("allowed %d messages in one window, want 3", allowed)
	}
	if len(*got) != 0 {
		t.Fatalf("summary emitted before window closed: %+v", *got)
	}
	if len(clock.pending) != 1 {
		t.Fatalf("expected one armed flush, got %d", len(clock.pending))
	}

	// The flood stops; the armed flush still reports it once the window closes.
	clock.now = clock.now.Add(time.Minute)
	clock.fire()
	if len(*got) != 1 {
		t.Fatalf("expected one summary, got %+v", *got)
	}
	s := (*got)[0]
	if s.suppressed != 97 || s.msg != meta.msg || s.name != meta.name || s.level != hclog.Error {
		t.Fatalf("unexpected summary %+v", s)
	}
	if len(l.entries) != 0 {
		t.Fatalf("expected state dropped after flush, have %d entries", len(l.entries))
	}
	if len(clock.pending) != 0 {
		t.Fatal("flush re-armed with nothing suppressed")
	}

	// A fresh window allows a new burst.
	if !l.allow("k", meta) {
		t.Fatal("message suppressed in a new window")
	}
}

func TestLogLimiterSummarizesOnNextMessageAfterWindow(t *testing.T) {
	l, clock, got := newTestLimiter(1, 16)
	meta := logSummary{name: "raft", level: hclog.Warn, msg: "m"}
	l.allow("k", meta)
	l.allow("k", meta)
	l.allow("k", meta)
	clock.now = clock.now.Add(2 * time.Minute)
	if !l.allow("k", meta) {
		t.Fatal("first message of a new window suppressed")
	}
	if len(*got) != 1 || (*got)[0].suppressed != 2 {
		t.Fatalf("expected summary of 2, got %+v", *got)
	}
}

func TestLogLimiterBoundsState(t *testing.T) {
	const maxKeys = 8
	l, clock, got := newTestLimiter(2, maxKeys)
	allowed := 0
	for i := range 10_000 {
		meta := logSummary{name: "raft.transport", level: hclog.Error, msg: fmt.Sprintf("msg-%d", i)}
		if l.allow(meta.msg, meta) {
			allowed++
		}
		if len(l.entries) > maxKeys+1 {
			t.Fatalf("suppression state grew to %d entries, bound is %d", len(l.entries), maxKeys+1)
		}
	}
	// maxKeys distinct keys each get their burst, then new keys share the
	// overflow bucket and its burst.
	if want := maxKeys + 2; allowed != want {
		t.Fatalf("allowed %d distinct messages, want %d", allowed, want)
	}

	clock.now = clock.now.Add(time.Minute)
	clock.fire()
	var overflow *logSummary
	for i := range *got {
		if (*got)[i].msg == raftLogOverflowMsg {
			overflow = &(*got)[i]
		}
	}
	if overflow == nil || overflow.suppressed != 10_000-maxKeys-2 {
		t.Fatalf("expected overflow summary of %d, got %+v", 10_000-maxKeys-2, *got)
	}
	if len(l.entries) != 0 {
		t.Fatalf("expected empty state after flush, have %d", len(l.entries))
	}
}

func TestLogLimiterReusesExpiredSlotsWhenFull(t *testing.T) {
	l, clock, _ := newTestLimiter(1, 2)
	l.allow("a", logSummary{msg: "a"})
	l.allow("b", logSummary{msg: "b"})
	clock.now = clock.now.Add(time.Minute)
	if !l.allow("c", logSummary{msg: "c"}) {
		t.Fatal("new key suppressed although expired slots were available")
	}
	if _, ok := l.entries["c"]; !ok {
		t.Fatal("new key went to overflow instead of an expired slot")
	}
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) records(t *testing.T) []map[string]any {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []map[string]any
	for line := range strings.SplitSeq(strings.TrimSpace(b.buf.String()), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("decode log line %q: %v", line, err)
		}
		out = append(out, rec)
	}
	return out
}

func newBufferLogger() (*slog.Logger, *syncBuffer) {
	buf := &syncBuffer{}
	return slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo})), buf
}

func TestRaftLoggerLevelNamesAndRedaction(t *testing.T) {
	out, buf := newBufferLogger()
	l := newRaftLogger(out, "raft")
	transport := l.Named("transport").With("peer", "10.0.0.2:8129")

	transport.Info("below warn")
	transport.Error("failed", "error", errors.New(strings.Repeat("x", 4096)), "data", []byte("secret payload"), "dangling")

	recs := buf.records(t)
	if len(recs) != 1 {
		t.Fatalf("expected one record at Warn+, got %d: %+v", len(recs), recs)
	}
	rec := recs[0]
	if rec["level"] != "ERROR" || rec["msg"] != "failed" || rec["logger"] != "raft.transport" || rec["peer"] != "10.0.0.2:8129" {
		t.Fatalf("unexpected record %+v", rec)
	}
	if got := rec["error"].(string); len(got) > raftLogMaxValue+64 || !strings.Contains(got, "truncated") {
		t.Fatalf("error value not truncated: %d bytes", len(got))
	}
	if got := rec["data"]; got != "<14 bytes elided>" {
		t.Fatalf("byte payload not elided: %v", got)
	}
	if strings.Contains(buf.buf.String(), "secret payload") {
		t.Fatal("raw bytes leaked into log output")
	}
	if rec[hclog.MissingKey] != "dangling" {
		t.Fatalf("odd argument dropped: %+v", rec)
	}
}

func TestRaftLoggerSuppressesPerPeer(t *testing.T) {
	out, buf := newBufferLogger()
	l := newRaftLogger(out, "raft")
	now := time.Unix(1_700_000_000, 0)
	var flush func()
	l.core.limiter.now = func() time.Time { return now }
	l.core.limiter.after = func(_ time.Duration, f func()) { flush = f }
	for range 50 {
		l.Error("failed to appendEntries to", "peer", "a", "error", errors.New("refused"))
		l.Error("failed to appendEntries to", "peer", "b", "error", errors.New("refused"))
	}
	perPeer := map[string]int{}
	for _, rec := range buf.records(t) {
		perPeer[rec["peer"].(string)]++
	}
	if perPeer["a"] != raftLogBurst || perPeer["b"] != raftLogBurst {
		t.Fatalf("expected %d records per peer, got %v", raftLogBurst, perPeer)
	}

	now = now.Add(raftLogInterval)
	flush()
	recs := buf.records(t)
	summaries := recs[2*raftLogBurst:]
	if len(summaries) != 2 {
		t.Fatalf("expected one summary per peer, got %+v", summaries)
	}
	for _, rec := range summaries {
		if rec["msg"] != "suppressed repeated Raft log messages" || rec["level"] != "ERROR" ||
			rec["message"] != "failed to appendEntries to" || rec["logger"] != "raft" || rec["suppressed"] != float64(50-raftLogBurst) {
			t.Fatalf("unexpected summary %+v", rec)
		}
	}
}

// A hostile or misconfigured client on the Raft port used to be logged to
// io.Discard. Its failures must now be visible, and bounded.
func TestRaftTransportErrorsAreLoggedAndBounded(t *testing.T) {
	out, buf := newBufferLogger()
	bind := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	store, err := NewRaftStore(RaftConfig{
		DataDir:       t.TempDir(),
		BindAddr:      bind,
		Advertise:     bind,
		ServerID:      bind,
		Bootstrap:     true,
		TLS:           testTLSConfig(t),
		AuthorizePeer: allowAnyRaftPeer,
		Logger:        out,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	waitLeader(t, store)

	const attempts = 40
	for range attempts {
		conn, err := net.Dial("tcp", bind)
		if err != nil {
			t.Fatal(err)
		}
		// Not a TLS ClientHello, so the server handshake fails.
		_, _ = conn.Write([]byte("GET / HTTP/1.1\r\n\r\n"))
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, _ = conn.Read(make([]byte, 1))
		_ = conn.Close()
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		count := 0
		for _, rec := range buf.records(t) {
			if rec["logger"] == "raft.transport" && rec["msg"] == "failed to decode incoming command" {
				count++
			}
		}
		if count == raftLogBurst {
			break
		}
		if count > raftLogBurst {
			t.Fatalf("transport logged %d decode failures, want at most %d", count, raftLogBurst)
		}
		if time.Now().After(deadline) {
			t.Fatalf("transport logged %d decode failures, want %d; log:\n%s", count, raftLogBurst, buf.buf.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}
