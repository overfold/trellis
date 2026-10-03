package state

import (
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/hashicorp/go-hclog"
)

// raftLogLevel is the minimum level Raft, its transport, and its snapshot
// store log at. Trellis has no configurable log level, so Raft stays quiet
// unless something is wrong.
const raftLogLevel = hclog.Warn

const (
	// raftLogInterval is the suppression window for repeated Raft messages.
	raftLogInterval = time.Minute
	// raftLogBurst is how many identical messages are logged per window
	// before the rest are counted and summarized.
	raftLogBurst = 5
	// raftLogMaxKeys bounds the suppression state. Distinct messages beyond it
	// share one overflow bucket.
	raftLogMaxKeys = 256
	// raftLogMaxValue bounds each logged attribute value, so an error that
	// embeds peer-controlled bytes cannot turn into a large log line.
	raftLogMaxValue = 512
	// raftLogMaxKeyPart bounds the peer part of a suppression key.
	raftLogMaxKeyPart = 128

	raftLogOverflowKey = "\x00overflow"
	raftLogOverflowMsg = "distinct Raft log messages beyond suppression capacity"
)

// raftLogCore is shared by a logger and all of its named or With children so
// they share one level and one suppression table.
type raftLogCore struct {
	out     *slog.Logger
	level   atomic.Int32
	limiter *logLimiter
}

// raftLogger adapts hclog, which hashicorp/raft requires, onto the process
// slog logger. Repeated messages are rate limited, and attribute values are
// truncated and byte slices elided so no Raft payload reaches the log.
type raftLogger struct {
	core    *raftLogCore
	name    string
	implied []any
}

var _ hclog.Logger = (*raftLogger)(nil)

func newRaftLogger(out *slog.Logger, name string) *raftLogger {
	if out == nil {
		out = slog.Default()
	}
	core := &raftLogCore{out: out}
	core.level.Store(int32(raftLogLevel))
	core.limiter = newLogLimiter(raftLogInterval, raftLogBurst, raftLogMaxKeys, core.emitSummaries)
	return &raftLogger{core: core, name: name}
}

func (c *raftLogCore) emitSummaries(summaries []logSummary) {
	for _, s := range summaries {
		c.out.LogAttrs(context.Background(), slogLevel(s.level), "suppressed repeated Raft log messages",
			slog.String("logger", s.name),
			slog.String("message", s.msg),
			slog.Int("suppressed", s.suppressed),
			slog.Duration("window", c.limiter.interval),
		)
	}
}

func (l *raftLogger) Log(level hclog.Level, msg string, args ...any) {
	if level == hclog.Off || level < l.GetLevel() {
		return
	}
	lvl := slogLevel(level)
	ctx := context.Background()
	if !l.core.out.Enabled(ctx, lvl) {
		return
	}
	all := args
	if len(l.implied) > 0 {
		all = append(append(make([]any, 0, len(l.implied)+len(args)), l.implied...), args...)
	}
	key := l.name + "\x00" + level.String() + "\x00" + msg + raftLogPeerKey(all)
	if !l.core.limiter.allow(key, logSummary{name: l.name, level: level, msg: msg}) {
		return
	}
	attrs := append([]slog.Attr{slog.String("logger", l.name)}, raftLogAttrs(all)...)
	l.core.out.LogAttrs(ctx, lvl, msg, attrs...)
}

func (l *raftLogger) Trace(msg string, args ...any) { l.Log(hclog.Trace, msg, args...) }
func (l *raftLogger) Debug(msg string, args ...any) { l.Log(hclog.Debug, msg, args...) }
func (l *raftLogger) Info(msg string, args ...any)  { l.Log(hclog.Info, msg, args...) }
func (l *raftLogger) Warn(msg string, args ...any)  { l.Log(hclog.Warn, msg, args...) }
func (l *raftLogger) Error(msg string, args ...any) { l.Log(hclog.Error, msg, args...) }

func (l *raftLogger) IsTrace() bool { return l.GetLevel() <= hclog.Trace }
func (l *raftLogger) IsDebug() bool { return l.GetLevel() <= hclog.Debug }
func (l *raftLogger) IsInfo() bool  { return l.GetLevel() <= hclog.Info }
func (l *raftLogger) IsWarn() bool  { return l.GetLevel() <= hclog.Warn }
func (l *raftLogger) IsError() bool { return l.GetLevel() <= hclog.Error }

func (l *raftLogger) ImpliedArgs() []any { return append([]any(nil), l.implied...) }

func (l *raftLogger) With(args ...any) hclog.Logger {
	implied := append(append(make([]any, 0, len(l.implied)+len(args)), l.implied...), args...)
	return &raftLogger{core: l.core, name: l.name, implied: implied}
}

func (l *raftLogger) Name() string { return l.name }

func (l *raftLogger) Named(name string) hclog.Logger {
	if l.name != "" {
		name = l.name + "." + name
	}
	return l.ResetNamed(name)
}

func (l *raftLogger) ResetNamed(name string) hclog.Logger {
	return &raftLogger{core: l.core, name: name, implied: l.implied}
}

func (l *raftLogger) SetLevel(level hclog.Level) { l.core.level.Store(int32(level)) }

func (l *raftLogger) GetLevel() hclog.Level { return hclog.Level(l.core.level.Load()) }

func (l *raftLogger) StandardLogger(opts *hclog.StandardLoggerOptions) *log.Logger {
	return log.New(l.StandardWriter(opts), "", 0)
}

func (l *raftLogger) StandardWriter(opts *hclog.StandardLoggerOptions) io.Writer {
	level := hclog.Info
	if opts != nil && opts.ForceLevel != hclog.NoLevel {
		level = opts.ForceLevel
	}
	return &raftLogWriter{logger: l, level: level}
}

type raftLogWriter struct {
	logger *raftLogger
	level  hclog.Level
}

func (w *raftLogWriter) Write(p []byte) (int, error) {
	for line := range strings.SplitSeq(strings.TrimRight(string(p), "\r\n"), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			w.logger.Log(w.level, line)
		}
	}
	return len(p), nil
}

func slogLevel(level hclog.Level) slog.Level {
	switch level {
	case hclog.Trace:
		return slog.LevelDebug - 4
	case hclog.Debug:
		return slog.LevelDebug
	case hclog.Warn:
		return slog.LevelWarn
	case hclog.Error:
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// raftLogPeerKey returns the peer-identifying part of a suppression key, so
// one misbehaving peer does not hide messages about another. Error text is
// deliberately excluded: it varies per connection and would defeat grouping.
func raftLogPeerKey(args []any) string {
	var b strings.Builder
	for i := 0; i+1 < len(args); i += 2 {
		switch fmt.Sprint(args[i]) {
		case "peer", "id", "address", "from", "server-id":
			b.WriteByte(0)
			b.WriteString(truncateLogValue(fmt.Sprint(args[i+1]), raftLogMaxKeyPart))
		}
	}
	return b.String()
}

func raftLogAttrs(args []any) []slog.Attr {
	attrs := make([]slog.Attr, 0, (len(args)+1)/2)
	for i := 0; i < len(args); i += 2 {
		if i+1 == len(args) {
			attrs = append(attrs, raftLogAttr(hclog.MissingKey, args[i]))
			break
		}
		attrs = append(attrs, raftLogAttr(truncateLogValue(fmt.Sprint(args[i]), raftLogMaxKeyPart), args[i+1]))
	}
	return attrs
}

func raftLogAttr(key string, value any) slog.Attr {
	switch v := value.(type) {
	case nil:
		return slog.Any(key, nil)
	case bool:
		return slog.Bool(key, v)
	case int:
		return slog.Int(key, v)
	case int64:
		return slog.Int64(key, v)
	case uint64:
		return slog.Uint64(key, v)
	case float64:
		return slog.Float64(key, v)
	case time.Duration:
		return slog.Duration(key, v)
	case []byte:
		// Never log raw bytes: they may be Raft entries or snapshot data.
		return slog.String(key, fmt.Sprintf("<%d bytes elided>", len(v)))
	case error:
		return slog.String(key, truncateLogValue(v.Error(), raftLogMaxValue))
	case fmt.Stringer:
		return slog.String(key, truncateLogValue(v.String(), raftLogMaxValue))
	case string:
		return slog.String(key, truncateLogValue(v, raftLogMaxValue))
	default:
		return slog.String(key, truncateLogValue(fmt.Sprintf("%v", v), raftLogMaxValue))
	}
}

func truncateLogValue(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return fmt.Sprintf("%s...(%d bytes truncated)", s[:cut], len(s)-cut)
}

// logSummary describes one group of suppressed messages.
type logSummary struct {
	name       string
	level      hclog.Level
	msg        string
	suppressed int
}

type logLimitEntry struct {
	meta  logSummary
	start time.Time
	count int
}

// logLimiter allows up to burst identical messages per key per fixed window
// and counts the rest. Counts are reported once the window closes, either on
// the key's next message or by a timer armed on first suppression, so a flood
// that stops still gets its summary. At most maxKeys keys are tracked, plus
// one overflow bucket shared by distinct keys that arrive while the table is
// full of open windows.
type logLimiter struct {
	interval time.Duration
	burst    int
	maxKeys  int
	emit     func([]logSummary)
	now      func() time.Time
	after    func(time.Duration, func())

	mu      sync.Mutex
	entries map[string]*logLimitEntry
	armed   bool
}

func newLogLimiter(interval time.Duration, burst, maxKeys int, emit func([]logSummary)) *logLimiter {
	return &logLimiter{
		interval: interval,
		burst:    burst,
		maxKeys:  maxKeys,
		emit:     emit,
		now:      time.Now,
		after:    func(d time.Duration, f func()) { time.AfterFunc(d, f) },
		entries:  make(map[string]*logLimitEntry),
	}
}

// allow reports whether a message with key should be logged.
func (l *logLimiter) allow(key string, meta logSummary) bool {
	l.mu.Lock()
	now := l.now()
	var out []logSummary
	e := l.entries[key]
	if e != nil && now.Sub(e.start) >= l.interval {
		out = appendSummary(out, e)
		delete(l.entries, key)
		e = nil
	}
	if e == nil && len(l.entries) >= l.maxKeys {
		out = append(out, l.expireLocked(now)...)
	}
	if e == nil && len(l.entries) >= l.maxKeys {
		key = raftLogOverflowKey
		meta = logSummary{name: meta.name, level: meta.level, msg: raftLogOverflowMsg}
		e = l.entries[key]
		if e != nil && now.Sub(e.start) >= l.interval {
			out = appendSummary(out, e)
			delete(l.entries, key)
			e = nil
		}
	}
	if e == nil {
		e = &logLimitEntry{meta: meta, start: now}
		l.entries[key] = e
	}
	e.count++
	allowed := e.count <= l.burst
	if !allowed {
		e.meta.suppressed++
		l.armLocked()
	}
	l.mu.Unlock()
	if len(out) > 0 {
		l.emit(out)
	}
	return allowed
}

// flush reports and drops every closed window.
func (l *logLimiter) flush() {
	l.mu.Lock()
	l.armed = false
	out := l.expireLocked(l.now())
	for _, e := range l.entries {
		if e.meta.suppressed > 0 {
			l.armLocked()
			break
		}
	}
	l.mu.Unlock()
	if len(out) > 0 {
		l.emit(out)
	}
}

func (l *logLimiter) armLocked() {
	if l.armed {
		return
	}
	l.armed = true
	l.after(l.interval, l.flush)
}

func (l *logLimiter) expireLocked(now time.Time) []logSummary {
	var out []logSummary
	for key, e := range l.entries {
		if now.Sub(e.start) >= l.interval {
			out = appendSummary(out, e)
			delete(l.entries, key)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].name != out[j].name {
			return out[i].name < out[j].name
		}
		return out[i].msg < out[j].msg
	})
	return out
}

func appendSummary(out []logSummary, e *logLimitEntry) []logSummary {
	if e.meta.suppressed == 0 {
		return out
	}
	return append(out, e.meta)
}
