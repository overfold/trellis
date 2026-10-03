// Package execstream implements the internal framed exec protocol that the
// control-plane leader and node agents speak over an HTTP/1.1 connection
// upgraded to Protocol. The public operator API adapts these frames to
// WebSocket messages rather than exposing this stream representation.
//
// Every frame is a one-byte type, a four-byte big-endian payload length, and
// at most MaxPayload payload bytes. Data frames carry raw bytes; control
// frames carry JSON objects from package api.
package execstream

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Protocol is the HTTP Upgrade token of the exec stream.
const Protocol = "trellis-exec.v1"

// MaxPayload bounds the payload of one frame. Data larger than this is split
// across several frames.
const MaxPayload = 32 << 10

const headerSize = 5

// FrameType identifies the meaning of a frame.
type FrameType byte

const (
	// FrameStdin carries process input from the client.
	FrameStdin FrameType = 1
	// FrameStdinClose closes process input. It has no payload.
	FrameStdinClose FrameType = 2
	// FrameResize carries an api.ExecResize from the client.
	FrameResize FrameType = 3
	// FrameStdout carries process output, including terminal output.
	FrameStdout FrameType = 4
	// FrameStderr carries process standard error of a non-TTY process.
	FrameStderr FrameType = 5
	// FrameExit carries the api.ExecExit that ends a stream.
	FrameExit FrameType = 6
	// FrameError carries the api.ExecStreamError that ends a stream without
	// an exit status.
	FrameError FrameType = 7
)

// ClientFrame reports whether t may be sent by the client side of a stream.
func ClientFrame(t FrameType) bool {
	return t == FrameStdin || t == FrameStdinClose || t == FrameResize
}

// ServerFrame reports whether t may be sent by the process side of a stream.
func ServerFrame(t FrameType) bool {
	return t == FrameStdout || t == FrameStderr || t == FrameExit || t == FrameError
}

// ErrInvalidFrame reports a malformed frame.
var ErrInvalidFrame = errors.New("invalid exec stream frame")

// Frame is one decoded frame. Its payload is only valid until the next read
// from the Reader that returned it.
type Frame struct {
	Type    FrameType
	Payload []byte
}

// Reader decodes frames using one bounded buffer.
type Reader struct {
	r      io.Reader
	header [headerSize]byte
	buf    []byte
}

// NewReader returns a frame reader for r.
func NewReader(r io.Reader) *Reader {
	return &Reader{r: r, buf: make([]byte, MaxPayload)}
}

// Next reads the next frame. A clean end of input between frames returns
// io.EOF; a truncated frame returns io.ErrUnexpectedEOF.
func (r *Reader) Next() (Frame, error) {
	if _, err := io.ReadFull(r.r, r.header[:]); err != nil {
		return Frame{}, err
	}
	frameType := FrameType(r.header[0])
	if !ClientFrame(frameType) && !ServerFrame(frameType) {
		return Frame{}, fmt.Errorf("%w: unknown type %d", ErrInvalidFrame, frameType)
	}
	length := binary.BigEndian.Uint32(r.header[1:])
	if length > MaxPayload {
		return Frame{}, fmt.Errorf("%w: payload of %d bytes exceeds %d", ErrInvalidFrame, length, MaxPayload)
	}
	payload := r.buf[:length]
	if _, err := io.ReadFull(r.r, payload); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return Frame{}, err
	}
	return Frame{Type: frameType, Payload: payload}, nil
}

// Writer encodes frames. It is safe for concurrent use; each frame is
// written with a single Write so frames never interleave.
type Writer struct {
	mu      sync.Mutex
	w       io.Writer
	timeout time.Duration
	buf     []byte
}

// NewWriter returns a frame writer for w. When timeout is positive and w
// supports write deadlines, a frame the peer does not accept within timeout
// fails the write.
func NewWriter(w io.Writer, timeout time.Duration) *Writer {
	return &Writer{w: w, timeout: timeout, buf: make([]byte, 0, headerSize+MaxPayload)}
}

// WriteFrame writes one frame. Payloads above MaxPayload are rejected.
func (w *Writer) WriteFrame(frameType FrameType, payload []byte) error {
	if len(payload) > MaxPayload {
		return fmt.Errorf("%w: payload of %d bytes exceeds %d", ErrInvalidFrame, len(payload), MaxPayload)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf = w.buf[:headerSize]
	w.buf[0] = byte(frameType)
	// WriteFrame rejects payloads above MaxPayload, which is well within uint32.
	binary.BigEndian.PutUint32(w.buf[1:headerSize], uint32(len(payload))) //nolint:gosec
	w.buf = append(w.buf, payload...)
	if deadline, ok := w.w.(interface{ SetWriteDeadline(time.Time) error }); ok && w.timeout > 0 {
		_ = deadline.SetWriteDeadline(time.Now().Add(w.timeout))
	}
	_, err := w.w.Write(w.buf)
	return err
}

// WriteData writes data as one or more frames of frameType.
func (w *Writer) WriteData(frameType FrameType, data []byte) error {
	for len(data) > 0 {
		chunk := data[:min(len(data), MaxPayload)]
		if err := w.WriteFrame(frameType, chunk); err != nil {
			return err
		}
		data = data[len(chunk):]
	}
	return nil
}

// WriteJSON writes a control frame carrying value.
func (w *Writer) WriteJSON(frameType FrameType, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode exec stream frame: %w", err)
	}
	return w.WriteFrame(frameType, payload)
}

// Stream returns an io.Writer that writes everything as frames of frameType.
func (w *Writer) Stream(frameType FrameType) io.Writer {
	return streamWriter{w: w, frameType: frameType}
}

type streamWriter struct {
	w         *Writer
	frameType FrameType
}

func (s streamWriter) Write(p []byte) (int, error) {
	if err := s.w.WriteData(s.frameType, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

// IsUpgradeRequest reports whether r asks to upgrade to Protocol.
func IsUpgradeRequest(r *http.Request) bool {
	return headerHasToken(r.Header, "Connection", "upgrade") && strings.EqualFold(r.Header.Get("Upgrade"), Protocol)
}

// SetUpgradeHeaders marks r as a request to upgrade to Protocol.
func SetUpgradeHeaders(r *http.Request) {
	r.Header.Set("Connection", "Upgrade")
	r.Header.Set("Upgrade", Protocol)
}

func headerHasToken(header http.Header, name, token string) bool {
	for _, value := range header.Values(name) {
		for _, candidate := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(candidate), token) {
				return true
			}
		}
	}
	return false
}

// Accept switches an upgrade request to Protocol and returns the hijacked
// connection. Deadlines the HTTP server set for the request are cleared; the
// caller owns the connection's lifetime.
func Accept(w http.ResponseWriter) (net.Conn, error) {
	conn, rw, err := http.NewResponseController(w).Hijack()
	if err != nil {
		return nil, fmt.Errorf("hijack exec stream connection: %w", err)
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("clear exec stream deadlines: %w", err)
	}
	if _, err := rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: " + Protocol + "\r\n\r\n"); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("write exec stream upgrade: %w", err)
	}
	if err := rw.Flush(); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("write exec stream upgrade: %w", err)
	}
	return &bufferedConn{Conn: conn, r: rw.Reader}, nil
}

// bufferedConn reads through the buffer the HTTP server may have filled
// beyond the request.
type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) { return c.r.Read(p) }

// Upgraded returns the stream of a response that switched to Protocol.
func Upgraded(response *http.Response) (io.ReadWriteCloser, error) {
	if response.StatusCode != http.StatusSwitchingProtocols || !strings.EqualFold(response.Header.Get("Upgrade"), Protocol) {
		return nil, fmt.Errorf("server did not switch to %s (status %d)", Protocol, response.StatusCode)
	}
	stream, ok := response.Body.(io.ReadWriteCloser)
	if !ok {
		return nil, fmt.Errorf("upgraded response body is not writable")
	}
	return stream, nil
}
