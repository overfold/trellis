// Package execwebsocket carries Trellis exec frames as WebSocket binary
// messages. Each message contains a one-byte execstream frame type followed by
// its payload. The Stream adapter exposes the internal length-prefixed stream
// representation so the public WebSocket boundary can reuse the existing exec
// client and relay logic without exposing that representation on the wire.
package execwebsocket

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/overfold/trellis/orchestrator/internal/execstream"
)

// Protocol is the WebSocket subprotocol used by the public exec API.
const Protocol = "trellis.exec.v1"

const headerSize = 5

// Stream adapts WebSocket exec messages to the internal exec frame stream.
type Stream struct {
	conn *websocket.Conn

	readMu  sync.Mutex
	readBuf []byte

	writeMu       sync.Mutex
	writeDeadline time.Time
}

// Accept accepts a public exec WebSocket request.
func Accept(w http.ResponseWriter, r *http.Request) (*Stream, error) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{Protocol}})
	if err != nil {
		return nil, fmt.Errorf("accept exec WebSocket: %w", err)
	}
	if conn.Subprotocol() != Protocol {
		_ = conn.Close(websocket.StatusPolicyViolation, "exec WebSocket subprotocol is required")
		return nil, fmt.Errorf("client did not negotiate WebSocket subprotocol %s", Protocol)
	}
	conn.SetReadLimit(execstream.MaxPayload + 1)
	return &Stream{conn: conn}, nil
}

// New wraps an established WebSocket connection as an exec stream.
func New(conn *websocket.Conn) (*Stream, error) {
	if conn.Subprotocol() != Protocol {
		_ = conn.CloseNow()
		return nil, fmt.Errorf("server did not negotiate WebSocket subprotocol %s", Protocol)
	}
	conn.SetReadLimit(execstream.MaxPayload + 1)
	return &Stream{conn: conn}, nil
}

// IsRequest reports whether r requests the public exec WebSocket subprotocol.
func IsRequest(r *http.Request) bool {
	return headerHasToken(r.Header, "Connection", "upgrade") &&
		strings.EqualFold(r.Header.Get("Upgrade"), "websocket") &&
		headerHasToken(r.Header, "Sec-WebSocket-Protocol", Protocol)
}

// SetRequestHeaders marks r as a request for the public exec WebSocket
// subprotocol. It is intended for request-rejection tests; WebSocket clients
// must still generate the RFC 6455 key and version headers.
func SetRequestHeaders(r *http.Request) {
	r.Header.Set("Connection", "Upgrade")
	r.Header.Set("Upgrade", "websocket")
	r.Header.Set("Sec-WebSocket-Protocol", Protocol)
}

func headerHasToken(header http.Header, name, token string) bool {
	for _, value := range header.Values(name) {
		for candidate := range strings.SplitSeq(value, ",") {
			if strings.EqualFold(strings.TrimSpace(candidate), token) {
				return true
			}
		}
	}
	return false
}

// Read reads WebSocket messages and presents their type and payload as an
// internal length-prefixed frame stream.
func (s *Stream) Read(p []byte) (int, error) {
	s.readMu.Lock()
	defer s.readMu.Unlock()
	if len(s.readBuf) == 0 {
		messageType, message, err := s.conn.Read(context.Background())
		if err != nil {
			if errors.Is(err, websocket.ErrMessageTooBig) || websocket.CloseStatus(err) == websocket.StatusMessageTooBig {
				return 0, fmt.Errorf("%w: exec WebSocket payload exceeds %d bytes: %w", execstream.ErrInvalidFrame, execstream.MaxPayload, err)
			}
			return 0, err
		}
		if messageType != websocket.MessageBinary {
			return 0, fmt.Errorf("%w: exec WebSocket messages must be binary", execstream.ErrInvalidFrame)
		}
		if len(message) == 0 {
			return 0, fmt.Errorf("%w: exec WebSocket message has no frame type", execstream.ErrInvalidFrame)
		}
		if len(message)-1 > execstream.MaxPayload {
			return 0, fmt.Errorf("%w: payload of %d bytes exceeds %d", execstream.ErrInvalidFrame, len(message)-1, execstream.MaxPayload)
		}
		s.readBuf = make([]byte, headerSize+len(message)-1)
		s.readBuf[0] = message[0]
		// The payload limit above is well within uint32.
		binary.BigEndian.PutUint32(s.readBuf[1:headerSize], uint32(len(message)-1)) //nolint:gosec
		copy(s.readBuf[headerSize:], message[1:])
	}
	n := copy(p, s.readBuf)
	s.readBuf = s.readBuf[n:]
	return n, nil
}

// Write converts one complete internal exec frame into one WebSocket message.
func (s *Stream) Write(p []byte) (int, error) {
	if len(p) < headerSize {
		return 0, io.ErrShortWrite
	}
	payloadLength := int(binary.BigEndian.Uint32(p[1:headerSize]))
	if payloadLength > execstream.MaxPayload || len(p) != headerSize+payloadLength {
		return 0, fmt.Errorf("%w: invalid encoded frame length", execstream.ErrInvalidFrame)
	}
	message := make([]byte, 1+payloadLength)
	message[0] = p[0]
	copy(message[1:], p[headerSize:])

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	ctx := context.Background()
	var cancel context.CancelFunc
	if !s.writeDeadline.IsZero() {
		ctx, cancel = context.WithDeadline(ctx, s.writeDeadline)
		defer cancel()
	}
	if err := s.conn.Write(ctx, websocket.MessageBinary, message); err != nil {
		return 0, err
	}
	return len(p), nil
}

// SetWriteDeadline sets the deadline used by the next WebSocket message write.
func (s *Stream) SetWriteDeadline(deadline time.Time) error {
	s.writeMu.Lock()
	s.writeDeadline = deadline
	s.writeMu.Unlock()
	return nil
}

// Close immediately closes the connection. Exec uses connection closure as a
// cancellation signal, so it must not wait for a WebSocket close handshake.
func (s *Stream) Close() error { return s.conn.CloseNow() }
