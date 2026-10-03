package execwebsocket

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/overfold/trellis/orchestrator/internal/execstream"
)

func TestStreamUsesOneBinaryMessagePerExecFrame(t *testing.T) {
	serverErr := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stream, err := Accept(w, r)
		if err != nil {
			serverErr <- err
			return
		}
		defer func() { _ = stream.Close() }()
		frame, err := execstream.NewReader(stream).Next()
		if err != nil {
			serverErr <- err
			return
		}
		if frame.Type != execstream.FrameStdin || string(frame.Payload) != "input" {
			serverErr <- errors.New("server received the wrong exec frame")
			return
		}
		serverErr <- execstream.NewWriter(stream, 0).WriteData(execstream.FrameStdout, []byte("output"))
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, response, err := websocket.Dial(ctx, server.URL, &websocket.DialOptions{Subprotocols: []string{Protocol}})
	if response != nil && response.Body != nil {
		defer func() { _ = response.Body.Close() }()
	}
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.CloseNow() }()
	if err := conn.Write(ctx, websocket.MessageBinary, append([]byte{byte(execstream.FrameStdin)}, []byte("input")...)); err != nil {
		t.Fatal(err)
	}
	messageType, message, err := conn.Read(ctx)
	if err != nil || messageType != websocket.MessageBinary || string(message) != string(append([]byte{byte(execstream.FrameStdout)}, []byte("output")...)) {
		t.Fatalf("message = %d %q, %v; want one stdout message", messageType, message, err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestStreamRejectsTextMessages(t *testing.T) {
	serverErr := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stream, err := Accept(w, r)
		if err != nil {
			serverErr <- err
			return
		}
		defer func() { _ = stream.Close() }()
		_, err = execstream.NewReader(stream).Next()
		serverErr <- err
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, response, err := websocket.Dial(ctx, server.URL, &websocket.DialOptions{Subprotocols: []string{Protocol}})
	if response != nil && response.Body != nil {
		defer func() { _ = response.Body.Close() }()
	}
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.CloseNow() }()
	if err := conn.Write(ctx, websocket.MessageText, []byte("input")); err != nil {
		t.Fatal(err)
	}
	if err := <-serverErr; !errors.Is(err, execstream.ErrInvalidFrame) {
		t.Fatalf("read error = %v, want invalid frame", err)
	}
}
