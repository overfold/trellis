package client

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/internal/adminsign"
	"github.com/overfold/trellis/orchestrator/internal/execstream"
	"github.com/overfold/trellis/orchestrator/internal/execwebsocket"
)

// serveExecTestStream answers exec WebSocket requests and hands the adapted
// frame stream to serve.
func serveExecTestStream(t *testing.T, check func(*http.Request), serve func(*execstream.Reader, *execstream.Writer)) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/namespaces/default/allocations/alloc-1/exec" || !execwebsocket.IsRequest(r) {
			http.Error(w, "unexpected request", http.StatusNotFound)
			return
		}
		if check != nil {
			check(r)
		}
		conn, err := execwebsocket.Accept(w, r)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		serve(execstream.NewReader(conn), execstream.NewWriter(conn, 0))
	}))
	t.Cleanup(server.Close)
	return server
}

func TestClientExecStream(t *testing.T) {
	server := serveExecTestStream(t, func(r *http.Request) {
		request, err := execstream.DecodeRequest(r.URL.Query())
		if err != nil {
			t.Errorf("decode request: %v", err)
		}
		if request.Task != "web" || strings.Join(request.Command, " ") != "/bin/sh -c cat" || !request.TTY || !request.Stdin || request.Term != "xterm-256color" || request.Cols != 120 || request.Rows != 32 {
			t.Errorf("unexpected request: %#v", request)
		}
		if r.Header.Get("Authorization") != "Bearer token" {
			t.Errorf("request is not authenticated: %v", r.Header)
		}
	}, func(reader *execstream.Reader, writer *execstream.Writer) {
		want := []struct {
			frameType execstream.FrameType
			payload   string
		}{
			{execstream.FrameStdin, "hi"},
			{execstream.FrameResize, `{"cols":90,"rows":40}`},
			{execstream.FrameStdinClose, ""},
		}
		for _, expected := range want {
			frame, err := reader.Next()
			if err != nil || frame.Type != expected.frameType || string(frame.Payload) != expected.payload {
				t.Errorf("frame = %d %q, %v; want %d %q", frame.Type, frame.Payload, err, expected.frameType, expected.payload)
				return
			}
		}
		_ = writer.WriteData(execstream.FrameStdout, []byte("out"))
		_ = writer.WriteData(execstream.FrameStderr, []byte("err"))
		_ = writer.WriteJSON(execstream.FrameExit, api.ExecExit{ExitCode: 5})
	})

	stream, err := mustNew(t, Config{Address: server.URL, Token: "token", Namespace: "default"}).Exec(context.Background(), "alloc-1", api.ExecRequest{
		Task: "web", Command: []string{"/bin/sh", "-c", "cat"}, TTY: true, Stdin: true, Term: "xterm-256color", Cols: 120, Rows: 32,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.Close() }()
	if _, err := stream.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	if err := stream.Resize(90, 40); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseStdin(); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code, err := stream.Wait(&stdout, &stderr)
	if err != nil || code != 5 || stdout.String() != "out" || stderr.String() != "err" {
		t.Fatalf("wait = %d, %v; stdout %q, stderr %q", code, err, stdout.String(), stderr.String())
	}
}

func TestClientExecStreamEndings(t *testing.T) {
	tests := []struct {
		name  string
		serve func(*execstream.Writer)
		want  string
		typed bool
	}{
		{name: "error frame", serve: func(w *execstream.Writer) {
			_ = w.WriteJSON(execstream.FrameError, api.ExecStreamError{Message: "exec session ended because control-plane leadership changed"})
		}, want: "exec session ended because control-plane leadership changed", typed: true},
		{name: "no exit status", serve: func(*execstream.Writer) {}, want: "ended without an exit status"},
		{name: "client frame from server", serve: func(w *execstream.Writer) { _ = w.WriteData(execstream.FrameStdin, []byte("x")) }, want: "unexpected type"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := serveExecTestStream(t, nil, func(_ *execstream.Reader, writer *execstream.Writer) { tt.serve(writer) })
			stream, err := mustNew(t, Config{Address: server.URL, Token: "token", Namespace: "default"}).Exec(context.Background(), "alloc-1", api.ExecRequest{Command: []string{"true"}})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = stream.Close() }()
			_, err = stream.Wait(io.Discard, io.Discard)
			var execErr *ExecError
			if err == nil || !strings.Contains(err.Error(), tt.want) || errors.As(err, &execErr) != tt.typed {
				t.Fatalf("wait error = %v, want %q (typed %t)", err, tt.want, tt.typed)
			}
		})
	}
}

func TestClientExecExitCodes(t *testing.T) {
	for _, code := range []int{0, 17, -1} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			server := serveExecTestStream(t, nil, func(_ *execstream.Reader, writer *execstream.Writer) {
				// Unknown fields remain forward-compatible.
				_ = writer.WriteFrame(execstream.FrameExit, fmt.Appendf(nil, `{"exit_code":%d,"extra":true}`, code))
			})
			stream, err := mustNew(t, Config{Address: server.URL, Token: "token", Namespace: "default"}).Exec(context.Background(), "alloc-1", api.ExecRequest{Command: []string{"true"}})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = stream.Close() }()
			if got, err := stream.Wait(io.Discard, io.Discard); err != nil || got != code {
				t.Fatalf("Wait = %d, %v; want %d", got, err, code)
			}
		})
	}
}

func TestClientExecStreamClosesWithContext(t *testing.T) {
	closed := make(chan struct{})
	server := serveExecTestStream(t, nil, func(reader *execstream.Reader, _ *execstream.Writer) {
		if _, err := reader.Next(); err == nil {
			t.Error("stream delivered a frame, want close")
		}
		close(closed)
	})
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := mustNew(t, Config{Address: server.URL, Token: "token", Namespace: "default"}).Exec(ctx, "alloc-1", api.ExecRequest{Command: []string{"sh"}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.Close() }()
	cancel()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("cancelling the context did not close the stream")
	}
}

func TestClientExecSignsAdministratorUpgrade(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/auth/administrator/challenge" {
			_ = json.NewEncoder(w).Encode(api.AdministratorChallengeResponse{Challenge: "challenge-1"})
			return
		}
		signature, err := base64.RawURLEncoding.DecodeString(r.Header.Get(adminsign.SignatureHeader))
		payload := adminsign.Payload("challenge-1", http.MethodGet, r.URL.RequestURI(), nil)
		if err != nil || r.Header.Get(adminsign.ChallengeHeader) != "challenge-1" || !ed25519.Verify(publicKey, payload, signature) {
			http.Error(w, "bad signature", http.StatusUnauthorized)
			return
		}
		conn, err := execwebsocket.Accept(w, r)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_ = execstream.NewWriter(conn, 0).WriteJSON(execstream.FrameExit, api.ExecExit{})
	}))
	defer server.Close()

	serverClient := mustNew(t, Config{Address: server.URL, Namespace: "default", AdministratorKey: privateKey})
	stream, err := serverClient.Exec(context.Background(), "alloc-1", api.ExecRequest{Command: []string{"id"}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.Close() }()
	if code, err := stream.Wait(io.Discard, io.Discard); err != nil || code != 0 {
		t.Fatalf("wait = %d, %v", code, err)
	}
}

func TestClientExecErrorExposesStatusAndMessage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"message":"node agent unavailable: agent is shutting down"}` + "\n"))
	}))
	defer server.Close()

	_, err := mustNew(t, Config{Address: server.URL, Token: "token", Namespace: "default"}).Exec(context.Background(), "alloc-1", api.ExecRequest{Command: []string{"true"}})
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) {
		t.Fatalf("exec error = %v, want *HTTPError", err)
	}
	if httpErr.Status != http.StatusServiceUnavailable || httpErr.Message() != "node agent unavailable: agent is shutting down" {
		t.Fatalf("status = %d, message = %q", httpErr.Status, httpErr.Message())
	}
}

func TestExecWaitTruncatedFrames(t *testing.T) {
	for _, data := range [][]byte{
		{4},                  // Truncated header.
		{4, 0, 0, 0, 2},      // Missing payload.
		{4, 0, 0, 0, 2, 'x'}, // Partial payload.
		{4, 0, 0, 128, 1},    // Oversized payload.
	} {
		stream := newExecStream(struct {
			io.Reader
			io.Writer
			io.Closer
		}{bytes.NewReader(data), io.Discard, io.NopCloser(strings.NewReader(""))})
		_, err := stream.Wait(io.Discard, io.Discard)
		if !errors.Is(err, ErrInvalidExecFrame) {
			t.Fatalf("frame %v: error = %v, want public invalid-frame sentinel", data, err)
		}
		if len(data) < 5 || data[3] == 0 {
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("frame %v: lost truncation cause: %v", data, err)
			}
		}
	}
}

func TestExecWaitPreservesIOErrors(t *testing.T) {
	cause := errors.New("I/O failed")
	stream := newExecStream(struct {
		io.Reader
		io.Writer
		io.Closer
	}{iotest.ErrReader(cause), io.Discard, io.NopCloser(strings.NewReader(""))})
	if _, err := stream.Wait(io.Discard, io.Discard); !errors.Is(err, cause) || errors.Is(err, ErrInvalidExecFrame) {
		t.Fatalf("read error = %v", err)
	}
	for _, frameType := range []execstream.FrameType{execstream.FrameStdout, execstream.FrameStderr} {
		server := serveExecTestStream(t, nil, func(_ *execstream.Reader, w *execstream.Writer) {
			_ = w.WriteData(frameType, []byte("output"))
		})
		stream, err := mustNew(t, Config{Address: server.URL, Token: "token", Namespace: "default"}).Exec(context.Background(), "alloc-1", api.ExecRequest{Command: []string{"true"}})
		if err != nil {
			t.Fatal(err)
		}
		reader, writer := io.Pipe()
		_ = reader.CloseWithError(cause)
		_, err = stream.Wait(writer, writer)
		_ = writer.Close()
		_ = stream.Close()
		if !errors.Is(err, cause) || errors.Is(err, ErrInvalidExecFrame) {
			t.Fatalf("output error = %v", err)
		}
	}
}
