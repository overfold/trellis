package client

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/internal/api"
	"github.com/overfold/trellis/internal/auth"
	"github.com/overfold/trellis/internal/execstream"
)

// serveExecTestStream answers exec upgrade requests by accepting them and
// handing the stream to serve.
func serveExecTestStream(t *testing.T, check func(*http.Request), serve func(*execstream.Reader, *execstream.Writer)) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/namespaces/default/allocations/alloc-1/exec" || !execstream.IsUpgradeRequest(r) {
			http.Error(w, "unexpected request", http.StatusNotFound)
			return
		}
		if check != nil {
			check(r)
		}
		conn, err := execstream.Accept(w)
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

func TestServerClientExecStream(t *testing.T) {
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

	stream, err := NewNamespaceServerClient("token", server.URL, "default", nil).Exec(context.Background(), "alloc-1", api.ExecRequest{
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

func TestServerClientExecStreamEndings(t *testing.T) {
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
			stream, err := NewNamespaceServerClient("token", server.URL, "default", nil).Exec(context.Background(), "alloc-1", api.ExecRequest{Command: []string{"true"}})
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

func TestServerClientExecStreamClosesWithContext(t *testing.T) {
	closed := make(chan struct{})
	server := serveExecTestStream(t, nil, func(reader *execstream.Reader, _ *execstream.Writer) {
		if _, err := reader.Next(); err == nil {
			t.Error("stream delivered a frame, want close")
		}
		close(closed)
	})
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := NewNamespaceServerClient("token", server.URL, "default", nil).Exec(ctx, "alloc-1", api.ExecRequest{Command: []string{"sh"}})
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

func TestServerClientExecSignsAdministratorUpgrade(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/auth/administrator/challenge" {
			_ = json.NewEncoder(w).Encode(api.AdministratorChallengeResponse{Challenge: "challenge-1"})
			return
		}
		signature, err := base64.RawURLEncoding.DecodeString(r.Header.Get(auth.AdministratorSignatureHeader))
		payload := auth.AdministratorSigningPayload("challenge-1", http.MethodGet, r.URL.RequestURI(), nil)
		if err != nil || r.Header.Get(auth.AdministratorChallengeHeader) != "challenge-1" || !ed25519.Verify(publicKey, payload, signature) {
			http.Error(w, "bad signature", http.StatusUnauthorized)
			return
		}
		conn, err := execstream.Accept(w)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_ = execstream.NewWriter(conn, 0).WriteJSON(execstream.FrameExit, api.ExecExit{})
	}))
	defer server.Close()

	serverClient := NewNamespaceServerClient("", server.URL, "default", nil)
	if err := serverClient.UseAdministratorKey(privateKey); err != nil {
		t.Fatal(err)
	}
	stream, err := serverClient.Exec(context.Background(), "alloc-1", api.ExecRequest{Command: []string{"id"}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.Close() }()
	if code, err := stream.Wait(io.Discard, io.Discard); err != nil || code != 0 {
		t.Fatalf("wait = %d, %v", code, err)
	}
}

func TestServerClientExecErrorExposesStatusAndMessage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"message":"node agent unavailable: agent is shutting down"}` + "\n"))
	}))
	defer server.Close()

	_, err := NewNamespaceServerClient("token", server.URL, "default", nil).Exec(context.Background(), "alloc-1", api.ExecRequest{Command: []string{"true"}})
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) {
		t.Fatalf("exec error = %v, want *HTTPError", err)
	}
	if httpErr.Status != http.StatusServiceUnavailable || httpErr.Message() != "node agent unavailable: agent is shutting down" {
		t.Fatalf("status = %d, message = %q", httpErr.Status, httpErr.Message())
	}
}

func TestHTTPErrorMessageFallsBackToBody(t *testing.T) {
	for body, want := range map[string]string{
		"plain failure\n":      "plain failure",
		`{"code":"x"}`:         `{"code":"x"}`,
		`{"message":"reason"}`: "reason",
	} {
		if got := (&HTTPError{Status: http.StatusBadGateway, Body: []byte(body)}).Message(); got != want {
			t.Fatalf("Message() for %q = %q, want %q", body, got, want)
		}
	}
}

func TestAgentClientNetworkPlanTransportUsesContextDeadline(t *testing.T) {
	client := NewAgentClient("token", nil)
	regularClient := client.clientFor(uuid.New(), 30*time.Second)
	regular, ok := regularClient.client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("regular transport type = %T", regularClient.client.Transport)
	}
	networkPlanClient := client.clientFor(uuid.New(), 0)
	networkPlans, ok := networkPlanClient.client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("network-plan transport type = %T", networkPlanClient.client.Transport)
	}
	if regular.ResponseHeaderTimeout != 30*time.Second {
		t.Fatalf("regular response header timeout = %s, want 30s", regular.ResponseHeaderTimeout)
	}
	if networkPlans.ResponseHeaderTimeout != 0 {
		t.Fatalf("network-plan response header timeout = %s, want context-governed zero", networkPlans.ResponseHeaderTimeout)
	}
}
