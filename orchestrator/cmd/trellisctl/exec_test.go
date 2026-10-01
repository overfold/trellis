package main

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/internal/execstream"
	"github.com/overfold/trellis/orchestrator/internal/execwebsocket"
)

// newExecTestServer accepts one exec WebSocket, checks its request, and hands
// its adapted frame stream to serve.
func newExecTestServer(t *testing.T, check func(api.ExecRequest), serve func(*execstream.Reader, *execstream.Writer)) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/namespaces/default/allocations/alloc-1/exec" || !execwebsocket.IsRequest(r) {
			http.Error(w, "unexpected request", http.StatusNotFound)
			return
		}
		request, err := execstream.DecodeRequest(r.URL.Query())
		if err != nil {
			t.Errorf("decode request: %v", err)
		}
		check(request)
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

func TestExecCommandStreamsOutputAndPreservesExitStatus(t *testing.T) {
	previousConfig := config
	t.Cleanup(func() { config = previousConfig })

	server := newExecTestServer(t, func(request api.ExecRequest) {
		if request.Task != "web" || strings.Join(request.Command, " ") != "sh -c" || request.TTY || request.Stdin {
			t.Errorf("unexpected request: %#v", request)
		}
	}, func(_ *execstream.Reader, writer *execstream.Writer) {
		_ = writer.WriteData(execstream.FrameStdout, []byte("stdout"))
		_ = writer.WriteData(execstream.FrameStderr, []byte("stderr"))
		_ = writer.WriteJSON(execstream.FrameExit, api.ExecExit{ExitCode: 7})
	})

	config = CLIConfig{ServerAddr: server.URL, Namespace: "default"}
	cmd := NewExecCmd()
	cmd.SetArgs([]string{"--task", "web", "alloc-1", "--", "sh", "-c"})
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)

	err := cmd.Execute()
	var exitErr *execExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("expected execExitError, got %v", err)
	}
	if exitErr.code != 7 {
		t.Fatalf("exit code = %d", exitErr.code)
	}
	if stdout.String() != "stdout" {
		t.Fatalf("stdout = %q", stdout.String())
	}
	if got := stderr.String(); !strings.HasPrefix(got, "stderr") || !strings.Contains(got, "remote command exited with status 7") {
		t.Fatalf("stderr = %q", got)
	}
}

func TestExecForwardsStdinAndClosesIt(t *testing.T) {
	previousConfig := config
	t.Cleanup(func() { config = previousConfig })

	server := newExecTestServer(t, func(request api.ExecRequest) {
		if !request.Stdin || request.TTY {
			t.Errorf("unexpected request: %#v", request)
		}
	}, func(reader *execstream.Reader, writer *execstream.Writer) {
		for {
			frame, err := reader.Next()
			if err != nil {
				t.Errorf("read: %v", err)
				return
			}
			switch frame.Type {
			case execstream.FrameStdin:
				_ = writer.WriteData(execstream.FrameStdout, frame.Payload)
			case execstream.FrameStdinClose:
				_ = writer.WriteJSON(execstream.FrameExit, api.ExecExit{})
				return
			default:
				t.Errorf("unexpected frame %d", frame.Type)
				return
			}
		}
	})

	config = CLIConfig{ServerAddr: server.URL, Namespace: "default"}
	cmd := NewExecCmd()
	cmd.SetArgs([]string{"-i", "alloc-1", "--", "cat"})
	cmd.SetIn(strings.NewReader("piped input"))
	var stdout bytes.Buffer
	cmd.SetOut(&stdout)
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if stdout.String() != "piped input" {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestExecReportsStreamError(t *testing.T) {
	previousConfig := config
	t.Cleanup(func() { config = previousConfig })

	server := newExecTestServer(t, func(api.ExecRequest) {}, func(_ *execstream.Reader, writer *execstream.Writer) {
		_ = writer.WriteJSON(execstream.FrameError, api.ExecStreamError{Message: "exec session ended because control-plane leadership changed"})
	})
	config = CLIConfig{ServerAddr: server.URL, Namespace: "default"}
	cmd := NewExecCmd()
	cmd.SetArgs([]string{"alloc-1", "--", "true"})
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	err := cmd.Execute()
	var exitErr *execExitError
	if err == nil || errors.As(err, &exitErr) || !strings.Contains(err.Error(), "leadership changed") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestExecTermRequiresTTY(t *testing.T) {
	cmd := NewExecCmd()
	cmd.SetArgs([]string{"--term", "xterm", "alloc-1", "--", "/bin/sh"})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "--term requires --tty") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestExecTTYRequiresTerminalInput(t *testing.T) {
	previousConfig := config
	t.Cleanup(func() { config = previousConfig })
	config = CLIConfig{ServerAddr: "http://127.0.0.1:1", Namespace: "default"}

	cmd := NewExecCmd()
	cmd.SetArgs([]string{"--tty", "alloc-1", "--", "/bin/sh"})
	cmd.SetIn(strings.NewReader(""))
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "--tty requires stdin to be a terminal") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBoundTerminalSize(t *testing.T) {
	cols, rows := boundTerminalSize(0, 2000)
	if cols != 1 || rows != 1000 {
		t.Fatalf("bounded size = %dx%d", cols, rows)
	}
}
