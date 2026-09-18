package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/clofour/trellis/internal/api"
)

func TestExecCommandWritesStreamsAndPreservesExitStatus(t *testing.T) {
	previousConfig := config
	t.Cleanup(func() { config = previousConfig })

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/allocations/alloc-1/exec" {
			http.Error(w, "unexpected request", http.StatusNotFound)
			return
		}
		var request api.ExecRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if request.Task != "web" || len(request.Command) != 2 || request.Command[0] != "sh" || request.Command[1] != "-c" {
			t.Fatalf("unexpected request: %#v", request)
		}
		_ = json.NewEncoder(w).Encode(api.ExecResponse{
			Stdout:   "stdout",
			Stderr:   "stderr",
			ExitCode: 7,
		})
	}))
	defer server.Close()

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

func TestExecStdinRequiresTTY(t *testing.T) {
	cmd := NewExecCmd()
	cmd.SetArgs([]string{"--stdin", "alloc-1", "--", "/bin/sh"})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "--stdin requires --tty") {
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
