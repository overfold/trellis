package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/clofour/trellis/internal/api"
	"github.com/google/uuid"
)

func TestServerClientExecSessionLifecycle(t *testing.T) {
	called := make(map[string]bool)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/allocations/alloc-1/exec":
			called["exec"] = true
			var request api.ExecRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode exec request: %v", err)
			}
			if request.Task != "web" || len(request.Command) != 2 || request.Command[0] != "echo" || request.Command[1] != "hello" {
				t.Fatalf("unexpected exec request: %#v", request)
			}
			_ = json.NewEncoder(w).Encode(api.ExecResponse{Stdout: "hello\n", ExitCode: 0})

		case r.Method == http.MethodPost && r.URL.Path == "/v1/allocations/alloc-1/exec/sessions":
			called["create"] = true
			var request api.ExecSessionCreateRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode create request: %v", err)
			}
			if request.Task != "web" || request.Term != "xterm-256color" || request.Cols != 120 || request.Rows != 32 {
				t.Fatalf("unexpected create request: %#v", request)
			}
			_ = json.NewEncoder(w).Encode(api.ExecSessionResponse{ID: "session-1"})

		case r.Method == http.MethodPost && r.URL.Path == "/v1/allocations/alloc-1/exec/sessions/session-1/input":
			called["input"] = true
			var request api.ExecSessionInputRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode input request: %v", err)
			}
			if request.DataBase64 != "aGk=" {
				t.Fatalf("input data = %q", request.DataBase64)
			}
			w.WriteHeader(http.StatusNoContent)

		case r.Method == http.MethodGet && r.URL.Path == "/v1/allocations/alloc-1/exec/sessions/session-1/output":
			called["output"] = true
			if got := r.URL.Query().Get("offset"); got != "7" {
				t.Fatalf("output offset = %q", got)
			}
			exitCode := 0
			_ = json.NewEncoder(w).Encode(api.ExecSessionOutputResponse{
				DataBase64: "b2s=",
				NextOffset: 9,
				Exited:     true,
				ExitCode:   &exitCode,
			})

		case r.Method == http.MethodPost && r.URL.Path == "/v1/allocations/alloc-1/exec/sessions/session-1/resize":
			called["resize"] = true
			var request api.ExecSessionResizeRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode resize request: %v", err)
			}
			if request.Cols != 90 || request.Rows != 40 {
				t.Fatalf("unexpected resize request: %#v", request)
			}
			w.WriteHeader(http.StatusNoContent)

		case r.Method == http.MethodDelete && r.URL.Path == "/v1/allocations/alloc-1/exec/sessions/session-1":
			called["close"] = true
			w.WriteHeader(http.StatusNoContent)

		default:
			http.Error(w, "unexpected request", http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := NewNamespaceServerClient("token", server.URL, "default", nil)
	ctx := context.Background()

	execResult, err := client.ExecAllocation(ctx, "alloc-1", "web", []string{"echo", "hello"})
	if err != nil {
		t.Fatalf("exec allocation: %v", err)
	}
	if execResult.Stdout != "hello\n" || execResult.ExitCode != 0 {
		t.Fatalf("unexpected exec result: %#v", execResult)
	}

	session, err := client.CreateExecSession(ctx, "alloc-1", &api.ExecSessionCreateRequest{
		Task:    "web",
		Command: []string{"/bin/sh"},
		Term:    "xterm-256color",
		Cols:    120,
		Rows:    32,
	})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	if session.ID != "session-1" {
		t.Fatalf("session id = %q", session.ID)
	}

	if err := client.WriteExecSession(ctx, "alloc-1", session.ID, &api.ExecSessionInputRequest{DataBase64: "aGk="}); err != nil {
		t.Fatalf("write session: %v", err)
	}

	output, err := client.ReadExecSession(ctx, "alloc-1", session.ID, 7)
	if err != nil {
		t.Fatalf("read session: %v", err)
	}
	if output.DataBase64 != "b2s=" || output.NextOffset != 9 || !output.Exited {
		t.Fatalf("unexpected output: %#v", output)
	}

	if err := client.ResizeExecSession(ctx, "alloc-1", session.ID, &api.ExecSessionResizeRequest{Cols: 90, Rows: 40}); err != nil {
		t.Fatalf("resize session: %v", err)
	}
	if err := client.CloseExecSession(ctx, "alloc-1", session.ID); err != nil {
		t.Fatalf("close session: %v", err)
	}

	for _, name := range []string{"exec", "create", "input", "output", "resize", "close"} {
		if !called[name] {
			t.Fatalf("%s request was not sent", name)
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
