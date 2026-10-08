package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/coder/websocket"
	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/client"
)

// Exercise the public wire contract without importing any internal packages.
func TestExecPublicErrors(t *testing.T) {
	for _, tc := range []struct {
		name        string
		message     []byte
		text        bool
		invalid     bool
		execError   bool
		syntaxError bool
		typeError   bool
		want        string
	}{
		{name: "unknown type", message: []byte{99}, invalid: true, want: "unknown type"},
		{name: "stdin from server", message: []byte{1, 'x'}, invalid: true, want: "unexpected type"},
		{name: "stdin close from server", message: []byte{2}, invalid: true, want: "unexpected type"},
		{name: "resize from server", message: []byte{3}, invalid: true, want: "unexpected type"},
		{name: "text", message: []byte("text"), text: true, invalid: true, want: "must be binary"},
		{name: "empty", message: []byte{}, invalid: true, want: "no frame type"},
		{name: "oversized", message: append([]byte{4}, make([]byte, 32769)...), invalid: true, want: "exceeds"},
		{name: "truncated exit JSON", message: []byte("\x06{"), invalid: true, syntaxError: true, want: "decode exec exit status"},
		{name: "wrong exit JSON type", message: []byte("\x06{\"exit_code\":\"bad\"}"), invalid: true, typeError: true, want: "decode exec exit status"},
		{name: "missing exit code", message: []byte("\x06{}"), invalid: true, want: "exit_code must be a non-null integer"},
		{name: "null exit code", message: []byte("\x06{\"exit_code\":null}"), invalid: true, want: "exit_code must be a non-null integer"},
		{name: "null exit object", message: []byte("\x06null"), invalid: true, want: "exit_code must be a non-null integer"},
		{name: "fractional exit code", message: []byte("\x06{\"exit_code\":0.5}"), invalid: true, typeError: true, want: "decode exec exit status"},
		{name: "decimal exit code", message: []byte("\x06{\"exit_code\":0.0}"), invalid: true, typeError: true, want: "decode exec exit status"},
		{name: "boolean exit code", message: []byte("\x06{\"exit_code\":false}"), invalid: true, typeError: true, want: "decode exec exit status"},
		{name: "overflow exit code", message: []byte("\x06{\"exit_code\":9223372036854775808}"), invalid: true, typeError: true, want: "decode exec exit status"},
		{name: "server error", message: []byte("\x07{\"message\":\"leader changed\"}"), execError: true, want: "leader changed"},
		{name: "malformed error fallback", message: []byte("\x07{"), execError: true, want: "exec stream failed"},
		{name: "empty error fallback", message: []byte("\x07{}"), execError: true, want: "exec stream failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"trellis.exec.v1"}})
				if err != nil {
					t.Errorf("accept: %v", err)
					return
				}
				defer func() { _ = conn.CloseNow() }()
				kind := websocket.MessageBinary
				if tc.text {
					kind = websocket.MessageText
				}
				if err := conn.Write(r.Context(), kind, tc.message); err != nil {
					t.Errorf("write: %v", err)
				}
			}))
			defer server.Close()
			c, err := client.New(client.Config{Address: server.URL, Token: "token", Namespace: "default"})
			if err != nil {
				t.Fatal(err)
			}
			stream, err := c.Exec(context.Background(), "alloc-1", api.ExecRequest{Command: []string{"true"}})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = stream.Close() }()
			code, err := stream.Wait(io.Discard, io.Discard)
			if code != 0 || err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Wait = %d, %v; want %q", code, err, tc.want)
			}
			wrapped := fmt.Errorf("caller context: %w", err)
			var execErr *client.ExecError
			var syntaxErr *json.SyntaxError
			var typeErr *json.UnmarshalTypeError
			if errors.Is(wrapped, client.ErrInvalidExecFrame) != tc.invalid ||
				errors.As(wrapped, &execErr) != tc.execError ||
				errors.As(wrapped, &syntaxErr) != tc.syntaxError ||
				errors.As(wrapped, &typeErr) != tc.typeError {
				t.Fatalf("public classification failed: %v", wrapped)
			}
			if tc.name == "oversized" && !errors.Is(wrapped, websocket.ErrMessageTooBig) {
				t.Fatalf("lost WebSocket size-limit cause: %v", wrapped)
			}
		})
	}
}
