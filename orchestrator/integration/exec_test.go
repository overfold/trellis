//go:build integration

package integration

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/overfold/trellis/internal/api"
	"github.com/overfold/trellis/internal/client"
)

// TestMultiNodeExecStream runs exec streams through a follower's API proxy,
// the leader's relay, and the owning node agent, then checks that a
// leadership change ends an open stream with an error instead of leaving it
// hanging.
func TestMultiNodeExecStream(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-process integration test")
	}
	h := newHarness(t, 3)
	defer h.close()
	h.waitNodes(3)
	h.waitVoters(3, 3)
	h.submit(job("shell", "v1", 1, "rolling"))
	h.waitJob("shell", 1, 1)

	leader := h.leader()
	follower := (leader + 1) % len(h.nodes)
	operator := client.NewNamespaceServerClient(h.token, addr(h.nodes[follower].ports[1]), "default", &tls.Config{InsecureSkipVerify: true})
	var allocationID string
	h.eventually(30*time.Second, func() bool {
		allocations, err := operator.ListAllocations(context.Background(), "")
		if err != nil {
			return false
		}
		for _, allocation := range *allocations {
			if allocation.Job == "shell" && allocation.Phase == "running" {
				allocationID = allocation.ID
				return true
			}
		}
		return false
	}, "no running allocation")

	run := func(request api.ExecRequest, input string) (string, string, int, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		stream, err := operator.Exec(ctx, allocationID, request)
		if err != nil {
			return "", "", 0, err
		}
		defer func() { _ = stream.Close() }()
		if input != "" {
			if _, err := stream.Write([]byte(input)); err != nil {
				return "", "", 0, err
			}
		}
		if request.TTY {
			if err := stream.Resize(100, 30); err != nil {
				return "", "", 0, err
			}
		}
		if request.Stdin {
			if err := stream.CloseStdin(); err != nil {
				return "", "", 0, err
			}
		}
		var stdout, stderr strings.Builder
		code, err := stream.Wait(&stdout, &stderr)
		return stdout.String(), stderr.String(), code, err
	}

	t.Run("output and exit status", func(t *testing.T) {
		stdout, _, code, err := run(api.ExecRequest{Command: []string{"echo", "hello", "exec"}}, "")
		if err != nil || code != 0 || stdout != "hello exec\n" {
			t.Fatalf("echo = %q, %d, %v", stdout, code, err)
		}
		_, stderr, _, err := run(api.ExecRequest{Command: []string{"stderr", "problem"}}, "")
		if err != nil || stderr != "problem\n" {
			t.Fatalf("stderr = %q, %v", stderr, err)
		}
		_, _, code, err = run(api.ExecRequest{Command: []string{"exit", "7"}}, "")
		if err != nil || code != 7 {
			t.Fatalf("exit = %d, %v; want 7", code, err)
		}
	})

	t.Run("stdin and terminal resize", func(t *testing.T) {
		stdout, _, code, err := run(api.ExecRequest{Command: []string{"cat"}, Stdin: true}, "piped input")
		if err != nil || code != 0 || stdout != "piped input" {
			t.Fatalf("cat = %q, %d, %v", stdout, code, err)
		}
		stdout, _, code, err = run(api.ExecRequest{Command: []string{"cat"}, Stdin: true, TTY: true, Term: "xterm"}, "typed")
		if err != nil || code != 0 || !strings.Contains(stdout, "typed") || !strings.Contains(stdout, "[resize 100x30]") {
			t.Fatalf("tty cat = %q, %d, %v", stdout, code, err)
		}
	})

	t.Run("leadership change ends open stream", func(t *testing.T) {
		stream, err := operator.Exec(context.Background(), allocationID, api.ExecRequest{Command: []string{"cat"}, Stdin: true})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = stream.Close() }()
		if _, err := stream.Write([]byte("before failover")); err != nil {
			t.Fatal(err)
		}
		administrator := client.NewServerClient("", addr(h.nodes[follower].ports[1]), &tls.Config{InsecureSkipVerify: true})
		if err := administrator.UseAdministratorKey(h.adminKey); err != nil {
			t.Fatal(err)
		}
		if err := administrator.TransferLeadership(context.Background()); err != nil {
			t.Fatal(err)
		}
		result := make(chan error, 1)
		go func() {
			_, err := stream.Wait(io.Discard, io.Discard)
			result <- err
		}()
		select {
		case err := <-result:
			var execErr *client.ExecError
			if !errors.As(err, &execErr) || !strings.Contains(execErr.Message, "leadership changed") {
				t.Fatalf("wait error = %v, want leadership change", err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("open exec stream did not end after the leadership change")
		}

		// The new leader serves new streams.
		h.eventually(45*time.Second, func() bool {
			stdout, _, code, err := run(api.ExecRequest{Command: []string{"echo", "after"}}, "")
			return err == nil && code == 0 && stdout == "after\n"
		}, "exec did not recover after the leadership change")
	})
}
