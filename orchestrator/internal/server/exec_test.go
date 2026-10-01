package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/labstack/echo/v5"
	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/client"
	"github.com/overfold/trellis/orchestrator/internal/auth"
	"github.com/overfold/trellis/orchestrator/internal/execstream"
	"github.com/overfold/trellis/orchestrator/internal/execwebsocket"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
)

// execRelayTest runs a leader whose single allocation is served by a fake
// agent that hands each accepted stream to serve.
type execRelayTest struct {
	server   *Server
	leader   *httptest.Server
	requests chan nodeapi.AgentExecRequest
	cancel   context.CancelFunc
}

func newExecRelayTest(t *testing.T, serve func(*execstream.Reader, *execstream.Writer)) *execRelayTest {
	t.Helper()
	test := &execRelayTest{requests: make(chan nodeapi.AgentExecRequest, 1)}
	agent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		execRequest, epoch, err := execstream.DecodeAgentRequest(r.URL.Query())
		request := nodeapi.AgentExecRequest{ExecRequest: execRequest, Epoch: epoch}
		if r.URL.Path != "/v1/allocations/alloc-1/exec" || !execstream.IsUpgradeRequest(r) || err != nil {
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		test.requests <- request
		conn, err := execstream.Accept(w)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		serve(execstream.NewReader(conn), execstream.NewWriter(conn, 0))
	}))
	t.Cleanup(agent.Close)

	e, s := newExecTestHandlerAt(t, agent.Listener.Addr().String())
	test.server = s
	term, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	test.server.exec.startTerm(term)
	test.cancel = cancel
	e.Pre(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			ctx := context.WithValue(c.Request().Context(), NamespaceContextKey, auth.EncodeScope(auth.AccessNamespace, auth.AccessWrite, "team"))
			c.SetRequest(c.Request().WithContext(ctx))
			return next(c)
		}
	})
	test.leader = httptest.NewServer(e)
	t.Cleanup(test.leader.Close)
	return test
}

func (test *execRelayTest) open(t *testing.T, request api.ExecRequest) *client.ExecStream {
	t.Helper()
	operator, err := client.New(client.Config{Address: test.leader.URL, Namespace: "team"})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := operator.Exec(context.Background(), "alloc-1", request)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stream.Close() })
	return stream
}

func (test *execRelayTest) waitReleased(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		test.server.exec.mu.Lock()
		active := test.server.exec.active
		test.server.exec.mu.Unlock()
		if active == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("relay slots still held: %d", active)
		}
		time.Sleep(time.Millisecond)
	}
}

// waitClosed reads until the peer closes, failing if it does not.
func waitClosed(t *testing.T, closed <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s was not closed", what)
	}
}

func drainUntilClosed(reader *execstream.Reader, closed chan<- struct{}) {
	for {
		if _, err := reader.Next(); err != nil {
			close(closed)
			return
		}
	}
}

func TestExecRelaysStreamBetweenClientAndAgent(t *testing.T) {
	test := newExecRelayTest(t, func(reader *execstream.Reader, writer *execstream.Writer) {
		for {
			frame, err := reader.Next()
			if err != nil {
				return
			}
			switch frame.Type {
			case execstream.FrameStdin:
				_ = writer.WriteData(execstream.FrameStdout, frame.Payload)
			case execstream.FrameResize:
				_ = writer.WriteData(execstream.FrameStderr, frame.Payload)
			case execstream.FrameStdinClose:
				_ = writer.WriteJSON(execstream.FrameExit, api.ExecExit{ExitCode: 4})
				return
			}
		}
	})
	stream := test.open(t, api.ExecRequest{Command: []string{"sh"}, TTY: true, Stdin: true, Term: "xterm"})
	if _, err := stream.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := stream.Resize(100, 30); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseStdin(); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code, err := stream.Wait(&stdout, &stderr)
	if err != nil || code != 4 || stdout.String() != "hello" || stderr.String() != `{"cols":100,"rows":30}` {
		t.Fatalf("wait = %d, %v; stdout %q, stderr %q", code, err, stdout.String(), stderr.String())
	}
	request := <-test.requests
	if request.Task != "web" || request.Epoch != 1 || !request.TTY || !request.Stdin || request.Term != "xterm" || request.Cols != execstream.DefaultCols || request.Rows != execstream.DefaultRows {
		t.Fatalf("agent request = %+v, want resolved task, epoch, and defaulted terminal", request)
	}
	test.waitReleased(t)
}

func TestExecRelayEndsCleanlyWhenLeadershipTermEnds(t *testing.T) {
	agentClosed := make(chan struct{})
	test := newExecRelayTest(t, func(reader *execstream.Reader, _ *execstream.Writer) {
		drainUntilClosed(reader, agentClosed)
	})
	stream := test.open(t, api.ExecRequest{Command: []string{"sh"}})
	<-test.requests
	test.cancel()

	_, err := stream.Wait(io.Discard, io.Discard)
	var execErr *client.ExecError
	if !errors.As(err, &execErr) || !strings.Contains(execErr.Message, "leadership changed") {
		t.Fatalf("wait error = %v, want leadership change", err)
	}
	waitClosed(t, agentClosed, "agent stream")
	test.waitReleased(t)
}

func TestExecRelayReportsLostAgent(t *testing.T) {
	test := newExecRelayTest(t, func(*execstream.Reader, *execstream.Writer) {})
	stream := test.open(t, api.ExecRequest{Command: []string{"sh"}})
	_, err := stream.Wait(io.Discard, io.Discard)
	var execErr *client.ExecError
	if !errors.As(err, &execErr) || !strings.Contains(execErr.Message, "node agent was lost") {
		t.Fatalf("wait error = %v, want lost agent", err)
	}
	test.waitReleased(t)
}

func TestExecRelayClientDisconnectClosesAgentStream(t *testing.T) {
	agentClosed := make(chan struct{})
	test := newExecRelayTest(t, func(reader *execstream.Reader, _ *execstream.Writer) {
		drainUntilClosed(reader, agentClosed)
	})
	stream := test.open(t, api.ExecRequest{Command: []string{"sh"}})
	<-test.requests
	_ = stream.Close()
	waitClosed(t, agentClosed, "agent stream")
	test.waitReleased(t)
}

func TestExecRelayRejectsServerFramesFromClient(t *testing.T) {
	agentClosed := make(chan struct{})
	test := newExecRelayTest(t, func(reader *execstream.Reader, _ *execstream.Writer) {
		drainUntilClosed(reader, agentClosed)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, test.leader.URL+"/v1/namespaces/team/allocations/alloc-1/exec?command=sh", &websocket.DialOptions{Subprotocols: []string{execwebsocket.Protocol}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.CloseNow() }()
	if err := conn.Write(ctx, websocket.MessageBinary, []byte{byte(execstream.FrameExit), '{', '}'}); err != nil {
		t.Fatal(err)
	}
	messageType, message, err := conn.Read(ctx)
	if err != nil || messageType != websocket.MessageBinary || len(message) == 0 || execstream.FrameType(message[0]) != execstream.FrameError || !strings.Contains(string(message[1:]), "unexpected frame type") {
		t.Fatalf("message = %d %q, %v; want error", messageType, message, err)
	}
	waitClosed(t, agentClosed, "agent stream")
}

func TestExecRequestRejections(t *testing.T) {
	e, s := newExecTestHandler(t, http.StatusOK, "")
	tests := []struct {
		name    string
		path    string
		access  auth.AccessLevel
		upgrade bool
		setup   func()
		want    int
	}{
		{name: "read-only credential", path: "/v1/namespaces/team/allocations/alloc-1/exec?command=sh", access: auth.AccessRead, upgrade: true, want: http.StatusForbidden},
		{name: "not an upgrade", path: "/v1/namespaces/team/allocations/alloc-1/exec?command=sh", access: auth.AccessWrite, want: http.StatusBadRequest},
		{name: "missing command", path: "/v1/namespaces/team/allocations/alloc-1/exec", access: auth.AccessWrite, upgrade: true, want: http.StatusBadRequest},
		{name: "invalid term", path: "/v1/namespaces/team/allocations/alloc-1/exec?command=sh&tty=true&term=x%3By", access: auth.AccessWrite, upgrade: true, want: http.StatusBadRequest},
		{name: "relay limit", path: "/v1/namespaces/team/allocations/alloc-1/exec?command=sh", access: auth.AccessWrite, upgrade: true, setup: func() { s.exec.active = execRelayLimit }, want: http.StatusTooManyRequests},
		{name: "no leadership term", path: "/v1/namespaces/team/allocations/alloc-1/exec?command=sh", access: auth.AccessWrite, upgrade: true, setup: func() { s.exec = execRelays{} }, want: http.StatusServiceUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setup != nil {
				tt.setup()
			}
			request := scopedRequest(t, http.MethodGet, tt.path, "", auth.AccessNamespace, tt.access, "team")
			if tt.upgrade {
				execwebsocket.SetRequestHeaders(request)
			}
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, request)
			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d; body: %s", rec.Code, tt.want, rec.Body.String())
			}
		})
	}
}
