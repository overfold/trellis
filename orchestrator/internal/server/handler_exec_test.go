package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/overfold/trellis/orchestrator/internal/auth"
	"github.com/overfold/trellis/orchestrator/internal/execstream"
	"github.com/overfold/trellis/orchestrator/internal/spec"
)

// newExecTestHandler places one allocation on a node whose agent answers
// every request with status and an Echo-style error body.
func newExecTestHandler(t *testing.T, status int, message string) (*echo.Echo, *Server) {
	t.Helper()
	agent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]string{"message": message})
	}))
	t.Cleanup(agent.Close)
	return newExecTestHandlerAt(t, agent.Listener.Addr().String())
}

func newExecTestHandlerAt(t *testing.T, address string) (*echo.Echo, *Server) {
	t.Helper()
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}
	s := newExecTestServer(t)
	s.allocations = []*Allocation{{
		Namespace: "team",
		ID:        "alloc-1",
		Tasks:     []spec.TaskSpec{{Name: "web"}},
		Node:      &Node{ID: uuid.New(), Host: "http://" + host, Port: port},
	}}
	e := echo.New()
	NewHandler(s).Register(e)
	return e, s
}

// newExecTestServer returns a server leading term 1 until the test ends.
func newExecTestServer(t *testing.T) *Server {
	t.Helper()
	s := &Server{log: slog.New(slog.DiscardHandler), client: newTestAgentClient(), controlEpoch: 1}
	term, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s.exec.startTerm(term)
	return s
}

type execErrorRoute struct {
	name   string
	method string
	path   string
	body   string
	// agentNotFound is the public status for an agent 404 on this route.
	agentNotFound int
}

var execErrorRoutes = []execErrorRoute{
	{name: "exec", method: http.MethodGet, path: "/v1/namespaces/team/allocations/alloc-1/exec?command=true", agentNotFound: http.StatusConflict},
	{name: "metrics", method: http.MethodGet, path: "/v1/namespaces/team/allocations/alloc-1/metrics", agentNotFound: http.StatusConflict},
}

func serveExecRequest(t *testing.T, e *echo.Echo, route execErrorRoute, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	request := scopedRequest(t, route.method, path, route.body, auth.AccessNamespace, auth.AccessWrite, "team")
	execstream.SetUpgradeHeaders(request)
	e.ServeHTTP(rec, request)
	return rec
}

func errorMessage(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error body %q: %v", rec.Body.String(), err)
	}
	return body.Message
}

func TestAgentExecErrorsMapToPublicStatus(t *testing.T) {
	tests := []struct {
		name        string
		agentStatus int
		agentBody   string
		// want is the public status; zero uses the route's agentNotFound.
		want        int
		wantMessage string
	}{
		{name: "task required", agentStatus: http.StatusBadRequest, agentBody: "exec task selection required: allocation alloc-1 has running tasks a, b; specify task", want: http.StatusBadRequest, wantMessage: "exec task selection required: allocation alloc-1 has running tasks a, b; specify task"},
		{name: "execution conflict", agentStatus: http.StatusConflict, agentBody: "allocation execution metadata conflict", want: http.StatusConflict, wantMessage: "allocation execution metadata conflict"},
		{name: "session limit", agentStatus: http.StatusTooManyRequests, agentBody: "exec session limit reached: node has 64 interactive sessions (maximum 64)", want: http.StatusTooManyRequests, wantMessage: "exec session limit reached: node has 64 interactive sessions (maximum 64)"},
		{name: "agent shutting down", agentStatus: http.StatusServiceUnavailable, agentBody: "agent is shutting down", want: http.StatusServiceUnavailable, wantMessage: "node agent unavailable: agent is shutting down"},
		{name: "stale epoch", agentStatus: http.StatusConflict, agentBody: "stale control-plane epoch: received 1, highest accepted 2", want: http.StatusConflict, wantMessage: "stale control-plane epoch"},
		{name: "agent not found", agentStatus: http.StatusNotFound, agentBody: "allocation not found"},
		{name: "agent internal failure", agentStatus: http.StatusInternalServerError, agentBody: "exec in container c-1: runtime detail", want: http.StatusBadGateway, wantMessage: "node agent failed to handle the request"},
		{name: "agent rejects server credentials", agentStatus: http.StatusForbidden, agentBody: "caller is not the leader", want: http.StatusBadGateway, wantMessage: "node agent failed to handle the request"},
	}
	for _, tt := range tests {
		for _, route := range execErrorRoutes {
			t.Run(tt.name+"/"+route.name, func(t *testing.T) {
				e, _ := newExecTestHandler(t, tt.agentStatus, tt.agentBody)
				rec := serveExecRequest(t, e, route, route.path)
				want, wantMessage := tt.want, tt.wantMessage
				if want == 0 {
					want = route.agentNotFound
					wantMessage = tt.agentBody
					if want == http.StatusConflict {
						wantMessage = "allocation alloc-1 "
					}
				}
				if rec.Code != want {
					t.Fatalf("status = %d, want %d; body: %s", rec.Code, want, rec.Body.String())
				}
				if got := errorMessage(t, rec); !strings.HasPrefix(got, wantMessage) {
					t.Fatalf("message = %q, want prefix %q", got, wantMessage)
				}
			})
		}
	}
}

func TestAgentExecNoRunningTargetNamesAllocationAndTask(t *testing.T) {
	e, _ := newExecTestHandler(t, http.StatusNotFound, "allocation not found: allocation alloc-1 has no running task \"web\"")
	rec := serveExecRequest(t, e, execErrorRoutes[0], execErrorRoutes[0].path)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body: %s", rec.Code, rec.Body.String())
	}
	route := execErrorRoute{method: http.MethodGet}
	rec = serveExecRequest(t, e, route, "/v1/namespaces/team/allocations/alloc-1/exec?task=web&command=true")
	if got, want := errorMessage(t, rec), `allocation alloc-1 task "web" is not running`; rec.Code != http.StatusConflict || got != want {
		t.Fatalf("status = %d, message = %q; want 409 %q", rec.Code, got, want)
	}
}

func TestAgentExecUnreachableAgentIsUnavailable(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	e, _ := newExecTestHandlerAt(t, address)
	for _, route := range execErrorRoutes {
		t.Run(route.name, func(t *testing.T) {
			rec := serveExecRequest(t, e, route, route.path)
			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want 503; body: %s", rec.Code, rec.Body.String())
			}
			if got := errorMessage(t, rec); got != "node agent unavailable" {
				t.Fatalf("message = %q, want %q", got, "node agent unavailable")
			}
		})
	}
}

func TestExecControlPlaneLookupErrors(t *testing.T) {
	e, _ := newExecTestHandler(t, http.StatusOK, "")
	for _, route := range execErrorRoutes {
		t.Run(route.name+"/unknown allocation", func(t *testing.T) {
			rec := serveExecRequest(t, e, route, strings.Replace(route.path, "alloc-1", "alloc-2", 1))
			if rec.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want 404; body: %s", rec.Code, rec.Body.String())
			}
		})
	}
	t.Run("exec/unknown task", func(t *testing.T) {
		rec := serveExecRequest(t, e, execErrorRoutes[0], "/v1/namespaces/team/allocations/alloc-1/exec?task=db&command=true")
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400; body: %s", rec.Code, rec.Body.String())
		}
	})
}
