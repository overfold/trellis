package server

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"

	"github.com/overfold/trellis/internal/api"
	"github.com/overfold/trellis/internal/catalog"
	"github.com/overfold/trellis/internal/client"
)

func newNopStateController() *StateController {
	return NewStateController(memoryStore{}, "test")
}

type testAgent struct {
	server     *httptest.Server
	host       string
	port       int
	mu         sync.Mutex
	calls      []agentCall
	failRun    bool
	failStop   bool
	failResume bool
}

type agentCall struct {
	method string
	path   string
	body   []byte
}

func newTestAgent() *testAgent {
	agent := &testAgent{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		agent.mu.Lock()
		agent.calls = append(agent.calls, agentCall{method: r.Method, path: r.URL.Path, body: body})
		failRun := agent.failRun
		failStop := agent.failStop
		failResume := agent.failResume
		agent.mu.Unlock()
		if failRun && r.Method == http.MethodPost && r.URL.Path == "/v1/allocations" {
			http.Error(w, "run unavailable", http.StatusServiceUnavailable)
			return
		}
		if failStop && r.Method == http.MethodDelete && !strings.HasSuffix(r.URL.Path, "/drain") {
			http.Error(w, "stop unavailable", http.StatusServiceUnavailable)
			return
		}
		if failResume && r.Method == http.MethodDelete {
			http.Error(w, "resume unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(api.OperationResponse{Code: "ok"})
	}))
	host, portStr, _ := net.SplitHostPort(ts.Listener.Addr().String())
	port, _ := strconv.Atoi(portStr)
	agent.server, agent.host, agent.port = ts, "http://"+host, port
	return agent
}

func (a *testAgent) recordedCalls() []agentCall {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]agentCall(nil), a.calls...)
}

func newTestAgentClient() *client.AgentClient {
	return client.NewAgentClient("test-token", nil)
}

func newNopCatalog() *catalog.ServiceCatalog {
	return catalog.New()
}
