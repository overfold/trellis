package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/internal/state"
)

// requestAdmin serves one administrator request against a fresh handler.
func requestAdmin(ctx context.Context, t *testing.T, s *Server, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	e := echo.New()
	NewHandler(s).Register(e)
	var reader *strings.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = strings.NewReader(string(raw))
	} else {
		reader = strings.NewReader("")
	}
	req := httptest.NewRequest(method, path, reader).WithContext(context.WithValue(ctx, AdminContextKey, true))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	return rec
}

// failStateWrites makes every durable write on s fail as a lost Raft commit
// would, while reads keep working.
func failStateWrites(s *Server) {
	s.state = NewStateController(undrainFailingStore{memoryStore{}}, "test")
}

func TestJobMutationStateFailuresAreUnavailable(t *testing.T) {
	ctx := context.Background()
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	if _, err := s.RegisterJob(ctx, "default", versionTestSpec("app:v1", 1), nil, nil); err != nil {
		t.Fatal(err)
	}
	failStateWrites(s)

	specJSON, _ := json.Marshal(versionTestSpec("app:v2", 1))
	for _, tc := range []struct {
		name, method, path string
		body               any
	}{
		{"apply", http.MethodPost, "/v1/namespaces/default/jobs", api.JobRegistrationRequest{Spec: specJSON}},
		{"delete", http.MethodDelete, "/v1/namespaces/default/jobs/web", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := requestAdmin(ctx, t, s, tc.method, tc.path, tc.body)
			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want 503; body %s", rec.Code, rec.Body.String())
			}
		})
	}
	if _, ok := s.jobs[jobKey("default", "web")]; !ok {
		t.Fatal("job removed from memory although its delete was not committed")
	}
}

func TestJobEndpointsReportMissingJobsAsNotFound(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	for _, tc := range []struct{ method, path string }{
		{http.MethodDelete, "/v1/namespaces/default/jobs/missing"},
		{http.MethodPost, "/v1/namespaces/default/jobs/missing/restart"},
		{http.MethodGet, "/v1/namespaces/default/jobs/missing/versions"},
	} {
		rec := requestAdmin(context.Background(), t, s, tc.method, tc.path, nil)
		if rec.Code != http.StatusNotFound {
			t.Errorf("%s %s: status = %d, want 404; body %s", tc.method, tc.path, rec.Code, rec.Body.String())
		}
	}
}

func TestRestartJobStateFailureIsUnavailable(t *testing.T) {
	ctx := context.Background()
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	if _, err := s.RegisterJob(ctx, "default", versionTestSpec("app:v1", 1), nil, nil); err != nil {
		t.Fatal(err)
	}
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy}
	s.nodes[node.ID] = node
	s.allocations = []*Allocation{{ID: "a1", Namespace: "default", JobName: "web", TaskGroupName: "api", Node: node, Phase: "running", JobIncarnation: s.jobs[jobKey("default", "web")].Incarnation}}
	failStateWrites(s)
	err := s.RestartJob(ctx, "default", "web")
	if !errors.Is(err, ErrStateUnavailable) {
		t.Fatalf("RestartJob error = %v, want ErrStateUnavailable", err)
	}
	if rec := requestAdmin(ctx, t, s, http.MethodPost, "/v1/namespaces/default/jobs/web/restart", nil); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("restart status = %d, want 503; body %s", rec.Code, rec.Body.String())
	}
}

func TestStopAllocationFailureStatuses(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	if rec := requestAdmin(context.Background(), t, s, http.MethodDelete, "/v1/namespaces/default/allocations/missing", nil); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown allocation status = %d, want 404; body %s", rec.Code, rec.Body.String())
	}

	// A cancelled request stopped waiting for the node's action slot; the
	// stop was never delivered, so it is not "not found".
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy}
	s.nodes[node.ID] = node
	s.allocations = []*Allocation{{ID: "a1", Namespace: "default", JobName: "web", TaskGroupName: "api", Node: node, Phase: "running"}}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	rec := requestAdmin(cancelled, t, s, http.MethodDelete, "/v1/namespaces/default/allocations/a1", nil)
	if rec.Code == http.StatusNotFound {
		t.Fatalf("cancelled stop reported 404: %s", rec.Body.String())
	}
}

func TestDrainNodeFailureStatuses(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	if err := s.DrainNode(context.Background(), uuid.New()); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("drain unknown node error = %v, want ErrNodeNotFound", err)
	}
	if rec := requestAdmin(context.Background(), t, s, http.MethodPost, "/v1/nodes/"+uuid.NewString()+"/drain", nil); rec.Code != http.StatusNotFound {
		t.Fatalf("drain unknown node status = %d, want 404", rec.Code)
	}

	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy}
	s.nodes[node.ID] = node
	failStateWrites(s)
	rec := requestAdmin(context.Background(), t, s, http.MethodPost, "/v1/nodes/"+node.ID.String()+"/drain", nil)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("drain with failed save status = %d, want 503; body %s", rec.Code, rec.Body.String())
	}
	if node.Status != NodeStatusHealthy {
		t.Fatalf("node status = %s after an uncommitted drain", node.Status)
	}
}

func TestRegisterNodeRejectsInvalidCapacityAsBadRequest(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	err := s.RegisterNode(context.Background(), &NodeRegistration{ID: uuid.New(), Host: "node", Port: 8127, CPUCapacity: 1000, MemoryCapacity: 1 << 30, CPUAllocatable: 2000, MemoryAllocatable: 1 << 30})
	if !errors.Is(err, ErrInvalidNodeRegistration) {
		t.Fatalf("RegisterNode error = %v, want ErrInvalidNodeRegistration", err)
	}
	if !strings.Contains(err.Error(), "allocatable CPU") {
		t.Fatalf("error %q does not explain the rejection", err)
	}
}

func TestStateUnavailableKeepsMessage(t *testing.T) {
	cause := errors.New("raft: leadership lost while committing log")
	err := stateUnavailable(cause)
	if !errors.Is(err, ErrStateUnavailable) || !errors.Is(err, cause) || err.Error() != cause.Error() {
		t.Fatalf("stateUnavailable(%v) = %v", cause, err)
	}
	if stateUnavailable(nil) != nil {
		t.Fatal("stateUnavailable(nil) must be nil")
	}
	if !isUnavailable(context.Canceled) || isUnavailable(errors.New("other")) {
		t.Fatal("isUnavailable misclassified an error")
	}
}

var _ state.Store = undrainFailingStore{}

func TestUndrainNodeStateFailureIsUnavailable(t *testing.T) {
	s, agent, node, _ := newDrainedNodeFixture(t)
	defer agent.server.Close()
	failStateWrites(s)
	rec := requestAdmin(context.Background(), t, s, http.MethodDelete, "/v1/nodes/"+node.ID.String()+"/drain", nil)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("undrain with failed save status = %d, want 503; body %s", rec.Code, rec.Body.String())
	}
}
