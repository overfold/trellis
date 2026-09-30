package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/overfold/trellis/orchestrator/internal/catalog"
	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
)

func TestInternalDiscoveryReturnsOnlyNamespacesAssignedToNode(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	requestingNode, otherNode := &Node{ID: uuid.New()}, &Node{ID: uuid.New()}
	s.allocations = []*Allocation{
		{ID: "acme-local", Namespace: "acme", Node: requestingNode, Phase: lifecycle.PhaseRunning},
		{ID: "other-remote", Namespace: "other", Node: otherNode, Phase: lifecycle.PhaseRunning},
		{ID: "stale-local", Namespace: "stale", Node: requestingNode, Phase: lifecycle.PhaseStopped},
	}
	s.catalog.Replace(map[string][]catalog.ServiceInstance{
		"acme":  {{ID: "acme-service", Job: "web", Group: "app", Address: "10.42.1.2"}},
		"other": {{ID: "other-service", Job: "web", Group: "app", Address: "10.42.2.2"}},
		"stale": {{ID: "stale-service", Job: "web", Group: "app", Address: "10.42.3.2"}},
	})

	e := echo.New()
	NewHandler(s).Register(e)
	request := httptest.NewRequest(http.MethodGet, "/v1/internal/discovery", nil)
	request = request.WithContext(context.WithValue(request.Context(), NodeContextKey, requestingNode.ID))
	recorder := httptest.NewRecorder()
	e.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var entries nodeapi.ServiceListResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &entries); err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Namespace != "acme" || entries[0].ID != "acme-service" {
		t.Fatalf("discovery entries = %#v, want only acme service", entries)
	}
}
