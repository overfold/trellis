package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/overfold/trellis/internal/api"
)

func TestNodeRegistrationRejectsZeroID(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	authenticatedID := uuid.New()
	body, err := json.Marshal(api.NodeRegistrationRequest{Host: "node-a", Port: 8127})
	if err != nil {
		t.Fatal(err)
	}
	e := echo.New()
	NewHandler(s).Register(e)
	request := httptest.NewRequest(http.MethodPost, "/v1/nodes", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request = request.WithContext(context.WithValue(request.Context(), NodeContextKey, authenticatedID))
	recorder := httptest.NewRecorder()
	e.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d; body: %s", recorder.Code, http.StatusForbidden, recorder.Body.String())
	}
	if nodes := s.ListNodes(); len(nodes) != 0 {
		t.Fatalf("rejected registration persisted nodes: %#v", nodes)
	}
}

func TestHeartbeatRejectsZeroPathID(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	initialHeartbeat := time.Date(2026, 9, 30, 11, 0, 0, 0, time.UTC)
	zeroNode := &Node{ID: uuid.Nil, LastHeartbeat: initialHeartbeat, Version: "before"}
	s.nodes[uuid.Nil] = zeroNode
	body, err := json.Marshal(api.HeartbeatRequest{Version: "after"})
	if err != nil {
		t.Fatal(err)
	}
	e := echo.New()
	NewHandler(s).Register(e)
	request := httptest.NewRequest(http.MethodPost, "/v1/nodes/"+uuid.Nil.String()+"/heartbeat", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request = request.WithContext(context.WithValue(request.Context(), NodeContextKey, uuid.New()))
	recorder := httptest.NewRecorder()
	e.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d; body: %s", recorder.Code, http.StatusForbidden, recorder.Body.String())
	}
	if zeroNode.Version != "before" || !zeroNode.LastHeartbeat.Equal(initialHeartbeat) {
		t.Fatalf("rejected heartbeat updated zero-ID node: version=%q heartbeat=%s", zeroNode.Version, zeroNode.LastHeartbeat)
	}
}
