package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
)

func TestNodeRegistrationValidatesEndpointBeforePersistence(t *testing.T) {
	for _, tc := range []struct {
		host  string
		port  int
		valid bool
	}{
		{"node-a", 1, true}, {"Node-A.example.test.", 65535, true},
		{"192.0.2.10", 8127, true}, {"2001:db8::1", 8127, true},
		{"::1", 8127, true}, {"fe80::1%eth0", 8127, true},
		{"fe80::1%eth_0", 8127, true}, {"fe80::1%2", 8127, true},
		{strings.Repeat("a", 63), 8127, true},
		{strings.Repeat(strings.Repeat("a", 63)+".", 3) + strings.Repeat("b", 61), 8127, true},
		{"", 8127, false}, {"bad/host", 8127, false}, {" node", 8127, false},
		{"999.999.999.999", 8127, false}, {"192.0.2", 8127, false}, {"node_name", 8127, false},
		{"https://node", 8127, false}, {"node:8127", 8127, false},
		{"[::1]", 8127, false}, {"2001:db8:::1", 8127, false},
		{"fe80::1%eth0/path", 8127, false}, {"fe80::1%bad zone", 8127, false},
		{"-node", 8127, false}, {"node-", 8127, false}, {"node..test", 8127, false},
		{strings.Repeat("a", 64), 8127, false},
		{strings.Repeat("a.", 127) + "a", 8127, false},
		{"node", -1, false}, {"node", 0, false}, {"node", 65536, false},
	} {
		t.Run(tc.host+":"+fmt.Sprint(tc.port), func(t *testing.T) {
			s, agent := newTestServerWithAgent()
			defer agent.server.Close()
			id := uuid.New()
			body, err := json.Marshal(nodeapi.NodeRegistrationRequest{ID: id, Host: tc.host, Port: tc.port})
			if err != nil {
				t.Fatal(err)
			}
			e := echo.New()
			NewHandler(s).Register(e)
			req := httptest.NewRequest(http.MethodPost, "/v1/nodes", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req = req.WithContext(context.WithValue(req.Context(), NodeContextKey, id))
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req)
			want := http.StatusBadRequest
			if tc.valid {
				want = http.StatusCreated
			}
			if rec.Code != want {
				t.Fatalf("status = %d, want %d: %s", rec.Code, want, rec.Body.String())
			}
			nodes, err := s.state.ListNodes(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if !tc.valid {
				if len(nodes) != 0 || len(s.ListNodes()) != 0 {
					t.Fatal("invalid endpoint persisted")
				}
				if err := s.RegisterNode(context.Background(), &NodeRegistration{ID: id, Host: "original", Port: 8127}); err != nil {
					t.Fatal(err)
				}
				if err := s.RegisterNode(context.Background(), &NodeRegistration{ID: id, Host: tc.host, Port: tc.port}); !errors.Is(err, ErrInvalidNodeRegistration) {
					t.Fatalf("invalid re-registration error = %v", err)
				}
				nodes, err = s.state.ListNodes(context.Background())
				if err != nil || nodes[id.String()].Host != "original" || nodes[id.String()].Port != 8127 {
					t.Fatalf("invalid re-registration changed state: %#v, %v", nodes, err)
				}
			} else if len(nodes) != 1 || nodes[id.String()].Host != tc.host || nodes[id.String()].Port != tc.port {
				t.Fatalf("persisted nodes = %#v", nodes)
			}
		})
	}
}

func TestNodeRegistrationRejectsZeroID(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	authenticatedID := uuid.New()
	body, err := json.Marshal(nodeapi.NodeRegistrationRequest{Host: "node-a", Port: 8127})
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
	zeroNode := &Node{ID: uuid.Nil, Version: "before"}
	addTestNode(s, zeroNode, initialHeartbeat)
	body, err := json.Marshal(nodeapi.HeartbeatRequest{Version: "after"})
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
	applyTestObservations(s)
	if heartbeat := s.liveness.lastHeartbeat(uuid.Nil); zeroNode.Version != "before" || !heartbeat.Equal(initialHeartbeat) {
		t.Fatalf("rejected heartbeat updated zero-ID node: version=%q heartbeat=%s", zeroNode.Version, heartbeat)
	}
}

func TestHeartbeatBodyIdentity(t *testing.T) {
	id, other := uuid.New(), uuid.New()
	for _, tc := range []struct {
		name, path, body string
		certificate      uuid.UUID
		admin            bool
		status           int
	}{
		{"matching", id.String(), `{"id":"` + id.String() + `","version":"after"}`, id, false, 200},
		{"missing", id.String(), `{"version":"after"}`, id, false, 200},
		{"zero", id.String(), `{"id":"` + uuid.Nil.String() + `","version":"after"}`, id, false, 200},
		{"null", id.String(), `{"id":null,"version":"after"}`, id, false, 200},
		{"mismatching", id.String(), `{"id":"` + other.String() + `","version":"after"}`, id, false, 400},
		{"malformed ID", id.String(), `{"id":"not-a-uuid","version":"after"}`, id, false, 400},
		{"malformed JSON", id.String(), `{"id":`, id, false, 400},
		{"malformed path", "not-a-uuid", `{}`, id, false, 400},
		{"body matches certificate not path", other.String(), `{"id":"` + id.String() + `"}`, id, false, 403},
		{"body matches path not certificate", other.String(), `{"id":"` + other.String() + `"}`, id, false, 403},
		{"unauthorized missing body ID", other.String(), `{}`, id, false, 403},
		{"authorization before decoding", other.String(), `{"id":`, id, false, 403},
		{"no certificate", id.String(), `{"id":"` + id.String() + `"}`, uuid.Nil, false, 403},
		{"administrator is not node", id.String(), `{"id":"` + id.String() + `"}`, uuid.Nil, true, 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, agent := newTestServerWithAgent()
			defer agent.server.Close()
			initial := time.Date(2026, 9, 30, 11, 0, 0, 0, time.UTC)
			s.now = func() time.Time { return initial.Add(time.Minute) }
			node, otherNode := &Node{ID: id, Version: "before"}, &Node{ID: other, Version: "other"}
			addTestNode(s, node, initial)
			addTestNode(s, otherNode, initial)
			e := echo.New()
			NewHandler(s).Register(e)
			req := httptest.NewRequest(http.MethodPost, "/v1/nodes/"+tc.path+"/heartbeat", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			ctx := req.Context()
			if tc.certificate != uuid.Nil {
				ctx = context.WithValue(ctx, NodeContextKey, tc.certificate)
			}
			if tc.admin {
				ctx = context.WithValue(ctx, AdminContextKey, true)
			}
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req.WithContext(ctx))
			if rec.Code != tc.status {
				t.Fatalf("status = %d, want %d: %s", rec.Code, tc.status, rec.Body.String())
			}
			applyTestObservations(s)
			if tc.status == http.StatusOK {
				if node.Version != "after" || !s.liveness.lastHeartbeat(id).After(initial) {
					t.Fatal("accepted heartbeat did not update path node")
				}
			} else if node.Version != "before" || !s.liveness.lastHeartbeat(id).Equal(initial) {
				t.Fatal("rejected heartbeat changed path node")
			}
			if otherNode.Version != "other" || !s.liveness.lastHeartbeat(other).Equal(initial) {
				t.Fatal("heartbeat changed other node")
			}
		})
	}
}
