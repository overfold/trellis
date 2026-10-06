package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/internal/auth"
)

func TestEventBusNamespaceSubscriptionSerializesOnlySelectedNamespace(t *testing.T) {
	bus := newEventBus()
	subscriber, ok := bus.subscribe("alpha")
	if !ok {
		t.Fatal("subscribe rejected")
	}
	defer bus.unsubscribe(subscriber)

	bus.publish(api.ClusterEvent{Type: api.EventJobRegistered, Namespace: "alpha", JobName: "web", At: time.Now()})
	bus.publish(api.ClusterEvent{Type: api.EventJobRegistered, Namespace: "beta", JobName: "private", At: time.Now()})

	select {
	case event := <-subscriber:
		serialized, err := json.Marshal(event)
		if err != nil {
			t.Fatalf("serialize event: %v", err)
		}
		if !bytes.Contains(serialized, []byte(`"namespace":"alpha"`)) ||
			!bytes.Contains(serialized, []byte(`"job":"web"`)) ||
			bytes.Contains(serialized, []byte("beta")) {
			t.Fatalf("serialized event = %s, want alpha web event", serialized)
		}
	default:
		t.Fatal("selected namespace event was not delivered")
	}

	select {
	case event := <-subscriber:
		t.Fatalf("received event outside selected namespace: %#v", event)
	default:
	}
}

func TestEventBusClusterSubscriptionReceivesAllNamespaces(t *testing.T) {
	bus := newEventBus()
	subscriber, ok := bus.subscribe("")
	if !ok {
		t.Fatal("subscribe rejected")
	}
	defer bus.unsubscribe(subscriber)

	bus.publish(api.ClusterEvent{Type: api.EventJobRegistered, Namespace: "alpha", At: time.Now()})
	bus.publish(api.ClusterEvent{Type: api.EventJobRegistered, Namespace: "beta", At: time.Now()})

	for _, namespace := range []string{"alpha", "beta"} {
		select {
		case event := <-subscriber:
			if event.Namespace != namespace {
				t.Fatalf("event namespace = %q, want %q", event.Namespace, namespace)
			}
		default:
			t.Fatalf("cluster subscription did not receive %q event", namespace)
		}
	}
}

func TestEventBusRejectsSubscribersAtLimitAndRecovers(t *testing.T) {
	bus := newEventBusWithLimit(2)
	alpha, ok := bus.subscribe("alpha")
	if !ok {
		t.Fatal("first subscribe rejected")
	}
	beta, ok := bus.subscribe("beta")
	if !ok {
		t.Fatal("second subscribe rejected")
	}
	if subscriber, ok := bus.subscribe(""); ok || subscriber != nil {
		t.Fatal("subscriber above limit was admitted")
	}

	bus.publish(api.ClusterEvent{Namespace: "alpha"})
	select {
	case event := <-alpha:
		if event.Namespace != "alpha" {
			t.Fatalf("alpha received %#v", event)
		}
	default:
		t.Fatal("alpha did not receive its event")
	}
	select {
	case event := <-beta:
		t.Fatalf("beta received alpha event: %#v", event)
	default:
	}

	bus.unsubscribe(alpha)
	replacement, ok := bus.subscribe("alpha")
	if !ok {
		t.Fatal("subscriber slot was not recovered")
	}
	bus.unsubscribe(replacement)
	bus.unsubscribe(beta)
}

func TestEventHandlerRejectsOverloadAndReleasesCanceledSubscriber(t *testing.T) {
	bus := newEventBusWithLimit(1)
	handler := NewHandler(&Server{events: bus})

	ctx, cancel := context.WithCancel(t.Context())
	ctx = context.WithValue(ctx, NamespaceContextKey, auth.EncodeScope(auth.AccessCluster, auth.AccessRead))
	req := httptest.NewRequest(http.MethodGet, "/v1/events", nil).WithContext(ctx)
	c := echo.New().NewContext(req, httptest.NewRecorder())
	done := make(chan error, 1)
	go func() { done <- handler.handleClusterEvents(c) }()
	waitForSubscriberCount(t, bus, 1)

	overload := echo.New().NewContext(scopedRequest(t, http.MethodGet, "/v1/events", "", auth.AccessCluster, auth.AccessRead), httptest.NewRecorder())
	err := handler.handleClusterEvents(overload)
	var httpErr *echo.HTTPError
	if !errors.As(err, &httpErr) || httpErr.Code != http.StatusServiceUnavailable {
		t.Fatalf("overload error = %v, want HTTP 503", err)
	}
	if got := overload.Response().Header().Get("Retry-After"); got != "1" {
		t.Fatalf("Retry-After = %q, want 1", got)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("canceled event handler: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("event handler did not stop after cancellation")
	}
	waitForSubscriberCount(t, bus, 0)
	ch, ok := bus.subscribe("")
	if !ok {
		t.Fatal("canceled subscriber did not release admission slot")
	}
	bus.unsubscribe(ch)
}

func waitForSubscriberCount(t *testing.T, bus *EventBus, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		bus.mu.Lock()
		got := len(bus.subscribers)
		bus.mu.Unlock()
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("subscribers = %d, want %d", got, want)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestEventStreamSendsHeadersBeforeTheFirstEvent keeps a quiet event stream
// from looking like an unresponsive server to clients that bound the wait
// for response headers.
func TestEventStreamSendsHeadersBeforeTheFirstEvent(t *testing.T) {
	s, _ := newTestServerWithAgent()
	s.events = newEventBus()
	server := httptest.NewServer(authenticatedHandler(s, auth.AccessRead))
	defer server.Close()
	ctx := t.Context()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/v1/namespaces/default/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: &http.Transport{ResponseHeaderTimeout: 2 * time.Second}}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("event stream headers: %v", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("event stream response = %d %q", response.StatusCode, response.Header.Get("Content-Type"))
	}
}

// TestEventStreamWritesKeepalives lets a quiet stream detect a dead peer, and
// keeps intermediaries from dropping it.
func TestEventStreamWritesKeepalives(t *testing.T) {
	previous := eventKeepaliveInterval
	eventKeepaliveInterval = 10 * time.Millisecond
	t.Cleanup(func() { eventKeepaliveInterval = previous })

	s, _ := newTestServerWithAgent()
	s.events = newEventBus()
	server := httptest.NewServer(authenticatedHandler(s, auth.AccessRead))
	defer server.Close()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+"/v1/namespaces/default/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	line := make([]byte, len(": keepalive\n\n"))
	if _, err := io.ReadFull(response.Body, line); err != nil || string(line) != ": keepalive\n\n" {
		t.Fatalf("quiet stream wrote %q, err=%v; want a keepalive comment", line, err)
	}
}
