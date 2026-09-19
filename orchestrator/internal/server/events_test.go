package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/auth"
	"github.com/labstack/echo/v5"
)

func TestEventBusNamespaceSubscriptionSerializesOnlySelectedNamespace(t *testing.T) {
	bus := newEventBus()
	req := scopedRequest(t, http.MethodGet, "/v1/events", "", auth.AccessNamespace, auth.AccessRead, "alpha")
	subscriber := bus.subscribe(requestNamespace(echo.New().NewContext(req, httptest.NewRecorder())))
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
	req := scopedRequest(t, http.MethodGet, "/v1/events", "", auth.AccessCluster, auth.AccessRead, "")
	subscriber := bus.subscribe(requestNamespace(echo.New().NewContext(req, httptest.NewRecorder())))
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
