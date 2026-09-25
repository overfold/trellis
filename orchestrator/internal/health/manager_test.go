package health

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/clofour/trellis/internal/spec"
)

func TestNewHealthConfigUsesDefaults(t *testing.T) {
	config := newHealthConfig(&spec.HealthCheckSpec{Type: "tcp", Port: 8080})
	if config.Interval != defaultCheckInterval {
		t.Fatalf("interval = %s, want %s", config.Interval, defaultCheckInterval)
	}
	if config.Timeout != defaultCheckTimeout {
		t.Fatalf("timeout = %s, want %s", config.Timeout, defaultCheckTimeout)
	}
	if config.Threshold != defaultCheckThreshold {
		t.Fatalf("threshold = %d, want %d", config.Threshold, defaultCheckThreshold)
	}
}

func TestNetworkChecksUseProvidedDialer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	serverAddr := strings.TrimPrefix(server.URL, "http://")
	_, portText, err := net.SplitHostPort(serverAddr)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}
	var called atomic.Int32
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		called.Add(1)
		return (&net.Dialer{}).DialContext(ctx, network, serverAddr)
	}
	for _, check := range []func() (bool, error){
		func() (bool, error) { return checkHTTP(context.Background(), "127.0.0.1", port, "/health", dial) },
		func() (bool, error) { return checkTCP(context.Background(), "127.0.0.1", port, dial) },
	} {
		ok, err := check()
		if err != nil || !ok {
			t.Fatalf("check = %v, %v", ok, err)
		}
	}
	if called.Load() != 2 {
		t.Fatalf("dial calls = %d, want 2", called.Load())
	}
}

func TestNewHealthConfigUsesConfiguredValues(t *testing.T) {
	config := newHealthConfig(&spec.HealthCheckSpec{
		Type: "tcp", Port: 8080, Interval: 2 * time.Second, Timeout: time.Second, Threshold: 5,
	})
	if config.Interval != 2*time.Second || config.Timeout != time.Second || config.Threshold != 5 {
		t.Fatalf("unexpected health config: %#v", config)
	}
}
