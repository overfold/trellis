package health

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/clofour/trellis/internal/spec"
)

func TestNewHealthConfigUsesDefaults(t *testing.T) {
	config := newHealthConfig(&spec.HealthCheckSpec{Type: "tcp", Port: 8080}, "10.86.213.2")
	if config.Interval != defaultCheckInterval {
		t.Fatalf("interval = %s, want %s", config.Interval, defaultCheckInterval)
	}
	if config.Timeout != defaultCheckTimeout {
		t.Fatalf("timeout = %s, want %s", config.Timeout, defaultCheckTimeout)
	}
	if config.Threshold != defaultCheckThreshold {
		t.Fatalf("threshold = %d, want %d", config.Threshold, defaultCheckThreshold)
	}
	if config.Addr != "10.86.213.2" {
		t.Fatalf("address = %q, want 10.86.213.2", config.Addr)
	}
}

func TestNewHealthConfigUsesConfiguredValues(t *testing.T) {
	config := newHealthConfig(&spec.HealthCheckSpec{
		Type: "tcp", Port: 8080, Interval: 2 * time.Second, Timeout: time.Second, Threshold: 5,
	}, "127.0.0.1")
	if config.Interval != 2*time.Second || config.Timeout != time.Second || config.Threshold != 5 {
		t.Fatalf("unexpected health config: %#v", config)
	}
}

func TestNetworkCheckWithoutReachableAddressFailsClosed(t *testing.T) {
	h := NewHealthManager(nil, nil, nil)
	for _, kind := range []spec.HealthCheckType{spec.HealthCheckHTTP, spec.HealthCheckTCP} {
		t.Run(string(kind), func(t *testing.T) {
			config := newHealthConfig(&spec.HealthCheckSpec{Type: kind, Port: 8080}, "")
			ok, err := h.runHealthCheck(context.Background(), &trackedTask{config: config})
			if ok || err == nil {
				t.Fatalf("check = %v, %v; want explicit unreachable-address failure", ok, err)
			}
			if !strings.Contains(err.Error(), "no agent-reachable network address") {
				t.Fatalf("error = %q, want unreachable-address explanation", err)
			}
		})
	}
}
