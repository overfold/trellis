package health

import (
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
