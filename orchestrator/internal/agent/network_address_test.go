package agent

import (
	"testing"

	"github.com/clofour/trellis/internal/network"
	"github.com/clofour/trellis/internal/spec"
)

func TestAllocationNetworkAddress(t *testing.T) {
	if got := allocationNetworkAddress(nil); got != "" {
		t.Fatalf("nil allocation address = %q, want empty", got)
	}
	if got := allocationNetworkAddress(&Allocation{}); got != "" {
		t.Fatalf("host allocation address = %q, want empty", got)
	}
	allocation := &Allocation{Network: &network.Attachment{Address: "10.86.213.2/24"}}
	if got := allocationNetworkAddress(allocation); got != "10.86.213.2" {
		t.Fatalf("namespace allocation address = %q, want 10.86.213.2", got)
	}
}

func TestHealthCheckAddress(t *testing.T) {
	tests := []struct {
		name    string
		mode    spec.TaskNetworkMode
		address string
		want    string
	}{
		{name: "host", mode: spec.TaskNetworkHost, address: "10.86.213.2", want: "127.0.0.1"},
		{name: "namespace", mode: spec.TaskNetworkWireGuard, address: "10.86.213.2", want: "10.86.213.2"},
		{name: "isolated", mode: spec.TaskNetworkIsolated, address: "10.86.213.2", want: ""},
		{name: "default", mode: spec.TaskNetworkDefault, address: "10.86.213.2", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := &spec.TaskSpec{Networking: &spec.TaskNetworkingSpec{Mode: tt.mode}}
			if got := healthCheckAddress(task, tt.address); got != tt.want {
				t.Fatalf("health check address = %q, want %q", got, tt.want)
			}
		})
	}
	if got := healthCheckAddress(nil, "10.86.213.2"); got != "" {
		t.Fatalf("nil task health check address = %q, want empty", got)
	}
}
