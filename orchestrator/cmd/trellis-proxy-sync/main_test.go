package main

import (
	"testing"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/lifecycle"
)

func TestSelectUpstreamsExcludesStoppedHealthyAllocation(t *testing.T) {
	allocs := []api.AllocationResponse{
		{
			Phase:   lifecycle.PhaseStopped,
			Health:  lifecycle.HealthHealthy,
			Address: "10.0.0.1",
			Ports:   []api.PortMapping{{HostPort: 31000, ContainerPort: 8080}},
		},
		{
			Phase:   lifecycle.PhaseRunning,
			Health:  lifecycle.HealthHealthy,
			Address: "10.0.0.2",
			Ports:   []api.PortMapping{{HostPort: 32000, ContainerPort: 8080}},
		},
	}

	got := selectUpstreams(allocs, 8080)
	if len(got) != 1 || got[0] != (upstream{Address: "10.0.0.2", Port: 32000, Weight: 1}) {
		t.Fatalf("upstreams = %+v; want only the running healthy allocation", got)
	}
}

func TestSelectHostPort(t *testing.T) {
	ports := []api.PortMapping{
		{HostPort: 31000, ContainerPort: 8080},
		{HostPort: 32000, ContainerPort: 9090},
	}

	if got, ok := selectHostPort(ports, 0); !ok || got != 31000 {
		t.Fatalf("default port = %d, %v; want 31000, true", got, ok)
	}
	if got, ok := selectHostPort(ports, 9090); !ok || got != 32000 {
		t.Fatalf("selected port = %d, %v; want 32000, true", got, ok)
	}
	if got, ok := selectHostPort(ports, 7070); ok || got != 0 {
		t.Fatalf("missing port = %d, %v; want 0, false", got, ok)
	}
}
