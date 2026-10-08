package dns

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"testing/synctest"
	"time"

	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
)

type failingDiscovery struct {
	services *nodeapi.ServiceListResponse
	err      error
}

func (l *failingDiscovery) ListDiscovery(context.Context) (*nodeapi.ServiceListResponse, error) {
	return l.services, l.err
}

func TestDiscoveryExpiresDuringOutageAndRecovers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		services := nodeapi.ServiceListResponse{{Group: "app", Job: "web", Namespace: "acme", Address: "10.42.1.2"}}
		lookup := &failingDiscovery{services: &services}
		r := NewResolver(nil, lookup, nil, "trellis")
		r.refresh(t.Context())
		query := buildQuery("app.web.acme.trellis.")
		lookup.err = errors.New("leader unavailable")
		time.Sleep(14 * time.Second)
		r.refresh(t.Context())
		if got := r.resolve("app.web.acme.trellis.", ""); len(got) != 1 || got[0].String() != "10.42.1.2" {
			t.Fatalf("bounded outage grace lost endpoint: %v", got)
		}
		time.Sleep(time.Second)
		for _, transport := range []string{"udp", "tcp"} {
			response := r.handleQueryNetwork(query, transport, nil)
			if binary.BigEndian.Uint16(response[2:4])&15 != 2 || binary.BigEndian.Uint16(response[6:8]) != 0 {
				t.Fatalf("expired %s snapshot did not return SERVFAIL without answers: %v", transport, response)
			}
		}
		lookup.err, lookup.services = nil, nil
		r.refresh(t.Context())
		if r.cacheFresh() {
			t.Fatal("nil response renewed stale snapshot")
		}
		services[0].Address = "10.42.1.19"
		lookup.services = &services
		r.refresh(t.Context())
		if got := r.resolve("app.web.acme.trellis.", ""); len(got) != 1 || got[0].String() != "10.42.1.19" {
			t.Fatalf("recovery did not replace stale endpoint: %v", got)
		}
	})
}

func TestResolverSupervisorRecoversBindFailureAndWithdrawsReadiness(t *testing.T) {
	// Occupy TCP only: UDP bind succeeds, but the resolver must not be ready.
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = occupied.Close() }()
	address := occupied.Addr().String()
	empty := nodeapi.ServiceListResponse{}
	r := NewResolver(nil, &mockLookup{services: &empty}, nil, "trellis")
	if err := r.Run(t.Context(), address); err == nil || r.Ready() {
		t.Fatalf("partial listener bind returned %v, ready=%v", err, r.Ready())
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() { r.RunSupervised(ctx, address); close(done) }()
	if err := occupied.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for !r.Ready() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !r.Ready() {
		t.Fatal("resolver did not restart after bind conflict disappeared")
	}
	response, err := exchangeDNS(t.Context(), "tcp", address, buildQuery("missing.web.acme.trellis."))
	if err != nil || len(response) < 12 {
		t.Fatalf("restarted resolver does not serve TCP: %v", err)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("resolver supervisor did not stop")
	}
	if r.Ready() {
		t.Fatal("stopped listener retained readiness")
	}
}
