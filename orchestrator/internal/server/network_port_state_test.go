package server

import (
	"context"
	"testing"
)

func TestEnsureNetworkPortRegistrationsAreStableAndUnique(t *testing.T) {
	ctx := context.Background()
	store := memoryStore{}
	s := &Server{
		state:              NewStateController(store, "test"),
		wireGuardPortCount: 8,
	}

	first, err := s.ensureNetworkPortRegistrations(ctx, []string{"acme", "globex"})
	if err != nil {
		t.Fatal(err)
	}
	if first["acme"] == first["globex"] {
		t.Fatalf("namespaces share slot %d", first["acme"])
	}
	second, err := s.ensureNetworkPortRegistrations(ctx, []string{"globex", "acme"})
	if err != nil {
		t.Fatal(err)
	}
	if second["acme"] != first["acme"] || second["globex"] != first["globex"] {
		t.Fatalf("registrations changed: first=%v second=%v", first, second)
	}
}

func TestEnsureNetworkPortRegistrationsRejectsExhaustion(t *testing.T) {
	s := &Server{
		state:              NewStateController(memoryStore{}, "test"),
		wireGuardPortCount: 1,
	}
	if _, err := s.ensureNetworkPortRegistrations(context.Background(), []string{"acme", "globex"}); err == nil {
		t.Fatal("expected namespace WireGuard port range exhaustion")
	}
}
