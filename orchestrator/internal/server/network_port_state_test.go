package server

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
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


func TestRegisterNodeRejectsMismatchedWireGuardPortCount(t *testing.T) {
	s := &Server{
		state:              NewStateController(memoryStore{}, "test"),
		nodes:              map[uuid.UUID]*Node{},
		wireGuardPortCount: 256,
		now:                time.Now,
	}
	err := s.RegisterNode(context.Background(), &NodeRegistration{
		ID:                 uuid.New(),
		WireGuardPublicKey: "public-key",
		WireGuardEndpoint:  "node-a:51820",
		WireGuardPortBase:  51820,
		WireGuardPortCount: 64,
	})
	if err == nil {
		t.Fatal("expected mismatched WireGuard port count to be rejected")
	}
}


func TestEnsureNetworkPortRegistrationsReleasesUnusedNamespace(t *testing.T) {
	ctx := context.Background()
	store := memoryStore{}
	state := NewStateController(store, "test")
	s := &Server{state: state, wireGuardPortCount: 2}

	first, err := s.ensureNetworkPortRegistrations(ctx, []string{"acme", "globex"})
	if err != nil {
		t.Fatal(err)
	}
	acmeSlot := first["acme"]
	if _, err := s.ensureNetworkPortRegistrations(ctx, []string{"globex"}); err != nil {
		t.Fatal(err)
	}
	registrations, err := state.ListNetworkPortRegistrations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := registrations["acme"]; exists {
		t.Fatalf("unused namespace registration was not released: %v", registrations)
	}
	third, err := s.ensureNetworkPortRegistrations(ctx, []string{"globex", "initech"})
	if err != nil {
		t.Fatal(err)
	}
	if third["initech"] != acmeSlot {
		t.Fatalf("released slot %d was not reusable: %v", acmeSlot, third)
	}
}
