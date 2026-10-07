package server

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
)

func commitNetworkPortPlan(t *testing.T, s *Server, namespaces []string) (map[string]int, error) {
	t.Helper()
	registrations, additions, deletions, planErr := s.planNetworkPortRegistrations(context.Background(), namespaces)
	if planErr != nil && !errors.Is(planErr, errNetworkPortExhausted) {
		return nil, planErr
	}
	if err := s.state.CommitReconciliation(context.Background(), &ReconciliationCommit{
		NetworkPortRegistrations:    additions,
		DeleteNetworkPortNamespaces: deletions,
	}); err != nil {
		return nil, err
	}
	return registrations, planErr
}

func TestEnsureNetworkPortRegistrationsAreStableAndUnique(t *testing.T) {
	store := memoryStore{}
	s := &Server{
		state:              NewStateController(store, "test"),
		wireGuardPortCount: 8,
	}

	first, err := commitNetworkPortPlan(t, s, []string{"acme", "globex"})
	if err != nil {
		t.Fatal(err)
	}
	if first["acme"] == first["globex"] {
		t.Fatalf("namespaces share slot %d", first["acme"])
	}
	second, err := commitNetworkPortPlan(t, s, []string{"globex", "acme"})
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
	registrations, err := commitNetworkPortPlan(t, s, []string{"acme", "globex"})
	if !errors.Is(err, errNetworkPortExhausted) || len(registrations) != 1 {
		t.Fatalf("registrations = %v, error = %v; want one registration and exhaustion", registrations, err)
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
		Host:               "node-a",
		Port:               8127,
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

	first, err := commitNetworkPortPlan(t, s, []string{"acme", "globex"})
	if err != nil {
		t.Fatal(err)
	}
	acmeSlot := first["acme"]
	if _, err := commitNetworkPortPlan(t, s, []string{"globex"}); err != nil {
		t.Fatal(err)
	}
	registrations, err := state.ListNetworkPortRegistrations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := registrations["acme"]; exists {
		t.Fatalf("unused namespace registration was not released: %v", registrations)
	}
	third, err := commitNetworkPortPlan(t, s, []string{"globex", "initech"})
	if err != nil {
		t.Fatal(err)
	}
	if third["initech"] != acmeSlot {
		t.Fatalf("released slot %d was not reusable: %v", acmeSlot, third)
	}
}

func TestEnsureNetworkPortRegistrationsFailureIsAtomicAndDeterministic(t *testing.T) {
	ctx := context.Background()
	store := &failingBatchStore{memoryStore: memoryStore{}}
	state := NewStateController(store, "test")
	for namespace, slot := range map[string]int{"zeta": 0, "acme": 1} {
		if err := state.PutNetworkPortRegistration(ctx, &NetworkPortRegistration{Namespace: namespace, Slot: slot}); err != nil {
			t.Fatal(err)
		}
	}
	s := &Server{state: state, wireGuardPortCount: 2}

	registrations, err := commitNetworkPortPlan(t, s, []string{"gamma", "beta"})
	if err == nil || registrations != nil {
		t.Fatalf("registrations = %v, error = %v; want failed atomic commit", registrations, err)
	}
	persisted, err := state.ListNetworkPortRegistrations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(persisted, map[string]int{"zeta": 0, "acme": 1}) {
		t.Fatalf("persisted registrations after failed transition = %v", persisted)
	}
	if len(store.batches) != 1 {
		t.Fatalf("batch count = %d, want 1", len(store.batches))
	}
	keys := make([]string, len(store.batches[0]))
	for i, mutation := range store.batches[0] {
		keys[i] = mutation.Key
	}
	wantKeys := []string{
		"trellis/test/network-port-registrations/acme",
		"trellis/test/network-port-registrations/zeta",
		"trellis/test/network-port-registrations/beta",
		"trellis/test/network-port-registrations/gamma",
	}
	if !reflect.DeepEqual(keys, wantKeys) {
		t.Fatalf("batch mutation order = %v, want %v", keys, wantKeys)
	}
}
