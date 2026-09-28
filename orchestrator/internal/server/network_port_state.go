package server

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"

	"github.com/overfold/trellis/internal/state"
)

var errNetworkPortExhausted = errors.New("WireGuard namespace port range is exhausted")

// NetworkPortRegistration durably assigns one namespace to a slot in the
// configured WireGuard UDP port range. The same slot is used on every node;
// each node adds it to its own advertised and local base ports.
type NetworkPortRegistration struct {
	Namespace string `json:"namespace"`
	Slot      int    `json:"slot"`
}

// ListNetworkPortRegistrations loads the durable namespace -> port-slot map.
func (s *StateController) ListNetworkPortRegistrations(ctx context.Context) (map[string]int, error) {
	prefix := fmt.Sprintf("%s/%s/network-port-registrations/", trellisNamespace, s.cluster)
	values, err := listValues[NetworkPortRegistration](ctx, s.store, prefix)
	if err != nil {
		return nil, err
	}
	result := make(map[string]int, len(values))
	for _, registration := range values {
		if registration.Namespace == "" || registration.Slot < 0 {
			return nil, fmt.Errorf("invalid persisted network port registration %#v", registration)
		}
		result[registration.Namespace] = registration.Slot
	}
	return result, nil
}

// PutNetworkPortRegistration persists a namespace WireGuard port slot.
func (s *StateController) PutNetworkPortRegistration(ctx context.Context, registration *NetworkPortRegistration) error {
	if registration == nil || registration.Namespace == "" || registration.Slot < 0 {
		return fmt.Errorf("invalid network port registration")
	}
	key := s.networkPortRegistrationKey(registration.Namespace)
	if err := s.put(ctx, key, registration); err != nil {
		return fmt.Errorf("put network port registration: %w", err)
	}
	return nil
}

// DeleteNetworkPortRegistration releases a namespace WireGuard port slot.
func (s *StateController) DeleteNetworkPortRegistration(ctx context.Context, namespace string) error {
	if namespace == "" {
		return fmt.Errorf("network namespace is required")
	}
	key := s.networkPortRegistrationKey(namespace)
	if err := s.store.Delete(ctx, key); err != nil {
		return fmt.Errorf("delete network port registration: %w", err)
	}
	return nil
}

func (s *StateController) commitNetworkPortRegistrations(ctx context.Context, registrations []*NetworkPortRegistration, deleteNamespaces []string) error {
	if len(registrations)+len(deleteNamespaces) == 0 {
		return nil
	}
	atomic, ok := s.store.(state.AtomicStore)
	if !ok {
		return fmt.Errorf("state store does not support atomic network port registration updates")
	}
	mutations := make([]state.Mutation, 0, len(registrations)+len(deleteNamespaces))
	for _, namespace := range deleteNamespaces {
		if namespace == "" {
			return fmt.Errorf("network namespace is required")
		}
		mutations = append(mutations, state.Mutation{Key: s.networkPortRegistrationKey(namespace)})
	}
	for _, registration := range registrations {
		if registration == nil || registration.Namespace == "" || registration.Slot < 0 {
			return fmt.Errorf("invalid network port registration")
		}
		raw, err := json.Marshal(registration)
		if err != nil {
			return fmt.Errorf("marshal network port registration for %s: %w", registration.Namespace, err)
		}
		mutations = append(mutations, state.Mutation{Key: s.networkPortRegistrationKey(registration.Namespace), Value: raw})
	}
	if err := atomic.Batch(ctx, mutations); err != nil {
		return fmt.Errorf("commit network port registrations: %w", err)
	}
	return nil
}

func (s *StateController) networkPortRegistrationKey(namespace string) string {
	return fmt.Sprintf(
		"%s/%s/network-port-registrations/%s",
		trellisNamespace,
		s.cluster,
		url.QueryEscape(namespace),
	)
}

func (s *Server) ensureNetworkPortRegistrations(ctx context.Context, namespaces []string) (map[string]int, error) {
	s.networkPortMu.Lock()
	defer s.networkPortMu.Unlock()

	registrations, err := s.state.ListNetworkPortRegistrations(ctx)
	if err != nil {
		return nil, fmt.Errorf("load network port registrations: %w", err)
	}
	wanted := make(map[string]struct{}, len(namespaces))
	for _, namespace := range namespaces {
		if namespace == "" {
			return nil, fmt.Errorf("network namespace is required")
		}
		wanted[namespace] = struct{}{}
	}
	deleteNamespaces := make([]string, 0)
	for namespace := range registrations {
		if _, keep := wanted[namespace]; keep {
			continue
		}
		deleteNamespaces = append(deleteNamespaces, namespace)
		delete(registrations, namespace)
	}
	sort.Strings(deleteNamespaces)
	if len(namespaces) == 0 {
		if err := s.state.commitNetworkPortRegistrations(ctx, nil, deleteNamespaces); err != nil {
			return nil, err
		}
		return registrations, nil
	}

	count := s.wireGuardPortCount
	if count < 1 {
		return nil, fmt.Errorf("WireGuard namespace port range is not configured")
	}
	used := make(map[int]string, len(registrations))
	for namespace, slot := range registrations {
		if slot >= count {
			return nil, fmt.Errorf("namespace %q uses WireGuard port slot %d outside configured range of %d ports", namespace, slot, count)
		}
		if previous, exists := used[slot]; exists && previous != namespace {
			return nil, fmt.Errorf("WireGuard port slot %d is registered to both %q and %q", slot, previous, namespace)
		}
		used[slot] = namespace
	}

	namespaces = append([]string(nil), namespaces...)
	sort.Strings(namespaces)
	newRegistrations := make([]*NetworkPortRegistration, 0)
	var planErr error
	for _, namespace := range namespaces {
		if _, exists := registrations[namespace]; exists {
			continue
		}
		hash := sha256.Sum256([]byte("wireguard-port\x00" + namespace))
		start := int(binary.BigEndian.Uint32(hash[:4]) % uint32(count))
		assigned := -1
		for offset := 0; offset < count; offset++ {
			slot := (start + offset) % count
			if _, occupied := used[slot]; !occupied {
				assigned = slot
				break
			}
		}
		if assigned < 0 {
			planErr = fmt.Errorf("%w (%d ports); increase wireguard_port_count on every node", errNetworkPortExhausted, count)
			break
		}
		newRegistrations = append(newRegistrations, &NetworkPortRegistration{Namespace: namespace, Slot: assigned})
		registrations[namespace] = assigned
		used[assigned] = namespace
	}
	if err := s.state.commitNetworkPortRegistrations(ctx, newRegistrations, deleteNamespaces); err != nil {
		return nil, err
	}
	return registrations, planErr
}
