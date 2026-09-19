package server

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"net/url"
	"sort"
)

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
	key := fmt.Sprintf(
		"%s/%s/network-port-registrations/%s",
		trellisNamespace,
		s.cluster,
		url.QueryEscape(registration.Namespace),
	)
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
	key := fmt.Sprintf(
		"%s/%s/network-port-registrations/%s",
		trellisNamespace,
		s.cluster,
		url.QueryEscape(namespace),
	)
	if err := s.store.Delete(ctx, key); err != nil {
		return fmt.Errorf("delete network port registration: %w", err)
	}
	return nil
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
	for namespace := range registrations {
		if _, keep := wanted[namespace]; keep {
			continue
		}
		if err := s.state.DeleteNetworkPortRegistration(ctx, namespace); err != nil {
			return nil, err
		}
		delete(registrations, namespace)
	}
	if len(namespaces) == 0 {
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
			return nil, fmt.Errorf("WireGuard namespace port range is exhausted (%d ports); increase wireguard_port_count on every node", count)
		}
		if err := s.state.PutNetworkPortRegistration(ctx, &NetworkPortRegistration{Namespace: namespace, Slot: assigned}); err != nil {
			return nil, err
		}
		registrations[namespace] = assigned
		used[assigned] = namespace
	}
	return registrations, nil
}
