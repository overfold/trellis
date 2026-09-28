package server

import (
	"context"
	"fmt"
	"net/url"

	"github.com/google/uuid"
)

// VolumeRegistration durably binds a namespace-scoped volume identity to the
// node selected for its first placement. The backing host path remains part of
// the job spec and may change without changing this scheduling identity.
type VolumeRegistration struct {
	Namespace string    `json:"namespace"`
	Name      string    `json:"name"`
	NodeID    uuid.UUID `json:"node_id"`
}

// ListVolumeRegistrations loads the durable namespace/name -> node registry.
func (s *StateController) ListVolumeRegistrations(ctx context.Context) (map[string]uuid.UUID, error) {
	prefix := fmt.Sprintf("%s/%s/volume-registrations/", trellisNamespace, s.cluster)
	values, err := listValues[VolumeRegistration](ctx, s.store, prefix)
	if err != nil {
		return nil, err
	}
	result := make(map[string]uuid.UUID, len(values))
	for _, registration := range values {
		if registration.Namespace == "" || registration.Name == "" || registration.NodeID == uuid.Nil {
			return nil, fmt.Errorf("invalid persisted volume registration %#v", registration)
		}
		result[volumeRegistrationKey(registration.Namespace, registration.Name)] = registration.NodeID
	}
	return result, nil
}

// PutVolumeRegistration persists the first-placement owner for a volume.
func (s *StateController) PutVolumeRegistration(ctx context.Context, registration *VolumeRegistration) error {
	if registration == nil || registration.Namespace == "" || registration.Name == "" || registration.NodeID == uuid.Nil {
		return fmt.Errorf("invalid volume registration")
	}
	key := s.volumeRegistrationStorageKey(registration.Namespace, registration.Name)
	if err := s.put(ctx, key, registration); err != nil {
		return fmt.Errorf("put volume registration: %w", err)
	}
	return nil
}

func (s *StateController) volumeRegistrationStorageKey(namespace, name string) string {
	return fmt.Sprintf(
		"%s/%s/volume-registrations/%s",
		trellisNamespace,
		s.cluster,
		url.QueryEscape(volumeRegistrationKey(namespace, name)),
	)
}
