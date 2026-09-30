package api

import (
	"encoding/json"
	"time"
)

// BackupFormatVersion is the current desired-state backup format. A backup is
// restored only by a Trellis release that uses the same format version.
const BackupFormatVersion = 5

// BackupSnapshot contains desired state and the replicated cluster settings.
// Secret values remain encrypted exactly as stored in Raft and still require
// the separately managed KEK. Jobs are canonical: every default is explicit.
// The record maps are opaque to clients; a backup is meant to be restored
// unchanged.
type BackupSnapshot struct {
	FormatVersion int `json:"format_version"`
	// TrellisVersion is the version of the Trellis release that created the
	// backup, so an operator can pick a release able to restore it.
	TrellisVersion           string                     `json:"trellis_version"`
	CreatedAt                time.Time                  `json:"created_at"`
	ClusterSettings          ClusterSettings            `json:"cluster_settings"`
	Jobs                     map[string]json.RawMessage `json:"jobs"`
	JobRevisions             map[string]json.RawMessage `json:"job_revisions"`
	Secrets                  map[string]json.RawMessage `json:"secrets"`
	VolumeRegistrations      map[string]json.RawMessage `json:"volume_registrations"`
	NetworkPortRegistrations map[string]json.RawMessage `json:"network_port_registrations"`
	// NetworkSubnetRegistrations are keyed by node ID; a restore into a
	// cluster without those nodes releases them on the first reconciliation.
	NetworkSubnetRegistrations map[string]json.RawMessage `json:"network_subnet_registrations"`
}
