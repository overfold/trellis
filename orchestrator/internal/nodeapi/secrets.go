package nodeapi

import "github.com/overfold/trellis/orchestrator/internal/spec"

// DeliveredSecret exists only on the mutually-authenticated leader-to-agent request.
// It is never persisted in desired allocation state.
type DeliveredSecret struct {
	Task    string            `json:"task"`
	Name    string            `json:"name"`
	Version uint64            `json:"version"`
	Target  spec.SecretTarget `json:"target"`
	Env     string            `json:"env,omitempty"`
	Path    string            `json:"path,omitempty"`
	Mode    uint32            `json:"mode,omitempty"`
	Value   []byte            `json:"value"`
}
