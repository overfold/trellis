package api

import (
	"time"

	"github.com/overfold/trellis/internal/spec"
)

// SecretWriteRequest contains a secret value and optional version precondition.
type SecretWriteRequest struct {
	ValueBase64     string  `json:"value_base64"`
	ExpectedVersion *uint64 `json:"expected_version,omitempty"`
}

// SecretMetadata describes a stored secret without exposing its value.
type SecretMetadata struct {
	Namespace      string    `json:"namespace"`
	Name           string    `json:"name"`
	Version        uint64    `json:"version"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
	CiphertextSize int       `json:"ciphertext_size"`
	KeyID          string    `json:"key_id"`
}

// SecretListResponse is the metadata returned when listing secrets.
type SecretListResponse = []SecretMetadata

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
