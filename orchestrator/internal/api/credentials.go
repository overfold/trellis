package api

import "time"

// CredentialCreateRequest asks the administrator to mint a scoped API credential.
// A positive TTLSeconds makes the credential expire that many seconds after
// creation; zero or absent means it does not expire.
type CredentialCreateRequest struct {
	Scope      string `json:"scope"`
	Access     string `json:"access"`
	Namespace  string `json:"namespace,omitempty"`
	TTLSeconds int64  `json:"ttl_seconds,omitempty"`
}

// CredentialCreateResponse returns the newly minted bearer token exactly once,
// together with the metadata later listings show for it.
type CredentialCreateResponse struct {
	Token string `json:"token"`
	CredentialResponse
}

// CredentialResponse is the listable metadata of an operator credential. It
// never contains the bearer token.
type CredentialResponse struct {
	ID        string     `json:"id"`
	Scope     string     `json:"scope"`
	Access    string     `json:"access"`
	Namespace string     `json:"namespace,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
}

// CredentialListResponse lists operator credentials, including expired ones,
// ordered by creation time.
type CredentialListResponse []CredentialResponse

// CredentialSubjectResponse identifies the workload owning a workload credential.
type CredentialSubjectResponse struct {
	Namespace string `json:"namespace"`
	Job       string `json:"job"`
	TaskGroup string `json:"task_group"`
}

// CredentialInfoResponse describes the credential authenticating the current request.
type CredentialInfoResponse struct {
	Kind      string                     `json:"kind"`
	Scope     string                     `json:"scope"`
	Access    string                     `json:"access"`
	Namespace string                     `json:"namespace,omitempty"`
	Subject   *CredentialSubjectResponse `json:"subject,omitempty"`
	CreatedAt *time.Time                 `json:"created_at,omitempty"`
	ExpiresAt *time.Time                 `json:"expires_at,omitempty"`
}
