package api

import "time"

// CredentialCreateRequest asks the administrator to mint a scoped API credential.
// A positive TTLSeconds makes the credential expire that many seconds after
// creation; zero or absent means it does not expire.
type CredentialCreateRequest struct {
	Scope      string `json:"scope"`
	Access     string `json:"access"`
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
	Subject   *CredentialSubjectResponse `json:"subject,omitempty"`
	CreatedAt *time.Time                 `json:"created_at,omitempty"`
	ExpiresAt *time.Time                 `json:"expires_at,omitempty"`
}

// JoinTokenCreateRequest asks the administrator to mint a node join token.
// TTLSeconds defaults to one hour and may not exceed seven days. A positive
// MaxUses limits how many nodes may enroll with the token; zero or absent
// allows any number until it expires.
type JoinTokenCreateRequest struct {
	TTLSeconds int64 `json:"ttl_seconds,omitempty"`
	MaxUses    int   `json:"max_uses,omitempty"`
}

// JoinTokenResponse is the listable metadata of a node join token. It never
// contains the token itself.
type JoinTokenResponse struct {
	ID        string    `json:"id"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
	MaxUses   int       `json:"max_uses,omitempty"`
	Uses      int       `json:"uses"`
}

// JoinTokenCreateResponse returns a newly minted join token exactly once,
// together with the metadata later listings show for it.
type JoinTokenCreateResponse struct {
	Token string `json:"token"`
	JoinTokenResponse
}

// JoinTokenListResponse lists unexpired join tokens ordered by creation time.
type JoinTokenListResponse []JoinTokenResponse
