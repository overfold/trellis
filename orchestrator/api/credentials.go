package api

import "time"

// CredentialCreateRequest asks the administrator to mint a scoped API credential.
type CredentialCreateRequest struct {
	Scope     string `json:"scope"`
	Access    string `json:"access"`
	Namespace string `json:"namespace,omitempty"`
}

// CredentialCreateResponse returns the newly minted bearer token exactly once.
type CredentialCreateResponse struct {
	Token string `json:"token"`
}

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
}
