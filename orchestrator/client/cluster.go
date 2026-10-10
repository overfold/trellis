package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/api"
)

// CredentialInfo returns the identity and effective authorization of the
// client's credential.
func (c *Client) CredentialInfo(ctx context.Context) (*api.CredentialInfoResponse, error) {
	var response api.CredentialInfoResponse
	if err := c.request(ctx, http.MethodGet, c.clusterPath("/v1/auth/whoami"), nil, &response); err != nil {
		return nil, fmt.Errorf("inspect credential: %w", err)
	}
	return &response, nil
}

// CreateCredential mints a scoped API credential. It requires the
// administrator key. The response carries the token exactly once.
func (c *Client) CreateCredential(ctx context.Context, request *api.CredentialCreateRequest) (*api.CredentialCreateResponse, error) {
	var response api.CredentialCreateResponse
	if err := c.request(ctx, http.MethodPost, c.clusterPath("/v1/credentials"), request, &response); err != nil {
		return nil, fmt.Errorf("create credential: %w", err)
	}
	return &response, nil
}

// ListCredentials lists operator credential metadata with the administrator
// credential. Listings never include bearer tokens.
func (c *Client) ListCredentials(ctx context.Context) (api.CredentialListResponse, error) {
	var response api.CredentialListResponse
	if err := c.request(ctx, http.MethodGet, c.clusterPath("/v1/credentials"), nil, &response); err != nil {
		return nil, fmt.Errorf("list credentials: %w", err)
	}
	return response, nil
}

// RevokeCredential deletes an operator credential by its listed ID with the
// administrator credential.
func (c *Client) RevokeCredential(ctx context.Context, id string) error {
	if err := c.request(ctx, http.MethodDelete, c.clusterPath("/v1/credentials/%s", url.PathEscape(id)), nil, nil); err != nil {
		return fmt.Errorf("revoke credential: %w", err)
	}
	return nil
}

// CreateJoinToken mints a node join token with the administrator credential.
func (c *Client) CreateJoinToken(ctx context.Context, request *api.JoinTokenCreateRequest) (*api.JoinTokenCreateResponse, error) {
	var response api.JoinTokenCreateResponse
	if err := c.request(ctx, http.MethodPost, c.clusterPath("/v1/nodes/join-tokens"), request, &response); err != nil {
		return nil, fmt.Errorf("create join token: %w", err)
	}
	return &response, nil
}

// ListJoinTokens lists unexpired join token metadata with the administrator
// credential. Listings never include tokens.
func (c *Client) ListJoinTokens(ctx context.Context) (api.JoinTokenListResponse, error) {
	var response api.JoinTokenListResponse
	if err := c.request(ctx, http.MethodGet, c.clusterPath("/v1/nodes/join-tokens"), nil, &response); err != nil {
		return nil, fmt.Errorf("list join tokens: %w", err)
	}
	return response, nil
}

// RevokeJoinToken deletes a join token by its listed ID with the administrator
// credential.
func (c *Client) RevokeJoinToken(ctx context.Context, id string) error {
	if err := c.request(ctx, http.MethodDelete, c.clusterPath("/v1/nodes/join-tokens/%s", url.PathEscape(id)), nil, nil); err != nil {
		return fmt.Errorf("revoke join token: %w", err)
	}
	return nil
}

// ListNamespaces returns the namespaces visible to the client's credential.
func (c *Client) ListNamespaces(ctx context.Context) (api.NamespaceListResponse, error) {
	var response api.NamespaceListResponse
	if err := c.request(ctx, http.MethodGet, c.clusterPath("/v1/namespaces"), nil, &response); err != nil {
		return nil, fmt.Errorf("list namespaces: %w", err)
	}
	return response, nil
}

// ListNodes returns every registered node. It requires cluster scope.
func (c *Client) ListNodes(ctx context.Context) (api.NodeListResponse, error) {
	var response api.NodeListResponse
	if err := c.request(ctx, http.MethodGet, c.clusterPath("/v1/nodes"), nil, &response); err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}
	return response, nil
}

// PromoteNode grants control-plane eligibility to an enrolled worker. It
// requires administrator signing and does not reconfigure the node daemon.
func (c *Client) PromoteNode(ctx context.Context, id uuid.UUID) error {
	if err := c.request(ctx, http.MethodPost, c.clusterPath("/v1/nodes/%s/promote", id), nil, nil); err != nil {
		return fmt.Errorf("promote node: %w", err)
	}
	return nil
}

// EnrollNodeIdentity binds an externally signed node certificate and role.
// It requires administrator signing and is available only in external mode.
func (c *Client) EnrollNodeIdentity(ctx context.Context, request *api.NodeIdentityCreateRequest) (*api.NodeIdentityCreateResponse, error) {
	var response api.NodeIdentityCreateResponse
	if err := c.request(ctx, http.MethodPost, c.clusterPath("/v1/nodes/identities"), request, &response); err != nil {
		return nil, fmt.Errorf("enroll external node: %w", err)
	}
	return &response, nil
}

// DrainNode stops scheduling onto a node and evacuates its allocations.
func (c *Client) DrainNode(ctx context.Context, id uuid.UUID) error {
	if err := c.request(ctx, http.MethodPost, c.clusterPath("/v1/nodes/%s/drain", id), nil, nil); err != nil {
		return fmt.Errorf("drain node: %w", err)
	}
	return nil
}

// UndrainNode makes a drained node schedulable again.
func (c *Client) UndrainNode(ctx context.Context, id uuid.UUID) error {
	if err := c.request(ctx, http.MethodDelete, c.clusterPath("/v1/nodes/%s/drain", id), nil, nil); err != nil {
		return fmt.Errorf("undrain node: %w", err)
	}
	return nil
}

// TransferLeadership asks the control plane to move leadership to another
// voter. It requires the administrator key.
func (c *Client) TransferLeadership(ctx context.Context) error {
	if err := c.request(ctx, http.MethodPost, c.clusterPath("/v1/raft/leadership-transfer"), nil, nil); err != nil {
		return fmt.Errorf("transfer leadership: %w", err)
	}
	return nil
}

// RemoveRaftMember permanently removes a server from the control plane's
// Raft configuration. id is its Raft server ID. It requires the
// administrator key.
func (c *Client) RemoveRaftMember(ctx context.Context, id string) error {
	if err := c.request(ctx, http.MethodDelete, c.clusterPath("/v1/raft/members/%s", url.PathEscape(id)), nil, nil); err != nil {
		return fmt.Errorf("remove raft member: %w", err)
	}
	return nil
}

// ClusterLeader identifies the current serving leader. It requires cluster
// scope. A later membership mutation still checks leadership and quorum itself.
func (c *Client) ClusterLeader(ctx context.Context) (*api.ClusterLeaderResponse, error) {
	var response api.ClusterLeaderResponse
	if err := c.request(ctx, http.MethodGet, c.clusterPath("/v1/cluster/leader"), nil, &response); err != nil {
		return nil, fmt.Errorf("get cluster leader: %w", err)
	}
	if response.LeaderID == uuid.Nil {
		return nil, fmt.Errorf("get cluster leader: response has no leader identity")
	}
	return &response, nil
}

// ClusterSettings returns the replicated cluster-wide settings.
func (c *Client) ClusterSettings(ctx context.Context) (*api.ClusterSettings, error) {
	var response api.ClusterSettings
	if err := c.request(ctx, http.MethodGet, c.clusterPath("/v1/cluster/settings"), nil, &response); err != nil {
		return nil, fmt.Errorf("get cluster settings: %w", err)
	}
	return &response, nil
}

// UpdateJobLimits replaces the cluster's job limits. It requires the
// administrator key.
func (c *Client) UpdateJobLimits(ctx context.Context, limits api.JobLimits) (*api.ClusterSettings, error) {
	var response api.ClusterSettings
	if err := c.request(ctx, http.MethodPut, c.clusterPath("/v1/cluster/settings/job-limits"), limits, &response); err != nil {
		return nil, fmt.Errorf("update job limits: %w", err)
	}
	return &response, nil
}

// UpdateReconciliationSettings replaces the cluster's reconciliation
// settings. It requires the administrator key.
func (c *Client) UpdateReconciliationSettings(ctx context.Context, settings api.ReconciliationSettings) (*api.ClusterSettings, error) {
	var response api.ClusterSettings
	if err := c.request(ctx, http.MethodPut, c.clusterPath("/v1/cluster/settings/reconciliation"), settings, &response); err != nil {
		return nil, fmt.Errorf("update reconciliation settings: %w", err)
	}
	return &response, nil
}

// CreateBackup downloads a desired-state backup. It requires the
// administrator key.
func (c *Client) CreateBackup(ctx context.Context) (*api.BackupSnapshot, error) {
	body, err := c.transport.Stream(ctx, c.clusterPath("/v1/backup"))
	if err != nil {
		return nil, fmt.Errorf("create backup: %w", publicError(err))
	}
	defer func() { _ = body.Close() }()
	var snapshot api.BackupSnapshot
	decoder := json.NewDecoder(body)
	if err := decoder.Decode(&snapshot); err != nil {
		return nil, fmt.Errorf("create backup: %w", err)
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, fmt.Errorf("create backup: unexpected trailing data")
	}
	return &snapshot, nil
}

// RestoreBackup restores a desired-state backup. It requires the
// administrator key.
func (c *Client) RestoreBackup(ctx context.Context, snapshot *api.BackupSnapshot) error {
	if err := c.request(ctx, http.MethodPost, c.clusterPath("/v1/backup/restore"), snapshot, nil); err != nil {
		return fmt.Errorf("restore backup: %w", err)
	}
	return nil
}
