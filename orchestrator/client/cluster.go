package client

import (
	"context"
	"fmt"
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
	var snapshot api.BackupSnapshot
	if err := c.request(ctx, http.MethodGet, c.clusterPath("/v1/backup"), nil, &snapshot); err != nil {
		return nil, fmt.Errorf("create backup: %w", err)
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
