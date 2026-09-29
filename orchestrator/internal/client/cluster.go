package client

import (
	"context"
	"fmt"
	"net/http"

	"github.com/overfold/trellis/internal/api"
	"github.com/overfold/trellis/internal/spec"
)

// ClusterSettings returns the replicated cluster-wide settings.
func (s *ServerClient) ClusterSettings(ctx context.Context) (*api.ClusterSettings, error) {
	var response api.ClusterSettings
	if err := s.client.request(ctx, http.MethodGet, s.address()+"/v1/cluster/settings", nil, &response); err != nil {
		return nil, fmt.Errorf("get cluster settings: %w", err)
	}
	return &response, nil
}

// UpdateJobLimits replaces the cluster's job limits with the administrator credential.
func (s *ServerClient) UpdateJobLimits(ctx context.Context, limits spec.Limits) (*api.ClusterSettings, error) {
	var response api.ClusterSettings
	if err := s.client.request(ctx, http.MethodPut, s.address()+"/v1/cluster/settings/job-limits", limits, &response); err != nil {
		return nil, fmt.Errorf("update job limits: %w", err)
	}
	return &response, nil
}
