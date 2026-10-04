package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/overfold/trellis/orchestrator/api"
)

// ListJobs returns the jobs in the client's namespace.
func (c *Client) ListJobs(ctx context.Context) (api.JobListResponse, error) {
	path, err := c.namespacedPath("/jobs")
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	var response api.JobListResponse
	if err := c.request(ctx, http.MethodGet, path, nil, &response); err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	return response, nil
}

// GetJob returns a job, its canonical specification, and its allocations.
func (c *Client) GetJob(ctx context.Context, name string) (*api.JobStatusResponse, error) {
	path, err := c.namespacedPath("/jobs/%s", url.PathEscape(name))
	if err != nil {
		return nil, fmt.Errorf("get job: %w", err)
	}
	var response api.JobStatusResponse
	if err := c.request(ctx, http.MethodGet, path, nil, &response); err != nil {
		return nil, fmt.Errorf("get job: %w", err)
	}
	return &response, nil
}

// PlanJob returns the control plane's semantic plan for applying spec, a
// JSON job specification in the client's namespace. Passing the plan's
// BaseVersion and BaseIncarnation as the preconditions of ApplyJob applies
// the specification only if the job has not changed since it was planned.
// Also pass ResolvedImages to deploy the reviewed digests even if tags move.
func (c *Client) PlanJob(ctx context.Context, spec json.RawMessage) (*api.JobPlanResponse, error) {
	path, err := c.namespacedPath("/jobs/plan")
	if err != nil {
		return nil, fmt.Errorf("plan job: %w", err)
	}
	var response api.JobPlanResponse
	if err := c.request(ctx, http.MethodPost, path, &api.JobRegistrationRequest{Spec: spec}, &response); err != nil {
		return nil, fmt.Errorf("plan job: %w", err)
	}
	return &response, nil
}

// ApplyJob creates or updates a job in the client's namespace, which must
// be the specification's namespace. Without ResolvedImages it resolves tags
// afresh; with the complete plan/history pins it uses those artifacts.
// A failed precondition returns an
// *HTTPError with status 409 Conflict; an invalid specification returns
// status 422 Unprocessable Entity.
func (c *Client) ApplyJob(ctx context.Context, request *api.JobRegistrationRequest) (*api.JobRegistrationResponse, error) {
	path, err := c.namespacedPath("/jobs")
	if err != nil {
		return nil, fmt.Errorf("apply job: %w", err)
	}
	var response api.JobRegistrationResponse
	if err := c.request(ctx, http.MethodPost, path, request, &response); err != nil {
		return nil, fmt.Errorf("apply job: %w", err)
	}
	return &response, nil
}

// DeleteJob deletes a job and stops its allocations.
func (c *Client) DeleteJob(ctx context.Context, name string) error {
	path, err := c.namespacedPath("/jobs/%s", url.PathEscape(name))
	if err != nil {
		return fmt.Errorf("delete job: %w", err)
	}
	if err := c.request(ctx, http.MethodDelete, path, nil, nil); err != nil {
		return fmt.Errorf("delete job: %w", err)
	}
	return nil
}

// RestartJob replaces a job's allocations without changing its
// specification.
func (c *Client) RestartJob(ctx context.Context, name string) error {
	path, err := c.namespacedPath("/jobs/%s/restart", url.PathEscape(name))
	if err != nil {
		return fmt.Errorf("restart job: %w", err)
	}
	if err := c.request(ctx, http.MethodPost, path, nil, nil); err != nil {
		return fmt.Errorf("restart job: %w", err)
	}
	return nil
}

// ListJobVersions returns the retained versions of a job.
func (c *Client) ListJobVersions(ctx context.Context, name string) (api.JobVersionListResponse, error) {
	path, err := c.namespacedPath("/jobs/%s/versions", url.PathEscape(name))
	if err != nil {
		return nil, fmt.Errorf("list job versions: %w", err)
	}
	var response api.JobVersionListResponse
	if err := c.request(ctx, http.MethodGet, path, nil, &response); err != nil {
		return nil, fmt.Errorf("list job versions: %w", err)
	}
	return response, nil
}

// ResetReplacementBackoff clears the replacement backoff of a job's task
// group so its failed allocations are replaced without waiting.
func (c *Client) ResetReplacementBackoff(ctx context.Context, job, group string) error {
	path, err := c.namespacedPath("/jobs/%s/groups/%s/replacement-backoff/reset", url.PathEscape(job), url.PathEscape(group))
	if err != nil {
		return fmt.Errorf("reset replacement backoff: %w", err)
	}
	if err := c.request(ctx, http.MethodPost, path, nil, nil); err != nil {
		return fmt.Errorf("reset replacement backoff: %w", err)
	}
	return nil
}
