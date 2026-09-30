package client

import (
	"context"
	"fmt"
	"net/http"

	"github.com/overfold/trellis/orchestrator/internal/api"
	"github.com/overfold/trellis/orchestrator/internal/plan"
	"github.com/overfold/trellis/orchestrator/internal/spec"
)

// PlanJob returns the control plane's canonical semantic plan for a desired
// job in the client's namespace.
func (s *ServerClient) PlanJob(ctx context.Context, desired *spec.JobSpec) (*plan.Result, error) {
	path, err := s.namespaced("/jobs/plan")
	if err != nil {
		return nil, fmt.Errorf("plan job: %w", err)
	}
	request := &api.JobRegistrationRequest{Spec: *desired}
	var result plan.Result
	if err := s.client.request(ctx, http.MethodPost, path, request, &result); err != nil {
		return nil, fmt.Errorf("plan job: %w", err)
	}
	return &result, nil
}
