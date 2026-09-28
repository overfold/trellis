package client

import (
	"context"
	"fmt"
	"net/http"

	"github.com/overfold/trellis/internal/api"
	"github.com/overfold/trellis/internal/plan"
	"github.com/overfold/trellis/internal/spec"
)

// PlanJob returns the control plane's canonical semantic plan for a desired job.
func (s *ServerClient) PlanJob(ctx context.Context, desired *spec.JobSpec) (*plan.Result, error) {
	request := &api.JobRegistrationRequest{Spec: *desired}
	var result plan.Result
	if err := s.client.request(ctx, http.MethodPost, s.address()+"/v1/jobs/plan", request, &result); err != nil {
		return nil, fmt.Errorf("plan job: %w", err)
	}
	return &result, nil
}
