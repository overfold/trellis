package client

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/google/uuid"
)

// TaskLogs streams one task's logs for a scheduler allocation from an agent.
func (s *AgentClient) TaskLogs(ctx context.Context, nodeID uuid.UUID, address, allocationID, task string, follow bool, tail int) (io.ReadCloser, error) {
	query := url.Values{
		"follow": {fmt.Sprint(follow)},
		"tail":   {fmt.Sprint(tail)},
	}
	if task != "" {
		query.Set("task", task)
	}
	return s.clientFor(nodeID, 30*time.Second).stream(ctx, normalizeBaseURL(address)+"/v1/allocations/"+url.PathEscape(allocationID)+"/logs?"+query.Encode())
}

// AllocationTaskLogs streams one task's logs for a scheduler allocation from the control plane.
func (s *ServerClient) AllocationTaskLogs(ctx context.Context, allocationID, task string, follow bool, tail int) (io.ReadCloser, error) {
	query := url.Values{
		"follow": {fmt.Sprint(follow)},
		"tail":   {fmt.Sprint(tail)},
	}
	if task != "" {
		query.Set("task", task)
	}
	return s.client.stream(ctx, fmt.Sprintf("%s/v1/allocations/%s/logs?%s", s.address(), url.PathEscape(allocationID), query.Encode()))
}
