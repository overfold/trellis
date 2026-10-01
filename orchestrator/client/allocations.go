package client

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/internal/execstream"
)

// AllocationFilter narrows an allocation listing. Zero fields do not filter.
type AllocationFilter struct {
	// Job selects the allocations of one job.
	Job string
	// Label selects allocations whose task group carries a label key, such
	// as "trellis.expose", or a key:value pair, such as "tier:web".
	Label string
}

func (f AllocationFilter) query() string {
	query := url.Values{}
	if f.Job != "" {
		query.Set("job", f.Job)
	}
	if f.Label != "" {
		query.Set("label", f.Label)
	}
	if len(query) == 0 {
		return ""
	}
	return "?" + query.Encode()
}

// ListAllocations returns the allocations in the client's namespace.
func (c *Client) ListAllocations(ctx context.Context, filter AllocationFilter) (api.AllocationListResponse, error) {
	path, err := c.namespacedPath("/allocations%s", filter.query())
	if err != nil {
		return nil, fmt.Errorf("list allocations: %w", err)
	}
	return c.listAllocations(ctx, path)
}

// ListClusterAllocations returns the allocations in every namespace. It
// requires cluster scope.
func (c *Client) ListClusterAllocations(ctx context.Context, filter AllocationFilter) (api.AllocationListResponse, error) {
	return c.listAllocations(ctx, c.clusterPath("/v1/allocations%s", filter.query()))
}

func (c *Client) listAllocations(ctx context.Context, target string) (api.AllocationListResponse, error) {
	var response api.AllocationListResponse
	if err := c.request(ctx, http.MethodGet, target, nil, &response); err != nil {
		return nil, fmt.Errorf("list allocations: %w", err)
	}
	return response, nil
}

// StopAllocation stops an allocation. The control plane replaces it if its
// job still wants it.
func (c *Client) StopAllocation(ctx context.Context, id string) error {
	path, err := c.namespacedPath("/allocations/%s", url.PathEscape(id))
	if err != nil {
		return fmt.Errorf("stop allocation: %w", err)
	}
	if err := c.request(ctx, http.MethodDelete, path, nil, nil); err != nil {
		return fmt.Errorf("stop allocation: %w", err)
	}
	return nil
}

// AllocationEvents returns the lifecycle event history of an allocation.
func (c *Client) AllocationEvents(ctx context.Context, id string) (api.AllocationEventListResponse, error) {
	path, err := c.namespacedPath("/allocations/%s/events", url.PathEscape(id))
	if err != nil {
		return nil, fmt.Errorf("list allocation events: %w", err)
	}
	var response api.AllocationEventListResponse
	if err := c.request(ctx, http.MethodGet, path, nil, &response); err != nil {
		return nil, fmt.Errorf("list allocation events: %w", err)
	}
	return response, nil
}

// AllocationMetrics returns the current resource usage of an allocation's
// tasks.
func (c *Client) AllocationMetrics(ctx context.Context, id string) (api.AllocationMetricsListResponse, error) {
	path, err := c.namespacedPath("/allocations/%s/metrics", url.PathEscape(id))
	if err != nil {
		return nil, fmt.Errorf("allocation metrics: %w", err)
	}
	var response api.AllocationMetricsListResponse
	if err := c.request(ctx, http.MethodGet, path, nil, &response); err != nil {
		return nil, fmt.Errorf("allocation metrics: %w", err)
	}
	return response, nil
}

// LogOptions select an allocation log stream.
type LogOptions struct {
	// Task selects the task; it may be empty when the allocation has one
	// task.
	Task string
	// Follow keeps the stream open for new output.
	Follow bool
	// Tail is the number of trailing lines to return; 0 returns the whole
	// retained log.
	Tail int
}

// AllocationLogs streams an allocation task's logs. The caller closes the
// returned reader; with Follow set, it stays open until ctx ends.
func (c *Client) AllocationLogs(ctx context.Context, id string, options LogOptions) (io.ReadCloser, error) {
	query := url.Values{"follow": {strconv.FormatBool(options.Follow)}, "tail": {strconv.Itoa(options.Tail)}}
	if options.Task != "" {
		query.Set("task", options.Task)
	}
	path, err := c.namespacedPath("/allocations/%s/logs?%s", url.PathEscape(id), query.Encode())
	if err != nil {
		return nil, fmt.Errorf("allocation logs: %w", err)
	}
	body, err := c.stream(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("allocation logs: %w", err)
	}
	return body, nil
}

// Exec starts a process in an allocation task and returns its exec stream.
// It requires write access. The stream is closed when ctx ends.
func (c *Client) Exec(ctx context.Context, id string, request api.ExecRequest) (*ExecStream, error) {
	path, err := c.namespacedPath("/allocations/%s/exec?%s", url.PathEscape(id), execstream.EncodeRequest(request).Encode())
	if err != nil {
		return nil, fmt.Errorf("exec allocation: %w", err)
	}
	conn, err := c.transport.ExecWebSocket(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("exec allocation: %w", publicError(err))
	}
	return newExecStream(conn), nil
}
