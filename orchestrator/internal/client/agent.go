// Package client provides clients for Trellis server and agent APIs.
package client

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/clofour/trellis/internal/api"
)

// AgentClient sends authenticated requests to a Trellis agent.
type AgentClient struct {
	client *client
}

// AgentOperationError reports a rejected agent operation.
type AgentOperationError struct {
	Response api.OperationResponse
}

func (e *AgentOperationError) Error() string {
	return fmt.Sprintf("agent operation %s: %s", e.Response.Code, e.Response.Message)
}

func decodeOperationError(err error) error {
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) {
		return err
	}
	var response api.OperationResponse
	if json.Unmarshal(httpErr.Body, &response) == nil && response.Code != "" {
		return &AgentOperationError{Response: response}
	}
	var wrapped struct {
		Message json.RawMessage `json:"message"`
	}
	if json.Unmarshal(httpErr.Body, &wrapped) == nil && len(wrapped.Message) > 0 {
		var inner api.OperationResponse
		if json.Unmarshal(wrapped.Message, &inner) == nil && inner.Code != "" {
			return &AgentOperationError{Response: inner}
		}
		var str string
		if json.Unmarshal(wrapped.Message, &str) == nil {
			if json.Unmarshal([]byte(str), &inner) == nil && inner.Code != "" {
				return &AgentOperationError{Response: inner}
			}
		}
	}
	return err
}

// Logs streams logs for an allocation from an agent.
func (s *AgentClient) Logs(ctx context.Context, address, allocID string, follow bool, tail int) (io.ReadCloser, error) {
	query := url.Values{"follow": {fmt.Sprint(follow)}, "tail": {fmt.Sprint(tail)}}
	return s.client.stream(ctx, normalizeBaseURL(address)+"/v1/allocations/"+url.PathEscape(allocID)+"/logs?"+query.Encode())
}

// NewAgentClient creates a client for Trellis agent APIs.
func NewAgentClient(token string, tlsConfig *tls.Config) *AgentClient {
	c := &client{
		token:  token,
		client: newHTTPClient(tlsConfig),
	}

	return &AgentClient{
		client: c,
	}
}

// RunAllocation asks an agent to start an allocation.
func (s *AgentClient) RunAllocation(ctx context.Context, address string, allocation *api.AllocationRequest) error {
	var response api.OperationResponse
	err := s.client.request(ctx, http.MethodPost, normalizeBaseURL(address)+"/v1/allocations", allocation, &response)
	if err != nil {
		return fmt.Errorf("run allocation: %w", decodeOperationError(err))
	}
	return nil
}

// StopAllocation asks an agent to stop an allocation.
func (s *AgentClient) StopAllocation(ctx context.Context, address string, request *api.StopAllocationRequest) error {
	var response api.OperationResponse
	err := s.client.request(ctx, http.MethodDelete, normalizeBaseURL(address)+"/v1/allocations/"+url.PathEscape(request.AllocationID), request, &response)
	if err != nil {
		return fmt.Errorf("stop allocation: %w", decodeOperationError(err))
	}

	return nil
}

// ExecAllocation runs a command in an allocation task container via an agent.
func (s *AgentClient) ExecAllocation(ctx context.Context, address, allocID, task string, command []string) (*api.ExecResponse, error) {
	request := api.AgentExecRequest{Task: task, Command: command}
	var response api.AgentExecResponse
	err := s.client.request(ctx, http.MethodPost, normalizeBaseURL(address)+"/v1/allocations/"+url.PathEscape(allocID)+"/exec", &request, &response)
	if err != nil {
		return nil, fmt.Errorf("exec allocation: %w", err)
	}
	return &api.ExecResponse{
		Stdout:   response.Stdout,
		Stderr:   response.Stderr,
		ExitCode: response.ExitCode,
	}, nil
}

// CreateExecSession starts an interactive terminal in an allocation task via an agent.
func (s *AgentClient) CreateExecSession(ctx context.Context, address, allocID string, request *api.ExecSessionCreateRequest) (*api.ExecSessionResponse, error) {
	var response api.ExecSessionResponse
	err := s.client.request(ctx, http.MethodPost, normalizeBaseURL(address)+"/v1/allocations/"+url.PathEscape(allocID)+"/exec/sessions", request, &response)
	if err != nil {
		return nil, fmt.Errorf("create exec session: %w", err)
	}
	return &response, nil
}

// WriteExecSession sends terminal input to an allocation task via an agent.
func (s *AgentClient) WriteExecSession(ctx context.Context, address, allocID, sessionID string, request *api.ExecSessionInputRequest) error {
	err := s.client.request(ctx, http.MethodPost, normalizeBaseURL(address)+"/v1/allocations/"+url.PathEscape(allocID)+"/exec/sessions/"+url.PathEscape(sessionID)+"/input", request, nil)
	if err != nil {
		return fmt.Errorf("write exec session: %w", err)
	}
	return nil
}

// ReadExecSession reads terminal output from an allocation task via an agent.
func (s *AgentClient) ReadExecSession(ctx context.Context, address, allocID, sessionID string, offset int64) (*api.ExecSessionOutputResponse, error) {
	var response api.ExecSessionOutputResponse
	path := normalizeBaseURL(address) + "/v1/allocations/" + url.PathEscape(allocID) + "/exec/sessions/" + url.PathEscape(sessionID) + "/output?offset=" + strconv.FormatInt(offset, 10)
	if err := s.client.request(ctx, http.MethodGet, path, nil, &response); err != nil {
		return nil, fmt.Errorf("read exec session: %w", err)
	}
	return &response, nil
}

// ResizeExecSession changes terminal dimensions via an agent.
func (s *AgentClient) ResizeExecSession(ctx context.Context, address, allocID, sessionID string, request *api.ExecSessionResizeRequest) error {
	err := s.client.request(ctx, http.MethodPost, normalizeBaseURL(address)+"/v1/allocations/"+url.PathEscape(allocID)+"/exec/sessions/"+url.PathEscape(sessionID)+"/resize", request, nil)
	if err != nil {
		return fmt.Errorf("resize exec session: %w", err)
	}
	return nil
}

// CloseExecSession terminates an interactive terminal via an agent.
func (s *AgentClient) CloseExecSession(ctx context.Context, address, allocID, sessionID string) error {
	err := s.client.request(ctx, http.MethodDelete, normalizeBaseURL(address)+"/v1/allocations/"+url.PathEscape(allocID)+"/exec/sessions/"+url.PathEscape(sessionID), nil, nil)
	if err != nil {
		return fmt.Errorf("close exec session: %w", err)
	}
	return nil
}

// AllocationMetrics fetches resource usage for an allocation's tasks from an agent.
func (s *AgentClient) AllocationMetrics(ctx context.Context, address, allocID string) (api.AllocationMetricsListResponse, error) {
	var response []api.AgentTaskMetrics
	err := s.client.request(ctx, http.MethodGet, normalizeBaseURL(address)+"/v1/allocations/"+url.PathEscape(allocID)+"/metrics", nil, &response)
	if err != nil {
		return nil, fmt.Errorf("allocation metrics: %w", err)
	}
	now := time.Now().UTC()
	result := make(api.AllocationMetricsListResponse, 0, len(response))
	for _, m := range response {
		result = append(result, api.AllocationMetricsResponse{
			AllocationID:        allocID,
			Task:                m.Task,
			CPUUsageNanoseconds: m.CPUUsageNanoseconds,
			MemoryUsageBytes:    m.MemoryUsageBytes,
			CollectedAt:         now,
		})
	}
	return result, nil
}
