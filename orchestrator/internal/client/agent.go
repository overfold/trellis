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
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/internal/api"
	"github.com/overfold/trellis/orchestrator/internal/execstream"
	"github.com/overfold/trellis/orchestrator/internal/tlsutil"
)

// AgentClient sends authenticated requests to a Trellis agent.
type AgentClient struct {
	token              string
	tlsConfig          *tls.Config
	mu                 sync.Mutex
	clients            map[uuid.UUID]*client
	networkPlanClients map[uuid.UUID]*client
}

const agentOperationTimeout = 30 * time.Second

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
func (s *AgentClient) Logs(ctx context.Context, nodeID uuid.UUID, address, allocID string, follow bool, tail int) (io.ReadCloser, error) {
	query := url.Values{"follow": {fmt.Sprint(follow)}, "tail": {fmt.Sprint(tail)}}
	return s.clientFor(nodeID, 30*time.Second).stream(ctx, normalizeBaseURL(address)+"/v1/allocations/"+url.PathEscape(allocID)+"/logs?"+query.Encode())
}

// NewAgentClient creates a client for Trellis agent APIs.
func NewAgentClient(token string, tlsConfig *tls.Config) *AgentClient {
	return &AgentClient{
		token:              token,
		tlsConfig:          tlsConfig,
		clients:            make(map[uuid.UUID]*client),
		networkPlanClients: make(map[uuid.UUID]*client),
	}
}

func (s *AgentClient) clientFor(expectedNodeID uuid.UUID, responseHeaderTimeout time.Duration) *client {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.clients == nil {
		s.clients = make(map[uuid.UUID]*client)
	}
	if s.networkPlanClients == nil {
		s.networkPlanClients = make(map[uuid.UUID]*client)
	}
	clients := s.clients
	if responseHeaderTimeout == 0 {
		clients = s.networkPlanClients
	}
	if existing := clients[expectedNodeID]; existing != nil {
		return existing
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13}
	if s.tlsConfig != nil {
		tlsConfig = s.tlsConfig.Clone()
	}
	previousVerify := tlsConfig.VerifyConnection
	tlsConfig.VerifyConnection = func(state tls.ConnectionState) error {
		if previousVerify != nil {
			if err := previousVerify(state); err != nil {
				return err
			}
		}
		if len(state.PeerCertificates) == 0 {
			return fmt.Errorf("agent certificate is missing")
		}
		actualNodeID, err := tlsutil.NodeID(state.PeerCertificates[0])
		if err != nil {
			return fmt.Errorf("agent node identity: %w", err)
		}
		if actualNodeID != expectedNodeID {
			return fmt.Errorf("agent certificate identifies node %s, expected %s", actualNodeID, expectedNodeID)
		}
		return nil
	}
	created := &client{token: s.token, client: newHTTPClientWithResponseHeaderTimeout(tlsConfig, responseHeaderTimeout)}
	clients[expectedNodeID] = created
	return created
}

func (s *AgentClient) operationRequest(ctx context.Context, nodeID uuid.UUID, method, target string, requestData, responseData any) error {
	ctx, cancel := context.WithTimeout(ctx, agentOperationTimeout)
	defer cancel()
	return s.clientFor(nodeID, 30*time.Second).request(ctx, method, target, requestData, responseData)
}

// RetainNodes evicts cached transports for nodes that are no longer registered.
// Active requests keep their transport reference and are not interrupted.
func (s *AgentClient) RetainNodes(nodes map[uuid.UUID]struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, clients := range []map[uuid.UUID]*client{s.clients, s.networkPlanClients} {
		for nodeID, cached := range clients {
			if _, keep := nodes[nodeID]; keep {
				continue
			}
			cached.client.CloseIdleConnections()
			delete(clients, nodeID)
		}
	}
}

// RunAllocation asks an agent to start an allocation.
func (s *AgentClient) RunAllocation(ctx context.Context, nodeID uuid.UUID, address string, allocation *api.AllocationRequest) error {
	var response api.OperationResponse
	err := s.operationRequest(ctx, nodeID, http.MethodPost, normalizeBaseURL(address)+"/v1/allocations", allocation, &response)
	if err != nil {
		return fmt.Errorf("run allocation: %w", decodeOperationError(err))
	}
	return nil
}

// DrainAllocation suppresses automatic restarts until the allocation is stopped.
func (s *AgentClient) DrainAllocation(ctx context.Context, nodeID uuid.UUID, address string, request *api.DrainAllocationRequest) error {
	var response api.OperationResponse
	err := s.operationRequest(ctx, nodeID, http.MethodPost, normalizeBaseURL(address)+"/v1/allocations/"+url.PathEscape(request.AllocationID)+"/drain", request, &response)
	if err != nil {
		return fmt.Errorf("drain allocation: %w", decodeOperationError(err))
	}
	return nil
}

// ResumeAllocation restores automatic restarts for a retained allocation.
func (s *AgentClient) ResumeAllocation(ctx context.Context, nodeID uuid.UUID, address string, request *api.DrainAllocationRequest) error {
	var response api.OperationResponse
	err := s.operationRequest(ctx, nodeID, http.MethodDelete, normalizeBaseURL(address)+"/v1/allocations/"+url.PathEscape(request.AllocationID)+"/drain", request, &response)
	if err != nil {
		return fmt.Errorf("resume allocation: %w", decodeOperationError(err))
	}
	return nil
}

// StopAllocation asks an agent to stop an allocation.
func (s *AgentClient) StopAllocation(ctx context.Context, nodeID uuid.UUID, address string, request *api.StopAllocationRequest) error {
	var response api.OperationResponse
	err := s.operationRequest(ctx, nodeID, http.MethodDelete, normalizeBaseURL(address)+"/v1/allocations/"+url.PathEscape(request.AllocationID), request, &response)
	if err != nil {
		return fmt.Errorf("stop allocation: %w", decodeOperationError(err))
	}

	return nil
}

// UpdateNetworkPlan reconciles an active namespace network on an agent.
func (s *AgentClient) UpdateNetworkPlan(ctx context.Context, nodeID uuid.UUID, address string, request *api.NetworkPlanRequest) error {
	var response api.OperationResponse
	if err := s.clientFor(nodeID, 0).request(ctx, http.MethodPost, normalizeBaseURL(address)+"/v1/network-plans", request, &response); err != nil {
		return fmt.Errorf("update network plan: %w", decodeOperationError(err))
	}
	return nil
}

// Exec opens a leader-to-agent exec stream to a new process in an
// allocation task. The returned connection carries raw exec stream frames
// and is closed when ctx ends.
func (s *AgentClient) Exec(ctx context.Context, nodeID uuid.UUID, address, allocID string, request api.AgentExecRequest) (io.ReadWriteCloser, error) {
	target := normalizeBaseURL(address) + "/v1/allocations/" + url.PathEscape(allocID) + "/exec?" + execstream.EncodeAgentRequest(request).Encode()
	return s.clientFor(nodeID, 30*time.Second).upgrade(ctx, target)
}

// AllocationMetrics fetches resource usage for an allocation's tasks from an agent.
func (s *AgentClient) AllocationMetrics(ctx context.Context, nodeID uuid.UUID, address, allocID string) (api.AllocationMetricsListResponse, error) {
	var response []api.AgentTaskMetrics
	err := s.operationRequest(ctx, nodeID, http.MethodGet, normalizeBaseURL(address)+"/v1/allocations/"+url.PathEscape(allocID)+"/metrics", nil, &response)
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
