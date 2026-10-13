// Package client provides the node-internal clients: the leader's client for
// agent operations and a node's client for registration, heartbeats, and
// internal discovery. Operators and integrations use the public package
// github.com/overfold/trellis/orchestrator/client.
package client

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/internal/execstream"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	"github.com/overfold/trellis/orchestrator/internal/tlsutil"
	"github.com/overfold/trellis/orchestrator/internal/transport"
)

// AgentClient sends authenticated requests to a Trellis agent.
type AgentClient struct {
	token              string
	tlsConfig          *tls.Config
	mu                 sync.Mutex
	clients            map[uuid.UUID]*transport.Client
	networkPlanClients map[uuid.UUID]*transport.Client
}

const agentOperationTimeout = 30 * time.Second

// Compatibility verifies the binary currently serving an authenticated agent.
func (s *AgentClient) Compatibility(ctx context.Context, nodeID uuid.UUID, address string) (uint64, error) {
	var response struct {
		Release *uint64 `json:"release"`
	}
	if err := s.clientFor(nodeID, agentOperationTimeout).Request(ctx, http.MethodGet, transport.NormalizeBaseURL(address)+"/v1/compatibility", nil, &response); err != nil {
		return 0, err
	}
	if response.Release == nil {
		return 0, fmt.Errorf("agent did not report its compatibility release")
	}
	return *response.Release, nil
}

// AgentOperationError reports a rejected agent operation.
type AgentOperationError struct {
	Response nodeapi.OperationResponse
	cause    error
}

func (e *AgentOperationError) Error() string {
	return fmt.Sprintf("agent operation %s: %s", e.Response.Code, e.Response.Message)
}

// Unwrap preserves transport status for callers deciding whether a refusal
// consumed an execution attempt, without changing the operation wire response.
func (e *AgentOperationError) Unwrap() error { return e.cause }

// Logs streams logs for an allocation from an agent.
func (s *AgentClient) Logs(ctx context.Context, nodeID uuid.UUID, address, allocID string, follow bool, tail int) (io.ReadCloser, error) {
	query := url.Values{"follow": {fmt.Sprint(follow)}, "tail": {fmt.Sprint(tail)}}
	return s.clientFor(nodeID, 30*time.Second).Stream(ctx, transport.NormalizeBaseURL(address)+"/v1/allocations/"+url.PathEscape(allocID)+"/logs?"+query.Encode())
}

// NewAgentClient creates a client for Trellis agent APIs.
func NewAgentClient(token string, tlsConfig *tls.Config) *AgentClient {
	return &AgentClient{
		token:              token,
		tlsConfig:          tlsConfig,
		clients:            make(map[uuid.UUID]*transport.Client),
		networkPlanClients: make(map[uuid.UUID]*transport.Client),
	}
}

func (s *AgentClient) clientFor(expectedNodeID uuid.UUID, responseHeaderTimeout time.Duration) *transport.Client {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.clients == nil {
		s.clients = make(map[uuid.UUID]*transport.Client)
	}
	if s.networkPlanClients == nil {
		s.networkPlanClients = make(map[uuid.UUID]*transport.Client)
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
	tlsConfig.ServerName = tlsutil.NodeServerName(expectedNodeID)
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
	created := &transport.Client{Token: s.token, HTTP: transport.NewHTTPClient(tlsConfig, responseHeaderTimeout)}
	clients[expectedNodeID] = created
	return created
}

func (s *AgentClient) operationRequest(ctx context.Context, nodeID uuid.UUID, method, target string, requestData, responseData any) error {
	ctx, cancel := context.WithTimeout(ctx, agentOperationTimeout)
	defer cancel()
	return s.clientFor(nodeID, 30*time.Second).Request(ctx, method, target, requestData, responseData)
}

// RetainNodes evicts cached transports for nodes that are no longer registered.
// Active requests keep their transport reference and are not interrupted.
func (s *AgentClient) RetainNodes(nodes map[uuid.UUID]struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, clients := range []map[uuid.UUID]*transport.Client{s.clients, s.networkPlanClients} {
		for nodeID, cached := range clients {
			if _, keep := nodes[nodeID]; keep {
				continue
			}
			cached.HTTP.CloseIdleConnections()
			delete(clients, nodeID)
		}
	}
}

// Exec opens a leader-to-agent exec stream to a new process in an
// allocation task. The returned connection carries raw exec stream frames
// and is closed when ctx ends.
func (s *AgentClient) Exec(ctx context.Context, nodeID uuid.UUID, address, allocID string, request nodeapi.AgentExecRequest) (io.ReadWriteCloser, error) {
	target := transport.NormalizeBaseURL(address) + "/v1/allocations/" + url.PathEscape(allocID) + "/exec?" + execstream.EncodeAgentRequest(request.ExecRequest, request.Epoch).Encode()
	return s.clientFor(nodeID, 30*time.Second).Upgrade(ctx, target)
}

// AllocationMetrics fetches resource usage for an allocation's tasks from an agent.
func (s *AgentClient) AllocationMetrics(ctx context.Context, nodeID uuid.UUID, address, allocID string) (api.AllocationMetricsListResponse, error) {
	var response []nodeapi.AgentTaskMetrics
	err := s.operationRequest(ctx, nodeID, http.MethodGet, transport.NormalizeBaseURL(address)+"/v1/allocations/"+url.PathEscape(allocID)+"/metrics", nil, &response)
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
