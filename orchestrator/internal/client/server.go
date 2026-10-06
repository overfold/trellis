package client

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	"github.com/overfold/trellis/orchestrator/internal/spec"
	"github.com/overfold/trellis/orchestrator/internal/transport"
)

// ServerClient sends a node's requests to the Trellis control plane:
// registration, heartbeats, and internal service discovery.
type ServerClient struct {
	baseURL      string
	client       *transport.Client
	mu           sync.RWMutex
	controlPlane nodeapi.ControlPlaneResponse
}

// NodeInfo contains the identity and capacity used to register a node.
type NodeInfo struct {
	ID                 uuid.UUID
	Host               string
	Port               int
	CPUCapacity        int
	MemoryCapacity     int64
	CPUAllocatable     int
	MemoryAllocatable  int64
	OS                 string
	Arch               string
	Labels             map[string]string
	Volumes            []string
	Capabilities       []spec.NodeCapability
	WireGuardPublicKey string
	WireGuardEndpoint  string
	WireGuardPortBase  int
	WireGuardPortCount int
	RunsWorkloads      bool
}

// Heartbeat contains the state periodically reported by a node.
type Heartbeat struct {
	NodeID            uuid.UUID
	Timestamp         time.Time
	Allocations       []nodeapi.AllocationStatus
	Volumes           []string
	Capabilities      []spec.NodeCapability
	Version           string
	CPUCapacity       int
	MemoryCapacity    int64
	CPUAllocatable    int
	MemoryAllocatable int64
	CPUUsage          *float64
	MemoryUsed        *int64
	MemoryAvailable   *int64
	MetricsAt         *time.Time
	RaftAppliedIndex  uint64
	// Task log usage is omitted when the runtime cannot measure it.
	TaskLogBytes               *int64
	TaskLogFilesystemAvailable *int64
	TaskLogFilesystemCapacity  *int64
}

// NewServerClient creates a node client for the control plane at addr.
func NewServerClient(token string, addr string, tlsConfig *tls.Config) *ServerClient {
	return &ServerClient{
		baseURL: transport.NormalizeBaseURL(addr),
		client:  &transport.Client{Token: token, HTTP: transport.NewHTTPClient(tlsConfig, 30*time.Second)},
	}
}

// SetAddress updates the server address used by the client.
func (s *ServerClient) SetAddress(addr string) {
	s.mu.Lock()
	s.baseURL = transport.NormalizeBaseURL(addr)
	s.mu.Unlock()
}

func (s *ServerClient) address() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.baseURL
}

// Ready reports whether the client has a server address.
func (s *ServerClient) Ready() bool {
	return s.address() != ""
}

// RegisterNode registers a node with the cluster.
func (s *ServerClient) RegisterNode(ctx context.Context, nodeInfo *NodeInfo) (*nodeapi.NodeRegistrationResponse, error) {
	requestData := &nodeapi.NodeRegistrationRequest{
		ID:                 nodeInfo.ID,
		Host:               nodeInfo.Host,
		Port:               nodeInfo.Port,
		CPU:                nodeInfo.CPUAllocatable,
		Memory:             nodeInfo.MemoryAllocatable,
		CPUCapacity:        nodeInfo.CPUCapacity,
		MemoryCapacity:     nodeInfo.MemoryCapacity,
		CPUAllocatable:     nodeInfo.CPUAllocatable,
		MemoryAllocatable:  nodeInfo.MemoryAllocatable,
		OS:                 nodeInfo.OS,
		Arch:               nodeInfo.Arch,
		Labels:             nodeInfo.Labels,
		Volumes:            nodeInfo.Volumes,
		Capabilities:       nodeInfo.Capabilities,
		WireGuardPublicKey: nodeInfo.WireGuardPublicKey,
		WireGuardEndpoint:  nodeInfo.WireGuardEndpoint,
		WireGuardPortBase:  nodeInfo.WireGuardPortBase,
		WireGuardPortCount: nodeInfo.WireGuardPortCount,
		RunsWorkloads:      &nodeInfo.RunsWorkloads,
	}
	var responseData nodeapi.NodeRegistrationResponse

	err := s.client.Request(ctx, http.MethodPost, s.address()+"/v1/nodes", requestData, &responseData)
	if err != nil {
		return nil, fmt.Errorf("register node: %w", err)
	}
	s.storeControlPlane(responseData.ControlPlaneResponse)

	return &responseData, nil
}

// ListDiscovery returns discoverable service instances.
func (s *ServerClient) ListDiscovery(ctx context.Context) (*nodeapi.ServiceListResponse, error) {
	var responseData nodeapi.ServiceListResponse
	if err := s.client.Request(ctx, http.MethodGet, s.address()+"/v1/internal/discovery", nil, &responseData); err != nil {
		return nil, fmt.Errorf("list discovery records: %w", err)
	}
	return &responseData, nil
}

// SendHeartbeat reports observed node state.
func (s *ServerClient) SendHeartbeat(ctx context.Context, id uuid.UUID, heartbeat *Heartbeat) error {
	requestData := &nodeapi.HeartbeatRequest{
		NodeID:            heartbeat.NodeID,
		Timestamp:         heartbeat.Timestamp,
		Allocations:       heartbeat.Allocations,
		Volumes:           heartbeat.Volumes,
		Capabilities:      heartbeat.Capabilities,
		Version:           heartbeat.Version,
		CPUCapacity:       heartbeat.CPUCapacity,
		MemoryCapacity:    heartbeat.MemoryCapacity,
		CPUAllocatable:    heartbeat.CPUAllocatable,
		MemoryAllocatable: heartbeat.MemoryAllocatable,
		CPUUsage:          heartbeat.CPUUsage,
		MemoryUsed:        heartbeat.MemoryUsed,
		MemoryAvailable:   heartbeat.MemoryAvailable,
		MetricsAt:         heartbeat.MetricsAt,
		RaftAppliedIndex:  heartbeat.RaftAppliedIndex,
		// Task log usage is omitted when the runtime cannot measure it.
		TaskLogBytes:               heartbeat.TaskLogBytes,
		TaskLogFilesystemAvailable: heartbeat.TaskLogFilesystemAvailable,
		TaskLogFilesystemCapacity:  heartbeat.TaskLogFilesystemCapacity,
	}
	url := fmt.Sprintf("%s/v1/nodes/%s/heartbeat", s.address(), id)
	var response nodeapi.HeartbeatResponse
	if err := s.client.Request(ctx, http.MethodPost, url, requestData, &response); err != nil {
		return fmt.Errorf("send heartbeat: %w", err)
	}
	s.storeControlPlane(response.ControlPlaneResponse)
	return nil
}

func (s *ServerClient) storeControlPlane(topology nodeapi.ControlPlaneResponse) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.controlPlane = topology
}

// ControlPlane returns the latest topology received from registration or heartbeat.
func (s *ServerClient) ControlPlane() nodeapi.ControlPlaneResponse {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.controlPlane
}
