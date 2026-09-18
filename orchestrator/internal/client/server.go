package client

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/spec"
	"github.com/google/uuid"
)

// ServerClient sends authenticated requests to the Trellis server API.
type ServerClient struct {
	baseURL string
	client  *client
	mu      sync.RWMutex
}

// ExecAllocation runs a non-interactive command in an allocation task.
func (s *ServerClient) ExecAllocation(ctx context.Context, id, task string, command []string) (*api.ExecResponse, error) {
	request := api.ExecRequest{Task: task, Command: command}
	var response api.ExecResponse
	path := fmt.Sprintf("%s/v1/allocations/%s/exec", s.address(), url.PathEscape(id))
	if err := s.client.request(ctx, http.MethodPost, path, &request, &response); err != nil {
		return nil, fmt.Errorf("exec allocation: %w", err)
	}
	return &response, nil
}

// CreateExecSession starts an interactive terminal in an allocation task.
func (s *ServerClient) CreateExecSession(ctx context.Context, id string, request *api.ExecSessionCreateRequest) (*api.ExecSessionResponse, error) {
	var response api.ExecSessionResponse
	path := fmt.Sprintf("%s/v1/allocations/%s/exec/sessions", s.address(), url.PathEscape(id))
	if err := s.client.request(ctx, http.MethodPost, path, request, &response); err != nil {
		return nil, fmt.Errorf("create exec session: %w", err)
	}
	return &response, nil
}

// WriteExecSession appends terminal input to an interactive allocation session.
func (s *ServerClient) WriteExecSession(ctx context.Context, id, sessionID string, request *api.ExecSessionInputRequest) error {
	path := fmt.Sprintf("%s/v1/allocations/%s/exec/sessions/%s/input", s.address(), url.PathEscape(id), url.PathEscape(sessionID))
	if err := s.client.request(ctx, http.MethodPost, path, request, nil); err != nil {
		return fmt.Errorf("write exec session: %w", err)
	}
	return nil
}

// ReadExecSession reads terminal output produced since offset.
func (s *ServerClient) ReadExecSession(ctx context.Context, id, sessionID string, offset int64) (*api.ExecSessionOutputResponse, error) {
	var response api.ExecSessionOutputResponse
	path := fmt.Sprintf("%s/v1/allocations/%s/exec/sessions/%s/output?offset=%d", s.address(), url.PathEscape(id), url.PathEscape(sessionID), offset)
	if err := s.client.request(ctx, http.MethodGet, path, nil, &response); err != nil {
		return nil, fmt.Errorf("read exec session: %w", err)
	}
	return &response, nil
}

// ResizeExecSession changes the dimensions of an interactive allocation terminal.
func (s *ServerClient) ResizeExecSession(ctx context.Context, id, sessionID string, request *api.ExecSessionResizeRequest) error {
	path := fmt.Sprintf("%s/v1/allocations/%s/exec/sessions/%s/resize", s.address(), url.PathEscape(id), url.PathEscape(sessionID))
	if err := s.client.request(ctx, http.MethodPost, path, request, nil); err != nil {
		return fmt.Errorf("resize exec session: %w", err)
	}
	return nil
}

// CloseExecSession terminates an interactive allocation terminal.
func (s *ServerClient) CloseExecSession(ctx context.Context, id, sessionID string) error {
	path := fmt.Sprintf("%s/v1/allocations/%s/exec/sessions/%s", s.address(), url.PathEscape(id), url.PathEscape(sessionID))
	if err := s.client.request(ctx, http.MethodDelete, path, nil, nil); err != nil {
		return fmt.Errorf("close exec session: %w", err)
	}
	return nil
}

// AllocationLogs streams logs for an allocation.
func (s *ServerClient) AllocationLogs(ctx context.Context, id string, follow bool, tail int) (io.ReadCloser, error) {
	return s.client.stream(ctx, fmt.Sprintf("%s/v1/allocations/%s/logs?follow=%t&tail=%d", s.address(), url.PathEscape(id), follow, tail))
}

// AllocationEvents returns the lifecycle event history for an allocation.
func (s *ServerClient) AllocationEvents(ctx context.Context, id string) (*api.AllocationEventListResponse, error) {
	var response api.AllocationEventListResponse
	path := fmt.Sprintf("%s/v1/allocations/%s/events", s.address(), url.PathEscape(id))
	if err := s.client.request(ctx, http.MethodGet, path, nil, &response); err != nil {
		return nil, fmt.Errorf("list allocation events: %w", err)
	}
	return &response, nil
}

// SetAddress updates the server address used by the client.
func (s *ServerClient) SetAddress(addr string) {
	s.mu.Lock()
	s.baseURL = normalizeBaseURL(addr)
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

// CreateBackup downloads a desired-state backup.
func (s *ServerClient) CreateBackup(ctx context.Context) (*api.BackupSnapshot, error) {
	var snapshot api.BackupSnapshot
	if err := s.client.request(ctx, http.MethodGet, s.address()+"/v1/backup", nil, &snapshot); err != nil {
		return nil, fmt.Errorf("create backup: %w", err)
	}
	return &snapshot, nil
}

// RestoreBackup uploads a desired-state backup.
func (s *ServerClient) RestoreBackup(ctx context.Context, snapshot *api.BackupSnapshot) error {
	if err := s.client.request(ctx, http.MethodPost, s.address()+"/v1/backup/restore", snapshot, nil); err != nil {
		return fmt.Errorf("restore backup: %w", err)
	}
	return nil
}

// NodeInfo contains the identity and capacity used to register a node.
type NodeInfo struct {
	ID                 uuid.UUID
	Host               string
	Port               int
	CPU                int
	Memory             int64
	OS                 string
	Arch               string
	Labels             map[string]string
	Volumes            []string
	WireGuardPublicKey string
	WireGuardEndpoint  string
}

// Heartbeat contains the state periodically reported by a node.
type Heartbeat struct {
	NodeID      uuid.UUID              `json:"id"`
	Timestamp   time.Time              `json:"timestamp"`
	Allocations []api.AllocationStatus `json:"allocations,omitempty"`
	Volumes     []string               `json:"volumes,omitempty"`
	Version     string                 `json:"version,omitempty"`
}

// NewServerClient creates a client for cluster-scoped server APIs.
func NewServerClient(token string, addr string, tlsConfig *tls.Config) *ServerClient {
	return NewNamespaceServerClient(token, addr, "", tlsConfig)
}

// NewNamespaceServerClient creates a client for namespace-scoped server APIs.
func NewNamespaceServerClient(token string, addr string, namespace string, tlsConfig *tls.Config) *ServerClient {
	baseURL := normalizeBaseURL(addr)
	c := &client{
		token:     token,
		namespace: namespace,
		client:    newHTTPClient(tlsConfig),
	}

	return &ServerClient{
		baseURL: baseURL,
		client:  c,
	}
}

// ListNodes returns all registered nodes.
func (s *ServerClient) ListNodes(ctx context.Context) (*api.NodeListResponse, error) {
	var responseData api.NodeListResponse

	err := s.client.request(ctx, http.MethodGet, s.address()+"/v1/nodes", nil, &responseData)
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}

	return &responseData, nil
}

// DrainNode requests allocation evacuation from a node.
func (s *ServerClient) DrainNode(ctx context.Context, id uuid.UUID) error {
	if err := s.client.request(ctx, http.MethodPost, fmt.Sprintf("%s/v1/nodes/%s/drain", s.address(), id), nil, nil); err != nil {
		return fmt.Errorf("drain node: %w", err)
	}
	return nil
}

// UndrainNode makes a drained node schedulable.
func (s *ServerClient) UndrainNode(ctx context.Context, id uuid.UUID) error {
	if err := s.client.request(ctx, http.MethodDelete, fmt.Sprintf("%s/v1/nodes/%s/drain", s.address(), id), nil, nil); err != nil {
		return fmt.Errorf("un-drain node: %w", err)
	}
	return nil
}

// TransferLeadership asks Raft to transfer leadership.
func (s *ServerClient) TransferLeadership(ctx context.Context) error {
	if err := s.client.request(ctx, http.MethodPost, s.address()+"/v1/raft/leadership-transfer", nil, nil); err != nil {
		return fmt.Errorf("transfer Raft leadership: %w", err)
	}
	return nil
}

// RemoveRaftMember permanently removes a server from Raft.
func (s *ServerClient) RemoveRaftMember(ctx context.Context, id string) error {
	path := fmt.Sprintf("%s/v1/raft/members/%s", s.address(), url.PathEscape(id))
	if err := s.client.request(ctx, http.MethodDelete, path, nil, nil); err != nil {
		return fmt.Errorf("remove raft member: %w", err)
	}
	return nil
}

// RegisterNode registers a node with the cluster.
func (s *ServerClient) RegisterNode(ctx context.Context, nodeInfo *NodeInfo) (*api.NodeRegistrationResponse, error) {
	requestData := &api.NodeRegistrationRequest{
		ID:                 nodeInfo.ID,
		Host:               nodeInfo.Host,
		Port:               nodeInfo.Port,
		CPU:                nodeInfo.CPU,
		Memory:             nodeInfo.Memory,
		OS:                 nodeInfo.OS,
		Arch:               nodeInfo.Arch,
		Labels:             nodeInfo.Labels,
		Volumes:            nodeInfo.Volumes,
		WireGuardPublicKey: nodeInfo.WireGuardPublicKey,
		WireGuardEndpoint:  nodeInfo.WireGuardEndpoint,
	}
	var responseData api.NodeRegistrationResponse

	err := s.client.request(ctx, http.MethodPost, s.address()+"/v1/nodes", requestData, &responseData)
	if err != nil {
		return nil, fmt.Errorf("register node: %w", err)
	}

	return &responseData, nil
}

// GetJob returns a job and its allocations.
func (s *ServerClient) GetJob(ctx context.Context, name string) (*api.JobStatusResponse, error) {
	var response api.JobStatusResponse
	if err := s.client.request(ctx, http.MethodGet, s.address()+"/v1/jobs/"+url.PathEscape(name), nil, &response); err != nil {
		return nil, fmt.Errorf("get job: %w", err)
	}
	return &response, nil
}

// ListJobs returns jobs in the configured namespace.
func (s *ServerClient) ListJobs(ctx context.Context) (*api.JobListResponse, error) {
	var response api.JobListResponse
	if err := s.client.request(ctx, http.MethodGet, s.address()+"/v1/jobs", nil, &response); err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	return &response, nil
}

// SubmitJob creates or updates a job.
func (s *ServerClient) SubmitJob(ctx context.Context, spec *spec.JobSpec) error {
	requestData := &api.JobRegistrationRequest{
		Spec: *spec,
	}

	err := s.client.request(ctx, http.MethodPost, s.address()+"/v1/jobs", requestData, nil)
	if err != nil {
		return fmt.Errorf("submit job: %w", err)
	}

	return nil
}

// DeleteJob deletes a job.
func (s *ServerClient) DeleteJob(ctx context.Context, name string) error {
	if err := s.client.request(ctx, http.MethodDelete, s.address()+"/v1/jobs/"+url.PathEscape(name), nil, nil); err != nil {
		return fmt.Errorf("delete job: %w", err)
	}
	return nil
}

// SetSecret creates or updates a namespace secret.
func (s *ServerClient) SetSecret(ctx context.Context, namespace, name string, value []byte, expected *uint64) (*api.SecretMetadata, error) {
	request := api.SecretWriteRequest{ValueBase64: base64.StdEncoding.EncodeToString(value), ExpectedVersion: expected}
	var response api.SecretMetadata
	path := fmt.Sprintf("%s/v1/namespaces/%s/secrets/%s", s.address(), url.PathEscape(namespace), url.PathEscape(name))
	if err := s.client.request(ctx, http.MethodPut, path, &request, &response); err != nil {
		return nil, fmt.Errorf("set secret: %w", err)
	}
	return &response, nil
}

// ListSecrets returns secret metadata for a namespace.
func (s *ServerClient) ListSecrets(ctx context.Context, namespace string) (*api.SecretListResponse, error) {
	var response api.SecretListResponse
	path := fmt.Sprintf("%s/v1/namespaces/%s/secrets", s.address(), url.PathEscape(namespace))
	if err := s.client.request(ctx, http.MethodGet, path, nil, &response); err != nil {
		return nil, fmt.Errorf("list secrets: %w", err)
	}
	return &response, nil
}

// GetSecretMetadata returns metadata for a secret.
func (s *ServerClient) GetSecretMetadata(ctx context.Context, namespace, name string) (*api.SecretMetadata, error) {
	var response api.SecretMetadata
	path := fmt.Sprintf("%s/v1/namespaces/%s/secrets/%s", s.address(), url.PathEscape(namespace), url.PathEscape(name))
	if err := s.client.request(ctx, http.MethodGet, path, nil, &response); err != nil {
		return nil, fmt.Errorf("describe secret: %w", err)
	}
	return &response, nil
}

// DeleteSecret removes a secret.
func (s *ServerClient) DeleteSecret(ctx context.Context, namespace, name string) error {
	path := fmt.Sprintf("%s/v1/namespaces/%s/secrets/%s", s.address(), url.PathEscape(namespace), url.PathEscape(name))
	if err := s.client.request(ctx, http.MethodDelete, path, nil, nil); err != nil {
		return fmt.Errorf("delete secret: %w", err)
	}
	return nil
}

// ListDiscovery returns discoverable service instances.
func (s *ServerClient) ListDiscovery(ctx context.Context) (*api.ServiceListResponse, error) {
	var responseData api.ServiceListResponse
	if err := s.client.request(ctx, http.MethodGet, s.address()+"/v1/internal/discovery", nil, &responseData); err != nil {
		return nil, fmt.Errorf("list discovery records: %w", err)
	}
	return &responseData, nil
}

// ListAllocations fetches allocations from the public allocations API.
// label filters to allocations whose task group carries the given label key
// or key:value pair (e.g. "trellis.expose" or "trellis.expose:true").
// An empty label string returns all allocations visible to the caller.
func (s *ServerClient) ListAllocations(ctx context.Context, label string) (*api.AllocationListResponse, error) {
	u := s.address() + "/v1/allocations"
	if label != "" {
		u += "?label=" + url.QueryEscape(label)
	}
	var responseData api.AllocationListResponse
	if err := s.client.request(ctx, http.MethodGet, u, nil, &responseData); err != nil {
		return nil, fmt.Errorf("list allocations: %w", err)
	}
	return &responseData, nil
}

// SendHeartbeat reports node state and returns desired allocations.
func (s *ServerClient) SendHeartbeat(ctx context.Context, id uuid.UUID, heartbeat *Heartbeat) (*api.HeartbeatResponse, error) {
	requestData := &api.HeartbeatRequest{
		NodeID:      heartbeat.NodeID,
		Timestamp:   heartbeat.Timestamp,
		Allocations: heartbeat.Allocations,
		Volumes:     heartbeat.Volumes,
		Version:     heartbeat.Version,
	}
	url := fmt.Sprintf("%s/v1/nodes/%s/heartbeat", s.address(), id)

	var response api.HeartbeatResponse
	err := s.client.request(ctx, http.MethodPost, url, requestData, &response)
	if err != nil {
		return nil, fmt.Errorf("send heartbeat: %w", err)
	}

	return &response, nil
}

func normalizeBaseURL(addr string) string {
	addr = strings.TrimRight(strings.TrimSpace(addr), "/")
	if addr == "" {
		return ""
	}
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		addr = "https://" + addr
	}
	return addr
}
