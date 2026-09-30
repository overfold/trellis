package client

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/internal/api"
	"github.com/overfold/trellis/internal/execstream"
	"github.com/overfold/trellis/internal/spec"
)

// ServerClient sends authenticated requests to the Trellis server API.
type ServerClient struct {
	baseURL   string
	namespace string
	client    *client
	mu        sync.RWMutex
}

// ErrNamespaceRequired reports a namespaced request from a client created
// without a namespace.
var ErrNamespaceRequired = errors.New("a namespace is required")

// namespaced returns the URL of path within the client's namespace.
func (s *ServerClient) namespaced(format string, args ...any) (string, error) {
	if s.namespace == "" {
		return "", ErrNamespaceRequired
	}
	return s.address() + "/v1/namespaces/" + url.PathEscape(s.namespace) + fmt.Sprintf(format, args...), nil
}

// Namespace returns the namespace addressed by namespaced requests.
func (s *ServerClient) Namespace() string { return s.namespace }

// Exec opens an exec stream to a new process in an allocation task. The
// stream is closed when ctx ends.
func (s *ServerClient) Exec(ctx context.Context, id string, request api.ExecRequest) (*ExecStream, error) {
	path, err := s.namespaced("/allocations/%s/exec?%s", url.PathEscape(id), execstream.EncodeRequest(request).Encode())
	if err != nil {
		return nil, fmt.Errorf("exec allocation: %w", err)
	}
	conn, err := s.client.upgrade(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("exec allocation: %w", err)
	}
	return newExecStream(conn), nil
}

// AllocationLogs streams logs for an allocation.
func (s *ServerClient) AllocationLogs(ctx context.Context, id string, follow bool, tail int) (io.ReadCloser, error) {
	path, err := s.namespaced("/allocations/%s/logs?follow=%t&tail=%d", url.PathEscape(id), follow, tail)
	if err != nil {
		return nil, fmt.Errorf("allocation logs: %w", err)
	}
	return s.client.stream(ctx, path)
}

// AllocationEvents returns the lifecycle event history for an allocation.
func (s *ServerClient) AllocationEvents(ctx context.Context, id string) (*api.AllocationEventListResponse, error) {
	var response api.AllocationEventListResponse
	path, err := s.namespaced("/allocations/%s/events", url.PathEscape(id))
	if err != nil {
		return nil, fmt.Errorf("list allocation events: %w", err)
	}
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
}

// Heartbeat contains the state periodically reported by a node.
type Heartbeat struct {
	NodeID            uuid.UUID
	Timestamp         time.Time
	Allocations       []api.AllocationStatus
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
}

// NewServerClient creates a client for cluster-scoped server APIs. Its
// namespaced requests fail with ErrNamespaceRequired.
func NewServerClient(token string, addr string, tlsConfig *tls.Config) *ServerClient {
	return NewNamespaceServerClient(token, addr, "", tlsConfig)
}

// NewNamespaceServerClient creates a client whose namespaced requests address
// /v1/namespaces/{namespace}/... paths.
func NewNamespaceServerClient(token string, addr string, namespace string, tlsConfig *tls.Config) *ServerClient {
	return &ServerClient{
		baseURL:   normalizeBaseURL(addr),
		namespace: namespace,
		client:    &client{token: token, client: newHTTPClient(tlsConfig)},
	}
}

// UseAdministratorKey authenticates subsequent requests with Ed25519 request signatures.
func (s *ServerClient) UseAdministratorKey(privateKey ed25519.PrivateKey) error {
	if len(privateKey) != ed25519.PrivateKeySize {
		return fmt.Errorf("administrator private key must be %d bytes", ed25519.PrivateKeySize)
	}
	s.client.token = ""
	s.client.administratorKey = append(ed25519.PrivateKey(nil), privateKey...)
	return nil
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
	path, err := s.namespaced("/jobs/%s", url.PathEscape(name))
	if err != nil {
		return nil, fmt.Errorf("get job: %w", err)
	}
	if err := s.client.request(ctx, http.MethodGet, path, nil, &response); err != nil {
		return nil, fmt.Errorf("get job: %w", err)
	}
	return &response, nil
}

// ListJobs returns jobs in the configured namespace.
func (s *ServerClient) ListJobs(ctx context.Context) (*api.JobListResponse, error) {
	var response api.JobListResponse
	path, err := s.namespaced("/jobs")
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	if err := s.client.request(ctx, http.MethodGet, path, nil, &response); err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	return &response, nil
}

// SubmitJob creates or updates a job in the client's namespace, which must be
// the spec's namespace. A non-nil expectedVersion makes the
// apply conditional: 0 requires that the job does not exist and N requires
// that it is at version N. A failed precondition returns an *HTTPError with
// status 409 Conflict.
func (s *ServerClient) SubmitJob(ctx context.Context, spec *spec.JobSpec, expectedVersion *int) (*api.JobRegistrationResponse, error) {
	requestData := &api.JobRegistrationRequest{
		Spec:            *spec,
		ExpectedVersion: expectedVersion,
	}

	var response api.JobRegistrationResponse
	path, err := s.namespaced("/jobs")
	if err != nil {
		return nil, fmt.Errorf("submit job: %w", err)
	}
	if err := s.client.request(ctx, http.MethodPost, path, requestData, &response); err != nil {
		return nil, fmt.Errorf("submit job: %w", err)
	}
	return &response, nil
}

// DeleteJob deletes a job.
func (s *ServerClient) DeleteJob(ctx context.Context, name string) error {
	path, err := s.namespaced("/jobs/%s", url.PathEscape(name))
	if err != nil {
		return fmt.Errorf("delete job: %w", err)
	}
	if err := s.client.request(ctx, http.MethodDelete, path, nil, nil); err != nil {
		return fmt.Errorf("delete job: %w", err)
	}
	return nil
}

// ResetReplacementBackoff clears the replacement backoff of a job task group
// so its failed allocations are replaced without waiting.
func (s *ServerClient) ResetReplacementBackoff(ctx context.Context, job, group string) error {
	path, err := s.namespaced("/jobs/%s/groups/%s/replacement-backoff/reset", url.PathEscape(job), url.PathEscape(group))
	if err != nil {
		return fmt.Errorf("reset replacement backoff: %w", err)
	}
	if err := s.client.request(ctx, http.MethodPost, path, nil, nil); err != nil {
		return fmt.Errorf("reset replacement backoff: %w", err)
	}
	return nil
}

// SetSecret creates or updates a secret in the client's namespace.
func (s *ServerClient) SetSecret(ctx context.Context, name string, value []byte, expected *uint64) (*api.SecretMetadata, error) {
	path, err := s.namespaced("/secrets/%s", url.PathEscape(name))
	if err != nil {
		return nil, fmt.Errorf("set secret: %w", err)
	}
	request := make([]byte, 0, base64.StdEncoding.EncodedLen(len(value))+64)
	request = append(request, '{', '"')
	request = append(request, "value_base64"...)
	request = append(request, '"', ':', '"')
	request = base64.StdEncoding.AppendEncode(request, value)
	request = append(request, '"')
	if expected != nil {
		request = append(request, ',', '"')
		request = append(request, "expected_version"...)
		request = append(request, '"', ':')
		request = strconv.AppendUint(request, *expected, 10)
	}
	request = append(request, '}')
	var response api.SecretMetadata
	if err := s.client.requestBody(ctx, http.MethodPut, path, request, &response); err != nil {
		return nil, fmt.Errorf("set secret: %w", err)
	}
	return &response, nil
}

// ListSecrets returns secret metadata for the client's namespace.
func (s *ServerClient) ListSecrets(ctx context.Context) (*api.SecretListResponse, error) {
	var response api.SecretListResponse
	path, err := s.namespaced("/secrets")
	if err != nil {
		return nil, fmt.Errorf("list secrets: %w", err)
	}
	if err := s.client.request(ctx, http.MethodGet, path, nil, &response); err != nil {
		return nil, fmt.Errorf("list secrets: %w", err)
	}
	return &response, nil
}

// GetSecretMetadata returns metadata for a secret.
func (s *ServerClient) GetSecretMetadata(ctx context.Context, name string) (*api.SecretMetadata, error) {
	var response api.SecretMetadata
	path, err := s.namespaced("/secrets/%s", url.PathEscape(name))
	if err != nil {
		return nil, fmt.Errorf("describe secret: %w", err)
	}
	if err := s.client.request(ctx, http.MethodGet, path, nil, &response); err != nil {
		return nil, fmt.Errorf("describe secret: %w", err)
	}
	return &response, nil
}

// DeleteSecret removes a secret from the client's namespace.
func (s *ServerClient) DeleteSecret(ctx context.Context, name string) error {
	path, err := s.namespaced("/secrets/%s", url.PathEscape(name))
	if err != nil {
		return fmt.Errorf("delete secret: %w", err)
	}
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

// ListAllocations fetches allocations in the client's namespace. label
// filters to allocations whose task group carries the given label key or
// key:value pair (e.g. "trellis.expose" or "trellis.expose:true"); an empty
// label returns every allocation in the namespace.
func (s *ServerClient) ListAllocations(ctx context.Context, label string) (*api.AllocationListResponse, error) {
	u, err := s.namespaced("/allocations")
	if err != nil {
		return nil, fmt.Errorf("list allocations: %w", err)
	}
	return s.listAllocations(ctx, u, label)
}

// ListClusterAllocations fetches allocations across every namespace, which
// requires a cluster-scoped credential. label filters as in ListAllocations.
func (s *ServerClient) ListClusterAllocations(ctx context.Context, label string) (*api.AllocationListResponse, error) {
	return s.listAllocations(ctx, s.address()+"/v1/allocations", label)
}

func (s *ServerClient) listAllocations(ctx context.Context, u, label string) (*api.AllocationListResponse, error) {
	if label != "" {
		u += "?label=" + url.QueryEscape(label)
	}
	var responseData api.AllocationListResponse
	if err := s.client.request(ctx, http.MethodGet, u, nil, &responseData); err != nil {
		return nil, fmt.Errorf("list allocations: %w", err)
	}
	return &responseData, nil
}

// SendHeartbeat reports observed node state.
func (s *ServerClient) SendHeartbeat(ctx context.Context, id uuid.UUID, heartbeat *Heartbeat) error {
	requestData := &api.HeartbeatRequest{
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
	}
	url := fmt.Sprintf("%s/v1/nodes/%s/heartbeat", s.address(), id)
	if err := s.client.request(ctx, http.MethodPost, url, requestData, nil); err != nil {
		return fmt.Errorf("send heartbeat: %w", err)
	}
	return nil
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
