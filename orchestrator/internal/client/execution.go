package client

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	"github.com/overfold/trellis/orchestrator/internal/tlsutil"
	"github.com/overfold/trellis/orchestrator/internal/transport"
)

// SetControlPlane installs topology obtained from trusted local membership.
func (s *ServerClient) SetControlPlane(topology nodeapi.ControlPlaneResponse) {
	s.storeControlPlane(topology)
}

func (s *ServerClient) executionRequest(ctx context.Context, leader uuid.UUID, path string, budget time.Duration, out any) error {
	topology := s.ControlPlane()
	if leader == uuid.Nil || topology.LeaderID != leader || topology.LeaderAddress == "" {
		return fmt.Errorf("execution leader is not current trusted leader")
	}
	if s.tlsConfig == nil || s.tlsConfig.InsecureSkipVerify {
		return fmt.Errorf("execution requires verified mutual TLS")
	}
	config := s.tlsConfig.Clone()
	config.ServerName = leader.String() + ".node.trellis"
	previousVerify := config.VerifyConnection
	config.VerifyConnection = func(state tls.ConnectionState) error {
		if previousVerify != nil {
			if err := previousVerify(state); err != nil {
				return err
			}
		}
		if len(state.PeerCertificates) == 0 {
			return fmt.Errorf("missing leader certificate")
		}
		id, err := tlsutil.NodeID(state.PeerCertificates[0])
		if err != nil || id != leader {
			return fmt.Errorf("leader certificate identity mismatch: %s", id)
		}
		return nil
	}
	httpClient := transport.NewHTTPClient(config, budget)
	httpClient.Timeout = budget
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	defer httpClient.CloseIdleConnections()
	client := &transport.Client{Token: s.client.Token, HTTP: httpClient}
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	if err := client.Request(ctx, http.MethodGet, transport.NormalizeBaseURL(topology.LeaderAddress)+path, nil, out); err != nil {
		return err
	}
	current := s.ControlPlane()
	if current.LeaderID != leader || current.LeaderAddress != topology.LeaderAddress {
		return fmt.Errorf("execution leader changed during fetch")
	}
	return nil
}

// PullExecution long-polls the authenticated current leader for a complete view.
func (s *ServerClient) PullExecution(ctx context.Context, node, leader uuid.UUID, epoch, version uint64) (*nodeapi.ExecutionIntent, error) {
	var intent nodeapi.ExecutionIntent
	err := s.executionRequest(ctx, leader, fmt.Sprintf("/v1/nodes/%s/execution?epoch=%d&version=%d", node, epoch, version), 35*time.Second, &intent)
	if err != nil {
		return nil, err
	}
	if intent.LeaderID != leader || intent.NodeID != node {
		return nil, fmt.Errorf("execution identity mismatch")
	}
	return &intent, nil
}

// ExecutionCredentials retrieves credentials for one centrally authorized attempt.
func (s *ServerClient) ExecutionCredentials(ctx context.Context, node, leader uuid.UUID, version uint64, r nodeapi.AllocationRequest) (*nodeapi.ExecutionCredentials, error) {
	var credentials nodeapi.ExecutionCredentials
	path := fmt.Sprintf("/v1/nodes/%s/execution/%s/credentials?version=%d&generation=%d&epoch=%d&attempt=%d", node, url.PathEscape(r.AllocationID), version, r.Generation, r.Epoch, r.Attempt)
	if err := s.executionRequest(ctx, leader, path, 10*time.Second, &credentials); err != nil {
		return nil, err
	}
	return &credentials, nil
}
