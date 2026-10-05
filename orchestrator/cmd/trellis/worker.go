package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/internal/election"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	"github.com/overfold/trellis/orchestrator/internal/storage"
	"github.com/overfold/trellis/orchestrator/internal/tlsutil"
	"github.com/overfold/trellis/orchestrator/internal/transport"
)

func (c *config) controlPlane() bool  { return c.ControlPlane == nil || *c.ControlPlane }
func (c *config) runsWorkloads() bool { return c.RunsWorkloads == nil || *c.RunsWorkloads }
func (c *config) nodeRole() api.NodeRole {
	if c.controlPlane() {
		return api.NodeRoleControlPlane
	}
	return api.NodeRoleWorker
}

func confirmNodeRole(ctx context.Context, join string, config *tls.Config, expected api.NodeRole) error {
	if join == "" {
		return fmt.Errorf("role verification requires a control-plane join address")
	}
	client := &transport.Client{HTTP: transport.NewHTTPClient(config, 10*time.Second)}
	var response nodeapi.NodeRoleResponse
	if err := client.Request(ctx, http.MethodGet, normalizeAPIAddress(join)+"/v1/internal/node-role", nil, &response); err != nil {
		return fmt.Errorf("verify administrator-assigned node role: %w", err)
	}
	if response.Role != expected {
		return fmt.Errorf("node is enrolled as %s; administrator promotion is required before changing control_plane", response.Role)
	}
	return nil
}

func validateWorkerMaterials(c *config, m *tlsutil.Materials) error {
	if c.controlPlane() {
		return nil
	}
	if len(m.CAKey) != 0 || len(m.APICert) != 0 || len(m.APIKey) != 0 {
		return fmt.Errorf("worker storage must not contain CA or API signing material")
	}
	cert, err := tls.X509KeyPair(m.Cert, m.Key)
	if err != nil {
		return err
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return err
	}
	if leaf.VerifyHostname(tlsutil.ServerName) == nil {
		return fmt.Errorf("worker node certificate must not authenticate as trellis")
	}
	return nil
}

// workerElector observes authenticated control-plane responses, never Raft.
// The cached addresses are public routing information, not replicated state.
type workerElector struct {
	mu       sync.RWMutex
	topology nodeapi.ControlPlaneResponse
	client   *transport.Client
	local    *storage.LocalStorage
}

func newWorkerElector(join string, config *tls.Config, local *storage.LocalStorage) *workerElector {
	e := &workerElector{local: local, client: &transport.Client{HTTP: transport.NewHTTPClient(config, 5*time.Second)}}
	_ = local.Get("control-plane-addresses", &e.topology)
	if !slices.Contains(e.topology.Addresses, join) {
		e.topology.Addresses = append(e.topology.Addresses, join)
	}
	// Do not authorize an old leader until a live member confirms it.
	e.topology.LeaderID = uuid.Nil
	return e
}

func (e *workerElector) Run(ctx context.Context, _ chan<- election.Event) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		e.mu.RLock()
		addresses := append([]string{e.topology.LeaderAddress}, e.topology.Addresses...)
		e.mu.RUnlock()
		for _, address := range addresses {
			if address == "" {
				continue
			}
			var topology nodeapi.ControlPlaneResponse
			if err := e.client.Request(ctx, http.MethodGet, normalizeAPIAddress(address)+"/v1/internal/control-plane", nil, &topology); err != nil {
				continue
			}
			if topology.LeaderID == uuid.Nil || topology.LeaderAddress == "" {
				continue
			}
			e.mu.Lock()
			e.topology = topology
			e.mu.Unlock()
			if err := e.local.Put("control-plane-addresses", topology); err != nil {
				return err
			}
			break
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (e *workerElector) Current(context.Context) (*election.Leader, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.topology.LeaderID == uuid.Nil {
		return nil, nil
	}
	return &election.Leader{NodeID: e.topology.LeaderID, Address: e.topology.LeaderAddress}, nil
}

func (e *workerElector) CurrentID() (uuid.UUID, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.topology.LeaderID, nil
}

// runWorkerRelay copies opaque TCP bytes. It has no TLS configuration and
// cannot inspect credentials or substitute its own client identity.
func runWorkerRelay(ctx context.Context, address string, elector election.Elector) error {
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", address)
	if err != nil {
		return err
	}
	defer func() { _ = listener.Close() }()
	stop := context.AfterFunc(ctx, func() { _ = listener.Close() })
	defer stop()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		go relayWorkerConnection(ctx, conn, elector)
	}
}

func relayWorkerConnection(ctx context.Context, downstream net.Conn, elector election.Elector) {
	defer func() { _ = downstream.Close() }()
	leader, err := elector.Current(ctx)
	if err != nil || leader == nil {
		return
	}
	target, err := url.Parse(normalizeAPIAddress(leader.Address))
	if err != nil || target.Scheme != "https" {
		return
	}
	upstream, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "tcp", target.Host)
	if err != nil {
		return
	}
	defer func() { _ = upstream.Close() }()
	stop := context.AfterFunc(ctx, func() { _ = upstream.Close(); _ = downstream.Close() })
	defer stop()
	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(upstream, downstream)
		if tcp, ok := upstream.(*net.TCPConn); ok {
			_ = tcp.CloseWrite()
		}
		close(done)
	}()
	_, _ = io.Copy(downstream, upstream)
	_ = downstream.Close()
	<-done
}
