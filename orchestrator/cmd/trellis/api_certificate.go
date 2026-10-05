package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/internal/election"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	"github.com/overfold/trellis/orchestrator/internal/server"
	"github.com/overfold/trellis/orchestrator/internal/tlsutil"
	"github.com/overfold/trellis/orchestrator/internal/transport"
)

func configureAPICertificate(ctx context.Context, cfg *config, materials *tlsutil.Materials, control *server.Server, id uuid.UUID) error {
	if cfg.SigningMode == "external" {
		if cfg.APICert == "" || cfg.APIKey == "" {
			return fmt.Errorf("external control-plane nodes require api_cert and api_key")
		}
		var err error
		materials.APICert, err = os.ReadFile(cfg.APICert)
		if err != nil {
			return err
		}
		materials.APIKey, err = os.ReadFile(cfg.APIKey)
		if err != nil {
			return err
		}
	} else {
		csr, key, err := tlsutil.GenerateCSR()
		if err != nil {
			return err
		}
		// A new member can observe a leader before its enrollment transaction
		// has reached the local FSM. Wait for that binding, not merely an
		// empty local log whose applied index equals its last index.
		deadline := time.NewTimer(30 * time.Second)
		defer deadline.Stop()
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		var cert []byte
		for {
			cert, err = control.IssueAPICertificate(ctx, id, string(csr))
			if err == nil {
				break
			}
			select {
			case <-ctx.Done():
				clear(key)
				return ctx.Err()
			case <-deadline.C:
				clear(key)
				return err
			case <-ticker.C:
			}
		}
		materials.APICert, materials.APIKey = cert, key
	}
	cert, err := tls.X509KeyPair(materials.APICert, materials.APIKey)
	if err != nil {
		return err
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(materials.CACert) {
		return fmt.Errorf("invalid API CA")
	}
	_, err = leaf.Verify(x509.VerifyOptions{Roots: pool, DNSName: tlsutil.ServerName, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}})
	return err
}

// Only the elected leader renews API certificates, after checking the
// requesting node's durable authority and current Raft admission. A failed
// renewal leaves the old certificate to expire rather than extending trust.
func renewAPICertificate(ctx context.Context, log *slog.Logger, clientTLS *tls.Config, elector election.Elector, current *atomic.Pointer[tls.Certificate]) {
	client := &transport.Client{HTTP: transport.NewHTTPClient(clientTLS, 10*time.Second)}
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		leader, err := elector.Current(ctx)
		if err != nil || leader == nil {
			continue
		}
		csr, key, err := tlsutil.GenerateCSR()
		if err != nil {
			log.Error("generate API CSR", "error", err)
			continue
		}
		var response nodeapi.APICertificateResponse
		err = client.Request(ctx, http.MethodPost, normalizeAPIAddress(leader.Address)+"/v1/nodes/api-certificate", nodeapi.APICertificateRequest{CSR: string(csr)}, &response)
		if err == nil {
			var cert tls.Certificate
			cert, err = tls.X509KeyPair([]byte(response.Cert), key)
			if err == nil {
				current.Store(&cert)
			}
		}
		clear(key)
		if err != nil {
			log.Warn("API certificate renewal failed", "error", err)
		}
	}
}
