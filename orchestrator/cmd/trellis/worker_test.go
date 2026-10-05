package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/internal/election"
	"github.com/overfold/trellis/orchestrator/internal/tlsutil"
	"github.com/spf13/pflag"
)

func TestWorkerConfigurationPrecedenceAndKeyRejection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.yaml")
	if err := os.WriteFile(path, []byte("control_plane: false\nruns_workloads: false\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, override := range []bool{false, true} {
		cfg := &config{}
		flags := pflag.NewFlagSet("node", pflag.ContinueOnError)
		cfg.ControlPlane = flags.Bool("control-plane", true, "")
		cfg.RunsWorkloads = flags.Bool("runs-workloads", true, "")
		if override {
			if err := flags.Parse([]string{"--control-plane=true"}); err != nil {
				t.Fatal(err)
			}
		}
		if err := loadNodeConfig(path, cfg, flags); err != nil {
			t.Fatal(err)
		}
		if cfg.controlPlane() != override || cfg.runsWorkloads() {
			t.Fatalf("override=%v config=%+v", override, cfg)
		}
	}
	worker := false
	for _, field := range []string{"ca", "secrets", "api"} {
		cfg := &config{ControlPlane: &worker, Join: "control:8128"}
		switch field {
		case "ca":
			cfg.CAKey = "/must-not-read"
		case "secrets":
			cfg.SecretsKey = "/must-not-read"
		case "api":
			cfg.APIKey = "/must-not-read"
		}
		if err := run(t.Context(), cfg); err == nil || !strings.Contains(err.Error(), "workers require join") {
			t.Fatalf("%s key accepted: %v", field, err)
		}
	}
}

func TestWorkerRelayPreservesEndToEndTLSAndNodeIdentity(t *testing.T) {
	ca, caKey, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.New()
	nodeCert, nodeKey, err := tlsutil.GenerateNodeCert(ca, caKey, id)
	if err != nil {
		t.Fatal(err)
	}
	apiCert, apiKey, err := tlsutil.GenerateAPICert(ca, caKey)
	if err != nil {
		t.Fatal(err)
	}
	m := &tlsutil.Materials{CACert: ca, Cert: nodeCert, Key: nodeKey, APICert: apiCert, APIKey: apiKey}
	serverTLS, err := tlsutil.LeaderTLSConfig(m)
	if err != nil {
		t.Fatal(err)
	}
	upstream := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(r.TLS.VerifiedChains) == 0 {
			http.Error(w, "no original node certificate", http.StatusUnauthorized)
			return
		}
		actual, err := tlsutil.NodeID(r.TLS.VerifiedChains[0][0])
		if err != nil || actual != id || r.Header.Get("Authorization") != "Bearer test-token" {
			http.Error(w, "identity changed", http.StatusForbidden)
			return
		}
		_, _ = fmt.Fprint(w, "original node and bearer identity preserved")
	}))
	upstream.TLS = serverTLS
	upstream.StartTLS()
	defer upstream.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err == nil {
			relayWorkerConnection(ctx, conn, fixedElector{leader: &election.Leader{NodeID: uuid.New(), Address: upstream.URL}})
		}
	}()
	clientTLS, err := tlsutil.ClientTLSConfig(m)
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{TLSClientConfig: clientTLS, DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+listener.Addr().String()+"/v1/internal/discovery", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer test-token")
	response, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != http.StatusOK || string(body) != "original node and bearer identity preserved" {
		t.Fatalf("relay response status=%d body=%q error=%v", response.StatusCode, body, err)
	}
	if response.TLS.PeerCertificates[0].VerifyHostname(tlsutil.ServerName) != nil {
		t.Fatal("client did not authenticate the upstream API")
	}
	cancel()
	<-done
	worker := false
	if err := validateWorkerMaterials(&config{ControlPlane: &worker}, &tlsutil.Materials{CACert: ca, Cert: nodeCert, Key: nodeKey}); err != nil {
		t.Fatal(err)
	}
	if err := validateWorkerMaterials(&config{ControlPlane: &worker}, m); err == nil {
		t.Fatal("worker accepted API credentials")
	}
}

func TestPromotionRoleCheckUsesAuthenticatedControlPlane(t *testing.T) {
	ca, caKey, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	cert, key, err := tlsutil.GenerateAPICert(ca, caKey)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair(cert, key)
	if err != nil {
		t.Fatal(err)
	}
	role := api.NodeRoleWorker
	s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = fmt.Fprintf(w, `{"role":%q}`, role) }))
	s.TLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{pair}}
	s.StartTLS()
	defer s.Close()
	config, err := tlsutil.CAClientTLSConfig(ca)
	if err != nil {
		t.Fatal(err)
	}
	if err := confirmNodeRole(t.Context(), s.URL, config, api.NodeRoleControlPlane); err == nil {
		t.Fatal("local configuration implicitly promoted worker")
	}
	role = api.NodeRoleControlPlane
	if err := confirmNodeRole(t.Context(), s.URL, config, api.NodeRoleControlPlane); err != nil {
		t.Fatal(err)
	}
}
