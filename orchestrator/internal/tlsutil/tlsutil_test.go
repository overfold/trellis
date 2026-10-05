package tlsutil

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
)

func generateTestMaterials(t *testing.T) *Materials {
	t.Helper()
	caCert, caKey, err := GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	nodeCert, nodeKey, err := GenerateNodeCert(caCert, caKey, uuid.New())
	if err != nil {
		t.Fatal(err)
	}
	apiCert, apiKey, err := GenerateAPICert(caCert, caKey)
	if err != nil {
		t.Fatal(err)
	}
	return &Materials{CACert: caCert, CAKey: caKey, Cert: nodeCert, Key: nodeKey, APICert: apiCert, APIKey: apiKey}
}

func TestGenerateCA(t *testing.T) {
	certPEM, keyPEM, err := GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	if len(certPEM) == 0 {
		t.Fatal("empty CA cert")
	}
	if len(keyPEM) == 0 {
		t.Fatal("empty CA key")
	}
}

func TestGenerateNodeCert(t *testing.T) {
	caCert, caKey, err := GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	nodeID := uuid.New()
	certPEM, keyPEM, err := GenerateNodeCert(caCert, caKey, nodeID)
	if err != nil {
		t.Fatal(err)
	}
	if len(certPEM) == 0 {
		t.Fatal("empty node cert")
	}
	if len(keyPEM) == 0 {
		t.Fatal("empty node key")
	}
	if err := ValidateMaterials(&Materials{CACert: caCert, Cert: certPEM, Key: keyPEM}, nodeID); err != nil {
		t.Fatalf("validate node materials: %v", err)
	}
}

func TestMutualTLSHandshake(t *testing.T) {
	m := generateTestMaterials(t)

	serverCfg, err := LeaderTLSConfig(m)
	if err != nil {
		t.Fatal(err)
	}
	clientCfg, err := ClientTLSConfig(m)
	if err != nil {
		t.Fatal(err)
	}

	ln, err := tls.Listen("tcp", "127.0.0.1:0", serverCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})}
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Close() }()

	client := &http.Client{Transport: &http.Transport{TLSClientConfig: clientCfg}}
	resp, err := client.Get("https://" + ln.Addr().String())
	if err != nil {
		t.Fatalf("mTLS request failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "ok" {
		t.Fatalf("unexpected response: %s", body)
	}
}

func TestRejectUntrustedClientCert(t *testing.T) {
	serverMaterials := generateTestMaterials(t)
	untrustedMaterials := generateTestMaterials(t)

	serverCfg, err := ServerTLSConfig(serverMaterials)
	if err != nil {
		t.Fatal(err)
	}

	ln, err := tls.Listen("tcp", "127.0.0.1:0", serverCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("should not reach here"))
	})}
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Close() }()

	clientCfg, err := ClientTLSConfig(untrustedMaterials)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: clientCfg}}
	resp, err := client.Get("https://" + ln.Addr().String())
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("expected TLS handshake to fail with untrusted cert")
	}
}

func TestRejectNoClientCert(t *testing.T) {
	m := generateTestMaterials(t)

	serverCfg, err := ServerTLSConfig(m)
	if err != nil {
		t.Fatal(err)
	}

	ln, err := tls.Listen("tcp", "127.0.0.1:0", serverCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("should not reach here"))
	})}
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Close() }()

	caOnlyCfg, err := CAClientTLSConfig(m.CACert)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: caOnlyCfg}}
	resp, err := client.Get("https://" + ln.Addr().String())
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("expected TLS handshake to fail without client cert")
	}
}

func TestPeerTLSConfig(t *testing.T) {
	m := generateTestMaterials(t)

	cfg, err := PeerTLSConfig(m)
	if err != nil {
		t.Fatal(err)
	}

	ln, err := tls.Listen("tcp", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	done := make(chan string, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		buf := make([]byte, 16)
		n, _ := conn.Read(buf)
		done <- string(buf[:n])
	}()

	conn, err := tls.Dial("tcp", ln.Addr().String(), cfg)
	if err != nil {
		t.Fatalf("peer TLS dial failed: %v", err)
	}
	_, _ = conn.Write([]byte("hello"))
	_ = conn.Close()

	msg := <-done
	if msg != "hello" {
		t.Fatalf("expected 'hello', got %q", msg)
	}
}

func TestGenerateNodeCertExtraSANs(t *testing.T) {
	caCert, caKey, err := GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	certPEM, _, err := GenerateNodeCert(caCert, caKey, uuid.New(), "10.19.0.5:8128", "myhost:8127")
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(certPEM)
	if block == nil {
		t.Fatal("failed to decode cert PEM")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if len(cert.IPAddresses) != 0 {
		t.Errorf("cert IPAddresses = %v, want none", cert.IPAddresses)
	}
	if len(cert.DNSNames) != 1 || cert.DNSNames[0] == ServerName || cert.DNSNames[0] == "myhost" {
		t.Errorf("cert DNSNames = %v, want only UUID node name", cert.DNSNames)
	}
}

func TestSigningIgnoresMaliciousCSRNames(t *testing.T) {
	caCert, caKey, err := GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		DNSNames: []string{ServerName, "attacker.example"},
	}, key)
	if err != nil {
		t.Fatal(err)
	}
	csr := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER})
	nodeID := uuid.New()
	nodePEM, err := SignNodeCSR(caCert, caKey, csr, nodeID)
	if err != nil {
		t.Fatal(err)
	}
	node := parseTestCertificate(t, nodePEM)
	if len(node.DNSNames) != 1 || node.DNSNames[0] != NodeServerName(nodeID) {
		t.Fatalf("node DNS names = %v", node.DNSNames)
	}
	apiPEM, err := SignAPICSR(caCert, caKey, csr)
	if err != nil {
		t.Fatal(err)
	}
	api := parseTestCertificate(t, apiPEM)
	if len(api.DNSNames) != 1 || api.DNSNames[0] != ServerName {
		t.Fatalf("API DNS names = %v", api.DNSNames)
	}
}

func TestAPICertificateLifetimeAndUsages(t *testing.T) {
	caCert, caKey, err := GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	certPEM, _, err := GenerateAPICert(caCert, caKey)
	if err != nil {
		t.Fatal(err)
	}
	cert := parseTestCertificate(t, certPEM)
	if got := cert.NotAfter.Sub(cert.NotBefore); got != APICertificateLifetime {
		t.Fatalf("API certificate lifetime = %v, want %v", got, APICertificateLifetime)
	}
	if len(cert.ExtKeyUsage) != 1 || cert.ExtKeyUsage[0] != x509.ExtKeyUsageServerAuth {
		t.Fatalf("API extended usages = %v, want server auth only", cert.ExtKeyUsage)
	}
	if cert.NotAfter.After(time.Now().Add(APICertificateLifetime)) {
		t.Fatalf("API certificate expires too late: %v", cert.NotAfter)
	}
}

func TestNodeCertificateCannotServeAPIName(t *testing.T) {
	caCert, caKey, err := GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	certPEM, _, err := GenerateNodeCert(caCert, caKey, uuid.New(), ServerName)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caCert)
	cert := parseTestCertificate(t, certPEM)
	if _, err := cert.Verify(x509.VerifyOptions{Roots: pool, DNSName: ServerName, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}); err == nil {
		t.Fatal("node certificate unexpectedly verifies for API name")
	}
}

func parseTestCertificate(t *testing.T, certPEM []byte) *x509.Certificate {
	t.Helper()
	block, _ := pem.Decode(certPEM)
	if block == nil {
		t.Fatal("failed to decode certificate")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

func TestLeaderTLSConfigAllowsNoClientCert(t *testing.T) {
	m := generateTestMaterials(t)

	serverCfg, err := LeaderTLSConfig(m)
	if err != nil {
		t.Fatal(err)
	}

	ln, err := tls.Listen("tcp", "127.0.0.1:0", serverCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})}
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Close() }()

	caOnlyCfg, err := CAClientTLSConfig(m.CACert)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: caOnlyCfg}}
	resp, err := client.Get("https://" + ln.Addr().String())
	if err != nil {
		t.Fatalf("leader TLS request without client cert should succeed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "ok" {
		t.Fatalf("unexpected response: %s", body)
	}
}
