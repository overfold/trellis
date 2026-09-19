package tlsutil

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io"
	"net"
	"net/http"
	"testing"
)

func generateTestMaterials(t *testing.T) *Materials {
	t.Helper()
	caCert, caKey, err := GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	nodeCert, nodeKey, err := GenerateNodeCert(caCert, caKey, "test-node")
	if err != nil {
		t.Fatal(err)
	}
	return &Materials{CACert: caCert, CAKey: caKey, Cert: nodeCert, Key: nodeKey}
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
	certPEM, keyPEM, err := GenerateNodeCert(caCert, caKey, "test-node")
	if err != nil {
		t.Fatal(err)
	}
	if len(certPEM) == 0 {
		t.Fatal("empty node cert")
	}
	if len(keyPEM) == 0 {
		t.Fatal("empty node key")
	}
}

func TestMutualTLSHandshake(t *testing.T) {
	m := generateTestMaterials(t)

	serverCfg, err := ServerTLSConfig(m)
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
	_, err = client.Get("https://" + ln.Addr().String())
	if err == nil {
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
	_, err = client.Get("https://" + ln.Addr().String())
	if err == nil {
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
	certPEM, _, err := GenerateNodeCert(caCert, caKey, "test-node", "10.19.0.5:8128", "myhost:8127")
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
	foundIP := false
	for _, ip := range cert.IPAddresses {
		if ip.Equal(net.ParseIP("10.19.0.5")) {
			foundIP = true
		}
	}
	if !foundIP {
		t.Errorf("cert IPAddresses %v does not contain 10.19.0.5", cert.IPAddresses)
	}
	foundDNS := false
	for _, name := range cert.DNSNames {
		if name == "myhost" {
			foundDNS = true
		}
	}
	if !foundDNS {
		t.Errorf("cert DNSNames %v does not contain myhost", cert.DNSNames)
	}
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
