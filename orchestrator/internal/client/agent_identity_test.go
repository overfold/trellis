package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/clofour/trellis/internal/tlsutil"
	"github.com/google/uuid"
)

func TestAgentClientRejectsValidCertificateForDifferentNode(t *testing.T) {
	caCert, caKey, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	clientNodeID, serverNodeID := uuid.New(), uuid.New()
	clientCert, clientKey, err := tlsutil.GenerateNodeCert(caCert, caKey, clientNodeID)
	if err != nil {
		t.Fatal(err)
	}
	serverCert, serverKey, err := tlsutil.GenerateNodeCert(caCert, caKey, serverNodeID)
	if err != nil {
		t.Fatal(err)
	}
	serverTLS, err := tlsutil.ServerTLSConfig(&tlsutil.Materials{CACert: caCert, Cert: serverCert, Key: serverKey})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("[]"))
	}))
	server.TLS = serverTLS
	server.StartTLS()
	t.Cleanup(server.Close)

	clientTLS, err := tlsutil.ClientTLSConfig(&tlsutil.Materials{CACert: caCert, Cert: clientCert, Key: clientKey})
	if err != nil {
		t.Fatal(err)
	}
	agent := NewAgentClient("", clientTLS)
	address := strings.TrimPrefix(server.URL, "https://")
	if _, err := agent.AllocationMetrics(context.Background(), clientNodeID, address, "alloc-1"); err == nil || !strings.Contains(err.Error(), serverNodeID.String()) {
		t.Fatalf("wrong-node agent certificate error = %v", err)
	}
	if _, err := agent.AllocationMetrics(context.Background(), serverNodeID, address, "alloc-1"); err != nil {
		t.Fatalf("matching node certificate was rejected: %v", err)
	}
}
