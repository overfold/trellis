package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	"github.com/overfold/trellis/orchestrator/internal/tlsutil"
)

func TestPullExecutionAuthenticatesCurrentLeaderAndBoundsBody(t *testing.T) {
	ca, key, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	node, leader := uuid.New(), uuid.New()
	cert, certKey, err := tlsutil.GenerateNodeCert(ca, key, leader)
	if err != nil {
		t.Fatal(err)
	}
	serverTLS, err := tlsutil.ServerTLSConfig(&tlsutil.Materials{CACert: ca, Cert: cert, Key: certKey})
	if err != nil {
		t.Fatal(err)
	}
	clientCert, clientKey, err := tlsutil.GenerateNodeCert(ca, key, node)
	if err != nil {
		t.Fatal(err)
	}
	clientTLS, err := tlsutil.ClientTLSConfig(&tlsutil.Materials{CACert: ca, Cert: clientCert, Key: clientKey})
	if err != nil {
		t.Fatal(err)
	}
	var client *ServerClient
	mode := "normal"
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS.ServerName != leader.String()+".node.trellis" {
			t.Errorf("unexpected SNI %q", r.TLS.ServerName)
		}
		switch mode {
		case "changed":
			client.SetControlPlane(nodeapi.ControlPlaneResponse{LeaderID: uuid.New(), LeaderAddress: "https://changed.invalid"})
		case "redirect":
			http.Redirect(w, r, "https://redirect.invalid", http.StatusTemporaryRedirect)
			return
		case "body":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"node_id":`))
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			return
		}
		_ = json.NewEncoder(w).Encode(nodeapi.ExecutionIntent{NodeID: node, LeaderID: leader, Version: 1, Epoch: 1, Complete: true})
	}))
	server.TLS = serverTLS
	server.StartTLS()
	defer server.Close()
	client = NewServerClient("", server.URL, clientTLS)
	topology := nodeapi.ControlPlaneResponse{LeaderID: leader, LeaderAddress: server.URL}
	client.SetControlPlane(topology)
	if _, err := client.PullExecution(context.Background(), node, leader, 0); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"changed", "redirect", "body"} {
		mode = value
		client.SetControlPlane(topology)
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		_, err := client.PullExecution(ctx, node, leader, 0)
		cancel()
		if err == nil {
			t.Fatalf("%s fetch accepted", mode)
		}
	}
	mode = "normal"
	client.SetControlPlane(nodeapi.ControlPlaneResponse{LeaderID: node, LeaderAddress: server.URL})
	if _, err := client.PullExecution(context.Background(), node, node, 0); err == nil {
		t.Fatal("wrong identity certificate accepted")
	}
}
