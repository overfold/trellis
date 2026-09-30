package client

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/internal/adminsign"
)

func TestAdministratorClientRetriesWithFreshChallengeAfterLeadershipChange(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	challenges := 0
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/auth/administrator/challenge" {
			challenges++
			_ = json.NewEncoder(w).Encode(api.AdministratorChallengeResponse{Challenge: "term-" + string(rune('0'+challenges)), ExpiresAt: time.Now().Add(time.Minute)})
			return
		}
		requests++
		if requests == 1 {
			w.Header().Set(adminsign.ChallengeStatusHeader, adminsign.ChallengeInvalid)
			http.Error(w, "leadership changed", http.StatusUnauthorized)
			return
		}
		body, _ := io.ReadAll(r.Body)
		challenge := r.Header.Get(adminsign.ChallengeHeader)
		signature, err := base64.RawURLEncoding.DecodeString(r.Header.Get(adminsign.SignatureHeader))
		payload := adminsign.Payload(challenge, r.Method, r.URL.RequestURI(), body)
		if err != nil || challenge != "term-2" || !ed25519.Verify(publicKey, payload, signature) {
			http.Error(w, "invalid signature", http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"token":"trls_op_created"}`)
	}))
	defer server.Close()

	serverClient, err := New(Config{Address: server.URL, AdministratorKey: privateKey})
	if err != nil {
		t.Fatal(err)
	}
	response, err := serverClient.CreateCredential(t.Context(), &api.CredentialCreateRequest{Scope: "cluster", Access: "write"})
	if err != nil {
		t.Fatal(err)
	}
	if response.Token != "trls_op_created" || challenges != 2 || requests != 2 {
		t.Fatalf("response=%#v challenges=%d requests=%d", response, challenges, requests)
	}
}
