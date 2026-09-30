package transport

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/internal/adminsign"
)

func TestRequestBodyIsClearedAfterSend(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{}`)
	}))
	defer server.Close()
	body := []byte("sensitive request body")
	c := &Client{HTTP: server.Client()}
	if err := c.RequestBody(context.Background(), http.MethodPut, server.URL, body, &struct{}{}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(body, make([]byte, len(body))) {
		t.Fatal("marshalled request body was not cleared")
	}
}

func TestHTTPErrorMessageFallsBackToBody(t *testing.T) {
	for body, want := range map[string]string{
		"plain failure\n":      "plain failure",
		`{"code":"x"}`:         `{"code":"x"}`,
		`{"message":"reason"}`: "reason",
	} {
		if got := (&HTTPError{Status: http.StatusBadGateway, Body: []byte(body)}).Message(); got != want {
			t.Fatalf("Message() for %q = %q, want %q", body, got, want)
		}
	}
}

func TestStreamReportsHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"message":"allocation not found"}`, http.StatusNotFound)
	}))
	defer server.Close()
	_, err := (&Client{HTTP: server.Client()}).Stream(context.Background(), server.URL)
	httpErr, ok := err.(*HTTPError)
	if !ok || httpErr.Status != http.StatusNotFound || httpErr.Message() != "allocation not found" {
		t.Fatalf("Stream error = %#v", err)
	}
}

func TestStreamRetriesWithFreshAdministratorChallenge(t *testing.T) {
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	challenges, streams := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/auth/administrator/challenge" {
			challenges++
			_ = json.NewEncoder(w).Encode(api.AdministratorChallengeResponse{Challenge: fmt.Sprintf("c%d", challenges)})
			return
		}
		streams++
		if r.Header.Get(adminsign.ChallengeHeader) == "c1" {
			w.Header().Set(adminsign.ChallengeStatusHeader, adminsign.ChallengeInvalid)
			http.Error(w, "leadership changed", http.StatusUnauthorized)
			return
		}
		_, _ = io.WriteString(w, "stream")
	}))
	defer server.Close()
	body, err := (&Client{AdministratorKey: key, HTTP: server.Client()}).Stream(context.Background(), server.URL+"/v1/events")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = body.Close() }()
	if data, _ := io.ReadAll(body); string(data) != "stream" || challenges != 2 || streams != 2 {
		t.Fatalf("body = %q after %d challenges and %d stream requests", data, challenges, streams)
	}
}
