package client

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRequestBodyIsClearedAfterSend(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{}`)
	}))
	defer server.Close()
	body := []byte("sensitive request body")
	c := &client{client: server.Client()}
	if err := c.requestBody(context.Background(), http.MethodPut, server.URL, body, &struct{}{}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(body, make([]byte, len(body))) {
		t.Fatal("marshalled request body was not cleared")
	}
}

func TestSetSecretBuildsValidRequestWithoutImmutableBase64Copy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			ValueBase64     string  `json:"value_base64"`
			ExpectedVersion *uint64 `json:"expected_version"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		if request.ValueBase64 != "c2VjcmV0" || request.ExpectedVersion == nil || *request.ExpectedVersion != 7 {
			t.Fatalf("request = %#v", request)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"namespace": "default", "name": "token", "version": 8})
	}))
	defer server.Close()
	c := NewServerClient("", server.URL, nil)
	expected := uint64(7)
	metadata, err := c.SetSecret(context.Background(), "default", "token", []byte("secret"), &expected)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Version != 8 {
		t.Fatalf("metadata = %#v", metadata)
	}
}
