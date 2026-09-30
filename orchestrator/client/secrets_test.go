package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSetSecretBuildsValidRequestWithoutImmutableBase64Copy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			ValueBase64     string  `json:"value_base64"`
			ExpectedVersion *uint64 `json:"expected_version"`
		}
		if r.URL.Path != "/v1/namespaces/default/secrets/token" {
			t.Errorf("path = %s", r.URL.Path)
		}
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&request); err != nil {
			t.Fatal(err)
		}
		if request.ValueBase64 != "c2VjcmV0" || request.ExpectedVersion == nil || *request.ExpectedVersion != 7 {
			t.Fatalf("request = %#v", request)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"namespace": "default", "name": "token", "version": 8})
	}))
	defer server.Close()
	c := mustNew(t, Config{Address: server.URL, Namespace: "default"})
	expected := uint64(7)
	metadata, err := c.SetSecret(context.Background(), "token", []byte("secret"), &expected)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Version != 8 {
		t.Fatalf("metadata = %#v", metadata)
	}
}
