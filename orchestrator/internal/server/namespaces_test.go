package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/overfold/trellis/internal/api"
	"github.com/overfold/trellis/internal/auth"
	"github.com/overfold/trellis/internal/spec"
)

func namespaceTestServer() *Server {
	return &Server{jobs: map[string]*Job{
		jobKey("zeta", "api"):  {Spec: &spec.JobSpec{Name: "api", Namespace: "zeta"}},
		jobKey("alpha", "web"): {Spec: &spec.JobSpec{Name: "web", Namespace: "alpha"}},
		jobKey("alpha", "db"):  {Spec: &spec.JobSpec{Name: "db", Namespace: "alpha"}},
	}}
}

func TestListNamespacesReturnsSortedUniqueDesiredNamespaces(t *testing.T) {
	got, err := namespaceTestServer().ListNamespaces(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	want := api.NamespaceListResponse{"alpha", "zeta"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("namespaces = %#v, want %#v", got, want)
	}
}

func TestListNamespacesIncludesSecretAndCredentialNamespaces(t *testing.T) {
	s, _ := newAPIAccessServer(t)
	s.jobs = namespaceTestServer().jobs
	ctx := t.Context()
	if _, err := s.SetSecret(ctx, "vault", "token", []byte("value"), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetSecret(ctx, "alpha", "shared", []byte("value"), nil); err != nil {
		t.Fatal(err)
	}
	for _, principal := range []auth.Principal{
		{Kind: auth.CredentialOperator, Scope: auth.AccessNamespace, Access: auth.AccessRead, Namespace: "team"},
		{Kind: auth.CredentialOperator, Scope: auth.AccessCluster, Access: auth.AccessWrite},
	} {
		if _, err := s.tokenManager.CreateToken(ctx, principal); err != nil {
			t.Fatal(err)
		}
	}

	got, err := s.ListNamespaces(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := api.NamespaceListResponse{"alpha", "team", "vault", "zeta"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("namespaces = %#v, want %#v", got, want)
	}
}

func TestNamespaceDiscoveryRespectsCredentialScope(t *testing.T) {
	e := echo.New()
	NewHandler(namespaceTestServer()).Register(e)

	tests := []struct {
		name      string
		scope     auth.AccessScope
		namespace string
		want      api.NamespaceListResponse
	}{
		{name: "cluster", scope: auth.AccessCluster, want: api.NamespaceListResponse{"alpha", "zeta"}},
		{name: "namespace", scope: auth.AccessNamespace, namespace: "private", want: api.NamespaceListResponse{"private"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := scopedRequest(t, http.MethodGet, "/v1/namespaces", "", tt.scope, auth.AccessRead, tt.namespace)
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d; body: %s", rec.Code, rec.Body.String())
			}
			var got api.NamespaceListResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("namespaces = %#v, want %#v", got, tt.want)
			}
		})
	}
}
