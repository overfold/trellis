package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/internal/auth"
	"github.com/overfold/trellis/orchestrator/internal/spec"
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

func TestListNamespacesIncludesSecretNamespaces(t *testing.T) {
	s, _ := newAPIAccessServer(t)
	s.jobs = namespaceTestServer().jobs
	ctx := t.Context()
	if _, err := s.SetSecret(ctx, "vault", "token", []byte("value"), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetSecret(ctx, "alpha", "shared", []byte("value"), nil); err != nil {
		t.Fatal(err)
	}
	got, err := s.ListNamespaces(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := api.NamespaceListResponse{"alpha", "vault", "zeta"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("namespaces = %#v, want %#v", got, want)
	}
}

func TestNamespaceDiscoveryRequiresClusterScope(t *testing.T) {
	e := echo.New()
	NewHandler(namespaceTestServer()).Register(e)

	tests := []struct {
		name  string
		scope auth.AccessScope
		want  api.NamespaceListResponse
	}{
		{name: "cluster", scope: auth.AccessCluster, want: api.NamespaceListResponse{"alpha", "zeta"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := scopedRequest(t, http.MethodGet, "/v1/namespaces", "", tt.scope, auth.AccessRead)
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
