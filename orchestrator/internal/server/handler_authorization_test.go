package server

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/overfold/trellis/orchestrator/internal/auth"
)

func scopedRequest(t *testing.T, method, target, body string, scope auth.AccessScope, access auth.AccessLevel) *http.Request {
	t.Helper()
	req := httptest.NewRequest(method, target, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	ctx := context.WithValue(req.Context(), NamespaceContextKey, auth.EncodeScope(scope, access))
	return req.WithContext(ctx)
}

func TestCredentialCreationRejectsNamespaceScope(t *testing.T) {
	e := echo.New()
	NewHandler(&Server{}).Register(e)
	req := httptest.NewRequest(http.MethodPost, "/v1/credentials", strings.NewReader(`{"scope":"namespace","access":"read"}`))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(context.WithValue(req.Context(), AdminContextKey, true))
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "scope must be cluster") {
		t.Fatalf("status = %d, body: %s; want 400 rejecting namespace scope", rec.Code, rec.Body.String())
	}
}

func TestPlanRejectsAPIAccessAboveCallerAuthority(t *testing.T) {
	clusterWrite := `{"spec":{"name":"demo","namespace":"team","task_groups":[{"name":"web","count":1,"api_access":{"scope":"cluster","access":"write"},"tasks":[{"name":"app","image":"example.invalid/app:1","networking":{"mode":"host"}}]}]}}`
	tests := []struct {
		name   string
		scope  auth.AccessScope
		access auth.AccessLevel
		body   string
		want   int
	}{
		{name: "cluster read cannot delegate cluster write", scope: auth.AccessCluster, access: auth.AccessRead, body: clusterWrite, want: http.StatusForbidden},
		{name: "cluster write may delegate cluster write", scope: auth.AccessCluster, access: auth.AccessWrite, body: clusterWrite, want: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			control := &Server{jobs: make(map[string]*Job), resolveImage: testImageResolver}
			e := echo.New()
			NewHandler(control).Register(e)

			req := scopedRequest(t, http.MethodPost, "/v1/namespaces/team/jobs/plan", tt.body, tt.scope, tt.access)
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req)

			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d; body: %s", rec.Code, tt.want, rec.Body.String())
			}
		})
	}
}

func TestNamespacedRoutesAuthorizeThePathNamespace(t *testing.T) {
	type principal struct {
		scope  auth.AccessScope
		access auth.AccessLevel
	}
	var (
		clusterRead  = principal{auth.AccessCluster, auth.AccessRead}
		clusterWrite = principal{auth.AccessCluster, auth.AccessWrite}
		anonymous    = principal{}
	)
	const secret = `{"value_base64":"dGVzdA=="}`
	tests := []struct {
		name   string
		caller principal
		method string
		path   string
		body   string
		want   int
	}{
		// Cluster credentials may route to any explicit namespace.
		{"cluster read lists jobs", clusterRead, http.MethodGet, "/v1/namespaces/team/jobs", "", http.StatusOK},
		{"cluster read lists allocations", clusterRead, http.MethodGet, "/v1/namespaces/team/allocations", "", http.StatusOK},
		{"cluster read missing job", clusterRead, http.MethodGet, "/v1/namespaces/other/jobs/web", "", http.StatusNotFound},
		{"cluster read lists other allocations", clusterRead, http.MethodGet, "/v1/namespaces/other/allocations", "", http.StatusOK},
		{"cluster write missing job", clusterWrite, http.MethodDelete, "/v1/namespaces/other/jobs/web", "", http.StatusNotFound},
		{"cluster write missing allocation", clusterWrite, http.MethodDelete, "/v1/namespaces/other/allocations/a", "", http.StatusNotFound},
		{"cluster read cannot delete job", clusterRead, http.MethodDelete, "/v1/namespaces/team/jobs/web", "", http.StatusForbidden},
		{"invalid namespace", clusterRead, http.MethodGet, "/v1/namespaces/-bad/jobs", "", http.StatusBadRequest},
		{"unauthenticated namespaced read", anonymous, http.MethodGet, "/v1/namespaces/team/jobs", "", http.StatusForbidden},
		{"cluster read lists any namespace", clusterRead, http.MethodGet, "/v1/namespaces/other/jobs", "", http.StatusOK},

		// Cross-namespace listing is explicit and cluster-scoped.
		{"cluster read lists all allocations", clusterRead, http.MethodGet, "/v1/allocations", "", http.StatusOK},
		{"unauthenticated cannot list all allocations", anonymous, http.MethodGet, "/v1/allocations", "", http.StatusForbidden},

		// Read access sees secret metadata; write access manages secrets.
		{"cluster read lists secrets", clusterRead, http.MethodGet, "/v1/namespaces/team/secrets", "", http.StatusOK},
		{"cluster read describes secret", clusterRead, http.MethodGet, "/v1/namespaces/team/secrets/existing", "", http.StatusOK},
		{"cluster read cannot delete secret", clusterRead, http.MethodDelete, "/v1/namespaces/team/secrets/existing", "", http.StatusForbidden},
		{"cluster read lists other secrets", clusterRead, http.MethodGet, "/v1/namespaces/other/secrets", "", http.StatusOK},
		{"cluster write sets secret", clusterWrite, http.MethodPut, "/v1/namespaces/team/secrets/new", secret, http.StatusOK},
		{"cluster write deletes secret", clusterWrite, http.MethodDelete, "/v1/namespaces/team/secrets/existing", "", http.StatusNoContent},
		{"cluster write deletes other secret", clusterWrite, http.MethodDelete, "/v1/namespaces/other/secrets/existing", "", http.StatusNoContent},
		{"cluster write describes other secret", clusterWrite, http.MethodGet, "/v1/namespaces/other/secrets/existing", "", http.StatusOK},
		{"cluster read describes any secret", clusterRead, http.MethodGet, "/v1/namespaces/other/secrets/existing", "", http.StatusOK},
		{"cluster read cannot set secret", clusterRead, http.MethodPut, "/v1/namespaces/team/secrets/new", secret, http.StatusForbidden},
		{"cluster write sets any secret", clusterWrite, http.MethodPut, "/v1/namespaces/other/secrets/new", secret, http.StatusOK},
		{"unauthenticated cannot list secrets", anonymous, http.MethodGet, "/v1/namespaces/team/secrets", "", http.StatusForbidden},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, store := secretHandler(t)
			for _, namespace := range []string{"team", "other"} {
				if _, err := store.Set(t.Context(), namespace, "existing", []byte("value"), nil); err != nil {
					t.Fatal(err)
				}
			}
			req := httptest.NewRequest(tt.method, tt.path, bytes.NewBufferString(tt.body))
			req.Header.Set("Content-Type", "application/json")
			if tt.caller != anonymous {
				req = scopedRequest(t, tt.method, tt.path, tt.body, tt.caller.scope, tt.caller.access)
			}
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req)
			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d; body: %s", rec.Code, tt.want, rec.Body.String())
			}
		})
	}
}

func TestJobSubmissionRejectsNamespaceMismatch(t *testing.T) {
	body := `{"spec":{"name":"demo","namespace":"other","task_groups":[{"name":"web","count":1,"tasks":[{"name":"app","image":"example.invalid/app:1","networking":{"mode":"host"}}]}]}}`
	for _, path := range []string{"/v1/namespaces/team/jobs", "/v1/namespaces/team/jobs/plan"} {
		t.Run(path, func(t *testing.T) {
			e := echo.New()
			NewHandler(&Server{jobs: make(map[string]*Job)}).Register(e)
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, scopedRequest(t, http.MethodPost, path, body, auth.AccessCluster, auth.AccessWrite))
			if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), `spec.namespace \"other\" does not match request namespace \"team\"`) {
				t.Fatalf("status = %d, want 400 naming both namespaces; body: %s", rec.Code, rec.Body.String())
			}
		})
	}
}
