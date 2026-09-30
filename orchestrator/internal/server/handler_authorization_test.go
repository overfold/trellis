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

func scopedRequest(t *testing.T, method, target, body string, scope auth.AccessScope, access auth.AccessLevel, namespace string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(method, target, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	ctx := context.WithValue(req.Context(), NamespaceContextKey, auth.EncodeScope(scope, access, namespace))
	return req.WithContext(ctx)
}

func TestPlanRejectsAPIAccessAboveCallerAuthority(t *testing.T) {
	clusterWrite := `{"spec":{"name":"demo","namespace":"team","task_groups":[{"name":"web","count":1,"api_access":{"scope":"cluster","access":"write"},"tasks":[{"name":"app","image":"example.invalid/app:1","networking":{"mode":"host"}}]}]}}`
	namespaceWrite := `{"spec":{"name":"demo","namespace":"team","task_groups":[{"name":"web","count":1,"api_access":{"scope":"namespace","access":"write"},"tasks":[{"name":"app","image":"example.invalid/app:1","networking":{"mode":"host"}}]}]}}`

	tests := []struct {
		name      string
		scope     auth.AccessScope
		access    auth.AccessLevel
		namespace string
		body      string
		want      int
	}{
		{name: "namespace write cannot delegate cluster write", scope: auth.AccessNamespace, access: auth.AccessWrite, namespace: "team", body: clusterWrite, want: http.StatusForbidden},
		{name: "cluster read cannot delegate cluster write", scope: auth.AccessCluster, access: auth.AccessRead, body: clusterWrite, want: http.StatusForbidden},
		{name: "namespace read cannot delegate namespace write", scope: auth.AccessNamespace, access: auth.AccessRead, namespace: "team", body: namespaceWrite, want: http.StatusForbidden},
		{name: "namespace write may delegate namespace write", scope: auth.AccessNamespace, access: auth.AccessWrite, namespace: "team", body: namespaceWrite, want: http.StatusOK},
		{name: "cluster write may delegate cluster write", scope: auth.AccessCluster, access: auth.AccessWrite, body: clusterWrite, want: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			control := &Server{jobs: make(map[string]*Job)}
			e := echo.New()
			NewHandler(control).Register(e)

			req := scopedRequest(t, http.MethodPost, "/v1/namespaces/team/jobs/plan", tt.body, tt.scope, tt.access, tt.namespace)
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req)

			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d; body: %s", rec.Code, tt.want, rec.Body.String())
			}
		})
	}
}

func TestApplyRejectsAPIAccessAboveCallerAuthority(t *testing.T) {
	body := `{"spec":{"name":"demo","namespace":"team","task_groups":[{"name":"web","count":1,"api_access":{"scope":"cluster","access":"write"},"tasks":[{"name":"app","image":"example.invalid/app:1","networking":{"mode":"host"}}]}]}}`
	control := &Server{}
	e := echo.New()
	NewHandler(control).Register(e)

	req := scopedRequest(t, http.MethodPost, "/v1/namespaces/team/jobs", body, auth.AccessNamespace, auth.AccessWrite, "team")
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusForbidden, rec.Body.String())
	}
}

func TestNamespacedRoutesAuthorizeThePathNamespace(t *testing.T) {
	type principal struct {
		scope     auth.AccessScope
		access    auth.AccessLevel
		namespace string
	}
	var (
		teamRead     = principal{auth.AccessNamespace, auth.AccessRead, "team"}
		teamWrite    = principal{auth.AccessNamespace, auth.AccessWrite, "team"}
		clusterRead  = principal{auth.AccessCluster, auth.AccessRead, ""}
		clusterWrite = principal{auth.AccessCluster, auth.AccessWrite, ""}
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
		// Reads of the caller's own namespace succeed; another namespace is 403.
		{"namespace read lists own jobs", teamRead, http.MethodGet, "/v1/namespaces/team/jobs", "", http.StatusOK},
		{"namespace read lists own allocations", teamRead, http.MethodGet, "/v1/namespaces/team/allocations", "", http.StatusOK},
		{"namespace read cannot list other jobs", teamRead, http.MethodGet, "/v1/namespaces/other/jobs", "", http.StatusForbidden},
		{"namespace read cannot read other job", teamRead, http.MethodGet, "/v1/namespaces/other/jobs/web", "", http.StatusForbidden},
		{"namespace read cannot list other allocations", teamRead, http.MethodGet, "/v1/namespaces/other/allocations", "", http.StatusForbidden},
		{"namespace read cannot stream other events", teamRead, http.MethodGet, "/v1/namespaces/other/events", "", http.StatusForbidden},
		{"namespace write cannot delete other job", teamWrite, http.MethodDelete, "/v1/namespaces/other/jobs/web", "", http.StatusForbidden},
		{"namespace write cannot stop other allocation", teamWrite, http.MethodDelete, "/v1/namespaces/other/allocations/a", "", http.StatusForbidden},
		{"namespace read cannot delete own job", teamRead, http.MethodDelete, "/v1/namespaces/team/jobs/web", "", http.StatusForbidden},
		{"invalid namespace", clusterRead, http.MethodGet, "/v1/namespaces/-bad/jobs", "", http.StatusBadRequest},
		{"unauthenticated namespaced read", anonymous, http.MethodGet, "/v1/namespaces/team/jobs", "", http.StatusForbidden},
		{"cluster read lists any namespace", clusterRead, http.MethodGet, "/v1/namespaces/other/jobs", "", http.StatusOK},

		// Cross-namespace listing is explicit and cluster-scoped.
		{"cluster read lists all allocations", clusterRead, http.MethodGet, "/v1/allocations", "", http.StatusOK},
		{"namespace read cannot list all allocations", teamRead, http.MethodGet, "/v1/allocations", "", http.StatusForbidden},
		{"namespace write cannot stream all events", teamWrite, http.MethodGet, "/v1/events", "", http.StatusForbidden},
		{"unauthenticated cannot list all allocations", anonymous, http.MethodGet, "/v1/allocations", "", http.StatusForbidden},

		// Secrets: namespace read sees metadata, namespace write manages
		// secrets, and neither reaches another namespace.
		{"namespace read lists own secrets", teamRead, http.MethodGet, "/v1/namespaces/team/secrets", "", http.StatusOK},
		{"namespace read describes own secret", teamRead, http.MethodGet, "/v1/namespaces/team/secrets/existing", "", http.StatusOK},
		{"namespace read cannot set own secret", teamRead, http.MethodPut, "/v1/namespaces/team/secrets/new", secret, http.StatusForbidden},
		{"namespace read cannot delete own secret", teamRead, http.MethodDelete, "/v1/namespaces/team/secrets/existing", "", http.StatusForbidden},
		{"namespace read cannot list other secrets", teamRead, http.MethodGet, "/v1/namespaces/other/secrets", "", http.StatusForbidden},
		{"namespace write sets own secret", teamWrite, http.MethodPut, "/v1/namespaces/team/secrets/new", secret, http.StatusOK},
		{"namespace write deletes own secret", teamWrite, http.MethodDelete, "/v1/namespaces/team/secrets/existing", "", http.StatusNoContent},
		{"namespace write cannot set other secret", teamWrite, http.MethodPut, "/v1/namespaces/other/secrets/new", secret, http.StatusForbidden},
		{"namespace write cannot delete other secret", teamWrite, http.MethodDelete, "/v1/namespaces/other/secrets/existing", "", http.StatusForbidden},
		{"namespace write cannot describe other secret", teamWrite, http.MethodGet, "/v1/namespaces/other/secrets/existing", "", http.StatusForbidden},
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
				req = scopedRequest(t, tt.method, tt.path, tt.body, tt.caller.scope, tt.caller.access, tt.caller.namespace)
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
			e.ServeHTTP(rec, scopedRequest(t, http.MethodPost, path, body, auth.AccessCluster, auth.AccessWrite, ""))
			if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), `spec.namespace \"other\" does not match request namespace \"team\"`) {
				t.Fatalf("status = %d, want 400 naming both namespaces; body: %s", rec.Code, rec.Body.String())
			}
		})
	}
}
