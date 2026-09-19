package server

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/clofour/trellis/internal/auth"
	"github.com/labstack/echo/v5"
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

			req := scopedRequest(t, http.MethodPost, "/v1/jobs/plan", tt.body, tt.scope, tt.access, tt.namespace)
			if tt.scope == auth.AccessCluster {
				req.Header.Set("X-Trellis-Namespace", "team")
			}
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

	req := scopedRequest(t, http.MethodPost, "/v1/jobs", body, auth.AccessNamespace, auth.AccessWrite, "team")
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusForbidden, rec.Body.String())
	}
}

func TestClusterReadCanReadSecretMetadataButCannotMutateSecrets(t *testing.T) {
	control := &Server{}
	e := echo.New()
	NewHandler(control).Register(e)

	readReq := scopedRequest(t, http.MethodGet, "/v1/namespaces/default/secrets", "", auth.AccessCluster, auth.AccessRead, "")
	readReq.Header.Set("X-Trellis-Namespace", "default")
	readRec := httptest.NewRecorder()
	e.ServeHTTP(readRec, readReq)
	if readRec.Code == http.StatusForbidden {
		t.Fatalf("cluster/read was forbidden from reading secret metadata: %s", readRec.Body.String())
	}

	writeReq := scopedRequest(t, http.MethodPut, "/v1/namespaces/default/secrets/example", `{"value_base64":"dGVzdA=="}`, auth.AccessCluster, auth.AccessRead, "")
	writeReq.Header.Set("X-Trellis-Namespace", "default")
	writeRec := httptest.NewRecorder()
	e.ServeHTTP(writeRec, writeReq)
	if writeRec.Code != http.StatusForbidden {
		t.Fatalf("secret write status = %d, want %d; body: %s", writeRec.Code, http.StatusForbidden, writeRec.Body.String())
	}
}
