package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/overfold/trellis/orchestrator/internal/api"
	"github.com/overfold/trellis/orchestrator/internal/auth"
)

func TestHandleWhoAmI(t *testing.T) {
	created := time.Date(2026, 9, 2, 20, 0, 0, 0, time.UTC)
	principal := auth.Principal{
		Kind:      auth.CredentialWorkload,
		Scope:     auth.AccessNamespace,
		Access:    auth.AccessRead,
		Namespace: "team",
		Subject: &auth.CredentialSubject{
			Namespace: "team",
			Job:       "controller",
			TaskGroup: "worker",
		},
		CreatedAt: created,
	}
	e := echo.New()
	e.GET("/v1/auth/whoami", HandleWhoAmI)
	req := httptest.NewRequest(http.MethodGet, "/v1/auth/whoami", nil)
	req = req.WithContext(context.WithValue(req.Context(), PrincipalContextKey, principal))
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; body: %s", rec.Code, rec.Body.String())
	}
	var got api.CredentialInfoResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Kind != "workload" || got.Scope != "namespace" || got.Access != "read" || got.Namespace != "team" || got.Subject == nil || got.Subject.Job != "controller" || got.CreatedAt == nil || !got.CreatedAt.Equal(created) {
		t.Fatalf("unexpected response: %#v", got)
	}
}

func TestHandleWhoAmIRejectsMissingPrincipal(t *testing.T) {
	e := echo.New()
	e.GET("/v1/auth/whoami", HandleWhoAmI)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/auth/whoami", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}
