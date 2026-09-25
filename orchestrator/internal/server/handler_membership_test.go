package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/clofour/trellis/internal/storage"
	"github.com/clofour/trellis/internal/tlsutil"
	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

type recordingClusterJoiner struct {
	removedID string
	err       error
}

func (*recordingClusterJoiner) AddVoter(string, string) error { return nil }
func (*recordingClusterJoiner) LeadershipTransfer() error     { return nil }

func (j *recordingClusterJoiner) RemoveServer(id string) error {
	j.removedID = id
	return j.err
}

func TestHandleRaftMemberRemove(t *testing.T) {
	joiner := &recordingClusterJoiner{}
	control := &Server{joiner: joiner}
	e := echo.New()
	NewHandler(control).Register(e)

	req := httptest.NewRequest(http.MethodDelete, "/v1/raft/members/node-2.example:8128", nil)
	req = req.WithContext(context.WithValue(req.Context(), AdminContextKey, true))
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusNoContent, rec.Body.String())
	}
	if joiner.removedID != "node-2.example:8128" {
		t.Fatalf("removed ID = %q, want %q", joiner.removedID, "node-2.example:8128")
	}
}

func TestHandleRaftMemberRemoveFailure(t *testing.T) {
	joiner := &recordingClusterJoiner{err: errors.New("not the leader")}
	control := &Server{joiner: joiner}
	e := echo.New()
	NewHandler(control).Register(e)

	req := httptest.NewRequest(http.MethodDelete, "/v1/raft/members/node-2", nil)
	req = req.WithContext(context.WithValue(req.Context(), AdminContextKey, true))
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusInternalServerError, rec.Body.String())
	}
}

func TestHandleRaftMemberRemoveRequiresClusterAuthorization(t *testing.T) {
	joiner := &recordingClusterJoiner{}
	control := &Server{joiner: joiner}
	e := echo.New()
	NewHandler(control).Register(e)

	req := httptest.NewRequest(http.MethodDelete, "/v1/raft/members/node-2", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusForbidden, rec.Body.String())
	}
	if joiner.removedID != "" {
		t.Fatalf("unexpected removal of %q", joiner.removedID)
	}
}

func TestManagedNodeEnrollmentIssuesCertificateForRequestedIdentity(t *testing.T) {
	local := storage.NewLocalStorage(t.TempDir())
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	caCert, caKey, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	if err := local.Put("tls/ca-cert", string(caCert)); err != nil {
		t.Fatal(err)
	}
	if err := local.Put("tls/ca-key", string(caKey)); err != nil {
		t.Fatal(err)
	}
	id := uuid.New()
	response, err := (&Server{storage: local}).EnrollNode(id, "node-b:8128", "node-b:8127")
	if err != nil {
		t.Fatal(err)
	}
	m := &tlsutil.Materials{CACert: []byte(response.CACert), Cert: []byte(response.Cert), Key: []byte(response.Key)}
	if err := tlsutil.ValidateMaterials(m, id); err != nil {
		t.Fatalf("enrolled certificate: %v", err)
	}
}

func TestExternalSigningCannotEnrollWithoutCAKey(t *testing.T) {
	local := storage.NewLocalStorage(t.TempDir())
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	caCert, _, err := tlsutil.GenerateCA()
	if err != nil {
		t.Fatal(err)
	}
	if err := local.Put("tls/ca-cert", string(caCert)); err != nil {
		t.Fatal(err)
	}
	if _, err := (&Server{storage: local}).EnrollNode(uuid.New()); err == nil {
		t.Fatal("external-signing node unexpectedly enrolled another node")
	}
}
