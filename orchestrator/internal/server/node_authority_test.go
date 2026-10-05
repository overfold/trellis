package server

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/internal/tlsutil"
)

func TestPromotionRequiresAdministratorAndPreservesBinding(t *testing.T) {
	f := newNodeTrustFixture(t)
	id := uuid.New()
	cert := f.certificateFor(id)
	ctx := context.Background()
	if err := f.server.BindNodeCertificateRole(ctx, id, cert, api.NodeRoleWorker); err != nil {
		t.Fatal(err)
	}
	e := echo.New()
	NewHandler(f.server).Register(e)
	for _, admin := range []bool{false, true} {
		r := httptest.NewRequest(http.MethodPost, "/v1/nodes/"+id.String()+"/promote", nil)
		r = r.WithContext(context.WithValue(ctx, AdminContextKey, admin))
		w := httptest.NewRecorder()
		e.ServeHTTP(w, r)
		want := http.StatusForbidden
		if admin {
			want = http.StatusNoContent
		}
		if w.Code != want {
			t.Fatalf("admin=%v status=%d body=%s", admin, w.Code, w.Body.String())
		}
		role, _, err := f.server.state.NodeRole(ctx, id.String())
		if err != nil {
			t.Fatal(err)
		}
		wantRole := api.NodeRoleWorker
		if admin {
			wantRole = api.NodeRoleControlPlane
		}
		if role != wantRole || !f.server.AuthorizeNodeCertificate(ctx, id, cert) {
			t.Fatalf("role=%s, want %s with unchanged certificate", role, wantRole)
		}
	}
	if err := f.server.state.PutNodeTombstone(ctx, id.String(), NodeTombstone{RemovedAt: f.now}); err != nil {
		t.Fatal(err)
	}
	if err := f.server.PromoteNode(ctx, id); err == nil {
		t.Fatal("promoted removed identity")
	}
}

func TestExternalWorkerEnrollmentRequiresAdministratorAndRejectsAPIName(t *testing.T) {
	f := newNodeTrustFixture(t)
	if err := f.server.storage.Delete("tls/ca-key"); err != nil {
		t.Fatal(err)
	}
	id := uuid.New()
	certPEM, _, err := tlsutil.GenerateNodeCert(f.caCert, f.caKey, id)
	if err != nil {
		t.Fatal(err)
	}
	e := echo.New()
	NewHandler(f.server).Register(e)
	for _, admin := range []bool{false, true} {
		body, _ := json.Marshal(api.NodeIdentityCreateRequest{Certificate: string(certPEM), Role: api.NodeRoleWorker})
		r := httptest.NewRequest(http.MethodPost, "/v1/nodes/identities", bytes.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		r = r.WithContext(context.WithValue(r.Context(), AdminContextKey, admin))
		w := httptest.NewRecorder()
		e.ServeHTTP(w, r)
		want := http.StatusForbidden
		if admin {
			want = http.StatusCreated
		}
		if w.Code != want {
			t.Fatalf("admin=%v status=%d body=%s", admin, w.Code, w.Body.String())
		}
	}
	role, found, err := f.server.state.NodeRole(context.Background(), id.String())
	if err != nil || !found || role != api.NodeRoleWorker {
		t.Fatalf("external role=%s found=%v err=%v", role, found, err)
	}
	// Construct an externally signed identity with both DNS names to test
	// rejection independently of the managed signer, which ignores names.
	ca := parseCertificatePEM(t, f.caCert)
	block, _ := pem.Decode(f.caKey)
	caKey, err := x509.ParseECPrivateKey(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	malicious := *parseCertificatePEM(t, certPEM)
	malicious.SerialNumber = big.NewInt(42)
	malicious.NotBefore, malicious.NotAfter = time.Now().Add(-time.Hour), time.Now().Add(time.Hour)
	malicious.DNSNames = append(malicious.DNSNames, tlsutil.ServerName)
	der, err := x509.CreateCertificate(rand.Reader, &malicious, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	badPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if _, err := f.server.EnrollExternalNode(context.Background(), string(badPEM), api.NodeRoleWorker); err == nil {
		t.Fatal("enrolled worker with API DNS authority")
	}
}
