package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/overfold/trellis/internal/api"
	"github.com/overfold/trellis/internal/auth"
	"github.com/overfold/trellis/internal/lifecycle"
	secretstore "github.com/overfold/trellis/internal/secrets"
	"github.com/overfold/trellis/internal/spec"
	"github.com/overfold/trellis/internal/state"
)

func secretHandler(t *testing.T) (*echo.Echo, *secretstore.Store) {
	t.Helper()
	store, err := secretstore.NewStore(memoryStore{}, "test", "key-1", bytes.Repeat([]byte{1}, 32))
	if err != nil {
		t.Fatal(err)
	}
	control := &Server{secrets: store, log: slog.Default()}
	e := echo.New()
	NewHandler(control).Register(e)
	return e, store
}

func TestSecretMetadataEndpointsNeverReturnPlaintext(t *testing.T) {
	e, store := secretHandler(t)
	const plaintext = "metadata-sentinel-plaintext"
	if _, err := store.Set(context.Background(), "default", "token", []byte(plaintext), nil); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/v1/namespaces/default/secrets", "/v1/namespaces/default/secrets/token"} {
		req := scopedRequest(t, http.MethodGet, path, "", auth.AccessCluster, auth.AccessRead, "")
		req.Header.Set("X-Trellis-Namespace", "default")
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s status = %d: %s", path, rec.Code, rec.Body.String())
		}
		if strings.Contains(rec.Body.String(), plaintext) || strings.Contains(rec.Body.String(), "value_base64") {
			t.Fatalf("GET %s exposed secret material: %s", path, rec.Body.String())
		}
	}
}

func TestSecretHandlerEnforcesExactDecodedSizeBoundary(t *testing.T) {
	e, _ := secretHandler(t)
	request := func(name string, size int) *httptest.ResponseRecorder {
		t.Helper()
		encoded := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{'x'}, size))
		body := fmt.Sprintf(`{"value_base64":%q}`, encoded)
		req := scopedRequest(t, http.MethodPut, "/v1/namespaces/default/secrets/"+name, body, auth.AccessCluster, auth.AccessWrite, "")
		req.Header.Set("X-Trellis-Namespace", "default")
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		return rec
	}
	if rec := request("exact", secretstore.MaxValueSize); rec.Code != http.StatusOK {
		t.Fatalf("exact boundary status = %d: %s", rec.Code, rec.Body.String())
	}
	if rec := request("oversized", secretstore.MaxValueSize+1); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized status = %d, want 413: %s", rec.Code, rec.Body.String())
	}
}

func TestAllocationReceivesSecretFromMatchingNamespace(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	store, err := secretstore.NewStore(memoryStore{}, "test", "key-1", bytes.Repeat([]byte{1}, 32))
	if err != nil {
		t.Fatal(err)
	}
	for namespace, value := range map[string]string{"default": "matching-value", "other": "wrong-value"} {
		if _, err := store.Set(context.Background(), namespace, "token", []byte(value), nil); err != nil {
			t.Fatal(err)
		}
	}
	s.secrets = store
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy}
	task := spec.TaskSpec{Name: "app", Image: "app", Secrets: []spec.SecretRefSpec{{Name: "token", Target: spec.SecretTargetEnv, Env: "TOKEN"}}}
	s.jobs[jobKey("default", "web")] = &Job{Spec: canonicalTestSpec(&spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "app", Tasks: []spec.TaskSpec{task}}}}), Revision: 1}
	allocation := &Allocation{ID: "allocation", Namespace: "default", JobName: "web", TaskGroupName: "app", Tasks: []spec.TaskSpec{task}, Node: node, Generation: 1, JobRevision: 1, Phase: lifecycle.PhasePlaced}
	if err := s.Execute(context.Background(), &Action{Type: ActionStart, Allocation: allocation}); err != nil {
		t.Fatal(err)
	}
	calls := agent.recordedCalls()
	if len(calls) != 1 {
		t.Fatalf("start calls = %d, want 1", len(calls))
	}
	var request api.AllocationRequest
	if err := json.Unmarshal(calls[0].body, &request); err != nil {
		t.Fatal(err)
	}
	defer clear(calls[0].body)
	if len(request.Secrets) != 1 || string(request.Secrets[0].Value) != "matching-value" {
		t.Fatalf("delivered secrets = %#v", request.Secrets)
	}
	clear(request.Secrets[0].Value)
}

func TestBackupContainsCiphertextOnlySecretRecords(t *testing.T) {
	ctx := context.Background()
	data := memoryStore{}
	store, err := secretstore.NewStore(data, "test", "key-1", bytes.Repeat([]byte{1}, 32))
	if err != nil {
		t.Fatal(err)
	}
	const plaintext = "backup-plaintext-sentinel"
	if _, err := store.Set(ctx, "default", "token", []byte(plaintext), nil); err != nil {
		t.Fatal(err)
	}
	raw := data["trellis/test/secrets/default/token"]
	backupState := &backupStore{snapshot: &state.DesiredSnapshot{
		Jobs: map[string][]byte{}, JobRevisions: map[string][]byte{},
		Secrets: map[string][]byte{"default/token": raw}, VolumeRegistrations: map[string][]byte{}, NetworkPortRegistrations: map[string][]byte{},
	}}
	backupState.data = memoryStore{}
	s := newBackupTestServer(t, backupState, DefaultClusterSettings())
	backup, err := s.Backup(ctx)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(backup)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte(plaintext)) || len(backup.Secrets) != 1 {
		t.Fatalf("backup exposed plaintext or omitted ciphertext: %s", encoded)
	}
}
