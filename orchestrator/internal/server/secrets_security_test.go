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
	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/internal/auth"
	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	secretstore "github.com/overfold/trellis/orchestrator/internal/secrets"
	"github.com/overfold/trellis/orchestrator/internal/spec"
	"github.com/overfold/trellis/orchestrator/internal/state"
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

func TestSecretIdentifierMethodParity(t *testing.T) {
	e, _ := secretHandler(t)
	for _, method := range []string{http.MethodPut, http.MethodGet, http.MethodDelete} {
		for _, name := range []string{"bad%20name", "bad%2Bname", "%3F", "-bad"} {
			req := authorizedRequest(t, method, "/v1/namespaces/default/secrets/"+name, `{"value_base64":"eA=="}`, auth.RoleOperator)
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest || strings.TrimSpace(rec.Body.String()) != `{"message":"invalid secret name"}` {
				t.Errorf("%s %s: %d %s", method, name, rec.Code, rec.Body.String())
			}
		}
	}
	for _, method := range []string{http.MethodGet, http.MethodDelete} {
		req := authorizedRequest(t, method, "/v1/namespaces/default/secrets/valid-missing", "", auth.RoleOperator)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound || strings.TrimSpace(rec.Body.String()) != `{"message":"secret not found"}` {
			t.Errorf("%s missing: %d %s", method, rec.Code, rec.Body.String())
		}
	}
}

func TestSecretMetadataEndpointsNeverReturnPlaintext(t *testing.T) {
	e, store := secretHandler(t)
	const plaintext = "metadata-sentinel-plaintext"
	if _, err := store.Set(context.Background(), "default", "token", []byte(plaintext), nil); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/v1/namespaces/default/secrets", "/v1/namespaces/default/secrets/token"} {
		req := authorizedRequest(t, http.MethodGet, path, "", auth.RoleObserver)
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
		req := authorizedRequest(t, http.MethodPut, "/v1/namespaces/default/secrets/"+name, body, auth.RoleOperator)
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

func TestInvalidSecretWritesPreserveExistingValue(t *testing.T) {
	e, store := secretHandler(t)
	path := "/v1/namespaces/default/secrets/token"
	write := func(body string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, authorizedRequest(t, http.MethodPut, path, body, auth.RoleOperator))
		return rec
	}
	initial := write(`{"value_base64":"eA=="}`)
	if initial.Code != http.StatusOK {
		t.Fatalf("one-byte write: %d %s", initial.Code, initial.Body.String())
	}
	var before api.SecretMetadata
	if err := json.Unmarshal(initial.Body.Bytes(), &before); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		body   string
		status int
	}{
		{`{}`, http.StatusBadRequest},
		{`{"value_base64":""}`, http.StatusBadRequest},
		{`{"value_base64":null}`, http.StatusBadRequest},
		{`{"value_base64":"\r\n"}`, http.StatusBadRequest},
		{`{"value_base64":"!"}`, http.StatusBadRequest},
		{fmt.Sprintf(`{"value_base64":%q}`, base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{'z'}, secretstore.MaxValueSize+1))), http.StatusRequestEntityTooLarge},
	} {
		rec := write(tc.body)
		if rec.Code != tc.status {
			t.Fatalf("invalid write: %d %s, want %d", rec.Code, rec.Body.String(), tc.status)
		}
		if tc.status == http.StatusBadRequest && !strings.Contains(rec.Body.String(), "value_base64") {
			t.Fatalf("missing validation context: %s", rec.Body.String())
		}
		meta := httptest.NewRecorder()
		e.ServeHTTP(meta, authorizedRequest(t, http.MethodGet, path, "", auth.RoleObserver))
		var after api.SecretMetadata
		if meta.Code != http.StatusOK || json.Unmarshal(meta.Body.Bytes(), &after) != nil || after != before {
			t.Fatalf("invalid write changed metadata: %d %s; before %s", meta.Code, meta.Body.String(), initial.Body.String())
		}
		value, _, err := store.Resolve(t.Context(), "default", "token")
		if err != nil || string(value) != "x" {
			t.Fatalf("invalid write changed value: %v", err)
		}
		clear(value)
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
	s.nodes[node.ID] = node
	task := spec.TaskSpec{Name: "app", Image: "app", Secrets: []spec.SecretRefSpec{{Name: "token", Target: spec.SecretTargetEnv, Env: "TOKEN"}}}
	s.jobs[jobKey("default", "web")] = &Job{Spec: canonicalTestSpec(&spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "app", Tasks: []spec.TaskSpec{task}}}}), Revision: 1}
	allocation := &Allocation{ID: "allocation", Namespace: "default", JobName: "web", TaskGroupName: "app", Tasks: []spec.TaskSpec{task}, Node: node, Generation: 1, JobRevision: 1, Phase: lifecycle.PhasePlaced}
	s.allocations = []*Allocation{allocation}
	if err := s.Execute(context.Background(), &Action{Type: ActionStart, Allocation: allocation}); err != nil {
		t.Fatal(err)
	}
	doc, err := s.ExecutionIntent(t.Context(), node.ID, 0)
	if err != nil || doc == nil {
		t.Fatal(err)
	}
	request := committedExecutionRequest(t, s, node.ID, allocation.ID, allocation.Generation)
	if len(request.Secrets) != 0 {
		t.Fatal("secret plaintext persisted in execution authority")
	}
	credentials := fetchExecutionCredentials(t, s, node.ID, doc.Version, request)
	defer clear(credentials.Secrets[0].Value)
	if len(credentials.Secrets) != 1 || string(credentials.Secrets[0].Value) != "matching-value" {
		t.Fatal("credential pull selected another namespace")
	}

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
