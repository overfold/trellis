package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/internal/auth"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	"github.com/overfold/trellis/orchestrator/internal/spec"
)

func TestJobJSONRejectsSchemaProhibitedShapes(t *testing.T) {
	for _, field := range []string{
		`"Name":"web"`, `"NAME":"web"`, `"TaskGroups":[]`,
		`"task_groups":[{"name":"api","count":1,"tasks":[{"name":"app","image":"app","env":{"TOKEN":null}}]}]`,
		`"task_groups":[{"name":"api","count":1,"tasks":[{"name":"app","image":"app","Resources":{"cpu":100,"memory":128}}]}]`,
		`"task_groups":[{"name":"api","count":1,"tasks":[{"name":"app","image":"app","resources":null}]}]`,
		`"task_groups":[{"name":"api","count":1,"constraints":null,"tasks":[{"name":"app","image":"app"}]}]`,
		`"namespace":null`,
	} {
		var job spec.JobSpec
		if err := decodeJobSpec(json.RawMessage(`{`+field+`}`), &job); err == nil {
			t.Fatalf("schema-prohibited job shape accepted: %s", field)
		}
		for _, path := range []string{"/v1/namespaces/team/jobs/plan", "/v1/namespaces/team/jobs"} {
			e := echo.New()
			NewHandler(&Server{jobs: map[string]*Job{}}).Register(e)
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, scopedRequest(t, http.MethodPost, path, `{"spec":{`+field+`}}`, auth.AccessCluster, auth.AccessWrite))
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("POST %s accepted prohibited shape %s: %d %s", path, field, rec.Code, rec.Body.String())
			}
		}
	}
	for _, raw := range []string{`{"Spec":{}}`, `{"spec":{},"ResolvedImages":{}}`, `{"spec":{},"EXPECTED_VERSION":0}`} {
		var request api.JobRegistrationRequest
		if err := decodeRequest(t, "application/json", raw, maxJobRequestBytes, &request); err == nil {
			t.Fatalf("alternate envelope field accepted: %s", raw)
		}
	}
	// Empty strings and exact-case map keys remain literal values, not null.
	var job spec.JobSpec
	if err := decodeJobSpec(json.RawMessage(`{"name":"web","namespace":"default","task_groups":[{"name":"api","count":1,"tasks":[{"name":"app","image":"app","env":{"EMPTY":"","MixedCase":"literal"}}]}]}`), &job); err != nil {
		t.Fatal(err)
	}
	if job.TaskGroups[0].Tasks[0].Env["MixedCase"] != "literal" {
		t.Fatal("literal map key was changed")
	}
}

func decodeRequest(t *testing.T, contentType, body string, limit int64, dst any) error {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	return decodeJSON(echo.New().NewContext(req, httptest.NewRecorder()), dst, limit)
}

func TestJobRoutesRejectDuplicateKeysRecursively(t *testing.T) {
	e := echo.New()
	NewHandler(&Server{jobs: map[string]*Job{}}).Register(e)
	for _, raw := range []string{
		`{"spec":{},"spec":{}}`,
		`{"spec":{"name":"first","name":"last"}}`,
		`{"spec":{"name":"first","\u006eame":"last"}}`,
		`{"spec":{"task_groups":[{"tasks":[{"image":"first","image":"last"}]}]}}`,
		`{"spec":{"task_groups":[{"labels":{"tier":"first","tier":"last"}}]}}`,
		`{"spec":{"task_groups":[{"tasks":[{"env":{"TOKEN":"first","TOKEN":"last"}}]}]}}`,
		`{"spec":{},"resolved_images":{"app:1":"first","app:1":"last"}}`,
	} {
		for _, path := range []string{"/v1/namespaces/team/jobs/plan", "/v1/namespaces/team/jobs"} {
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, scopedRequest(t, http.MethodPost, path, raw, auth.AccessCluster, auth.AccessWrite))
			if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "duplicate JSON object key") {
				t.Fatalf("POST %s with %s = %d %s", path, raw, rec.Code, rec.Body.String())
			}
		}
	}
	// Separate objects may reuse keys; case-distinct map keys stay distinct.
	var job spec.JobSpec
	raw := json.RawMessage(`{"task_groups":[{"name":"a","tasks":[{"name":"x","env":{"TOKEN":"a","token":"b"}}]},{"name":"b","tasks":[{"name":"x"}]}]}`)
	if err := decodeJobSpec(raw, &job); err != nil || job.TaskGroups[0].Tasks[0].Env["token"] != "b" {
		t.Fatalf("distinct objects/keys rejected or changed: %v", err)
	}
	if err := decodeJobSpec(json.RawMessage(`{"task_groups":[{"labels":{"x":"a","x":"b"}}]}`), &job); err == nil {
		t.Fatal("standalone spec decoder accepted a nested duplicate")
	}
}

func TestDecodeBackupBeyondOrdinaryBodyLimit(t *testing.T) {
	body := strings.Repeat(" ", (64<<20)+1) + `{"format_version":6,"jobs":{"marker":{}}}`
	var backup api.BackupSnapshot
	if err := decodeRequest(t, "application/json", body, maxBackupRequestBytes, &backup); err != nil {
		t.Fatal(err)
	}
	if backup.FormatVersion != api.BackupFormatVersion || string(backup.Jobs["marker"]) != "{}" {
		t.Fatal("backup truncated")
	}
	if err := decodeRequest(t, "application/json", `{"jobs":{"marker":{"spec":{"name":"first","name":"last"}}}}`, maxBackupRequestBytes, &backup); err == nil || !strings.Contains(err.Error(), "duplicate JSON object key") {
		t.Fatalf("opaque backup record hid a nested duplicate: %v", err)
	}
}

func TestJobRoutesUnicodeLabelBoundary(t *testing.T) {
	for _, path := range []string{"/v1/namespaces/default/jobs/plan", "/v1/namespaces/default/jobs"} {
		for _, count := range []int{256, 257} {
			s, agent := newTestServerWithAgent()
			t.Cleanup(agent.server.Close)
			job := versionTestSpec("app:1", 1)
			job.TaskGroups[0].Labels = map[string]string{"title": strings.Repeat("😀", count)}
			raw, _ := json.Marshal(job)
			rec := httptest.NewRecorder()
			authenticatedHandler(s, auth.AccessWrite).ServeHTTP(rec, scopedRequest(t, http.MethodPost, path, `{"spec":`+string(raw)+`}`, auth.AccessCluster, auth.AccessWrite))
			want := http.StatusOK
			if path == "/v1/namespaces/default/jobs" {
				want = http.StatusAccepted
			}
			if count == 257 {
				want = http.StatusUnprocessableEntity
			}
			if rec.Code != want {
				t.Fatalf("POST %s label length %d = %d %s, want %d", path, count, rec.Code, rec.Body.String(), want)
			}
		}
	}
}

func TestDecodeJSONRejectsLooseRequests(t *testing.T) {
	tests := []struct {
		name        string
		contentType string
		body        string
		limit       int64
		want        int
		message     string
	}{
		{name: "unknown field", body: `{"scope":"cluster","access":"read","acess":"write"}`, want: http.StatusBadRequest, message: `unknown field "acess"`},
		{name: "duplicate field", body: `{"scope":"cluster","access":"read","access":"write"}`, want: http.StatusBadRequest, message: "duplicate JSON object key"},
		{name: "trailing value", body: `{"scope":"cluster","access":"read"}{}`, want: http.StatusBadRequest, message: "unexpected data after the JSON value"},
		{name: "trailing garbage", body: `{"scope":"cluster","access":"read"} x`, want: http.StatusBadRequest, message: "malformed JSON"},
		{name: "empty body", body: ``, want: http.StatusBadRequest, message: "request body is empty"},
		{name: "truncated", body: `{"scope":"cluster"`, want: http.StatusBadRequest, message: "truncated JSON"},
		{name: "wrong type", body: `{"scope":1}`, want: http.StatusBadRequest, message: "scope must be a string"},
		{name: "not JSON media type", contentType: "text/plain", body: `{}`, want: http.StatusUnsupportedMediaType},
		{name: "missing media type", contentType: "-", body: `{}`, want: http.StatusUnsupportedMediaType},
		{name: "too large", body: `{"namespace":"` + strings.Repeat("a", 64) + `"}`, limit: 32, want: http.StatusRequestEntityTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			contentType := tt.contentType
			switch contentType {
			case "":
				contentType = "application/json"
			case "-":
				contentType = ""
			}
			limit := tt.limit
			if limit == 0 {
				limit = maxSmallRequestBytes
			}
			var request api.CredentialCreateRequest
			err := decodeRequest(t, contentType, tt.body, limit, &request)
			var httpErr *echo.HTTPError
			ok := errors.As(err, &httpErr)
			if !ok || httpErr.Code != tt.want {
				t.Fatalf("error = %v, want HTTP %d", err, tt.want)
			}
			if !strings.Contains(httpErr.Message, tt.message) {
				t.Fatalf("message = %q, want it to contain %q", httpErr.Message, tt.message)
			}
		})
	}
}

func TestDecodeJSONAcceptsOneValueWithWhitespace(t *testing.T) {
	var request api.CredentialCreateRequest
	if err := decodeRequest(t, "application/json; charset=utf-8", " {\"scope\":\"cluster\",\"access\":\"read\"}\n\n", maxSmallRequestBytes, &request); err != nil {
		t.Fatal(err)
	}
	if request.Scope != "cluster" {
		t.Fatalf("request = %#v", request)
	}
}

func TestCredentialRequestRejectsNamespaceMetadata(t *testing.T) {
	var request api.CredentialCreateRequest
	err := decodeRequest(t, "application/json", `{"scope":"cluster","access":"read","namespace":"team"}`, maxSmallRequestBytes, &request)
	if err == nil || !strings.Contains(err.Error(), `unknown field "namespace"`) {
		t.Fatalf("error = %v, want unknown namespace field", err)
	}
}

func TestDecodeJSONErrorsDoNotEchoSecretValues(t *testing.T) {
	const sentinel = "c2VjcmV0LXNlbnRpbmVs"
	for _, body := range []string{
		`{"value_base64":"` + sentinel + `","extra":"` + sentinel + `"}`,
		`{"value_base64":"` + sentinel + `"} "` + sentinel + `"`,
		`{"value_base64":"` + sentinel + `","expected_version":"` + sentinel + `"}`,
		`{"value_base64":"` + sentinel,
		`{"value_base64":"` + sentinel + `","value_base64":"second"}`,
		`{"` + sentinel + `":"first","` + sentinel + `":"second"}`,
	} {
		var request api.SecretWriteRequest
		err := decodeRequest(t, "application/json", body, maxSecretRequestBytes, &request)
		if err == nil {
			t.Fatalf("body %q was accepted", body)
		}
		if strings.Contains(err.Error(), sentinel) {
			t.Fatalf("decode error echoed the secret value: %v", err)
		}
	}
}

func TestJobRoutesRejectUnknownFields(t *testing.T) {
	e := echo.New()
	NewHandler(&Server{jobs: map[string]*Job{}}).Register(e)
	for _, body := range []string{
		`{"spec":{"name":"demo","namespace":"team","task_groups":[{"name":"web","count":1,"tasks":[{"name":"app","image":"example.invalid/app:1","imgae":"typo"}]}]}}`,
		`{"spec":{"name":"demo","namespace":"team","task_groups":[{"name":"web","count":1,"tasks":[{"name":"app","image":"example.invalid/app:1"}]}]},"expected_verison":1}`,
	} {
		for _, path := range []string{"/v1/namespaces/team/jobs/plan", "/v1/namespaces/team/jobs"} {
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, scopedRequest(t, http.MethodPost, path, body, auth.AccessCluster, auth.AccessWrite))
			if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "unknown field") {
				t.Fatalf("POST %s status = %d, want 400 unknown field; body: %s", path, rec.Code, rec.Body.String())
			}
		}
	}
}

// TestFirstPartyRequestsDecodeStrictly checks that the request bodies
// first-party clients marshal from shared API types survive strict decoding.
func TestFirstPartyRequestsDecodeStrictly(t *testing.T) {
	cpu, memory := 0.5, int64(1<<20)
	now := time.Now().UTC()
	expected := 3
	requests := []struct {
		name string
		body any
		dst  any
	}{
		{name: "heartbeat", body: nodeapi.HeartbeatRequest{NodeID: uuid.New(), Timestamp: now, CPUUsage: &cpu, MemoryUsed: &memory, MetricsAt: &now, Allocations: []nodeapi.AllocationStatus{{ID: "a", Generation: 1, Task: "web"}}}, dst: &nodeapi.HeartbeatRequest{}},
		{name: "registration", body: nodeapi.NodeRegistrationRequest{ID: uuid.New(), Host: "node", Port: 8127, Labels: map[string]string{"zone": "a"}}, dst: &nodeapi.NodeRegistrationRequest{}},
		{name: "enrollment", body: nodeapi.NodeEnrollmentRequest{ServerAdvertise: "a:8128", AgentAdvertise: "a:8127", RaftAdvertise: "a:8129"}, dst: &nodeapi.NodeEnrollmentRequest{}},
		{name: "raft join", body: nodeapi.RaftJoinRequest{ServerAddress: "a:8128", RaftAddress: "a:8129"}, dst: &nodeapi.RaftJoinRequest{}},
		{name: "credential", body: api.CredentialCreateRequest{Scope: "cluster", Access: "read"}, dst: &api.CredentialCreateRequest{}},
		{name: "job limits", body: JobLimitsAPI(spec.DefaultLimits()), dst: &api.JobLimits{}},
		{name: "backup", body: api.BackupSnapshot{}, dst: &api.BackupSnapshot{}},
	}

	root := filepath.Join("..", "..", "..", "examples")
	if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || filepath.Ext(path) != ".yaml" {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		job, err := spec.ParseYAML(raw)
		if err != nil {
			return err
		}
		if err := spec.Canonicalize(job, spec.DefaultLimits()); err != nil {
			return err
		}
		rawSpec, err := json.Marshal(job)
		if err != nil {
			return err
		}
		requests = append(requests, struct {
			name string
			body any
			dst  any
		}{name: path, body: api.JobRegistrationRequest{Spec: rawSpec, ExpectedVersion: &expected, ExpectedIncarnation: "incarnation"}, dst: &api.JobRegistrationRequest{}}, struct {
			name string
			body any
			dst  any
		}{name: path + " spec", body: job, dst: &spec.JobSpec{}})
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	for _, request := range requests {
		t.Run(request.name, func(t *testing.T) {
			body, err := json.Marshal(request.body)
			if err != nil {
				t.Fatal(err)
			}
			if err := decodeRequest(t, "application/json", string(body), maxBackupRequestBytes, request.dst); err != nil {
				t.Fatalf("strict decode of %s: %v", bytes.TrimSpace(body), err)
			}
		})
	}
}
