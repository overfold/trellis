package server

import (
	"bytes"
	"encoding/json"
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

func decodeRequest(t *testing.T, contentType, body string, limit int64, dst any) error {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	return decodeJSON(echo.New().NewContext(req, httptest.NewRecorder()), dst, limit)
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
			httpErr, ok := err.(*echo.HTTPError)
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
	if err := decodeRequest(t, "application/json; charset=utf-8", " {\"scope\":\"namespace\",\"access\":\"read\",\"namespace\":\"team\"}\n\n", maxSmallRequestBytes, &request); err != nil {
		t.Fatal(err)
	}
	if request.Scope != "namespace" || request.Namespace != "team" {
		t.Fatalf("request = %#v", request)
	}
}

func TestDecodeJSONErrorsDoNotEchoSecretValues(t *testing.T) {
	const sentinel = "c2VjcmV0LXNlbnRpbmVs"
	for _, body := range []string{
		`{"value_base64":"` + sentinel + `","extra":"` + sentinel + `"}`,
		`{"value_base64":"` + sentinel + `"} "` + sentinel + `"`,
		`{"value_base64":"` + sentinel + `","expected_version":"` + sentinel + `"}`,
		`{"value_base64":"` + sentinel,
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
			e.ServeHTTP(rec, scopedRequest(t, http.MethodPost, path, body, auth.AccessNamespace, auth.AccessWrite, "team"))
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
		{name: "registration", body: nodeapi.NodeRegistrationRequest{ID: uuid.New(), Host: "node", Port: 8127, Labels: map[string]string{"zone": "a"}, Capabilities: []spec.NodeCapability{spec.CapabilityNamespaceNetworking}}, dst: &nodeapi.NodeRegistrationRequest{}},
		{name: "enrollment", body: nodeapi.NodeEnrollmentRequest{ServerAdvertise: "a:8128", AgentAdvertise: "a:8127", RaftAdvertise: "a:8129"}, dst: &nodeapi.NodeEnrollmentRequest{}},
		{name: "raft join", body: nodeapi.RaftJoinRequest{ServerAddress: "a:8128", RaftAddress: "a:8129"}, dst: &nodeapi.RaftJoinRequest{}},
		{name: "credential", body: api.CredentialCreateRequest{Scope: "namespace", Access: "read", Namespace: "team"}, dst: &api.CredentialCreateRequest{}},
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
