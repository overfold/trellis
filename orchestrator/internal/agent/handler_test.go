package agent

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/spec"
	"github.com/labstack/echo/v5"
)

func TestMutationHandlersRequirePositiveFences(t *testing.T) {
	tests := []struct {
		name   string
		method string
		path   string
		body   string
		want   string
	}{
		{
			name:   "start epoch",
			method: http.MethodPost,
			path:   "/v1/allocations",
			body:   `{"allocation_id":"alloc","generation":1,"job_revision":1,"execution_hash":"hash","tasks":[{"name":"task","image":"example.invalid/task:1"}]}`,
			want:   ErrInvalidEpoch.Error(),
		},
		{
			name:   "stop epoch",
			method: http.MethodDelete,
			path:   "/v1/allocations/alloc",
			body:   `{"generation":1}`,
			want:   ErrInvalidEpoch.Error(),
		},
		{
			name:   "stop generation",
			method: http.MethodDelete,
			path:   "/v1/allocations/alloc",
			body:   `{"epoch":1}`,
			want:   ErrInvalidGeneration.Error(),
		},
		{
			name:   "drain epoch",
			method: http.MethodPost,
			path:   "/v1/allocations/alloc/drain",
			body:   `{"generation":1}`,
			want:   ErrInvalidEpoch.Error(),
		},
		{
			name:   "resume epoch",
			method: http.MethodDelete,
			path:   "/v1/allocations/alloc/drain",
			body:   `{"generation":1}`,
			want:   ErrInvalidEpoch.Error(),
		},
		{
			name:   "network plan epoch",
			method: http.MethodPost,
			path:   "/v1/network-plans",
			body:   `{"namespace":"default","plan":{}}`,
			want:   ErrInvalidEpoch.Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := echo.New()
			NewHandler(&Agent{}).Register(e)
			req := httptest.NewRequest(tt.method, tt.path, bytes.NewBufferString(tt.body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()

			e.ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), tt.want) {
				t.Fatalf("body = %q, want error %q", rec.Body.String(), tt.want)
			}
		})
	}
}

func TestHandleRunRequiresPositiveJobRevision(t *testing.T) {
	for _, test := range []struct {
		name     string
		revision int
	}{
		{name: "missing", revision: 0},
		{name: "negative", revision: -1},
	} {
		t.Run(test.name, func(t *testing.T) {
			agent := &Agent{allocations: map[string]*Allocation{
				"allocation-g1-task": {
					ID: "allocation-g1-task", AllocationID: "allocation", Generation: 1,
					JobRevision: 1, ExecutionHash: "hash", Status: "running",
				},
			}}
			e := echo.New()
			NewHandler(agent).Register(e)
			body, err := json.Marshal(api.AllocationRequest{
				AllocationID: "allocation", Generation: 1, JobRevision: test.revision, ExecutionHash: "hash",
				Tasks: []spec.TaskSpec{{Name: "task", Image: "image"}},
			})
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodPost, "/v1/allocations", bytes.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			recorder := httptest.NewRecorder()

			e.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
			}
			if !strings.Contains(recorder.Body.String(), "job_revision must be greater than zero") {
				t.Fatalf("body = %q, want revision validation error", recorder.Body.String())
			}
		})
	}
}

func TestOperationErrorReportsRestartExhaustion(t *testing.T) {
	var httpErr *echo.HTTPError
	if !errors.As(operationError(fmt.Errorf("%w: allocation x", ErrRestartBudgetExhausted)), &httpErr) {
		t.Fatal("operation error is not an HTTP error")
	}
	var response api.OperationResponse
	if err := json.Unmarshal([]byte(httpErr.Message), &response); err != nil {
		t.Fatal(err)
	}
	if httpErr.Code != http.StatusConflict || response.Code != api.OperationRestartExhausted {
		t.Fatalf("operation error = %d/%q, want %d/%q", httpErr.Code, response.Code, http.StatusConflict, api.OperationRestartExhausted)
	}
}
