package agent

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/spec"
	"github.com/labstack/echo/v5"
)

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
