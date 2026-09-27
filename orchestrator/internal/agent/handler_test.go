package agent

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
			body:   `{"allocation_id":"alloc","generation":1,"execution_hash":"hash","tasks":[{"name":"task","image":"example.invalid/task:1"}]}`,
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
