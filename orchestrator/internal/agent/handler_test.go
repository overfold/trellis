package agent

import (
	"github.com/labstack/echo/v5"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAgentDoesNotExposePushExecution(t *testing.T) {
	e := echo.New()
	NewHandler(&Agent{}).Register(e)
	for _, test := range []struct{ method, path string }{
		{http.MethodPost, "/v1/allocations"},
		{http.MethodDelete, "/v1/allocations/a"},
		{http.MethodPost, "/v1/allocations/a/drain"},
		{http.MethodDelete, "/v1/allocations/a/drain"},
		{http.MethodPost, "/v1/network-plans"},
	} {
		recorder := httptest.NewRecorder()
		e.ServeHTTP(recorder, httptest.NewRequest(test.method, test.path, nil))
		if recorder.Code != http.StatusNotFound && recorder.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s %s: %d", test.method, test.path, recorder.Code)
		}
	}
}
