package health

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/overfold/trellis/orchestrator/internal/spec"
)

func TestHealthWorkerLoggerIncludesTaskIdentity(t *testing.T) {
	var output bytes.Buffer
	h := NewHealthManager(slog.New(slog.NewJSONHandler(&output, nil)), &probeRuntime{}, nil)
	// A long interval keeps the worker idle; exercise the same logger used by
	// both failure paths without races or timing-dependent probe callbacks.
	h.SetContext(context.Background())
	h.RegisterTask("task-id", "container-id", &spec.HealthCheckSpec{Type: "script", Interval: time.Hour, Threshold: 2},
		"namespace", "platform", "job", "ingress", "allocation", "allocation-id", "task", "route-sync")
	defer h.DeregisterTask("task-id")
	h.tasks["task-id"].log.Error("health check failed", "error", context.DeadlineExceeded)
	for _, want := range []string{`"namespace":"platform"`, `"job":"ingress"`, `"allocation":"allocation-id"`, `"task":"route-sync"`, `"container":"container-id"`, `"error":"context deadline exceeded"`} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("log missing %s: %s", want, output.String())
		}
	}
}
