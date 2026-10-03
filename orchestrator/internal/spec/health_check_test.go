package spec

import (
	"errors"
	"strings"
	"testing"

	"github.com/overfold/trellis/orchestrator/internal/probepath"
)

func healthCheckJob(check *HealthCheckSpec) *JobSpec {
	job := validJob()
	job.TaskGroups[0].Tasks[0].HealthCheck = check
	return job
}

// The accepted path syntax is covered by internal/probepath; these tests
// cover how validation applies it.
func TestValidateHTTPHealthCheckPath(t *testing.T) {
	for _, path := range []string{"", "/health?ready=1"} {
		if err := Validate(healthCheckJob(&HealthCheckSpec{Type: HealthCheckHTTP, Port: 8080, Path: path})); err != nil {
			t.Fatalf("valid path %q rejected: %v", path, err)
		}
	}
	for _, path := range []string{"@169.254.169.254/latest", "/health check"} {
		err := Validate(healthCheckJob(&HealthCheckSpec{Type: HealthCheckHTTP, Port: 8080, Path: path}))
		var issues ValidationErrors
		if !errors.As(err, &issues) || len(issues) != 1 {
			t.Fatalf("path %q: expected one validation issue, got %T: %v", path, err, err)
		}
		if want := "task_groups[api].tasks[server].health_check.path"; issues[0].Path != want || issues[0].Code != "invalid" {
			t.Fatalf("path %q: issue = %+v, want invalid %s", path, issues[0], want)
		}
	}
}

func TestValidateReportsLongHTTPHealthCheckPath(t *testing.T) {
	path := "/" + strings.Repeat("a", probepath.MaxLength)
	var issues ValidationErrors
	err := Validate(healthCheckJob(&HealthCheckSpec{Type: HealthCheckHTTP, Port: 8080, Path: path}))
	if !errors.As(err, &issues) || len(issues) != 1 || issues[0].Code != "too_long" {
		t.Fatalf("issues = %+v, want one too_long issue", issues)
	}
}

func TestValidateIgnoresPathOnNonHTTPHealthChecks(t *testing.T) {
	for _, check := range []*HealthCheckSpec{
		{Type: HealthCheckTCP, Port: 8080, Path: "not a path"},
		{Type: HealthCheckScript, Command: []string{"/bin/true"}, Path: "not a path"},
	} {
		if err := Validate(healthCheckJob(check)); err != nil {
			t.Fatalf("%s check with ignored path rejected: %v", check.Type, err)
		}
	}
}
