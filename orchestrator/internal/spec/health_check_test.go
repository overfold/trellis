package spec

import (
	"strings"
	"testing"
)

func healthCheckJob(check *HealthCheckSpec) *JobSpec {
	job := validJob()
	job.TaskGroups[0].Tasks[0].HealthCheck = check
	return job
}

func TestValidateAcceptsHTTPHealthCheckPaths(t *testing.T) {
	for _, path := range []string{
		"",
		"/",
		"/health",
		"/health/ready?verbose=1&probe=trellis",
		"/v1/status%2Fready",
		"/@169.254.169.254/latest",
		"//health",
		"/" + strings.Repeat("a", MaxHealthCheckPathLength-1),
	} {
		t.Run(path, func(t *testing.T) {
			if err := Validate(healthCheckJob(&HealthCheckSpec{Type: HealthCheckHTTP, Port: 8080, Path: path})); err != nil {
				t.Fatalf("valid path rejected: %v", err)
			}
		})
	}
}

func TestValidateRejectsInvalidHTTPHealthCheckPaths(t *testing.T) {
	for _, path := range []string{
		"health",
		"@169.254.169.254/latest",
		"http://169.254.169.254/latest",
		"/health check",
		"/health\tcheck",
		"/health\r\nHost: evil",
		"/health\x00",
		"/health\x7f",
		"/héalth",
		"/health#fragment",
		"/health%zz",
		"/" + strings.Repeat("a", MaxHealthCheckPathLength),
	} {
		t.Run(path, func(t *testing.T) {
			err := Validate(healthCheckJob(&HealthCheckSpec{Type: HealthCheckHTTP, Port: 8080, Path: path}))
			issues, ok := err.(ValidationErrors)
			if !ok || len(issues) != 1 {
				t.Fatalf("expected one validation issue, got %T: %v", err, err)
			}
			if want := "task_groups[api].tasks[server].health_check.path"; issues[0].Path != want || issues[0].Code != "invalid" {
				t.Fatalf("issue = %+v, want invalid %s", issues[0], want)
			}
		})
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
