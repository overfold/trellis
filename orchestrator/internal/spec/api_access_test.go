package spec

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestAPIAccessReservedEnvironment(t *testing.T) {
	for _, name := range []string{"TRELLIS_TOKEN", "TRELLIS_ADDR", "TRELLIS_NAMESPACE", "TRELLIS_CA_CERT"} {
		for _, secret := range []bool{false, true} {
			t.Run(name+"/"+fmt.Sprint(secret), func(t *testing.T) {
				job := validJob()
				group := &job.TaskGroups[0]
				task := &group.Tasks[0]
				task.APIAccess = &APIAccessSpec{Role: APIAccessObserver}
				if secret {
					task.Secrets = []SecretRefSpec{{Name: "credential", Target: SecretTargetEnv, Env: name}}
				} else {
					task.Env = map[string]string{name: "authored-value"}
				}
				for _, check := range []func(*JobSpec) error{Validate, func(j *JobSpec) error { return Canonicalize(j, DefaultLimits()) }} {
					err := check(job)
					var issues ValidationErrors
					if !errors.As(err, &issues) {
						t.Fatalf("expected validation errors, got %v", err)
					}
					found := false
					for _, issue := range issues {
						found = found || issue.Code == "reserved" && strings.Contains(issue.Path, ".env")
					}
					if !found {
						t.Fatalf("missing reserved-name issue: %v", err)
					}
				}
				task.APIAccess = nil
				if err := Canonicalize(job, DefaultLimits()); err != nil {
					t.Fatalf("without api_access: %v", err)
				}
			})
		}
	}
}

func TestAPIAccessValidationIsTaskLocal(t *testing.T) {
	for _, name := range []string{"TRELLIS_TOKEN", "TRELLIS_ADDR", "TRELLIS_NAMESPACE", "TRELLIS_CA_CERT"} {
		job := validJob()
		job.TaskGroups[0].Tasks[0].APIAccess = &APIAccessSpec{Role: APIAccessOperator}
		job.TaskGroups[0].Tasks = append(job.TaskGroups[0].Tasks, TaskSpec{
			Name: "isolated", Image: "example/isolated:1",
			Networking: &TaskNetworkingSpec{Mode: TaskNetworkNone},
			Env:        map[string]string{name: "authored-value"},
			Secrets:    []SecretRefSpec{{Name: "credential", Target: SecretTargetEnv, Env: "TRELLIS_OTHER"}},
		})
		if err := Canonicalize(job, DefaultLimits()); err != nil {
			t.Fatalf("disabled sibling with %s rejected: %v", name, err)
		}
		job.TaskGroups[0].Tasks[1].Env = nil
		job.TaskGroups[0].Tasks[1].Secrets[0].Env = name
		if err := ValidateCanonical(job); err != nil {
			t.Fatalf("disabled sibling secret %s rejected: %v", name, err)
		}
	}
}

func TestParseRejectsOldTaskGroupFieldsAndAPIAccessScope(t *testing.T) {
	for _, field := range []string{
		"    runtime: runsc\n",
		"    api_access:\n      role: observer\n",
	} {
		raw := "namespace: default\nname: web\ntask_groups:\n  - name: api\n    count: 1\n" + field + "    tasks:\n      - name: server\n        image: example/server:1\n"
		if _, err := ParseYAML([]byte(raw)); err == nil {
			t.Fatalf("obsolete group field accepted: %s", field)
		}
	}
	for _, scope := range []string{"cluster", "namespace"} {
		raw := "namespace: default\nname: web\ntask_groups:\n  - name: api\n    count: 1\n    tasks:\n      - name: server\n        image: example/server:1\n        api_access:\n          role: observer\n          scope: " + scope + "\n"
		if _, err := ParseYAML([]byte(raw)); err == nil {
			t.Fatalf("obsolete scope %s accepted", scope)
		}
	}
}

func TestParseAPIAccess(t *testing.T) {
	raw := []byte("namespace: default\nname: api-client\ntask_groups:\n  - name: client\n    count: 1\n    tasks:\n      - name: client\n        image: example/client:1\n        api_access:\n          role: observer\n        networking:\n          mode: host\n")
	job, err := ParseYAML(raw)
	if err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	got := job.TaskGroups[0].Tasks[0].APIAccess
	if got == nil || got.Role != APIAccessObserver {
		t.Fatalf("api_access = %#v, want read", got)
	}
	if err := Validate(job); err != nil {
		t.Fatalf("validate manifest: %v", err)
	}
}

func TestValidateRejectsInvalidAPIAccess(t *testing.T) {
	// Administrator authority is never injected, and the former read/write
	// levels are not roles.
	for _, role := range []APIAccessRole{"", "administrator", "read", "write"} {
		job := validJob()
		job.TaskGroups[0].Tasks[0].APIAccess = &APIAccessSpec{Role: role}
		var issues ValidationErrors
		if err := Validate(job); !errors.As(err, &issues) || len(issues) != 1 || issues[0].Path != "task_groups[api].tasks[server].api_access.role" {
			t.Fatalf("role %q: expected task role validation issue, got %v", role, err)
		}
	}
}

func TestValidateRejectsAPIAccessWithoutRoutableNetworking(t *testing.T) {
	job := &JobSpec{Namespace: "default", Name: "api-client", TaskGroups: []TaskGroupSpec{{
		Name: "client", Count: 1,
		Tasks: []TaskSpec{{Name: "client", Image: "example/client:1", APIAccess: &APIAccessSpec{Role: APIAccessObserver}, Networking: &TaskNetworkingSpec{Mode: TaskNetworkNone}}},
	}}}
	if err := Validate(job); err == nil {
		t.Fatal("expected api_access without host or namespace networking to be rejected")
	}
	// Omitted networking resolves to namespace mode, which can reach the API.
	job.TaskGroups[0].Tasks[0].Networking = nil
	if err := Validate(job); err != nil {
		t.Fatalf("api_access with default networking rejected: %v", err)
	}
}
