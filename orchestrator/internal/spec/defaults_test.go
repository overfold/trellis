package spec

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestCanonicalizeMakesEveryDefaultExplicit(t *testing.T) {
	job := validJob()
	task := &job.TaskGroups[0].Tasks[0]
	task.HealthCheck = &HealthCheckSpec{Type: HealthCheckHTTP, Port: 8080}
	task.Secrets = []SecretRefSpec{{Name: "key", Target: SecretTargetFile, Path: "/run/trellis-secrets/key"}, {Name: "env", Target: SecretTargetEnv, Env: "KEY"}}
	if err := Canonicalize(job, DefaultLimits()); err != nil {
		t.Fatalf("canonicalize: %v", err)
	}
	group := job.TaskGroups[0]
	if group.Runtime != DefaultRuntime {
		t.Fatalf("runtime = %q, want %q", group.Runtime, DefaultRuntime)
	}
	if group.Restart == nil || group.Restart.MaxRestarts != DefaultMaxRestarts || group.Restart.Window != DefaultRestartWindow {
		t.Fatalf("restart = %#v, want defaults", group.Restart)
	}
	if group.Update == nil || group.Update.Strategy != DefaultUpdateStrategy || group.Update.MaxParallel != DefaultMaxParallel {
		t.Fatalf("update = %#v, want defaults", group.Update)
	}
	if task.Networking == nil || task.Networking.Mode != DefaultTaskNetworkMode {
		t.Fatalf("networking = %#v, want namespace", task.Networking)
	}
	check := task.HealthCheck
	if check.Interval != DefaultHealthCheckInterval || check.Timeout != DefaultHealthCheckTimeout || check.Threshold != DefaultHealthCheckThreshold || check.Path != DefaultHealthCheckPath {
		t.Fatalf("health check = %#v, want defaults", check)
	}
	if task.Secrets[0].Mode != DefaultSecretFileMode || task.Secrets[1].Mode != 0 {
		t.Fatalf("secret modes = %#o/%#o, want file default and no env mode", task.Secrets[0].Mode, task.Secrets[1].Mode)
	}
	if err := ValidateCanonical(job); err != nil {
		t.Fatalf("canonical job is not canonical: %v", err)
	}
}

func TestCanonicalizeKeepsExplicitValuesAndIsIdempotent(t *testing.T) {
	job := validJob()
	job.TaskGroups[0].Runtime = RuntimeRunsc
	job.TaskGroups[0].Update = &UpdateSpec{Strategy: UpdateRolling, MaxParallel: 3}
	job.TaskGroups[0].Restart = &RestartPolicySpec{MaxRestarts: 0, Window: 1}
	job.TaskGroups[0].Tasks[0].Networking = &TaskNetworkingSpec{Mode: TaskNetworkHost, Ports: []PortSpec{{Port: 80}}}
	if err := Canonicalize(job, DefaultLimits()); err != nil {
		t.Fatal(err)
	}
	group := job.TaskGroups[0]
	if group.Runtime != RuntimeRunsc || group.Update.Strategy != UpdateRolling || group.Update.MaxParallel != 3 || group.Restart.MaxRestarts != 0 || group.Tasks[0].Networking.Mode != TaskNetworkHost {
		t.Fatalf("explicit values changed: %#v", group)
	}
	first, _ := json.Marshal(job)
	hash := TaskGroupContentHash(&job.TaskGroups[0])
	if err := Canonicalize(job, DefaultLimits()); err != nil {
		t.Fatal(err)
	}
	second, _ := json.Marshal(job)
	if string(first) != string(second) || TaskGroupContentHash(&job.TaskGroups[0]) != hash {
		t.Fatalf("canonicalization is not idempotent:\n%s\n%s", first, second)
	}
}

func TestCanonicalDefaultsDoNotDependOnLaterLimits(t *testing.T) {
	job := validJob()
	if err := Canonicalize(job, DefaultLimits()); err != nil {
		t.Fatal(err)
	}
	hash := TaskGroupContentHash(&job.TaskGroups[0])
	changed := DefaultLimits()
	changed.DefaultTaskCPU = 250
	if err := Canonicalize(job, changed); err != nil {
		t.Fatal(err)
	}
	if job.TaskGroups[0].Tasks[0].Resources.CPU != DefaultLimits().DefaultTaskCPU || TaskGroupContentHash(&job.TaskGroups[0]) != hash {
		t.Fatal("a later default change altered a canonical job")
	}
}

func TestValidateWithLimitsRejectsNonCanonicalJob(t *testing.T) {
	job := validJob()
	err := ValidateWithLimits(job, DefaultLimits())
	var issues ValidationErrors
	if !errors.As(err, &issues) {
		t.Fatalf("error = %v, want validation errors", err)
	}
	want := map[string]bool{
		"task_groups[api].runtime":                       false,
		"task_groups[api].restart":                       false,
		"task_groups[api].update":                        false,
		"task_groups[api].tasks[server].resources":       false,
		"task_groups[api].tasks[server].networking.mode": false,
	}
	for _, issue := range issues {
		if _, ok := want[issue.Path]; ok && issue.Code == "not_canonical" {
			want[issue.Path] = true
		}
	}
	for path, found := range want {
		if !found {
			t.Errorf("missing not_canonical issue for %s in %v", path, issues)
		}
	}
}
