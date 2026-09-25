package spec

import (
	"testing"
	"time"
)

func validJob() *JobSpec {
	return &JobSpec{
		Namespace: "default",
		Name:      "web",
		TaskGroups: []TaskGroupSpec{{
			Name:  "api",
			Count: 1,
			Tasks: []TaskSpec{{Name: "server", Image: "example/server:1"}},
		}},
	}
}

func TestParseYAML(t *testing.T) {
	raw := []byte("namespace: default\nname: web\ntask_groups:\n  - name: api\n    count: 1\n    tasks:\n      - name: server\n        image: example/server:1\n        networking:\n          mode: host\n          ports:\n            - port: 8080\n")
	job, err := ParseYAML(raw)
	if err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	if err := Validate(job); err != nil {
		t.Fatalf("validate parsed manifest: %v", err)
	}
	if got := job.TaskGroups[0].Tasks[0].Networking.Ports[0].Port; got != 8080 {
		t.Fatalf("port = %d, want 8080", got)
	}
}

func TestParseYAMLRejectsUnknownFields(t *testing.T) {
	raw := []byte("namespace: default\nname: web\nunknown: true\ntask_groups:\n  - name: api\n    count: 1\n    tasks:\n      - name: server\n        image: example/server:1\n")
	if _, err := ParseYAML(raw); err == nil {
		t.Fatal("expected unknown field to be rejected")
	}
}

func TestValidateAcceptsExtensions(t *testing.T) {
	job := validJob()
	group := &job.TaskGroups[0]
	group.APIAccess = &APIAccessSpec{Scope: APIAccessNamespace, Access: APIAccessRead}
	group.Labels = map[string]string{"trellis.expose": "true", "trellis/domain": "example.com"}
	group.Constraints = []ConstraintSpec{{Attribute: "arch", Value: "amd64"}}
	group.Count = 2
	group.Update = &UpdateSpec{Strategy: UpdateRolling, MaxParallel: 1}
	group.Restart = &RestartPolicySpec{MaxRestarts: 3, Window: 5 * time.Minute}
	group.Tasks[0].Networking = &TaskNetworkingSpec{Mode: TaskNetworkHost, Ports: []PortSpec{{Port: 8080}}}
	group.Tasks[0].HealthCheck = &HealthCheckSpec{Type: HealthCheckHTTP, Port: 8080, Path: "/", Interval: 5 * time.Second, Timeout: 2 * time.Second, Threshold: 2}
	if err := Validate(job); err != nil {
		t.Fatalf("valid extended job rejected: %v", err)
	}
}

func TestValidateRejectsInvalidJobs(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*JobSpec)
	}{
		{"missing name", func(j *JobSpec) { j.Name = "" }},
		{"unsafe name", func(j *JobSpec) { j.Name = "bad name" }},
		{"missing namespace", func(j *JobSpec) { j.Namespace = "" }},
		{"zero replicas", func(j *JobSpec) { j.TaskGroups[0].Count = 0 }},
		{"missing image", func(j *JobSpec) { j.TaskGroups[0].Tasks[0].Image = "" }},
		{"invalid port", func(j *JobSpec) {
			j.TaskGroups[0].Tasks[0].Networking = &TaskNetworkingSpec{Mode: TaskNetworkHost, Ports: []PortSpec{{Port: 70000}}}
		}},
		{"port without host networking", func(j *JobSpec) {
			j.TaskGroups[0].Tasks[0].Networking = &TaskNetworkingSpec{Ports: []PortSpec{{Port: 8080}}}
		}},
		{"invalid networking", func(j *JobSpec) { j.TaskGroups[0].Tasks[0].Networking = &TaskNetworkingSpec{Mode: "bridge"} }},
		{"invalid label", func(j *JobSpec) { j.TaskGroups[0].Labels = map[string]string{"123bad": "v"} }},
		{"invalid api scope", func(j *JobSpec) { j.TaskGroups[0].APIAccess = &APIAccessSpec{Scope: "other", Access: APIAccessRead} }},
		{"invalid api access", func(j *JobSpec) {
			j.TaskGroups[0].APIAccess = &APIAccessSpec{Scope: APIAccessNamespace, Access: "admin"}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			job := validJob()
			test.mutate(job)
			if err := Validate(job); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestValidateNetworkHealthCheckNetworking(t *testing.T) {
	for _, mode := range []TaskNetworkMode{TaskNetworkHost, TaskNetworkWireGuard} {
		t.Run(string(mode), func(t *testing.T) {
			job := validJob()
			job.TaskGroups[0].Tasks[0].Networking = &TaskNetworkingSpec{Mode: mode}
			job.TaskGroups[0].Tasks[0].HealthCheck = &HealthCheckSpec{Type: HealthCheckHTTP, Port: 8080, Path: "/health"}
			if err := Validate(job); err != nil {
				t.Fatalf("network health check rejected for %q networking: %v", mode, err)
			}
		})
	}

	for _, networking := range []*TaskNetworkingSpec{nil, {Mode: TaskNetworkIsolated}} {
		name := "default"
		if networking != nil {
			name = "isolated"
		}
		t.Run(name, func(t *testing.T) {
			job := validJob()
			job.TaskGroups[0].Tasks[0].Networking = networking
			job.TaskGroups[0].Tasks[0].HealthCheck = &HealthCheckSpec{Type: HealthCheckTCP, Port: 8080}
			if err := Validate(job); err == nil {
				t.Fatal("expected isolated network health check to be rejected")
			}
		})
	}

	job := validJob()
	job.TaskGroups[0].Tasks[0].HealthCheck = &HealthCheckSpec{Type: HealthCheckScript, Command: []string{"/bin/check-ready"}}
	if err := Validate(job); err != nil {
		t.Fatalf("script health check rejected for isolated networking: %v", err)
	}
}

func TestValidateAggregatesErrors(t *testing.T) {
	job := &JobSpec{Namespace: "", Name: "bad name", TaskGroups: []TaskGroupSpec{{Name: "api", Count: 0}}}
	err := Validate(job)
	issues, ok := err.(ValidationErrors)
	if !ok {
		t.Fatalf("expected ValidationErrors, got %T: %v", err, err)
	}
	if len(issues) < 4 {
		t.Fatalf("expected multiple issues, got %#v", issues)
	}
}

func TestParseConfigurableHealthAndRestartPolicy(t *testing.T) {
	raw := []byte("namespace: default\nname: web\ntask_groups:\n  - name: api\n    count: 1\n    restart:\n      max_restarts: 5\n      window: 2m\n    tasks:\n      - name: server\n        image: example/server:1\n        networking:\n          mode: host\n        health_check:\n          type: tcp\n          port: 8080\n          interval: 15s\n          timeout: 3s\n          threshold: 2\n")
	job, err := ParseYAML(raw)
	if err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	if err := Validate(job); err != nil {
		t.Fatalf("validate manifest: %v", err)
	}
	if got := job.TaskGroups[0].Restart.Window; got != 2*time.Minute {
		t.Fatalf("restart window = %s", got)
	}
	check := job.TaskGroups[0].Tasks[0].HealthCheck
	if check.Interval != 15*time.Second || check.Timeout != 3*time.Second || check.Threshold != 2 {
		t.Fatalf("unexpected health config: %#v", check)
	}
}

func TestValidateConstraints(t *testing.T) {
	valid := validJob()
	valid.TaskGroups[0].Constraints = []ConstraintSpec{{Attribute: "os", Value: "linux"}, {Attribute: "example.com/accelerator", Value: "gpu"}}
	if err := Validate(valid); err != nil {
		t.Fatalf("valid constraints rejected: %v", err)
	}

	invalid := validJob()
	invalid.TaskGroups[0].Constraints = []ConstraintSpec{{Attribute: "arch", Value: "amd64"}, {Attribute: "arch", Value: "arm64"}}
	if err := Validate(invalid); err == nil {
		t.Fatal("expected duplicate constraint to be rejected")
	}
}
