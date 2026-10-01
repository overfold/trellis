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
	group.APIAccess = &APIAccessSpec{Scope: APIAccessCluster, Access: APIAccessRead}
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
		{"port without network", func(j *JobSpec) {
			j.TaskGroups[0].Tasks[0].Networking = &TaskNetworkingSpec{Mode: TaskNetworkNone, Ports: []PortSpec{{Port: 8080}}}
		}},
		{"invalid networking", func(j *JobSpec) { j.TaskGroups[0].Tasks[0].Networking = &TaskNetworkingSpec{Mode: "bridge"} }},
		{"reserved health probe volume path", func(j *JobSpec) {
			j.TaskGroups[0].Tasks[0].Volumes = []VolumeSpec{{Name: "data", HostPath: "/srv/data", ContainerPath: "/run/trellis"}}
		}},
		{"invalid label", func(j *JobSpec) { j.TaskGroups[0].Labels = map[string]string{"123bad": "v"} }},
		{"invalid api scope", func(j *JobSpec) { j.TaskGroups[0].APIAccess = &APIAccessSpec{Scope: "other", Access: APIAccessRead} }},
		{"invalid api access", func(j *JobSpec) {
			j.TaskGroups[0].APIAccess = &APIAccessSpec{Scope: APIAccessCluster, Access: "admin"}
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

func TestValidateRejectsDuplicateHostPortsInGroup(t *testing.T) {
	for _, test := range []struct {
		name  string
		tasks []TaskSpec
		path  string
	}{
		{
			name:  "same task",
			tasks: []TaskSpec{{Name: "server", Image: "example/server:1", Networking: &TaskNetworkingSpec{Mode: TaskNetworkHost, Ports: []PortSpec{{Port: 8080}, {Port: 8080}}}}},
			path:  "task_groups[api].tasks[server].networking.ports[1].port",
		},
		{
			name: "different tasks",
			tasks: []TaskSpec{
				{Name: "server", Image: "example/server:1", Networking: &TaskNetworkingSpec{Mode: TaskNetworkHost, Ports: []PortSpec{{Port: 8080}}}},
				{Name: "sidecar", Image: "example/sidecar:1", Networking: &TaskNetworkingSpec{Mode: TaskNetworkHost, Ports: []PortSpec{{Port: 8080}}}},
			},
			path: "task_groups[api].tasks[sidecar].networking.ports[0].port",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			job := validJob()
			job.TaskGroups[0].Tasks = test.tasks
			issues, ok := Validate(job).(ValidationErrors)
			if !ok || len(issues) != 1 || issues[0].Path != test.path || issues[0].Code != "duplicate" {
				t.Fatalf("expected duplicate port at %s, got %#v", test.path, issues)
			}
		})
	}

	job := validJob()
	job.TaskGroups[0].Tasks[0].Networking = &TaskNetworkingSpec{Mode: TaskNetworkHost, Ports: []PortSpec{{Port: 8080}}}
	otherGroup := job.TaskGroups[0]
	otherGroup.Name = "worker"
	job.TaskGroups = append(job.TaskGroups, otherGroup)
	if err := Validate(job); err != nil {
		t.Fatalf("same port in separate groups rejected: %v", err)
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
	raw := []byte("namespace: default\nname: web\ntask_groups:\n  - name: api\n    count: 1\n    restart:\n      max_restarts: 5\n      window: 2m\n    tasks:\n      - name: server\n        image: example/server:1\n        health_check:\n          type: tcp\n          port: 8080\n          interval: 15s\n          timeout: 3s\n          threshold: 2\n")
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
