package spec

import (
	"errors"
	"strings"
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

func TestDiscoveryIdentityValidation(t *testing.T) {
	for _, field := range []string{"namespace", "job", "group"} {
		t.Run(field, func(t *testing.T) {
			job := validJob()
			switch field {
			case "namespace":
				job.Namespace = "team.prod"
			case "job":
				job.Name = "web.v1"
			case "group":
				job.TaskGroups[0].Name = "api.v1"
			}
			for _, validate := range []func(*JobSpec) error{Validate, func(job *JobSpec) error { return Canonicalize(job, DefaultLimits()) }, ValidateCanonical} {
				var issues ValidationErrors
				if err := validate(job); !errors.As(err, &issues) || len(issues) != 1 || issues[0].Code != "invalid_identifier" {
					t.Fatalf("expected one identifier error: %v", err)
				}
			}
		})
	}
	job := validJob()
	job.Name, job.Namespace, job.TaskGroups[0].Name = "Web-1", "Team_2", "API_3"
	task := &job.TaskGroups[0].Tasks[0]
	task.Name = "app.v1"
	task.Secrets = []SecretRefSpec{{Name: "db.password", Target: SecretTargetEnv, Env: "PASSWORD"}}
	task.Volumes = []VolumeSpec{{Name: "data.v1", HostPath: "@/data", ContainerPath: "/data"}}
	if err := Canonicalize(job, DefaultLimits()); err != nil {
		t.Fatal(err)
	}
	if job.Name != "Web-1" || job.Namespace != "Team_2" || job.TaskGroups[0].Name != "API_3" {
		t.Fatal("canonicalization changed identity case")
	}
	for _, value := range []string{"a", "A_1-", strings.Repeat("a", 63), strings.Repeat("a", 64), ".a", "a.b", "a.", "a..b", ""} {
		want := value == "a" || value == "A_1-" || value == strings.Repeat("a", 63)
		if ValidDiscoveryIdentifier(value) != want {
			t.Fatalf("ValidDiscoveryIdentifier(%q) != %v", value, want)
		}
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
		{"image URL", func(j *JobSpec) { j.TaskGroups[0].Tasks[0].Image = "https://registry.example/app:main" }},
		{"invalid image digest", func(j *JobSpec) { j.TaskGroups[0].Tasks[0].Image = "app@sha256:abc" }},
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
			var issues ValidationErrors
			err := Validate(job)
			if !errors.As(err, &issues) || len(issues) != 1 || issues[0].Path != test.path || issues[0].Code != "duplicate" {
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
	var issues ValidationErrors
	if !errors.As(err, &issues) {
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

func TestValidateVolumeAndEnvConflicts(t *testing.T) {
	secret := SecretRefSpec{Name: "key", Target: SecretTargetFile, Path: "/run/trellis-secrets/key"}
	vol := func(name, path string) VolumeSpec {
		return VolumeSpec{Name: name, HostPath: "/srv/" + name, ContainerPath: path}
	}
	tests := []struct {
		name   string
		mutate func(*TaskSpec)
	}{
		{"duplicate container path", func(t *TaskSpec) { t.Volumes = []VolumeSpec{vol("a", "/data"), vol("b", "/data")} }},
		{"volume over secret dir", func(t *TaskSpec) {
			t.Secrets = []SecretRefSpec{secret}
			t.Volumes = []VolumeSpec{vol("a", "/run/trellis-secrets")}
		}},
		{"volume under secret dir", func(t *TaskSpec) { t.Volumes = []VolumeSpec{vol("a", "/run/trellis-secrets/sub")} }},
		{"volume equals secret path", func(t *TaskSpec) {
			t.Secrets = []SecretRefSpec{secret}
			t.Volumes = []VolumeSpec{vol("a", "/run/trellis-secrets/key")}
		}},
		{"empty env key", func(t *TaskSpec) { t.Env = map[string]string{"": "x"} }},
		{"env key with space", func(t *TaskSpec) { t.Env = map[string]string{"BAD KEY": "x"} }},
		{"env key with equals", func(t *TaskSpec) { t.Env = map[string]string{"A=B": "x"} }},
		{"env key leading digit", func(t *TaskSpec) { t.Env = map[string]string{"1A": "x"} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			job := validJob()
			test.mutate(&job.TaskGroups[0].Tasks[0])
			if err := Validate(job); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
	job := validJob()
	job.TaskGroups[0].Tasks[0].Env = map[string]string{"GOOD_1": "x"}
	job.TaskGroups[0].Tasks[0].Volumes = []VolumeSpec{vol("a", "/data"), vol("b", "/cache")}
	if err := Validate(job); err != nil {
		t.Fatalf("valid job rejected: %v", err)
	}
}

func TestMinimumCPURequest(t *testing.T) {
	for _, cpu := range []int{1, 9, 10, 11} {
		job := validJob()
		job.TaskGroups[0].Tasks[0].Resources = &ResourcesSpec{CPU: cpu, Memory: 128 << 20}
		if err := Canonicalize(job, DefaultLimits()); (err == nil) != (cpu >= 10) {
			t.Fatalf("CPU=%d: error=%v", cpu, err)
		}
		limits := DefaultLimits()
		limits.DefaultTaskCPU = cpu
		if err := ValidateLimits(limits); (err == nil) != (cpu >= 10) {
			t.Fatalf("default CPU=%d: error=%v", cpu, err)
		}
		limits.DefaultTaskCPU, limits.MaxTaskCPU = 10, cpu
		if err := ValidateLimits(limits); (err == nil) != (cpu >= 10) {
			t.Fatalf("maximum CPU=%d: error=%v", cpu, err)
		}
	}
}
