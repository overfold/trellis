package spec

import "testing"

func TestValidateNamespaceNetworkAndTaskRuntime(t *testing.T) {
	job := &JobSpec{
		Namespace: "acme",
		Name:      "web",
		TaskGroups: []TaskGroupSpec{{
			Name: "web", Count: 2,
			Tasks: []TaskSpec{{Name: "app", Image: "image", Runtime: RuntimeRunsc, Resources: &ResourcesSpec{CPU: 500, Memory: 512}, Networking: &TaskNetworkingSpec{Mode: TaskNetworkWireGuard}}},
		}},
	}
	if err := Validate(job); err != nil {
		t.Fatalf("expected valid namespaced job with runsc: %v", err)
	}
	job.TaskGroups[0].Tasks[0].Runtime = ""
	if err := Validate(job); err != nil {
		t.Fatalf("expected valid namespaced job with default runtime: %v", err)
	}
	job.TaskGroups[0].Tasks[0].Runtime = "runc"
	if err := Validate(job); err != nil {
		t.Fatalf("expected valid namespaced job with runc: %v", err)
	}
	job.TaskGroups[0].Tasks[0].Runtime = "other"
	if err := Validate(job); err == nil {
		t.Fatal("expected unsupported task runtime to fail")
	}
}

func TestValidateRequiresSafeNamespace(t *testing.T) {
	job := &JobSpec{Namespace: "../other", Name: "web", TaskGroups: []TaskGroupSpec{{Name: "g", Count: 1, Tasks: []TaskSpec{{Name: "t", Image: "image"}}}}}
	if err := Validate(job); err == nil {
		t.Fatal("expected unsafe namespace to fail")
	}
}
