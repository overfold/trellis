package spec

import "testing"

func TestParseAPIAccess(t *testing.T) {
	raw := []byte("namespace: default\nname: api-client\ntask_groups:\n  - name: client\n    count: 1\n    api_access:\n      scope: namespace\n      access: read\n    tasks:\n      - name: client\n        image: example/client:1\n        networking:\n          mode: host\n")
	job, err := ParseYAML(raw)
	if err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	got := job.TaskGroups[0].APIAccess
	if got == nil || got.Scope != APIAccessNamespace || got.Access != APIAccessRead {
		t.Fatalf("api_access = %#v, want namespace/read", got)
	}
	if err := Validate(job); err != nil {
		t.Fatalf("validate manifest: %v", err)
	}
}

func TestValidateRejectsInvalidAPIAccess(t *testing.T) {
	job := &JobSpec{Namespace: "default", Name: "api-client", TaskGroups: []TaskGroupSpec{{
		Name: "client", Count: 1,
		APIAccess: &APIAccessSpec{Scope: "other", Access: APIAccessRead},
		Tasks: []TaskSpec{{Name: "client", Image: "example/client:1", Networking: &TaskNetworkingSpec{Mode: TaskNetworkHost}}},
	}}}
	if err := Validate(job); err == nil {
		t.Fatal("expected invalid api_access scope to be rejected")
	}
}

func TestValidateRejectsAPIAccessWithoutRoutableNetworking(t *testing.T) {
	job := &JobSpec{Namespace: "default", Name: "api-client", TaskGroups: []TaskGroupSpec{{
		Name: "client", Count: 1,
		APIAccess: &APIAccessSpec{Scope: APIAccessNamespace, Access: APIAccessRead},
		Tasks: []TaskSpec{{Name: "client", Image: "example/client:1"}},
	}}}
	if err := Validate(job); err == nil {
		t.Fatal("expected api_access without host or namespace networking to be rejected")
	}
}
