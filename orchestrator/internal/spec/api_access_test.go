package spec

import "testing"

func TestParseAPIAccess(t *testing.T) {
	raw := []byte("namespace: default\nname: api-client\ntask_groups:\n  - name: client\n    count: 1\n    api_access:\n      scope: cluster\n      access: read\n    tasks:\n      - name: client\n        image: example/client:1\n        networking:\n          mode: host\n")
	job, err := ParseYAML(raw)
	if err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	got := job.TaskGroups[0].APIAccess
	if got == nil || got.Scope != APIAccessCluster || got.Access != APIAccessRead {
		t.Fatalf("api_access = %#v, want cluster/read", got)
	}
	if err := Validate(job); err != nil {
		t.Fatalf("validate manifest: %v", err)
	}
}

func TestValidateRejectsInvalidAPIAccess(t *testing.T) {
	job := &JobSpec{Namespace: "default", Name: "api-client", TaskGroups: []TaskGroupSpec{{
		Name: "client", Count: 1,
		APIAccess: &APIAccessSpec{Scope: "other", Access: APIAccessRead},
		Tasks:     []TaskSpec{{Name: "client", Image: "example/client:1", Networking: &TaskNetworkingSpec{Mode: TaskNetworkHost}}},
	}}}
	if err := Validate(job); err == nil {
		t.Fatal("expected invalid api_access scope to be rejected")
	}
}

func TestValidateRejectsNamespaceAPIAccess(t *testing.T) {
	job := &JobSpec{Namespace: "default", Name: "api-client", TaskGroups: []TaskGroupSpec{{
		Name: "client", Count: 1,
		APIAccess: &APIAccessSpec{Scope: "namespace", Access: APIAccessRead},
		Tasks:     []TaskSpec{{Name: "client", Image: "example/client:1", Networking: &TaskNetworkingSpec{Mode: TaskNetworkHost}}},
	}}}
	if err := Validate(job); err == nil {
		t.Fatal("expected namespace api_access scope to be rejected")
	}
}

func TestValidateRejectsAPIAccessWithoutRoutableNetworking(t *testing.T) {
	job := &JobSpec{Namespace: "default", Name: "api-client", TaskGroups: []TaskGroupSpec{{
		Name: "client", Count: 1,
		APIAccess: &APIAccessSpec{Scope: APIAccessCluster, Access: APIAccessRead},
		Tasks:     []TaskSpec{{Name: "client", Image: "example/client:1", Networking: &TaskNetworkingSpec{Mode: TaskNetworkNone}}},
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
