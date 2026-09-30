package plan

import (
	"testing"

	"github.com/overfold/trellis/orchestrator/internal/spec"
)

func task(name string) spec.TaskSpec {
	return spec.TaskSpec{Name: name, Image: "example.invalid/" + name + ":1"}
}

func group(name string, tasks ...spec.TaskSpec) spec.TaskGroupSpec {
	return spec.TaskGroupSpec{Name: name, Count: 1, Tasks: tasks}
}

func TestDiffTreatsTaskOrderAsSemantic(t *testing.T) {
	before := &spec.JobSpec{
		Name:      "demo",
		Namespace: "default",
		TaskGroups: []spec.TaskGroupSpec{
			group("web", task("app"), task("sidecar")),
		},
	}
	after := &spec.JobSpec{
		Name:      "demo",
		Namespace: "default",
		TaskGroups: []spec.TaskGroupSpec{
			group("web", task("sidecar"), task("app")),
		},
	}

	changes := Diff(before, after)
	if len(changes) == 0 {
		t.Fatal("task reorder produced no semantic changes")
	}
	if result := Build(before, 6, 4, after); result.Action != "update" {
		t.Fatalf("plan action = %q, want update", result.Action)
	}
}

func TestDiffIgnoresTaskGroupOrder(t *testing.T) {
	before := &spec.JobSpec{
		Name:      "demo",
		Namespace: "default",
		TaskGroups: []spec.TaskGroupSpec{
			group("web", task("app")),
			group("worker", task("worker")),
		},
	}
	after := &spec.JobSpec{
		Name:      "demo",
		Namespace: "default",
		TaskGroups: []spec.TaskGroupSpec{
			group("worker", task("worker")),
			group("web", task("app")),
		},
	}

	if changes := Diff(before, after); len(changes) != 0 {
		t.Fatalf("task-group reorder produced changes: %#v", changes)
	}
	if result := Build(before, 6, 4, after); result.Action != "none" {
		t.Fatalf("plan action = %q, want none", result.Action)
	}
}
