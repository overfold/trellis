package plan

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/overfold/trellis/orchestrator/api"
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
	if result := Build(before, Base{Incarnation: "i", Version: 6, Revision: 4}, after); result.Action != "update" {
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
	if result := Build(before, Base{Incarnation: "i", Version: 6, Revision: 4}, after); result.Action != "none" {
		t.Fatalf("plan action = %q, want none", result.Action)
	}
}

func TestDiffPreservesInt64ThroughWireRoundTrip(t *testing.T) {
	before := &spec.JobSpec{Name: "web", Namespace: "default", TaskGroups: []spec.TaskGroupSpec{group("web", task("app"))}}
	before.TaskGroups[0].Restart = &spec.RestartPolicySpec{Window: time.Duration(9007199254740992)}
	before.TaskGroups[0].Tasks[0].Resources = &spec.ResourcesSpec{CPU: 100, Memory: 9007199254740992}
	encoded, _ := json.Marshal(before)
	var after spec.JobSpec
	if err := json.Unmarshal(encoded, &after); err != nil {
		t.Fatal(err)
	}
	after.TaskGroups[0].Restart.Window++
	after.TaskGroups[0].Tasks[0].Resources.Memory++
	changes := Diff(before, &after)
	if len(changes) != 2 {
		t.Fatalf("one-unit int64 changes lost: %+v", changes)
	}
	raw, err := json.Marshal(changes)
	if err != nil {
		t.Fatal(err)
	}
	var decoded []api.JobPlanChange
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	for _, change := range decoded {
		if change.Before != json.Number("9007199254740992") || change.After != json.Number("9007199254740993") {
			t.Fatalf("wire change lost precision: %+v", change)
		}
	}
}

func TestEmptyConstraintsAgreeWithHash(t *testing.T) {
	before := &spec.JobSpec{Name: "web", Namespace: "default", TaskGroups: []spec.TaskGroupSpec{group("web", task("app"))}}
	after := &spec.JobSpec{Name: "web", Namespace: "default", TaskGroups: []spec.TaskGroupSpec{group("web", task("app"))}}
	after.TaskGroups[0].Constraints = []spec.ConstraintSpec{}
	if len(Diff(before, after)) != 0 || spec.TaskGroupContentHash(&before.TaskGroups[0]) != spec.TaskGroupContentHash(&after.TaskGroups[0]) {
		t.Fatal("nil/empty constraints disagree between diff and hash")
	}
	after.TaskGroups[0].Constraints = []spec.ConstraintSpec{{Attribute: "arch", Value: "arm64"}}
	if len(Diff(before, after)) == 0 || spec.TaskGroupContentHash(&before.TaskGroups[0]) == spec.TaskGroupContentHash(&after.TaskGroups[0]) {
		t.Fatal("nonempty constraints must change both diff and hash")
	}
}
