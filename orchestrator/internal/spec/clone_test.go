package spec

import (
	"reflect"
	"testing"

	"github.com/overfold/trellis/orchestrator/internal/deepcopytest"
)

func TestTaskSpecCloneIsDeep(t *testing.T) {
	var original TaskSpec
	deepcopytest.Fill(t, &original)
	clone := original.Clone()
	if !reflect.DeepEqual(original, clone) {
		t.Fatalf("clone differs from original:\noriginal=%#v\nclone=%#v", original, clone)
	}
	deepcopytest.AssertDisjoint(t, &original, &clone)
}

func TestCloneTasksPreservesNil(t *testing.T) {
	if CloneTasks(nil) != nil {
		t.Fatal("CloneTasks(nil) returned a non-nil list")
	}
	if clone := CloneTasks([]TaskSpec{}); clone == nil || len(clone) != 0 {
		t.Fatalf("CloneTasks(empty) = %#v, want an empty non-nil list", clone)
	}
}
