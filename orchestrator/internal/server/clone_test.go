package server

import (
	"reflect"
	"testing"

	"github.com/google/uuid"
	"github.com/overfold/trellis/internal/deepcopytest"
)

func filledAllocation(t *testing.T) *Allocation {
	t.Helper()
	var allocation Allocation
	deepcopytest.Fill(t, &allocation)
	return &allocation
}

func TestNodeCloneIsDeep(t *testing.T) {
	var node Node
	deepcopytest.Fill(t, &node)
	clone := node.Clone()
	if !reflect.DeepEqual(&node, clone) {
		t.Fatalf("clone differs from original:\noriginal=%#v\nclone=%#v", node, *clone)
	}
	deepcopytest.AssertDisjoint(t, &node, clone)
	if (*Node)(nil).Clone() != nil {
		t.Fatal("nil node cloned to non-nil")
	}
}

func TestAllocationCloneIsDeep(t *testing.T) {
	allocation := filledAllocation(t)
	clone := allocation.Clone()
	if !reflect.DeepEqual(allocation, clone) {
		t.Fatalf("clone differs from original:\noriginal=%#v\nclone=%#v", allocation, clone)
	}
	deepcopytest.AssertDisjoint(t, allocation, clone)
	if !allocation.sameRecord(clone) {
		t.Fatal("clone does not have the original's durable record")
	}
}

func TestAllocationCloneOntoSharesOnlyTheGivenNode(t *testing.T) {
	allocation := filledAllocation(t)
	node := allocation.Node.Clone()
	clone := allocation.cloneOnto(node)
	if clone.Node != node {
		t.Fatal("cloneOnto did not place the copy on the given node")
	}
	clone.Node = allocation.Node.Clone()
	deepcopytest.AssertDisjoint(t, allocation, clone)
}

// TestAllocationSameRecordComparesEveryDurableField changes each field of a
// copy in turn and checks that sameRecord notices. A field added to
// Allocation without a comparison fails here.
func TestAllocationSameRecordComparesEveryDurableField(t *testing.T) {
	notPersisted := map[string]bool{"mu": true, "Events": true}
	var check func(path []int, typ reflect.Type)
	check = func(path []int, typ reflect.Type) {
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			index := append(append([]int(nil), path...), i)
			if notPersisted[field.Name] {
				continue
			}
			if field.Anonymous {
				check(index, field.Type)
				continue
			}
			t.Run(field.Name, func(t *testing.T) {
				original := filledAllocation(t)
				changed := original.Clone()
				value := reflect.ValueOf(changed).Elem().FieldByIndex(index)
				if field.Name == "Node" {
					changed.Node.ID = uuid.New()
				} else {
					value.Set(reflect.Zero(field.Type))
				}
				if original.sameRecord(changed) {
					t.Fatalf("sameRecord ignores a change to %s", field.Name)
				}
			})
		}
	}
	check(nil, reflect.TypeFor[Allocation]())

	original := filledAllocation(t)
	changed := original.Clone()
	changed.Events = nil
	changed.Node.Version = "different observation"
	if !original.sameRecord(changed) {
		t.Fatal("sameRecord compares leader-local events or node observations")
	}
}

func TestNodeSummaryEqualComparesEveryField(t *testing.T) {
	var original NodeSummary
	deepcopytest.Fill(t, &original)
	typ := reflect.TypeFor[NodeSummary]()
	for i := 0; i < typ.NumField(); i++ {
		t.Run(typ.Field(i).Name, func(t *testing.T) {
			changed := original
			reflect.ValueOf(&changed).Elem().Field(i).Set(reflect.Zero(typ.Field(i).Type))
			if original.equal(&changed) {
				t.Fatalf("equal ignores a change to %s", typ.Field(i).Name)
			}
		})
	}
	same := original
	if !original.equal(&same) {
		t.Fatal("equal summaries compare unequal")
	}
}
