// Package plan computes semantic changes between desired and current Trellis job specifications.
package plan

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"

	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/internal/spec"
)

// Base identifies the current job a plan is computed against.
type Base struct {
	Incarnation string
	Version     int
	Revision    int
}

// Build returns the canonical semantic plan for desired. current is nil when
// the job does not exist; otherwise base identifies it, and an apply of the
// plan should expect it.
func Build(current *spec.JobSpec, base Base, desired *spec.JobSpec) api.JobPlanResponse {
	result := api.JobPlanResponse{
		Action:             "create",
		Namespace:          desired.Namespace,
		Job:                desired.Name,
		DesiredAllocations: desiredAllocationCount(desired),
		Changes:            []api.JobPlanChange{},
	}
	if current == nil {
		return result
	}

	result.BaseIncarnation = base.Incarnation
	result.BaseVersion = base.Version
	result.BaseRevision = base.Revision
	result.Changes = Diff(current, desired)
	if len(result.Changes) == 0 {
		result.Action = "none"
	} else {
		result.Action = "update"
	}
	return result
}

// Diff returns semantic changes between two job specifications. Task groups are
// matched by name because their order does not affect revision semantics. Other
// slices remain positional because their order participates in task-group
// content hashing and can therefore cause allocation replacement.
func Diff(before, after *spec.JobSpec) []api.JobPlanChange {
	var left any
	var right any
	leftRaw, _ := json.Marshal(before)
	rightRaw, _ := json.Marshal(after)
	_ = json.Unmarshal(leftRaw, &left)
	_ = json.Unmarshal(rightRaw, &right)
	changes := make([]api.JobPlanChange, 0)
	walk("", left, right, &changes)
	return changes
}

func desiredAllocationCount(job *spec.JobSpec) int {
	total := 0
	for _, group := range job.TaskGroups {
		total += group.Count
	}
	return total
}

func walk(path string, before, after any, changes *[]api.JobPlanChange) {
	if reflect.DeepEqual(before, after) {
		return
	}
	leftMap, leftOK := before.(map[string]any)
	rightMap, rightOK := after.(map[string]any)
	if leftOK && rightOK {
		keys := make(map[string]struct{}, len(leftMap)+len(rightMap))
		for key := range leftMap {
			keys[key] = struct{}{}
		}
		for key := range rightMap {
			keys[key] = struct{}{}
		}
		ordered := make([]string, 0, len(keys))
		for key := range keys {
			ordered = append(ordered, key)
		}
		sort.Strings(ordered)
		for _, key := range ordered {
			left, leftExists := leftMap[key]
			right, rightExists := rightMap[key]
			child := joinPath(path, key)
			switch {
			case !leftExists:
				*changes = append(*changes, api.JobPlanChange{Operation: "add", Path: child, After: right})
			case !rightExists:
				*changes = append(*changes, api.JobPlanChange{Operation: "remove", Path: child, Before: left})
			default:
				walk(child, left, right, changes)
			}
		}
		return
	}

	leftSlice, leftOK := before.([]any)
	rightSlice, rightOK := after.([]any)
	if leftOK && rightOK {
		if path == "task_groups" {
			if leftNamed, okLeft := namedSlice(leftSlice); okLeft {
				if rightNamed, okRight := namedSlice(rightSlice); okRight {
					names := make(map[string]struct{}, len(leftNamed)+len(rightNamed))
					for name := range leftNamed {
						names[name] = struct{}{}
					}
					for name := range rightNamed {
						names[name] = struct{}{}
					}
					ordered := make([]string, 0, len(names))
					for name := range names {
						ordered = append(ordered, name)
					}
					sort.Strings(ordered)
					for _, name := range ordered {
						left, leftExists := leftNamed[name]
						right, rightExists := rightNamed[name]
						child := fmt.Sprintf("%s[%s]", path, name)
						switch {
						case !leftExists:
							*changes = append(*changes, api.JobPlanChange{Operation: "add", Path: child, After: right})
						case !rightExists:
							*changes = append(*changes, api.JobPlanChange{Operation: "remove", Path: child, Before: left})
						default:
							walk(child, left, right, changes)
						}
					}
					return
				}
			}
		}
		limit := len(leftSlice)
		if len(rightSlice) > limit {
			limit = len(rightSlice)
		}
		for i := 0; i < limit; i++ {
			child := fmt.Sprintf("%s[%d]", path, i)
			switch {
			case i >= len(leftSlice):
				*changes = append(*changes, api.JobPlanChange{Operation: "add", Path: child, After: rightSlice[i]})
			case i >= len(rightSlice):
				*changes = append(*changes, api.JobPlanChange{Operation: "remove", Path: child, Before: leftSlice[i]})
			default:
				walk(child, leftSlice[i], rightSlice[i], changes)
			}
		}
		return
	}
	*changes = append(*changes, api.JobPlanChange{Operation: "change", Path: path, Before: before, After: after})
}

func namedSlice(values []any) (map[string]any, bool) {
	result := make(map[string]any, len(values))
	for _, value := range values {
		item, ok := value.(map[string]any)
		if !ok {
			return nil, false
		}
		name, ok := item["name"].(string)
		if !ok || name == "" {
			return nil, false
		}
		if _, duplicate := result[name]; duplicate {
			return nil, false
		}
		result[name] = value
	}
	return result, true
}

func joinPath(parent, child string) string {
	if parent == "" {
		return child
	}
	return parent + "." + child
}
