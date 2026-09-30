package spec

import (
	"maps"
	"slices"
)

// Clone returns a deep copy of the task specification: the copy shares no
// maps, slices, or pointers with t, and preserves nil and empty values.
func (t TaskSpec) Clone() TaskSpec {
	clone := t
	clone.Env = maps.Clone(t.Env)
	if t.Networking != nil {
		networking := *t.Networking
		networking.Ports = slices.Clone(t.Networking.Ports)
		clone.Networking = &networking
	}
	clone.Volumes = slices.Clone(t.Volumes)
	if t.Resources != nil {
		resources := *t.Resources
		clone.Resources = &resources
	}
	if t.HealthCheck != nil {
		healthCheck := *t.HealthCheck
		healthCheck.Command = slices.Clone(t.HealthCheck.Command)
		clone.HealthCheck = &healthCheck
	}
	clone.Secrets = slices.Clone(t.Secrets)
	return clone
}

// CloneTasks deep-copies a task list, preserving a nil list.
func CloneTasks(tasks []TaskSpec) []TaskSpec {
	if tasks == nil {
		return nil
	}
	clone := make([]TaskSpec, len(tasks))
	for i := range tasks {
		clone[i] = tasks[i].Clone()
	}
	return clone
}
