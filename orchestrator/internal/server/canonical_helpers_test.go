package server

import "github.com/overfold/trellis/internal/spec"

// canonicalTestSpec resolves job defaults in place, as registration does
// before a job is stored. Validation errors are ignored so tests can store
// jobs that reconciliation must refuse.
func canonicalTestSpec(job *spec.JobSpec) *spec.JobSpec {
	// Namespace networking, the default, needs a namespace port registration
	// before placement. Tests that do not exercise it omit networking and get
	// mode none instead; tests of namespace networking set it explicitly.
	for i := range job.TaskGroups {
		for j := range job.TaskGroups[i].Tasks {
			if task := &job.TaskGroups[i].Tasks[j]; task.Networking == nil {
				task.Networking = &spec.TaskNetworkingSpec{Mode: spec.TaskNetworkNone}
			}
		}
	}
	_ = spec.Canonicalize(job, spec.DefaultLimits())
	return job
}
