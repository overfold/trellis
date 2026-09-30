package server

import "github.com/overfold/trellis/orchestrator/internal/spec"

// canonicalTestSpec resolves job defaults in place, as registration does
// before a job is stored. Validation errors are ignored so tests can store
// jobs that reconciliation must refuse.
func canonicalTestSpec(job *spec.JobSpec) *spec.JobSpec {
	_ = spec.Canonicalize(job, spec.DefaultLimits())
	return job
}
