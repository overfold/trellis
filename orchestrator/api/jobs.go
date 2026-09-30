package api

import (
	"encoding/json"
	"time"
)

// JobRegistrationRequest applies a job specification. It is also the body of
// a plan request, which ignores the preconditions.
type JobRegistrationRequest struct {
	// Spec is the job specification as a canonical JSON document. Its
	// namespace must be the namespace in the request path.
	Spec json.RawMessage `json:"spec"`
	// ExpectedVersion makes the apply conditional on the job's current
	// version: 0 requires that the job does not exist, and N requires that
	// the job is at version N. A nonzero ExpectedVersion also requires
	// ExpectedIncarnation, because a version alone cannot tell a job from
	// one deleted and recreated since. When omitted, the apply is
	// unconditional.
	ExpectedVersion *int `json:"expected_version,omitempty"`
	// ExpectedIncarnation makes the apply conditional on the job's
	// incarnation: the job must exist and must not have been deleted and
	// recreated since the caller read it. It must not be combined with an
	// ExpectedVersion of 0.
	ExpectedIncarnation string `json:"expected_incarnation,omitempty"`
}

// JobRegistrationResponse reports the job version and revision after an apply.
type JobRegistrationResponse struct {
	Namespace   string `json:"namespace"`
	Name        string `json:"name"`
	Incarnation string `json:"incarnation"`
	Version     int    `json:"version"`
	Revision    int    `json:"revision"`
}

// JobStatusResponse summarizes the desired and observed state of a job.
type JobStatusResponse struct {
	Name string `json:"name"`
	// Incarnation identifies this lifetime of the job. It is assigned when
	// the job is created and never changes until the job is deleted; a job
	// recreated under the same name gets a new incarnation, and its version
	// restarts at 1.
	Incarnation string               `json:"incarnation"`
	Version     int                  `json:"version"`
	Revision    int                  `json:"revision"`
	Desired     int                  `json:"desired"`
	Running     int                  `json:"running"`
	Healthy     int                  `json:"healthy"`
	Allocations []AllocationResponse `json:"allocations"`
	// ReplacementBackoff lists task groups with consecutive failed
	// allocations. New placements for such a group wait until
	// next_replacement_at.
	ReplacementBackoff []ReplacementBackoffResponse `json:"replacement_backoff,omitempty"`
	// Spec is the job's canonical specification. It is omitted when
	// listing jobs.
	Spec json.RawMessage `json:"spec,omitempty"`
}

// JobListResponse is the response returned when listing jobs.
type JobListResponse = []JobStatusResponse

// ReplacementBackoffResponse describes why replacements for a task group are
// delayed after its allocations failed.
type ReplacementBackoffResponse struct {
	Group             string    `json:"group"`
	JobRevision       int       `json:"job_revision"`
	Failures          int       `json:"failures"`
	LastFailureAt     time.Time `json:"last_failure_at"`
	LastAllocationID  string    `json:"last_allocation_id,omitempty"`
	Reason            string    `json:"reason,omitempty"`
	Message           string    `json:"message,omitempty"`
	NextReplacementAt time.Time `json:"next_replacement_at"`
}

// JobVersionResponse describes one retained version of a job and the
// execution revision it ran. Spec is the version's canonical specification.
type JobVersionResponse struct {
	Version   int             `json:"version"`
	Revision  int             `json:"revision"`
	Spec      json.RawMessage `json:"spec"`
	CreatedAt time.Time       `json:"created_at"`
}

// JobVersionListResponse is the response returned when listing job versions.
type JobVersionListResponse = []JobVersionResponse

// JobPlanResponse describes what applying a desired job specification would
// do. The Base fields identify the current job and are absent when the job
// does not exist; an apply of the plan passes them as its preconditions.
type JobPlanResponse struct {
	// Action is create, update, or none.
	Action             string          `json:"action"`
	Namespace          string          `json:"namespace"`
	Job                string          `json:"job"`
	BaseIncarnation    string          `json:"base_incarnation,omitempty"`
	BaseVersion        int             `json:"base_version,omitempty"`
	BaseRevision       int             `json:"base_revision,omitempty"`
	DesiredAllocations int             `json:"desired_allocations"`
	Changes            []JobPlanChange `json:"changes"`
}

// JobPlanChange describes one semantic change to a job specification.
type JobPlanChange struct {
	// Operation is add, remove, or change.
	Operation string `json:"operation"`
	Path      string `json:"path"`
	Before    any    `json:"before,omitempty"`
	After     any    `json:"after,omitempty"`
}
