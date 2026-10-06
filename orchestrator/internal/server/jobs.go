package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	"github.com/overfold/trellis/orchestrator/internal/plan"
	"github.com/overfold/trellis/orchestrator/internal/spec"
)

// CanonicalizeJob resolves operator defaults and validates a job before use.
func (s *Server) CanonicalizeJob(job *spec.JobSpec) error {
	s.mu.RLock()
	limits := s.jobLimits
	s.mu.RUnlock()
	if limits == (spec.Limits{}) {
		limits = spec.DefaultLimits()
	}
	return spec.Canonicalize(job, limits)
}

func jobKey(namespace, name string) string {
	if namespace == "" {
		return name
	}
	return namespace + "\x00" + name
}

// RegisterJob creates or updates desired job state. A nil preconditions
// applies unconditionally.
func (s *Server) RegisterJob(ctx context.Context, namespace string, jobSpec *spec.JobSpec, preconditions *JobPreconditions, resolvedImages map[string]string) (*api.JobRegistrationResponse, error) {
	if preconditions == nil {
		preconditions = &JobPreconditions{}
	}
	if err := preconditions.validate(); err != nil {
		return nil, err
	}
	if err := s.CanonicalizeJob(jobSpec); err != nil {
		return nil, fmt.Errorf("validate job: %w", err)
	}
	if jobSpec.Namespace != namespace {
		return nil, fmt.Errorf("job namespace does not match request namespace")
	}
	images, err := s.resolveJobImages(ctx, jobSpec, resolvedImages)
	if err != nil {
		return nil, err
	}
	execution := executionSpec(jobSpec, images)
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	key := jobKey(namespace, jobSpec.Name)
	// The precondition is checked under mutationMu, which serializes every
	// job mutation on the leader, so the job cannot change between this
	// check and the commit below. Job records are replaced, never mutated,
	// so existing can be compared without holding s.mu.
	s.mu.RLock()
	existing := s.jobs[key]
	s.mu.RUnlock()
	if err := preconditions.check(existing); err != nil {
		return nil, err
	}
	if existing != nil && len(plan.Diff(existing.Spec, jobSpec)) == 0 && maps.Equal(existing.ResolvedImages, images) {
		return &api.JobRegistrationResponse{Namespace: namespace, Name: jobSpec.Name, Incarnation: existing.Incarnation, Version: existing.Version, Revision: existing.Revision}, nil
	}
	s.mu.RLock()
	// Job limits can change between canonicalization and this point.
	limits := s.jobLimits
	if limits == (spec.Limits{}) {
		limits = spec.DefaultLimits()
	}
	err = spec.ValidateWithLimits(jobSpec, limits)
	if err != nil {
		err = fmt.Errorf("validate job: %w", err)
	} else {
		err = s.validateNamespaceAllocationLimitLocked(namespace, jobSpec, key)
	}
	s.mu.RUnlock()
	if err != nil {
		return nil, err
	}

	hashes := make(map[string]string, len(jobSpec.TaskGroups))
	for i := range execution.TaskGroups {
		hashes[execution.TaskGroups[i].Name] = spec.TaskGroupContentHash(&execution.TaskGroups[i])
	}

	revision, version := 1, 1
	labelOnly := false
	if existing != nil {
		version = existing.Version + 1
		revision = existing.Revision + 1
		if isLabelOnlyChange(existing, jobSpec, hashes) {
			revision = existing.Revision
			labelOnly = true
		}
	}
	incarnation := uuid.NewString()
	if existing != nil {
		incarnation = existing.Incarnation
	}
	job := &Job{
		Spec:           jobSpec,
		ResolvedImages: images,
		Incarnation:    incarnation,
		Revision:       revision,
		Version:        version,
		ContentHashes:  hashes,
	}
	revisionRecord := &JobRevisionRecord{Version: version, Revision: revision, Spec: jobSpec, ResolvedImages: images, CreatedAt: s.now().UTC()}
	if err := s.state.PutJobWithRevision(ctx, key, job, revisionRecord); err != nil {
		return nil, fmt.Errorf("save job remotely: %w", stateUnavailable(err))
	}
	s.mu.Lock()
	s.jobs[key] = job
	s.mu.Unlock()

	if labelOnly {
		s.refreshCatalog()
	}
	s.events.publish(api.ClusterEvent{
		Type:      api.EventJobRegistered,
		Namespace: namespace,
		JobName:   jobSpec.Name,
		Version:   version,
		Revision:  revision,
		At:        s.now().UTC(),
	})

	return &api.JobRegistrationResponse{Namespace: namespace, Name: jobSpec.Name, Incarnation: incarnation, Version: version, Revision: revision}, nil
}

// ErrJobVersionConflict indicates that a job apply's preconditions did not
// match the job's current state.
var ErrJobVersionConflict = errors.New("job version conflict")

// ErrInvalidJobPreconditions indicates malformed job apply preconditions.
var ErrInvalidJobPreconditions = errors.New("invalid job apply preconditions")

// JobPreconditions make a job apply conditional on the job's current state.
// Zero fields do not constrain the apply.
type JobPreconditions struct {
	// Version 0 requires that the job does not exist; N requires that the
	// job is at version N and, because versions restart when a job is
	// deleted and recreated, also requires Incarnation.
	Version *int
	// Incarnation requires that the job exists with this incarnation.
	Incarnation string
}

func (p *JobPreconditions) validate() error {
	switch {
	case p.Version != nil && *p.Version < 0:
		return fmt.Errorf("%w: expected_version must not be negative", ErrInvalidJobPreconditions)
	case p.Version != nil && *p.Version > 0 && p.Incarnation == "":
		return fmt.Errorf("%w: expected_version %d requires expected_incarnation", ErrInvalidJobPreconditions, *p.Version)
	case p.Version != nil && *p.Version == 0 && p.Incarnation != "":
		return fmt.Errorf("%w: expected_version 0 requires that the job does not exist and cannot be combined with expected_incarnation", ErrInvalidJobPreconditions)
	}
	return nil
}

// check enforces the preconditions against the current job, which is nil
// when the job does not exist.
func (p *JobPreconditions) check(existing *Job) error {
	if p.Version == nil && p.Incarnation == "" {
		return nil
	}
	conflict := &JobVersionConflictError{}
	if p.Version != nil {
		conflict.Expected = *p.Version
	}
	if existing == nil {
		if p.Version != nil && *p.Version == 0 {
			return nil
		}
		return conflict
	}
	conflict.Exists, conflict.Current = true, existing.Version
	switch {
	case p.Incarnation != "" && p.Incarnation != existing.Incarnation:
		conflict.Recreated = true
		return conflict
	case p.Version != nil && *p.Version != existing.Version:
		return conflict
	}
	return nil
}

// JobVersionConflictError describes a failed job apply precondition.
type JobVersionConflictError struct {
	// Expected is the expected version, or 0 when the apply required that
	// the job does not exist or expected only an incarnation.
	Expected int
	// Current is the job's current version, or 0 when the job does not exist.
	Current int
	Exists  bool
	// Recreated reports that the job was deleted and recreated since the
	// expected incarnation was read.
	Recreated bool
}

func (e *JobVersionConflictError) Error() string {
	switch {
	case e.Recreated:
		return fmt.Sprintf("job version conflict: the job was deleted and recreated after it was read and is now at version %d; plan the manifest again", e.Current)
	case !e.Exists && e.Expected > 0:
		return fmt.Sprintf("job version conflict: expected version %d but the job does not exist; plan the manifest again", e.Expected)
	case !e.Exists:
		return "job version conflict: expected an existing job but the job does not exist; plan the manifest again"
	case e.Expected == 0:
		return fmt.Sprintf("job version conflict: job already exists at version %d; plan the manifest again", e.Current)
	default:
		return fmt.Sprintf("job version conflict: expected version %d but the job is at version %d; plan the manifest again", e.Expected, e.Current)
	}
}

// Is reports whether target is ErrJobVersionConflict.
func (e *JobVersionConflictError) Is(target error) bool { return target == ErrJobVersionConflict }

// ValidateNamespaceAllocationLimit checks a candidate job against the current
// namespace desired-allocation budget without mutating state.
func (s *Server) ValidateNamespaceAllocationLimit(namespace string, job *spec.JobSpec) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.validateNamespaceAllocationLimitLocked(namespace, job, jobKey(namespace, job.Name))
}

func (s *Server) validateNamespaceAllocationLimitLocked(namespace string, candidate *spec.JobSpec, replacingKey string) error {
	limits := s.jobLimits
	if limits == (spec.Limits{}) {
		limits = spec.DefaultLimits()
	}
	totals := make(map[string]int64)
	for key, job := range s.jobs {
		if key == replacingKey || job == nil || job.Spec == nil {
			continue
		}
		totals[job.Spec.Namespace] += desiredAllocations(job.Spec)
	}
	if candidate != nil {
		totals[namespace] += desiredAllocations(candidate)
	}
	for name, total := range totals {
		if total > int64(limits.MaxDesiredAllocationsPerNamespace) {
			return spec.ValidationErrors{{Path: "task_groups", Code: "limit_exceeded", Message: fmt.Sprintf("namespace %q desired allocations %d exceeds operator limit of %d", name, total, limits.MaxDesiredAllocationsPerNamespace)}}
		}
	}
	return nil
}

func desiredAllocations(job *spec.JobSpec) int64 {
	total := int64(0)
	for _, group := range job.TaskGroups {
		total += int64(group.Count)
	}
	return total
}

// isLabelOnlyChange returns true when the new job spec differs from the
// existing one only in task group labels (and count/update policy), so the
// change keeps the execution revision. The content hashes must have been
// computed from newSpec.
func isLabelOnlyChange(existing *Job, newSpec *spec.JobSpec, newHashes map[string]string) bool {
	if len(existing.Spec.TaskGroups) != len(newSpec.TaskGroups) {
		return false
	}
	if existing.ContentHashes == nil {
		return false
	}
	for name, newHash := range newHashes {
		oldHash, ok := existing.ContentHashes[name]
		if !ok || oldHash != newHash {
			return false
		}
	}
	return true
}

// ListJobs returns jobs in a namespace.
func (s *Server) ListJobs(namespace string) api.JobListResponse {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(api.JobListResponse, 0, len(s.jobs))
	jobs := make(map[string]*Job)
	positions := make(map[string]int)
	for _, job := range s.jobs {
		if job.Spec.Namespace != namespace {
			continue
		}
		name := job.Spec.Name
		r := api.JobStatusResponse{Name: name, Incarnation: job.Incarnation, Version: job.Version, Revision: job.Revision}
		for _, g := range job.Spec.TaskGroups {
			r.Desired += g.Count
		}
		r.ReplacementBackoff = s.replacementBackoffResponsesLocked(namespace, name)
		jobs[name], positions[name] = job, len(result)
		result = append(result, r)
	}
	for _, a := range s.allocations {
		a.mu.Lock()
		job := jobs[a.JobName]
		if a.Namespace != namespace || job == nil {
			a.mu.Unlock()
			continue
		}
		r := &result[positions[a.JobName]]
		r.Allocations = append(r.Allocations, s.allocationResponseLocked(a))
		if a.JobIncarnation == job.Incarnation && a.JobRevision == job.Revision && !a.Draining && a.Phase == lifecycle.PhaseRunning {
			r.Running++
		}
		if a.JobIncarnation == job.Incarnation && a.JobRevision == job.Revision && !a.Draining && a.Phase == lifecycle.PhaseRunning && a.Health == lifecycle.HealthHealthy {
			r.Healthy++
		}
		a.mu.Unlock()
	}
	return result
}

// ErrJobNotFound indicates that a namespace has no job with the requested name.
var ErrJobNotFound = errors.New("job not found")

// GetJob returns a job, its canonical specification, and its allocation
// state.
func (s *Server) GetJob(namespace, name string) (*api.JobStatusResponse, error) {
	r, jobSpec, ok := s.jobStatus(namespace, name)
	if !ok {
		return nil, ErrJobNotFound
	}
	// Job records are replaced, never mutated, so the spec is encoded
	// without holding s.mu.
	rawSpec, _ := json.Marshal(jobSpec)
	r.Spec = rawSpec
	return r, nil
}

func (s *Server) jobStatus(namespace, name string) (*api.JobStatusResponse, *spec.JobSpec, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, ok := s.jobs[jobKey(namespace, name)]
	if !ok {
		return nil, nil, false
	}
	r := &api.JobStatusResponse{Name: name, Incarnation: job.Incarnation, Version: job.Version, Revision: job.Revision, ResolvedImages: maps.Clone(job.ResolvedImages)}
	for _, g := range job.Spec.TaskGroups {
		r.Desired += g.Count
	}
	for _, a := range s.allocations {
		a.mu.Lock()
		if a.Namespace != namespace || a.JobName != name {
			a.mu.Unlock()
			continue
		}
		ar := s.allocationResponseLocked(a)
		r.Allocations = append(r.Allocations, ar)
		if a.JobIncarnation == job.Incarnation && a.JobRevision == job.Revision && !a.Draining && a.Phase == lifecycle.PhaseRunning {
			r.Running++
		}
		if a.JobIncarnation == job.Incarnation && a.JobRevision == job.Revision && !a.Draining && a.Phase == lifecycle.PhaseRunning && a.Health == lifecycle.HealthHealthy {
			r.Healthy++
		}
		a.mu.Unlock()
	}
	r.ReplacementBackoff = s.replacementBackoffResponsesLocked(namespace, name)
	return r, job.Spec, true
}

// PlanJob returns the semantic plan for applying desired, a canonical job.
func (s *Server) PlanJob(ctx context.Context, desired *spec.JobSpec) (api.JobPlanResponse, error) {
	images, err := s.resolveJobImages(ctx, desired, nil)
	if err != nil {
		return api.JobPlanResponse{}, err
	}
	execution := executionSpec(desired, images)
	s.mu.RLock()
	current := s.jobs[jobKey(desired.Namespace, desired.Name)]
	s.mu.RUnlock()
	var result api.JobPlanResponse
	if current == nil {
		result = plan.Build(nil, plan.Base{}, execution)
	} else {
		// Job records are replaced, never mutated, so current is read safely.
		result = plan.Build(executionSpec(current.Spec, current.ResolvedImages), plan.Base{Incarnation: current.Incarnation, Version: current.Version, Revision: current.Revision}, execution)
		// An authored reference change can resolve to the same execution ref.
		if result.Action == "none" {
			result = plan.Build(current.Spec, plan.Base{Incarnation: current.Incarnation, Version: current.Version, Revision: current.Revision}, desired)
		}
	}
	result.ResolvedImages = images
	return result, nil
}

// DeleteJob removes desired job state.
func (s *Server) DeleteJob(ctx context.Context, namespace, name string) error {
	s.mutationMu.Lock()
	s.mu.RLock()
	key := jobKey(namespace, name)
	_, ok := s.jobs[key]
	s.mu.RUnlock()
	if !ok {
		s.mutationMu.Unlock()
		return fmt.Errorf("%w: %s", ErrJobNotFound, name)
	}
	if err := s.state.DeleteJob(ctx, key); err != nil {
		s.mutationMu.Unlock()
		return stateUnavailable(err)
	}
	s.mu.Lock()
	delete(s.jobs, key)
	s.mu.Unlock()
	s.mutationMu.Unlock()
	s.events.publish(api.ClusterEvent{
		Type:      api.EventJobDeleted,
		Namespace: namespace,
		JobName:   name,
		At:        s.now().UTC(),
	})
	s.Reconcile(ctx)
	return nil
}

// RestartJob marks all running allocations for a job as draining and
// reconciles so they are replaced with fresh instances.
func (s *Server) RestartJob(ctx context.Context, namespace, name string) error {
	s.reconcileMu.Lock()
	err := s.persistJobRestart(ctx, namespace, name)
	s.reconcileMu.Unlock()
	if err != nil {
		return err
	}
	s.Reconcile(ctx)
	return nil
}

func (s *Server) persistJobRestart(ctx context.Context, namespace, name string) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	s.mu.RLock()
	key := jobKey(namespace, name)
	job := s.jobs[key]
	if job == nil {
		s.mu.RUnlock()
		return fmt.Errorf("%w: %s", ErrJobNotFound, name)
	}
	incarnation := job.Incarnation
	allocations := append([]*Allocation(nil), s.allocations...)

	// Copying an allocation reads its node's ID, which s.mu guards.
	updates := make([]*Allocation, 0)
	for _, alloc := range allocations {
		alloc.mu.Lock()
		if alloc.Namespace == namespace && alloc.JobName == name && alloc.JobIncarnation == incarnation && alloc.Node != nil && alloc.DrainReason != "restart" && activeAllocationPhase(alloc.Phase) {
			update := alloc.cloneRecord()
			alloc.mu.Unlock()
			update.Draining = true
			update.DrainSequence++
			update.DrainReason = "restart"
			updates = append(updates, update)
			continue
		}
		alloc.mu.Unlock()
	}
	s.mu.RUnlock()
	if err := s.state.PutAllocations(ctx, updates); err != nil {
		return fmt.Errorf("persist restart intent: %w", stateUnavailable(err))
	}
	for _, update := range updates {
		for _, alloc := range allocations {
			if alloc.ID != update.ID {
				continue
			}
			alloc.mu.Lock()
			if alloc.Generation == update.Generation {
				alloc.Draining = true
				alloc.DrainSequence = update.DrainSequence
				alloc.DrainReason = "restart"
			}
			alloc.mu.Unlock()
			break
		}
	}
	return nil
}

// ListJobVersions returns the retained spec history for a job, one entry per
// version in ascending order.
func (s *Server) ListJobVersions(ctx context.Context, namespace, name string) (api.JobVersionListResponse, error) {
	s.mu.RLock()
	key := jobKey(namespace, name)
	_, ok := s.jobs[key]
	s.mu.RUnlock()
	if !ok {
		return nil, ErrJobNotFound
	}
	records, err := s.state.ListJobRevisions(ctx, key)
	if err != nil {
		return nil, stateUnavailable(err)
	}
	result := make(api.JobVersionListResponse, 0, len(records))
	for _, r := range records {
		rawSpec, _ := json.Marshal(r.Spec)
		result = append(result, api.JobVersionResponse{
			Version:        r.Version,
			Revision:       r.Revision,
			Spec:           rawSpec,
			ResolvedImages: r.ResolvedImages,
			CreatedAt:      r.CreatedAt,
		})
	}
	return result, nil
}
