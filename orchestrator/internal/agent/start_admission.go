package agent

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
)

type startAdmission struct {
	ID            string                `json:"id"`
	Namespace     string                `json:"namespace"`
	Generation    uint64                `json:"generation"`
	JobRevision   int                   `json:"job_revision"`
	ExecutionHash string                `json:"execution_hash"`
	Tasks         []string              `json:"tasks"`
	Attempt       int                   `json:"attempt"`
	Epoch         uint64                `json:"epoch"`
	Message       string                `json:"message"`
	Code          nodeapi.OperationCode `json:"code,omitempty"`
}

type cleanupCompletion struct {
	ID         string `json:"id"`
	Generation uint64 `json:"generation"`
}

func completionKey(id string, generation uint64) string {
	return fmt.Sprintf("%s/%d", id, generation)
}

// recordCleanup records an exact receipt, never a generation watermark.
// Caller holds the allocation operation lock.
func (a *Agent) recordCleanup(id string, generation uint64) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.recordCleanupLocked(id, generation)
}

func (a *Agent) recordCleanupLocked(id string, generation uint64) error {
	record := cleanupCompletion{ID: id, Generation: generation}
	key := completionKey(id, generation)
	if a.local != nil {
		if err := a.local.Put("agent/cleanup-complete/"+allocationFileName(key), record); err != nil {
			return err
		}
	}
	if a.cleanupComplete == nil {
		a.cleanupComplete = map[string]cleanupCompletion{}
	}
	a.cleanupComplete[key] = record
	return nil
}

func (a *Agent) persistStartAdmission(id string, s *groupStart) error {
	if a.local == nil {
		return nil
	}
	message := "allocation start interrupted by agent restart"
	if s.failed && s.err != nil {
		message = truncateStartFailure(s.err.Error())
	}
	code := terminalStartCode(s.err)
	if s.failureCode != "" {
		code = s.failureCode
	}
	return a.local.Put("agent/start-admissions/"+allocationFileName(id), startAdmission{ID: id, Namespace: s.namespace, Generation: s.generation, JobRevision: s.jobRevision, ExecutionHash: s.executionHash, Tasks: s.tasks, Attempt: s.attempt, Epoch: s.epoch, Message: message, Code: code})
}

func (a *Agent) recoverStartAdmissions() error {
	if a.local == nil {
		return nil
	}
	completed, completionErrs := a.local.ListRaw("agent/cleanup-complete")
	if len(completionErrs) > 0 {
		return fmt.Errorf("load cleanup completions: %v", completionErrs)
	}
	for _, data := range completed {
		var record cleanupCompletion
		if err := json.Unmarshal(data, &record); err != nil {
			return err
		}
		if record.ID == "" || record.Generation == 0 {
			return fmt.Errorf("invalid cleanup completion")
		}
		if a.cleanupComplete == nil {
			a.cleanupComplete = map[string]cleanupCompletion{}
		}
		a.cleanupComplete[completionKey(record.ID, record.Generation)] = record
	}
	records, errs := a.local.ListRaw("agent/start-admissions")
	if len(errs) > 0 {
		return fmt.Errorf("load start admissions: %v", errs)
	}
	for _, data := range records {
		var r startAdmission
		if err := json.Unmarshal(data, &r); err != nil {
			return err
		}
		if r.ID == "" || r.Generation == 0 {
			return fmt.Errorf("invalid durable start admission")
		}
		if a.stoppedGenerations[r.ID] >= r.Generation {
			continue
		}
		established := true
		for _, task := range r.Tasks {
			record := a.allocations[taskRecordID(r.ID, r.Generation, task)]
			established = established && record != nil && record.ExecutionHash == r.ExecutionHash && record.Status != "starting" && record.Status != "stopping"
		}
		if established {
			continue
		}
		_, cancel := context.WithCancel(a.lifetimeContext())
		done := make(chan struct{})
		close(done)
		if a.starts == nil {
			a.starts = map[string]*groupStart{}
		}
		a.starts[r.ID] = &groupStart{namespace: r.Namespace, generation: r.Generation, jobRevision: r.JobRevision, executionHash: r.ExecutionHash, tasks: r.Tasks, attempt: r.Attempt, epoch: r.Epoch, cancel: cancel, done: done, failed: true, failureCode: r.Code, err: fmt.Errorf("%s", r.Message)}
	}
	return nil
}
