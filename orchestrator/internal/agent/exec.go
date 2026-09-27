package agent

import (
	"context"
	"encoding/base64"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/runtime"
	"github.com/google/uuid"
)

const (
	// execSessionIdleTimeout closes a live terminal nobody has written to,
	// read from, or resized for this long.
	execSessionIdleTimeout = 30 * time.Minute
	// execSessionExitRetention keeps an exited terminal's final output
	// readable for this long before its buffer is released.
	execSessionExitRetention = 2 * time.Minute
	execSessionReapInterval  = 30 * time.Second
	execSessionCloseTimeout  = 5 * time.Second
)

// execSession is an interactive terminal bound to one task record and container.
type execSession struct {
	AllocationID string
	TaskID       string
	ContainerID  string
	Task         string
	Terminal     runtime.TerminalSession

	lastActive time.Time
	exitedAt   time.Time
}

// expired reports whether the session should be reaped. It must be called
// with the agent lock held.
func (s *execSession) expired(now time.Time) bool {
	if s.exitedAt.IsZero() {
		// Reading from the end of the buffer reports exit without copying output.
		if _, _, exited, _, err := s.Terminal.Read(math.MaxInt64); err == nil && exited {
			s.exitedAt = now
		}
	}
	if !s.exitedAt.IsZero() {
		return now.Sub(s.exitedAt) >= execSessionExitRetention
	}
	return now.Sub(s.lastActive) >= execSessionIdleTimeout
}

// execTarget identifies the task record and container an exec request addresses.
type execTarget struct {
	ID          string
	ContainerID string
	TaskName    string
	Generation  uint64
}

func execTargetable(alloc *Allocation) bool {
	return alloc.Status == "running" && !alloc.ContainerOwnershipUnverified && alloc.ContainerID != ""
}

// execTargetsLocked returns the task records exec and metrics requests may
// address for a scheduler allocation: running, ownership-verified records of
// the newest generation this agent holds, sorted by task name. Records of an
// older generation may still be stopping and are never targets. It must be
// called with the agent lock held.
func (a *Agent) execTargetsLocked(allocID string) []execTarget {
	var generation uint64
	found := false
	for _, alloc := range a.allocations {
		if alloc.AllocationID == allocID && (!found || alloc.Generation > generation) {
			generation, found = alloc.Generation, true
		}
	}
	var targets []execTarget
	for _, alloc := range a.allocations {
		if alloc.AllocationID == allocID && alloc.Generation == generation && execTargetable(alloc) {
			targets = append(targets, execTarget{ID: alloc.ID, ContainerID: alloc.ContainerID, TaskName: alloc.TaskName, Generation: alloc.Generation})
		}
	}
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].TaskName != targets[j].TaskName {
			return targets[i].TaskName < targets[j].TaskName
		}
		return targets[i].ID < targets[j].ID
	})
	return targets
}

// selectExecTarget resolves the single task an exec request addresses. An
// empty task selects the only running task and is rejected when several run.
func (a *Agent) selectExecTarget(allocID, task string) (execTarget, error) {
	a.mu.RLock()
	targets := a.execTargetsLocked(allocID)
	a.mu.RUnlock()
	var matched []execTarget
	var names []string
	for _, target := range targets {
		if task != "" && target.TaskName != task {
			continue
		}
		if len(names) == 0 || names[len(names)-1] != target.TaskName {
			names = append(names, target.TaskName)
		}
		matched = append(matched, target)
	}
	switch {
	case len(matched) == 0 && task == "":
		return execTarget{}, fmt.Errorf("%w: allocation %s has no running task", ErrAllocationNotFound, allocID)
	case len(matched) == 0:
		return execTarget{}, fmt.Errorf("%w: allocation %s has no running task %q", ErrAllocationNotFound, allocID, task)
	case len(names) > 1:
		return execTarget{}, fmt.Errorf("%w: allocation %s has running tasks %s; specify task", ErrExecTaskRequired, allocID, strings.Join(names, ", "))
	case len(matched) > 1:
		return execTarget{}, fmt.Errorf("%w: allocation %s has %d running records for task %q", ErrExecutionConflict, allocID, len(matched), names[0])
	}
	return matched[0], nil
}

// ExecAllocation runs a command in an allocation task container and returns its output.
func (a *Agent) ExecAllocation(ctx context.Context, allocID, task string, command []string) (*api.AgentExecResponse, error) {
	target, err := a.selectExecTarget(allocID, task)
	if err != nil {
		return nil, err
	}
	stdout, stderr, exitCode, err := a.runtime.ExecOutput(ctx, target.ContainerID, command)
	if err != nil {
		return nil, fmt.Errorf("exec in container %s: %w", target.ContainerID, err)
	}
	return &api.AgentExecResponse{
		Stdout:   string(stdout),
		Stderr:   string(stderr),
		ExitCode: exitCode,
	}, nil
}

// CreateExecSession starts a persistent interactive terminal in an allocation task.
func (a *Agent) CreateExecSession(ctx context.Context, allocID, task string, command []string, term string, cols, rows uint32) (*api.ExecSessionResponse, error) {
	target, err := a.selectExecTarget(allocID, task)
	if err != nil {
		return nil, err
	}
	terminal, err := a.runtime.StartTerminal(ctx, target.ContainerID, command, term, cols, rows)
	if err != nil {
		return nil, fmt.Errorf("start terminal in container %s: %w", target.ContainerID, err)
	}
	sessionID := uuid.NewString()
	a.mu.Lock()
	// A stop marks the record stopping before it closes the record's sessions,
	// so a session registered here is either closed by that stop or refused.
	current := a.allocations[target.ID]
	if current == nil || current.ContainerID != target.ContainerID || current.Generation != target.Generation || !execTargetable(current) {
		a.mu.Unlock()
		a.closeTerminal(ctx, allocID, terminal)
		return nil, fmt.Errorf("%w: allocation %s task %s stopped while starting exec session", ErrAllocationNotFound, allocID, target.TaskName)
	}
	a.execSessions[sessionID] = &execSession{
		AllocationID: allocID, TaskID: target.ID, ContainerID: target.ContainerID, Task: target.TaskName,
		Terminal: terminal, lastActive: time.Now(),
	}
	a.mu.Unlock()
	return &api.ExecSessionResponse{ID: sessionID}, nil
}

// useExecSession returns a session addressed through allocID and records activity on it.
func (a *Agent) useExecSession(allocID, sessionID string) (*execSession, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	session := a.execSessions[sessionID]
	if session == nil || session.AllocationID != allocID {
		return nil, fmt.Errorf("%w: %s", ErrExecSessionNotFound, sessionID)
	}
	session.lastActive = time.Now()
	return session, nil
}

// WriteExecSession writes raw bytes to an interactive terminal.
func (a *Agent) WriteExecSession(allocID, sessionID string, data []byte) error {
	session, err := a.useExecSession(allocID, sessionID)
	if err != nil {
		return err
	}
	if _, err := session.Terminal.Write(data); err != nil {
		return fmt.Errorf("write exec session %s: %w", sessionID, err)
	}
	return nil
}

// ReadExecSession reads terminal bytes produced since offset.
func (a *Agent) ReadExecSession(allocID, sessionID string, offset int64) (*api.ExecSessionOutputResponse, error) {
	session, err := a.useExecSession(allocID, sessionID)
	if err != nil {
		return nil, err
	}
	data, next, exited, exitCode, err := session.Terminal.Read(offset)
	if err != nil {
		return nil, fmt.Errorf("read exec session %s: %w", sessionID, err)
	}
	return &api.ExecSessionOutputResponse{
		DataBase64: base64.StdEncoding.EncodeToString(data),
		NextOffset: next,
		Exited:     exited,
		ExitCode:   exitCode,
	}, nil
}

// ResizeExecSession updates the terminal dimensions.
func (a *Agent) ResizeExecSession(ctx context.Context, allocID, sessionID string, cols, rows uint32) error {
	session, err := a.useExecSession(allocID, sessionID)
	if err != nil {
		return err
	}
	if err := session.Terminal.Resize(ctx, cols, rows); err != nil {
		return fmt.Errorf("resize exec session %s: %w", sessionID, err)
	}
	return nil
}

// CloseExecSession terminates and forgets an interactive terminal.
func (a *Agent) CloseExecSession(ctx context.Context, allocID, sessionID string) error {
	a.mu.Lock()
	session := a.execSessions[sessionID]
	if session == nil || session.AllocationID != allocID {
		a.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrExecSessionNotFound, sessionID)
	}
	delete(a.execSessions, sessionID)
	a.mu.Unlock()
	if err := session.Terminal.Close(ctx); err != nil {
		return fmt.Errorf("close exec session %s: %w", sessionID, err)
	}
	return nil
}

// closeExecSessionsForTask closes the sessions running in one task record's container.
func (a *Agent) closeExecSessionsForTask(ctx context.Context, taskID, containerID string) {
	a.closeExecSessions(ctx, func(session *execSession) bool {
		return session.TaskID == taskID && session.ContainerID == containerID
	})
}

// closeExecSessions forgets and terminates every session match selects.
// match is called with the agent lock held.
func (a *Agent) closeExecSessions(ctx context.Context, match func(*execSession) bool) {
	a.mu.Lock()
	var sessions []*execSession
	for id, session := range a.execSessions {
		if match(session) {
			sessions = append(sessions, session)
			delete(a.execSessions, id)
		}
	}
	a.mu.Unlock()
	for _, session := range sessions {
		a.closeTerminal(ctx, session.AllocationID, session.Terminal)
	}
}

func (a *Agent) closeTerminal(ctx context.Context, allocID string, terminal runtime.TerminalSession) {
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), execSessionCloseTimeout)
	defer cancel()
	if err := terminal.Close(closeCtx); err != nil {
		a.log.Warn("close exec session", "allocation", allocID, "error", err)
	}
}

// reapExecSessions releases exited sessions after their retention period and
// closes live sessions that have been idle too long.
func (a *Agent) reapExecSessions(ctx context.Context, now time.Time) {
	a.closeExecSessions(ctx, func(session *execSession) bool { return session.expired(now) })
}

// runExecSessionReaper bounds session lifetimes and terminates every session
// when the agent shuts down, since sessions are not recovered after a restart.
func (a *Agent) runExecSessionReaper(ctx context.Context) {
	ticker := time.NewTicker(execSessionReapInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			a.closeExecSessions(ctx, func(*execSession) bool { return true })
			return
		case now := <-ticker.C:
			a.reapExecSessions(ctx, now)
		}
	}
}

// AllocationMetrics returns resource usage for the running tasks of an allocation.
func (a *Agent) AllocationMetrics(ctx context.Context, allocID string) ([]api.AgentTaskMetrics, error) {
	a.mu.RLock()
	tasks := a.execTargetsLocked(allocID)
	a.mu.RUnlock()
	if len(tasks) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrAllocationNotFound, allocID)
	}
	result := make([]api.AgentTaskMetrics, 0, len(tasks))
	for _, task := range tasks {
		m, err := a.runtime.Metrics(ctx, task.ContainerID)
		if err != nil {
			a.log.Warn("metrics unavailable", "container", task.ContainerID, "error", err)
			continue
		}
		result = append(result, api.AgentTaskMetrics{
			Task:                task.TaskName,
			CPUUsageNanoseconds: m.CPUUsageNanoseconds,
			MemoryUsageBytes:    m.MemoryUsageBytes,
		})
	}
	return result, nil
}
