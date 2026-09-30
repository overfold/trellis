package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/containerd/errdefs"
	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/internal/execstream"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	"github.com/overfold/trellis/orchestrator/internal/runtime"
)

// execTiming bounds exec streams. Tests shorten it.
type execTiming struct {
	// idleTimeout ends a stream that carried no input, output, or resize for
	// this long. It also bounds how long one output frame may wait for the
	// peer to accept it.
	idleTimeout time.Duration
	// maxLifetime bounds a stream even while it remains active.
	maxLifetime time.Duration
	// checkInterval is how often a stream checks its timeouts and whether a
	// newer leader has fenced this agent.
	checkInterval time.Duration
	// killWait is how long ending a stream waits for its killed process to
	// exit before retrying the kill in the background.
	killWait time.Duration
	// killRetryInterval spaces kills of a process that survived the first
	// one. Its session keeps its admission slot until it exits.
	killRetryInterval time.Duration
}

var defaultExecTiming = execTiming{
	idleTimeout:       30 * time.Minute,
	maxLifetime:       8 * time.Hour,
	checkInterval:     5 * time.Second,
	killWait:          5 * time.Second,
	killRetryInterval: 30 * time.Second,
}

const (
	// execSessionCloseTimeout bounds each step of ending a stream: killing
	// the process and delivering the final frame.
	execSessionCloseTimeout = 5 * time.Second
	// execStdinQueue bounds the stdin frames a stream accepts ahead of the
	// process; beyond it the stream stops reading and TCP pushes back.
	execStdinQueue = 4
	// Each session holds a runtime process, two connections through the
	// leader, and a bounded stdin queue, so admission is bounded both per
	// node agent and per allocation.
	execSessionGlobalLimit        = 64
	execSessionPerAllocationLimit = 8
	// agentExecStartTimeout bounds creating and starting an exec process.
	agentExecStartTimeout = 30 * time.Second
)

// Stream end causes reported to the client in an error frame.
var (
	errExecIdle           = errors.New("exec session closed after a period without activity")
	errExecLifetime       = errors.New("exec session reached its maximum lifetime")
	errExecLeaderChanged  = errors.New("exec session ended because control-plane leadership changed")
	errExecTaskStopped    = errors.New("exec session ended because its task stopped")
	errExecAgentShutdown  = errors.New("exec session ended because the node agent is shutting down")
	errExecPeerDisconnect = errors.New("exec client disconnected")
)

// execSession is a live exec stream bound to one task record and container.
type execSession struct {
	AllocationID string
	TaskID       string
	ContainerID  string
	cancel       context.CancelCauseFunc
	// finished is closed once the stream has ended and its process has been
	// killed or has exited.
	finished chan struct{}
}

// execClock anchors stream activity times so idleness is measured on the
// monotonic clock and wall-clock steps cannot expire or extend streams.
var execClock = time.Now()

func execClockNanos(t time.Time) int64 { return int64(t.Sub(execClock)) }

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
// the newest generation this agent holds that is not stopping, sorted by task
// name. Records of other generations are never targets. It must be
// called with the agent lock held. known reports whether the agent holds any
// record for the allocation.
func (a *Agent) execTargetsLocked(allocID string) (targets []execTarget, known bool) {
	var generation uint64
	current := false
	for _, alloc := range a.allocations {
		if alloc.AllocationID != allocID {
			continue
		}
		known = true
		// A stopping record, such as one left by a failed start of a newer
		// generation, does not replace the generation that still runs.
		if alloc.Status != "stopping" && (!current || alloc.Generation > generation) {
			generation, current = alloc.Generation, true
		}
	}
	if !current {
		return nil, known
	}
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
	return targets, known
}

// selectExecTarget resolves the single task an exec request addresses. An
// empty task selects the only running task and is rejected when several run.
func (a *Agent) selectExecTarget(allocID, task string) (execTarget, error) {
	a.mu.RLock()
	targets, _ := a.execTargetsLocked(allocID)
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

// selectRunningExecTarget resolves an exec target and confirms its container
// is running. A record stays running after its container exits when restarts
// are exhausted, so the record status alone is not enough.
func (a *Agent) selectRunningExecTarget(ctx context.Context, allocID, task string) (execTarget, error) {
	target, err := a.selectExecTarget(allocID, task)
	if err != nil {
		return execTarget{}, err
	}
	observed, err := a.runtime.Inspect(ctx, target.ContainerID)
	if errdefs.IsNotFound(err) || (err == nil && observed.Status != runtime.StatusRunning) {
		return execTarget{}, fmt.Errorf("%w: allocation %s task %s container is not running", ErrAllocationNotFound, allocID, target.TaskName)
	}
	if err != nil {
		return execTarget{}, fmt.Errorf("inspect container %s: %w", target.ContainerID, err)
	}
	return target, nil
}

// ExecReservation is an admitted exec stream whose process has not started.
type ExecReservation struct {
	allocationID string
	target       execTarget
	request      nodeapi.AgentExecRequest
}

// ReserveExec fences an exec request to its leadership epoch, resolves its
// running target, and claims an admission slot. The caller must pass the
// reservation to RunExec or ReleaseExec.
func (a *Agent) ReserveExec(ctx context.Context, allocID string, request nodeapi.AgentExecRequest) (*ExecReservation, error) {
	if err := a.AcceptEpoch(request.Epoch); err != nil {
		return nil, err
	}
	target, err := a.selectRunningExecTarget(ctx, allocID, request.Task)
	if err != nil {
		return nil, err
	}
	if err := a.reserveExecSession(allocID); err != nil {
		return nil, err
	}
	return &ExecReservation{allocationID: allocID, target: target, request: request}, nil
}

// ReleaseExec returns the admission slot of a reservation that will not run.
func (a *Agent) ReleaseExec(reservation *ExecReservation) {
	a.releaseExecSession(reservation.allocationID)
}

// reserveExecSession claims capacity before a runtime process is created.
// A slot stays claimed until the session's process has exited, so a process
// that survives a failed kill cannot free admission capacity.
func (a *Agent) reserveExecSession(allocID string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.execSessionsClosed {
		return ErrAgentShuttingDown
	}
	if a.execSessionCount >= execSessionGlobalLimit {
		return fmt.Errorf("%w: node has %d exec sessions (maximum %d)", ErrExecSessionLimit, a.execSessionCount, execSessionGlobalLimit)
	}
	if count := a.execSessionsByAllocation[allocID]; count >= execSessionPerAllocationLimit {
		return fmt.Errorf("%w: allocation %s has %d exec sessions (maximum %d)", ErrExecSessionLimit, allocID, count, execSessionPerAllocationLimit)
	}
	a.execSessionCount++
	a.execSessionsByAllocation[allocID]++
	return nil
}

func (a *Agent) releaseExecSession(allocID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.execSessionCount--
	a.execSessionsByAllocation[allocID]--
	if a.execSessionsByAllocation[allocID] == 0 {
		delete(a.execSessionsByAllocation, allocID)
	}
}

// execActivity records the last time a stream carried traffic.
type execActivity struct{ last atomic.Int64 }

func (t *execActivity) touch() { t.last.Store(execClockNanos(time.Now())) }

func (t *execActivity) idle(now time.Time) time.Duration {
	return time.Duration(execClockNanos(now) - t.last.Load())
}

// activityWriter records output as stream activity.
type activityWriter struct {
	w        io.Writer
	activity *execActivity
}

func (w activityWriter) Write(p []byte) (int, error) {
	w.activity.touch()
	return w.w.Write(p)
}

// RunExec starts the reserved process and serves its stream on conn until
// the process exits, the client disconnects, or the stream is ended by a
// timeout, a stop of its task, a newer leader, or agent shutdown. conn is
// closed on return.
func (a *Agent) RunExec(conn net.Conn, reservation *ExecReservation) {
	defer func() { _ = conn.Close() }()
	request, target := reservation.request, reservation.target
	timing := a.execTiming
	writer := execstream.NewWriter(conn, timing.idleTimeout)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	createdAt := time.Now()
	activity := &execActivity{}
	activity.touch()

	options := runtime.ExecOptions{
		Command: request.Command,
		TTY:     request.TTY,
		Term:    request.Term,
		Cols:    request.Cols,
		Rows:    request.Rows,
		Stdout:  activityWriter{w: writer.Stream(execstream.FrameStdout), activity: activity},
		Stderr:  activityWriter{w: writer.Stream(execstream.FrameStderr), activity: activity},
	}
	var stdinReader *io.PipeReader
	var stdinWriter *io.PipeWriter
	if request.Stdin {
		stdinReader, stdinWriter = io.Pipe()
		options.Stdin = stdinReader
		defer func() { _ = stdinReader.Close() }()
	}
	startCtx, cancelStart := context.WithTimeout(ctx, agentExecStartTimeout)
	process, err := a.runtime.StartExec(startCtx, target.ContainerID, options)
	cancelStart()
	if err != nil {
		a.releaseExecSession(reservation.allocationID)
		a.log.Warn("start exec process", "allocation", reservation.allocationID, "task", target.TaskName, "error", err)
		a.finishExecStream(conn, writer, execstream.FrameError, api.ExecStreamError{Message: fmt.Sprintf("start exec in task %s: %v", target.TaskName, err)}, execSessionCloseTimeout)
		return
	}

	sessionID := uuid.NewString()
	session := &execSession{
		AllocationID: reservation.allocationID, TaskID: target.ID, ContainerID: target.ContainerID,
		cancel: cancel, finished: make(chan struct{}),
	}
	// end forgets the session once its process is gone, so a stop waiting
	// for it is not held up while the final frame is delivered.
	end := sync.OnceFunc(func() {
		a.mu.Lock()
		delete(a.execSessions, sessionID)
		a.mu.Unlock()
		close(session.finished)
	})
	defer end()
	if err := a.registerExecSession(sessionID, session, target); err != nil {
		cancel(err)
	} else {
		go a.readExecInput(ctx, cancel, conn, request, process, stdinWriter, activity)
	}

	ticker := time.NewTicker(timing.checkInterval)
	defer ticker.Stop()
	for {
		select {
		case <-process.Done():
			a.releaseExecSession(reservation.allocationID)
			end()
			code, err := process.ExitCode()
			if err != nil {
				a.finishExecStream(conn, writer, execstream.FrameError, api.ExecStreamError{Message: fmt.Sprintf("exec process status unavailable: %v", err)}, execSessionCloseTimeout)
				return
			}
			a.finishExecStream(conn, writer, execstream.FrameExit, api.ExecExit{ExitCode: code}, timing.idleTimeout)
			return
		case <-ctx.Done():
			a.killExecProcess(reservation.allocationID, process, timing)
			end()
			cause := context.Cause(ctx)
			if errors.Is(cause, errExecPeerDisconnect) {
				return
			}
			a.finishExecStream(conn, writer, execstream.FrameError, api.ExecStreamError{Message: cause.Error()}, execSessionCloseTimeout)
			return
		case now := <-ticker.C:
			switch {
			case activity.idle(now) >= timing.idleTimeout:
				cancel(errExecIdle)
			case now.Sub(createdAt) >= timing.maxLifetime:
				cancel(errExecLifetime)
			case a.currentEpoch() > request.Epoch:
				cancel(errExecLeaderChanged)
			}
		}
	}
}

func (a *Agent) currentEpoch() uint64 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.epoch
}

// registerExecSession tracks a started session so a stop of its task or
// agent shutdown ends it. A stop marks the record stopping before it ends
// the record's sessions, so a session registered here is either ended by
// that stop or refused.
func (a *Agent) registerExecSession(sessionID string, session *execSession, target execTarget) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.execSessionsClosed {
		return errExecAgentShutdown
	}
	current := a.allocations[target.ID]
	if current == nil || current.ContainerID != target.ContainerID || current.Generation != target.Generation || !execTargetable(current) {
		return errExecTaskStopped
	}
	a.execSessions[sessionID] = session
	return nil
}

// readExecInput applies client frames to the process until the client
// disconnects or sends an invalid frame. Stdin is handed to a writer
// goroutine through a bounded queue so resizes are not stuck behind input
// the process has not read yet.
func (a *Agent) readExecInput(ctx context.Context, cancel context.CancelCauseFunc, conn net.Conn, request nodeapi.AgentExecRequest, process runtime.ExecProcess, stdin *io.PipeWriter, activity *execActivity) {
	var queue chan []byte
	if stdin != nil {
		queue = make(chan []byte, execStdinQueue)
		go func(queue <-chan []byte) {
			for data := range queue {
				if _, err := stdin.Write(data); err != nil {
					break
				}
			}
			// An abandoned queue has no further input, so the process sees
			// the end of its stdin either way.
			_ = stdin.Close()
		}(queue)
	}
	closeQueue := func() {
		if queue != nil {
			close(queue)
			queue = nil
		}
	}
	defer closeQueue()
	stdinOpen := stdin != nil
	reader := execstream.NewReader(conn)
	for {
		frame, err := reader.Next()
		if err != nil {
			if errors.Is(err, io.EOF) || ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				cancel(errExecPeerDisconnect)
			} else {
				cancel(fmt.Errorf("read exec stream: %w", err))
			}
			return
		}
		activity.touch()
		switch frame.Type {
		case execstream.FrameStdin:
			if !stdinOpen {
				cancel(errors.New("exec stream sent input without an open stdin"))
				return
			}
			select {
			case queue <- append([]byte(nil), frame.Payload...):
			case <-ctx.Done():
				return
			}
		case execstream.FrameStdinClose:
			if !stdinOpen {
				cancel(errors.New("exec stream closed stdin that is not open"))
				return
			}
			stdinOpen = false
			closeQueue()
		case execstream.FrameResize:
			var resize api.ExecResize
			if err := json.Unmarshal(frame.Payload, &resize); err != nil {
				cancel(errors.New("exec stream sent an invalid resize frame"))
				return
			}
			if !request.TTY {
				cancel(errors.New("exec stream resized a process without a tty"))
				return
			}
			if err := execstream.ValidateResize(resize); err != nil {
				cancel(err)
				return
			}
			resizeCtx, cancelResize := context.WithTimeout(ctx, execSessionCloseTimeout)
			err := process.Resize(resizeCtx, resize.Cols, resize.Rows)
			cancelResize()
			if err != nil {
				a.log.Warn("resize exec terminal", "error", err)
			}
		default:
			cancel(fmt.Errorf("exec stream sent unexpected frame type %d", frame.Type))
			return
		}
	}
}

// killExecProcess kills a session's process and releases its admission slot
// once it exits. A process that does not exit promptly keeps its slot while
// kills are retried in the background.
func (a *Agent) killExecProcess(allocID string, process runtime.ExecProcess, timing execTiming) {
	kill := func() {
		ctx, cancel := context.WithTimeout(context.Background(), execSessionCloseTimeout)
		defer cancel()
		if err := process.Kill(ctx); err != nil {
			a.log.Warn("kill exec process", "allocation", allocID, "error", err)
		}
	}
	kill()
	timer := time.NewTimer(timing.killWait)
	defer timer.Stop()
	select {
	case <-process.Done():
		a.releaseExecSession(allocID)
		return
	case <-timer.C:
	}
	go func() {
		retry := time.NewTicker(timing.killRetryInterval)
		defer retry.Stop()
		for {
			select {
			case <-process.Done():
				a.releaseExecSession(allocID)
				return
			case <-retry.C:
				kill()
			}
		}
	}()
}

// finishExecStream writes a stream's final frame, waiting at most wait for
// the peer to accept it; the caller then closes the connection.
func (a *Agent) finishExecStream(conn net.Conn, writer *execstream.Writer, frameType execstream.FrameType, value any, wait time.Duration) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = writer.WriteJSON(frameType, value)
	}()
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		_ = conn.Close()
		<-done
	}
}

// closeExecSessionsForTask ends the sessions running in one task record's container.
func (a *Agent) closeExecSessionsForTask(ctx context.Context, taskID, containerID string) {
	a.closeExecSessions(ctx, errExecTaskStopped, func(session *execSession) bool {
		return session.TaskID == taskID && session.ContainerID == containerID
	})
}

// closeExecSessions ends every session match selects and waits, until ctx
// ends, for their processes to be killed. match is called with the agent
// lock held.
func (a *Agent) closeExecSessions(ctx context.Context, cause error, match func(*execSession) bool) {
	a.mu.RLock()
	var sessions []*execSession
	for _, session := range a.execSessions {
		if match(session) {
			sessions = append(sessions, session)
		}
	}
	a.mu.RUnlock()
	for _, session := range sessions {
		session.cancel(cause)
	}
	for _, session := range sessions {
		select {
		case <-session.finished:
		case <-ctx.Done():
			return
		}
	}
}

// CloseExecSessions ends every exec session and refuses new ones. Sessions
// are not recovered after a restart, so the agent calls this on shutdown
// while its runtime is still available.
func (a *Agent) CloseExecSessions(ctx context.Context) {
	a.mu.Lock()
	a.execSessionsClosed = true
	a.mu.Unlock()
	a.closeExecSessions(ctx, errExecAgentShutdown, func(*execSession) bool { return true })
}

// AllocationMetrics returns resource usage for the running tasks of an
// allocation; it is empty while none of the current generation's tasks run.
func (a *Agent) AllocationMetrics(ctx context.Context, allocID string) ([]nodeapi.AgentTaskMetrics, error) {
	a.mu.RLock()
	tasks, known := a.execTargetsLocked(allocID)
	a.mu.RUnlock()
	if !known {
		return nil, fmt.Errorf("%w: %s", ErrAllocationNotFound, allocID)
	}
	result := make([]nodeapi.AgentTaskMetrics, 0, len(tasks))
	for _, task := range tasks {
		m, err := a.runtime.Metrics(ctx, task.ContainerID)
		if err != nil {
			a.log.Warn("metrics unavailable", "container", task.ContainerID, "error", err)
			continue
		}
		result = append(result, nodeapi.AgentTaskMetrics{
			Task:                task.TaskName,
			CPUUsageNanoseconds: m.CPUUsageNanoseconds,
			MemoryUsageBytes:    m.MemoryUsageBytes,
		})
	}
	return result, nil
}
