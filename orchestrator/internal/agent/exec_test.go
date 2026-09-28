package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/overfold/trellis/internal/api"
	"github.com/overfold/trellis/internal/runtime"
)

type execTestTerminal struct {
	mu       sync.Mutex
	exited   bool
	closed   int
	closeErr error
}

func (t *execTestTerminal) Write(p []byte) (int, error) { return len(p), nil }
func (t *execTestTerminal) Read(int64) ([]byte, int64, bool, *int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return nil, 0, t.exited, nil, nil
}
func (t *execTestTerminal) Resize(context.Context, uint32, uint32) error { return nil }
func (t *execTestTerminal) Close(context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed++
	if t.closeErr != nil {
		return t.closeErr
	}
	t.exited = true
	return nil
}

func (t *execTestTerminal) closeCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closed
}

type execTestRuntime struct {
	*failingStopRuntime
	mu          sync.Mutex
	execTargets []string
	metricIDs   []string
	terminals   map[string][]*execTestTerminal
	onTerminal  func(containerID string)
}

func newExecTestRuntime() *execTestRuntime {
	return &execTestRuntime{
		failingStopRuntime: &failingStopRuntime{reconcilerRuntime: &reconcilerRuntime{status: runtime.StatusRunning}},
		terminals:          map[string][]*execTestTerminal{},
	}
}

func (r *execTestRuntime) ExecOutput(_ context.Context, containerID string, _ []string) ([]byte, []byte, int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.execTargets = append(r.execTargets, containerID)
	return []byte(containerID), nil, 0, nil
}

func (r *execTestRuntime) StartTerminal(_ context.Context, containerID string, _ []string, _ string, _, _ uint32) (runtime.TerminalSession, error) {
	if r.onTerminal != nil {
		r.onTerminal(containerID)
	}
	terminal := &execTestTerminal{}
	r.mu.Lock()
	r.terminals[containerID] = append(r.terminals[containerID], terminal)
	r.mu.Unlock()
	return terminal, nil
}

func (r *execTestRuntime) Metrics(_ context.Context, containerID string) (*runtime.ContainerMetrics, error) {
	r.mu.Lock()
	r.metricIDs = append(r.metricIDs, containerID)
	r.mu.Unlock()
	return &runtime.ContainerMetrics{}, nil
}

func (r *execTestRuntime) terminal(containerID string) *execTestTerminal {
	r.mu.Lock()
	defer r.mu.Unlock()
	terminals := r.terminals[containerID]
	return terminals[len(terminals)-1]
}

func addExecTestTask(agent *Agent, id, taskName string, generation uint64, status string) *Allocation {
	alloc := &Allocation{ID: id, ContainerID: id, AllocationID: "allocation", Generation: generation, TaskName: taskName, Status: status}
	agent.allocations[id] = alloc
	return alloc
}

func TestExecTargetsOnlyRunningVerifiedCurrentGenerationTask(t *testing.T) {
	rt := newExecTestRuntime()
	agent := newOperationTestAgent(t, rt)
	addExecTestTask(agent, "allocation-g1-web", "web", 1, "running")
	addExecTestTask(agent, "allocation-g1-worker", "worker", 1, "running")
	addExecTestTask(agent, "allocation-g2-web", "web", 2, "running")
	addExecTestTask(agent, "allocation-g2-worker", "worker", 2, "starting")
	addExecTestTask(agent, "allocation-g2-sidecar", "sidecar", 2, "running").ContainerOwnershipUnverified = true
	addExecTestTask(agent, "allocation-g2-proxy", "proxy", 2, "stopping")

	for i := 0; i < 20; i++ {
		result, err := agent.ExecAllocation(context.Background(), "allocation", "", []string{"true"})
		if err != nil {
			t.Fatal(err)
		}
		if result.Stdout != "allocation-g2-web" {
			t.Fatalf("exec target = %q, want current-generation running task", result.Stdout)
		}
	}
	for _, task := range []string{"worker", "sidecar", "proxy", "missing"} {
		if _, err := agent.ExecAllocation(context.Background(), "allocation", task, []string{"true"}); !errors.Is(err, ErrAllocationNotFound) {
			t.Fatalf("exec task %s error = %v, want not found", task, err)
		}
		if _, err := agent.CreateExecSession(context.Background(), "allocation", task, []string{"sh"}, "", 80, 24); !errors.Is(err, ErrAllocationNotFound) {
			t.Fatalf("session task %s error = %v, want not found", task, err)
		}
	}
	if len(rt.terminals) != 0 {
		t.Fatalf("terminals started in ineligible containers: %v", rt.terminals)
	}

	metrics, err := agent.AllocationMetrics(context.Background(), "allocation")
	if err != nil {
		t.Fatal(err)
	}
	if len(metrics) != 1 || metrics[0].Task != "web" || len(rt.metricIDs) != 1 || rt.metricIDs[0] != "allocation-g2-web" {
		t.Fatalf("metrics = %#v from containers %v, want only current web task", metrics, rt.metricIDs)
	}
}

func TestExecRequiresTaskWhenSeveralTasksRun(t *testing.T) {
	rt := newExecTestRuntime()
	agent := newOperationTestAgent(t, rt)
	addExecTestTask(agent, "allocation-g1-web", "web", 1, "running")
	addExecTestTask(agent, "allocation-g1-worker", "worker", 1, "running")

	if _, err := agent.ExecAllocation(context.Background(), "allocation", "", []string{"true"}); !errors.Is(err, ErrExecTaskRequired) {
		t.Fatalf("exec error = %v, want task required", err)
	}
	if _, err := agent.CreateExecSession(context.Background(), "allocation", "", []string{"sh"}, "", 80, 24); !errors.Is(err, ErrExecTaskRequired) {
		t.Fatalf("session error = %v, want task required", err)
	}
	result, err := agent.ExecAllocation(context.Background(), "allocation", "worker", []string{"true"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Stdout != "allocation-g1-worker" {
		t.Fatalf("exec target = %q, want worker", result.Stdout)
	}

	metrics, err := agent.AllocationMetrics(context.Background(), "allocation")
	if err != nil {
		t.Fatal(err)
	}
	if len(metrics) != 2 || metrics[0].Task != "web" || metrics[1].Task != "worker" {
		t.Fatalf("metrics = %#v, want web and worker in order", metrics)
	}

	e := echo.New()
	NewHandler(agent).Register(e)
	body, err := json.Marshal(api.AgentExecRequest{Command: []string{"true"}})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/allocations/allocation/exec", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	e.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
	}
}

func TestStopClosesOnlyStoppedTaskSessionsEvenWhenStopFails(t *testing.T) {
	rt := newExecTestRuntime()
	rt.stopErr = errors.New("stop failed")
	agent := newOperationTestAgent(t, rt)
	addExecTestTask(agent, "allocation-g1-web", "web", 1, "running")
	addExecTestTask(agent, "allocation-g1-worker", "worker", 1, "running")
	for _, task := range []string{"web", "worker"} {
		if _, err := agent.CreateExecSession(context.Background(), "allocation", task, []string{"sh"}, "", 80, 24); err != nil {
			t.Fatal(err)
		}
	}

	if err := agent.StopAllocation(context.Background(), "allocation-g1-web"); !errors.Is(err, rt.stopErr) {
		t.Fatalf("stop error = %v, want %v", err, rt.stopErr)
	}
	if rt.terminal("allocation-g1-web").closeCount() != 1 {
		t.Fatal("stopped task session was not closed after the runtime stop failed")
	}
	if rt.terminal("allocation-g1-worker").closeCount() != 0 {
		t.Fatal("stopping one task closed a sibling task session")
	}
	agent.mu.RLock()
	defer agent.mu.RUnlock()
	if len(agent.execSessions) != 1 {
		t.Fatalf("sessions after stop = %d, want sibling session only", len(agent.execSessions))
	}
	for _, session := range agent.execSessions {
		if session.TaskID != "allocation-g1-worker" {
			t.Fatalf("remaining session belongs to %s", session.TaskID)
		}
	}
}

func TestExecSessionCreatedDuringStopIsClosed(t *testing.T) {
	rt := newExecTestRuntime()
	agent := newOperationTestAgent(t, rt)
	addExecTestTask(agent, "allocation-g1-web", "web", 1, "running")
	rt.onTerminal = func(string) {
		if err := agent.markAllocationStopping("allocation-g1-web"); err != nil {
			t.Error(err)
		}
	}

	if _, err := agent.CreateExecSession(context.Background(), "allocation", "web", []string{"sh"}, "", 80, 24); !errors.Is(err, ErrAllocationNotFound) {
		t.Fatalf("session error = %v, want not found", err)
	}
	if rt.terminal("allocation-g1-web").closeCount() != 1 {
		t.Fatal("terminal started during stop was not closed")
	}
	if len(agent.execSessions) != 0 {
		t.Fatalf("sessions = %d, want none", len(agent.execSessions))
	}
}

func TestReapExecSessionsReleasesExitedAndIdleSessions(t *testing.T) {
	rt := newExecTestRuntime()
	agent := newOperationTestAgent(t, rt)
	addExecTestTask(agent, "allocation-g1-web", "web", 1, "running")
	ids := map[string]string{}
	for _, name := range []string{"exited", "idle", "active"} {
		response, err := agent.CreateExecSession(context.Background(), "allocation", "web", []string{"sh"}, "", 80, 24)
		if err != nil {
			t.Fatal(err)
		}
		ids[name] = response.ID
	}
	exited := agent.execSessions[ids["exited"]].Terminal.(*execTestTerminal)
	exited.mu.Lock()
	exited.exited = true
	exited.mu.Unlock()

	start := time.Now()
	agent.reapExecSessions(context.Background(), start)
	if len(agent.execSessions) != 3 {
		t.Fatalf("sessions after first reap = %d, want 3", len(agent.execSessions))
	}
	if _, err := agent.ReadExecSession("allocation", ids["exited"], 0); err != nil {
		t.Fatalf("exited session output unavailable during retention: %v", err)
	}

	later := start.Add(execSessionIdleTimeout)
	agent.execSessions[ids["active"]].lastActive.Store(execClockNanos(later.Add(-time.Minute)))
	agent.reapExecSessions(context.Background(), later)
	if len(agent.execSessions) != 1 || agent.execSessions[ids["active"]] == nil {
		t.Fatalf("sessions after reap = %v, want only the active session", agent.execSessions)
	}
	idle := rt.terminals["allocation-g1-web"][1]
	if idle.closeCount() != 1 {
		t.Fatal("idle session was not terminated")
	}
	if _, err := agent.ReadExecSession("allocation", ids["exited"], 0); !errors.Is(err, ErrExecSessionNotFound) {
		t.Fatalf("reaped session read error = %v, want not found", err)
	}
}

func TestCloseExecSessionsTerminatesAndRefusesSessions(t *testing.T) {
	rt := newExecTestRuntime()
	agent := newOperationTestAgent(t, rt)
	addExecTestTask(agent, "allocation-g1-web", "web", 1, "running")
	if _, err := agent.CreateExecSession(context.Background(), "allocation", "web", []string{"sh"}, "", 80, 24); err != nil {
		t.Fatal(err)
	}

	agent.CloseExecSessions(context.Background())
	if rt.terminal("allocation-g1-web").closeCount() != 1 || len(agent.execSessions) != 0 {
		t.Fatal("shutdown left exec sessions running")
	}
	if _, err := agent.CreateExecSession(context.Background(), "allocation", "web", []string{"sh"}, "", 80, 24); !errors.Is(err, ErrAgentShuttingDown) {
		t.Fatalf("session after shutdown error = %v, want refusal", err)
	}
	if rt.terminal("allocation-g1-web").closeCount() != 1 || len(agent.execSessions) != 0 {
		t.Fatal("session started during shutdown was not closed")
	}
}

func TestExecRejectsRecordWhoseContainerStopped(t *testing.T) {
	rt := newExecTestRuntime()
	rt.status = runtime.StatusStopped
	agent := newOperationTestAgent(t, rt)
	addExecTestTask(agent, "allocation-g1-web", "web", 1, "running")

	if _, err := agent.ExecAllocation(context.Background(), "allocation", "web", []string{"true"}); !errors.Is(err, ErrAllocationNotFound) {
		t.Fatalf("exec error = %v, want not found", err)
	}
	if _, err := agent.CreateExecSession(context.Background(), "allocation", "web", []string{"sh"}, "", 80, 24); !errors.Is(err, ErrAllocationNotFound) {
		t.Fatalf("session error = %v, want not found", err)
	}
	if len(rt.execTargets) != 0 || len(rt.terminals) != 0 {
		t.Fatal("exec reached a stopped container")
	}
}

func TestAllocationMetricsEmptyWhileCurrentGenerationStarts(t *testing.T) {
	rt := newExecTestRuntime()
	agent := newOperationTestAgent(t, rt)
	addExecTestTask(agent, "allocation-g1-web", "web", 1, "stopping")
	addExecTestTask(agent, "allocation-g2-web", "web", 2, "starting")

	metrics, err := agent.AllocationMetrics(context.Background(), "allocation")
	if err != nil {
		t.Fatal(err)
	}
	if len(metrics) != 0 || len(rt.metricIDs) != 0 {
		t.Fatalf("metrics = %#v from %v, want none", metrics, rt.metricIDs)
	}
	if _, err := agent.AllocationMetrics(context.Background(), "missing"); !errors.Is(err, ErrAllocationNotFound) {
		t.Fatalf("unknown allocation metrics error = %v, want not found", err)
	}
}

func TestStoppingNewerGenerationDoesNotHideRunningGeneration(t *testing.T) {
	rt := newExecTestRuntime()
	agent := newOperationTestAgent(t, rt)
	addExecTestTask(agent, "allocation-g1-web", "web", 1, "running")
	addExecTestTask(agent, "allocation-g2-web", "web", 2, "stopping")

	result, err := agent.ExecAllocation(context.Background(), "allocation", "", []string{"true"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Stdout != "allocation-g1-web" {
		t.Fatalf("exec target = %q, want running generation", result.Stdout)
	}
}

func TestCancelledExecSessionCreateClosesTerminal(t *testing.T) {
	rt := newExecTestRuntime()
	agent := newOperationTestAgent(t, rt)
	addExecTestTask(agent, "allocation-g1-web", "web", 1, "running")
	ctx, cancel := context.WithCancel(context.Background())
	rt.onTerminal = func(string) { cancel() }

	if _, err := agent.CreateExecSession(ctx, "allocation", "web", []string{"sh"}, "", 80, 24); !errors.Is(err, context.Canceled) {
		t.Fatalf("session error = %v, want cancellation", err)
	}
	if rt.terminal("allocation-g1-web").closeCount() != 1 || len(agent.execSessions) != 0 {
		t.Fatal("abandoned session create kept its terminal")
	}
}

func TestFailedExecSessionCloseStaysTrackedForRetry(t *testing.T) {
	rt := newExecTestRuntime()
	agent := newOperationTestAgent(t, rt)
	addExecTestTask(agent, "allocation-g1-web", "web", 1, "running")
	response, err := agent.CreateExecSession(context.Background(), "allocation", "web", []string{"sh"}, "", 80, 24)
	if err != nil {
		t.Fatal(err)
	}
	terminal := rt.terminal("allocation-g1-web")
	terminal.mu.Lock()
	terminal.closeErr = errors.New("kill failed")
	terminal.mu.Unlock()

	if err := agent.CloseExecSession(context.Background(), "allocation", response.ID); err == nil {
		t.Fatal("close succeeded despite kill failure")
	}
	if agent.execSessions[response.ID] == nil {
		t.Fatal("session whose kill failed is no longer tracked")
	}
	if _, err := agent.ReadExecSession("allocation", response.ID, 0); err != nil {
		t.Fatal(err)
	}
	if err := agent.WriteExecSession("allocation", response.ID, []byte("ls\n")); !errors.Is(err, ErrExecSessionNotFound) {
		t.Fatalf("write to closing session error = %v, want not found", err)
	}

	terminal.mu.Lock()
	terminal.closeErr = nil
	terminal.mu.Unlock()
	agent.reapExecSessions(context.Background(), time.Now())
	if agent.execSessions[response.ID] != nil || terminal.closeCount() != 2 {
		t.Fatalf("reaper did not retry the failed close (closes = %d)", terminal.closeCount())
	}
}

func TestFailedCloseAtShutdownIsNotTrackedAgain(t *testing.T) {
	rt := newExecTestRuntime()
	agent := newOperationTestAgent(t, rt)
	addExecTestTask(agent, "allocation-g1-web", "web", 1, "running")
	if _, err := agent.CreateExecSession(context.Background(), "allocation", "web", []string{"sh"}, "", 80, 24); err != nil {
		t.Fatal(err)
	}
	terminal := rt.terminal("allocation-g1-web")
	terminal.mu.Lock()
	terminal.closeErr = errors.New("kill failed")
	terminal.mu.Unlock()

	agent.CloseExecSessions(context.Background())
	if len(agent.execSessions) != 0 {
		t.Fatal("session re-tracked after shutdown")
	}
}
