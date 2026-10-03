package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/internal/client"
	"github.com/overfold/trellis/orchestrator/internal/execstream"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	"github.com/overfold/trellis/orchestrator/internal/runtime"
	"github.com/overfold/trellis/orchestrator/internal/transport"
)

// execTestProcess copies stdin to stdout and exits with exitCode when stdin
// ends. Without stdin it runs until killed. Resizes are recorded.
type execTestProcess struct {
	options  runtime.ExecOptions
	exitCode int
	done     chan struct{}
	once     sync.Once
	code     int

	mu      sync.Mutex
	kills   int
	killErr error
	resizes []api.ExecResize
}

func (p *execTestProcess) run() {
	if p.options.Stdin == nil {
		return
	}
	_, _ = io.Copy(p.options.Stdout, p.options.Stdin)
	p.exit(p.exitCode)
}

func (p *execTestProcess) exit(code int) {
	p.once.Do(func() {
		p.code = code
		close(p.done)
	})
}

func (p *execTestProcess) Done() <-chan struct{} { return p.done }

func (p *execTestProcess) ExitCode() (int, error) {
	<-p.done
	return p.code, nil
}

func (p *execTestProcess) Resize(_ context.Context, cols, rows uint32) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.resizes = append(p.resizes, api.ExecResize{Cols: cols, Rows: rows})
	return nil
}

func (p *execTestProcess) Kill(context.Context) error {
	p.mu.Lock()
	p.kills++
	err := p.killErr
	p.mu.Unlock()
	if err != nil {
		return err
	}
	p.exit(137)
	return nil
}

func (p *execTestProcess) setKillErr(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.killErr = err
}

func (p *execTestProcess) killCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.kills
}

func (p *execTestProcess) resized() []api.ExecResize {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]api.ExecResize(nil), p.resizes...)
}

type execTestRuntime struct {
	*failingStopRuntime
	mu        sync.Mutex
	metricIDs []string
	processes map[string][]*execTestProcess
	startErr  error
	exitCode  int
	onStart   func(containerID string)
}

func newExecTestRuntime() *execTestRuntime {
	return &execTestRuntime{
		failingStopRuntime: &failingStopRuntime{reconcilerRuntime: &reconcilerRuntime{status: runtime.StatusRunning}},
		processes:          map[string][]*execTestProcess{},
	}
}

func (r *execTestRuntime) StartExec(_ context.Context, containerID string, options runtime.ExecOptions) (runtime.ExecProcess, error) {
	if r.onStart != nil {
		r.onStart(containerID)
	}
	if r.startErr != nil {
		return nil, r.startErr
	}
	r.mu.Lock()
	process := &execTestProcess{options: options, exitCode: r.exitCode, done: make(chan struct{})}
	r.processes[containerID] = append(r.processes[containerID], process)
	r.mu.Unlock()
	go process.run()
	return process, nil
}

func (r *execTestRuntime) Metrics(_ context.Context, containerID string) (*runtime.ContainerMetrics, error) {
	r.mu.Lock()
	r.metricIDs = append(r.metricIDs, containerID)
	r.mu.Unlock()
	return &runtime.ContainerMetrics{}, nil
}

func (r *execTestRuntime) process(t *testing.T, containerID string) *execTestProcess {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		r.mu.Lock()
		processes := r.processes[containerID]
		r.mu.Unlock()
		if len(processes) > 0 {
			return processes[len(processes)-1]
		}
		if time.Now().After(deadline) {
			t.Fatalf("no exec process started in %s", containerID)
		}
		time.Sleep(time.Millisecond)
	}
}

func (r *execTestRuntime) processCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	count := 0
	for _, processes := range r.processes {
		count += len(processes)
	}
	return count
}

func addExecTestTask(agent *Agent, id, taskName string, generation uint64, status string) *Allocation {
	alloc := &Allocation{ID: id, ContainerID: id, AllocationID: "allocation", Generation: generation, TaskName: taskName, Status: status}
	agent.allocations[id] = alloc
	return alloc
}

func addExecTestAllocation(agent *Agent, allocID string) {
	id := allocID + "-web"
	agent.allocations[id] = &Allocation{ID: id, ContainerID: id, AllocationID: allocID, Generation: 1, TaskName: "web", Status: "running"}
}

func execTestRequest(task string, command ...string) nodeapi.AgentExecRequest {
	return nodeapi.AgentExecRequest{ExecRequest: api.ExecRequest{Task: task, Command: command}, Epoch: 1}
}

// shortenExecTiming makes an agent's stream checks fast. It must be called
// before the agent serves a stream.
func shortenExecTiming(agent *Agent) {
	agent.execTiming.checkInterval = 10 * time.Millisecond
	agent.execTiming.killWait = 10 * time.Millisecond
	agent.execTiming.killRetryInterval = 10 * time.Millisecond
}

// execTestStream is the leader's side of an agent exec stream.
type execTestStream struct {
	t      *testing.T
	conn   io.ReadWriteCloser
	reader *execstream.Reader
	writer *execstream.Writer
}

func serveExecTestAgent(t *testing.T, agent *Agent) string {
	t.Helper()
	e := echo.New()
	NewHandler(agent).Register(e)
	server := httptest.NewServer(e)
	t.Cleanup(server.Close)
	return server.URL
}

func openExecTestStream(t *testing.T, address, allocID string, request nodeapi.AgentExecRequest) *execTestStream {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	conn, err := client.NewAgentClient("", nil).Exec(ctx, uuid.Nil, address, allocID, request)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &execTestStream{t: t, conn: conn, reader: execstream.NewReader(conn), writer: execstream.NewWriter(conn, 0)}
}

func (s *execTestStream) next() execstream.Frame {
	s.t.Helper()
	type result struct {
		frame execstream.Frame
		err   error
	}
	results := make(chan result, 1)
	go func() {
		frame, err := s.reader.Next()
		frame.Payload = append([]byte(nil), frame.Payload...)
		results <- result{frame: frame, err: err}
	}()
	select {
	case result := <-results:
		if result.err != nil {
			s.t.Fatalf("read frame: %v", result.err)
		}
		return result.frame
	case <-time.After(5 * time.Second):
		s.t.Fatal("timed out waiting for a frame")
		return execstream.Frame{}
	}
}

func (s *execTestStream) expectError(want string) {
	s.t.Helper()
	frame := s.next()
	var streamErr api.ExecStreamError
	if frame.Type != execstream.FrameError || json.Unmarshal(frame.Payload, &streamErr) != nil || !strings.Contains(streamErr.Message, want) {
		s.t.Fatalf("frame = %d %q, want error containing %q", frame.Type, frame.Payload, want)
	}
}

func waitForExec(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func (a *Agent) execSlots() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.execSessionCount
}

func (a *Agent) execSessionTotal() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.execSessions)
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

	for range 20 {
		reservation, err := agent.ReserveExec(context.Background(), "allocation", execTestRequest("", "true"))
		if err != nil {
			t.Fatal(err)
		}
		if reservation.target.ID != "allocation-g2-web" {
			t.Fatalf("exec target = %q, want current-generation running task", reservation.target.ID)
		}
		agent.ReleaseExec(reservation)
	}
	for _, task := range []string{"worker", "sidecar", "proxy", "missing"} {
		if _, err := agent.ReserveExec(context.Background(), "allocation", execTestRequest(task, "true")); !errors.Is(err, ErrAllocationNotFound) {
			t.Fatalf("exec task %s error = %v, want not found", task, err)
		}
	}
	if agent.execSlots() != 0 {
		t.Fatalf("rejected reservations hold %d slots", agent.execSlots())
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

	if _, err := agent.ReserveExec(context.Background(), "allocation", execTestRequest("", "true")); !errors.Is(err, ErrExecTaskRequired) {
		t.Fatalf("exec error = %v, want task required", err)
	}
	reservation, err := agent.ReserveExec(context.Background(), "allocation", execTestRequest("worker", "true"))
	if err != nil {
		t.Fatal(err)
	}
	if reservation.target.ID != "allocation-g1-worker" {
		t.Fatalf("exec target = %q, want worker", reservation.target.ID)
	}
	agent.ReleaseExec(reservation)

	metrics, err := agent.AllocationMetrics(context.Background(), "allocation")
	if err != nil {
		t.Fatal(err)
	}
	if len(metrics) != 2 || metrics[0].Task != "web" || metrics[1].Task != "worker" {
		t.Fatalf("metrics = %#v, want web and worker in order", metrics)
	}

	_, err = client.NewAgentClient("", nil).Exec(context.Background(), uuid.Nil, serveExecTestAgent(t, agent), "allocation", execTestRequest("", "true"))
	var httpErr *transport.HTTPError
	if !errors.As(err, &httpErr) || httpErr.Status != http.StatusBadRequest {
		t.Fatalf("exec error = %v, want 400", err)
	}
}

func TestExecRequestErrors(t *testing.T) {
	rt := newExecTestRuntime()
	agent := newOperationTestAgent(t, rt)
	addExecTestTask(agent, "allocation-g1-web", "web", 1, "running")
	if err := agent.AcceptEpoch(5); err != nil {
		t.Fatal(err)
	}
	e := echo.New()
	NewHandler(agent).Register(e)

	tests := []struct {
		name    string
		target  string
		upgrade bool
		want    int
	}{
		{name: "not an upgrade", target: "/v1/allocations/allocation/exec?command=sh&epoch=5", want: http.StatusBadRequest},
		{name: "missing command", target: "/v1/allocations/allocation/exec?epoch=5", upgrade: true, want: http.StatusBadRequest},
		{name: "missing epoch", target: "/v1/allocations/allocation/exec?command=sh", upgrade: true, want: http.StatusBadRequest},
		{name: "terminal size without tty", target: "/v1/allocations/allocation/exec?command=sh&epoch=5&cols=80", upgrade: true, want: http.StatusBadRequest},
		{name: "oversized terminal", target: "/v1/allocations/allocation/exec?command=sh&epoch=5&tty=true&cols=1001", upgrade: true, want: http.StatusBadRequest},
		{name: "stale epoch", target: "/v1/allocations/allocation/exec?command=sh&epoch=4", upgrade: true, want: http.StatusConflict},
		{name: "unknown allocation", target: "/v1/allocations/missing/exec?command=sh&epoch=5", upgrade: true, want: http.StatusNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, tt.target, nil)
			if tt.upgrade {
				execstream.SetUpgradeHeaders(request)
			}
			recorder := httptest.NewRecorder()
			e.ServeHTTP(recorder, request)
			if recorder.Code != tt.want {
				t.Fatalf("status = %d, want %d; body = %s", recorder.Code, tt.want, recorder.Body.String())
			}
		})
	}
	if rt.processCount() != 0 || agent.execSlots() != 0 {
		t.Fatalf("rejected requests started %d processes and hold %d slots", rt.processCount(), agent.execSlots())
	}
}

func TestExecStreamCarriesInputResizeOutputAndExitStatus(t *testing.T) {
	rt := newExecTestRuntime()
	rt.exitCode = 3
	agent := newOperationTestAgent(t, rt)
	addExecTestTask(agent, "allocation-g1-web", "web", 1, "running")
	request := execTestRequest("web", "sh")
	request.TTY, request.Stdin, request.Term, request.Cols, request.Rows = true, true, "xterm", 100, 40
	stream := openExecTestStream(t, serveExecTestAgent(t, agent), "allocation", request)

	process := rt.process(t, "allocation-g1-web")
	if got := process.options; !got.TTY || got.Term != "xterm" || got.Cols != 100 || got.Rows != 40 || strings.Join(got.Command, " ") != "sh" {
		t.Fatalf("process options = %+v", got)
	}
	if err := stream.writer.WriteData(execstream.FrameStdin, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if frame := stream.next(); frame.Type != execstream.FrameStdout || string(frame.Payload) != "hello" {
		t.Fatalf("frame = %d %q, want stdout echo", frame.Type, frame.Payload)
	}
	if err := stream.writer.WriteJSON(execstream.FrameResize, api.ExecResize{Cols: 120, Rows: 50}); err != nil {
		t.Fatal(err)
	}
	waitForExec(t, "resize", func() bool { return len(process.resized()) == 1 })
	if got := process.resized()[0]; got.Cols != 120 || got.Rows != 50 {
		t.Fatalf("resize = %+v", got)
	}
	if err := stream.writer.WriteFrame(execstream.FrameStdinClose, nil); err != nil {
		t.Fatal(err)
	}
	frame := stream.next()
	var exit api.ExecExit
	if frame.Type != execstream.FrameExit || json.Unmarshal(frame.Payload, &exit) != nil || exit.ExitCode != 3 {
		t.Fatalf("frame = %d %q, want exit status 3", frame.Type, frame.Payload)
	}
	waitForExec(t, "session release", func() bool { return agent.execSlots() == 0 && agent.execSessionTotal() == 0 })
	if process.killCount() != 0 {
		t.Fatal("exited process was killed")
	}
}

func TestExecClientDisconnectKillsProcess(t *testing.T) {
	rt := newExecTestRuntime()
	agent := newOperationTestAgent(t, rt)
	addExecTestTask(agent, "allocation-g1-web", "web", 1, "running")
	request := execTestRequest("web", "sh")
	request.Stdin = true
	stream := openExecTestStream(t, serveExecTestAgent(t, agent), "allocation", request)
	process := rt.process(t, "allocation-g1-web")
	waitForExec(t, "session registration", func() bool { return agent.execSessionTotal() == 1 })

	_ = stream.conn.Close()
	waitForExec(t, "process kill", func() bool { return process.killCount() == 1 })
	waitForExec(t, "session release", func() bool { return agent.execSlots() == 0 && agent.execSessionTotal() == 0 })
}

func TestExecStreamRejectsInvalidClientFrames(t *testing.T) {
	tests := []struct {
		name  string
		stdin bool
		tty   bool
		send  func(*execstream.Writer) error
		want  string
	}{
		{name: "input without stdin", send: func(w *execstream.Writer) error { return w.WriteData(execstream.FrameStdin, []byte("x")) }, want: "without an open stdin"},
		{name: "resize without tty", stdin: true, send: func(w *execstream.Writer) error {
			return w.WriteJSON(execstream.FrameResize, api.ExecResize{Cols: 1, Rows: 1})
		}, want: "without a tty"},
		{name: "zero resize", stdin: true, tty: true, send: func(w *execstream.Writer) error {
			return w.WriteJSON(execstream.FrameResize, api.ExecResize{})
		}, want: "terminal dimensions"},
		{name: "output from client", stdin: true, send: func(w *execstream.Writer) error { return w.WriteData(execstream.FrameStdout, []byte("x")) }, want: "unexpected frame type"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rt := newExecTestRuntime()
			agent := newOperationTestAgent(t, rt)
			addExecTestTask(agent, "allocation-g1-web", "web", 1, "running")
			request := execTestRequest("web", "sh")
			request.Stdin, request.TTY = tt.stdin, tt.tty
			stream := openExecTestStream(t, serveExecTestAgent(t, agent), "allocation", request)
			process := rt.process(t, "allocation-g1-web")
			waitForExec(t, "session registration", func() bool { return agent.execSessionTotal() == 1 })
			if err := tt.send(stream.writer); err != nil {
				t.Fatal(err)
			}
			stream.expectError(tt.want)
			if process.killCount() != 1 {
				t.Fatalf("kills = %d, want 1", process.killCount())
			}
		})
	}
}

func TestExecStreamEndsOnIdleLifetimeAndNewerEpoch(t *testing.T) {
	tests := []struct {
		name   string
		timing func(*execTiming)
		fence  bool
		want   string
	}{
		{name: "idle", timing: func(timing *execTiming) { timing.idleTimeout = 50 * time.Millisecond }, want: "without activity"},
		{name: "lifetime", timing: func(timing *execTiming) { timing.maxLifetime = 50 * time.Millisecond }, want: "maximum lifetime"},
		{name: "newer epoch", fence: true, want: "leadership changed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rt := newExecTestRuntime()
			agent := newOperationTestAgent(t, rt)
			shortenExecTiming(agent)
			if tt.timing != nil {
				tt.timing(&agent.execTiming)
			}
			addExecTestTask(agent, "allocation-g1-web", "web", 1, "running")
			stream := openExecTestStream(t, serveExecTestAgent(t, agent), "allocation", execTestRequest("web", "sh"))
			process := rt.process(t, "allocation-g1-web")
			if tt.fence {
				if err := agent.AcceptEpoch(2); err != nil {
					t.Fatal(err)
				}
			}
			stream.expectError(tt.want)
			if process.killCount() != 1 {
				t.Fatalf("kills = %d, want 1", process.killCount())
			}
			waitForExec(t, "session release", func() bool { return agent.execSlots() == 0 })
		})
	}
}

func TestStopEndsOnlyStoppedTaskSessionsEvenWhenStopFails(t *testing.T) {
	rt := newExecTestRuntime()
	rt.stopErr = errors.New("stop failed")
	agent := newOperationTestAgent(t, rt)
	addExecTestTask(agent, "allocation-g1-web", "web", 1, "running")
	addExecTestTask(agent, "allocation-g1-worker", "worker", 1, "running")
	address := serveExecTestAgent(t, agent)
	web := openExecTestStream(t, address, "allocation", execTestRequest("web", "sh"))
	openExecTestStream(t, address, "allocation", execTestRequest("worker", "sh"))
	waitForExec(t, "session registration", func() bool { return agent.execSessionTotal() == 2 })

	if err := agent.StopAllocation(context.Background(), "allocation-g1-web"); !errors.Is(err, rt.stopErr) {
		t.Fatalf("stop error = %v, want %v", err, rt.stopErr)
	}
	if rt.process(t, "allocation-g1-web").killCount() != 1 {
		t.Fatal("stopped task session was not killed after the runtime stop failed")
	}
	web.expectError("task stopped")
	if rt.process(t, "allocation-g1-worker").killCount() != 0 {
		t.Fatal("stopping one task killed a sibling task session")
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

func TestExecSessionStartedDuringStopIsKilled(t *testing.T) {
	rt := newExecTestRuntime()
	agent := newOperationTestAgent(t, rt)
	addExecTestTask(agent, "allocation-g1-web", "web", 1, "running")
	rt.onStart = func(string) {
		if err := agent.markAllocationStopping("allocation-g1-web"); err != nil {
			t.Error(err)
		}
	}

	stream := openExecTestStream(t, serveExecTestAgent(t, agent), "allocation", execTestRequest("web", "sh"))
	stream.expectError("task stopped")
	if rt.process(t, "allocation-g1-web").killCount() != 1 {
		t.Fatal("process started during stop was not killed")
	}
	waitForExec(t, "session release", func() bool { return agent.execSlots() == 0 && agent.execSessionTotal() == 0 })
}

func TestExecSessionPerAllocationLimitReturnsTooManyRequests(t *testing.T) {
	rt := newExecTestRuntime()
	agent := newOperationTestAgent(t, rt)
	addExecTestTask(agent, "allocation-g1-web", "web", 1, "running")
	for i := range execSessionPerAllocationLimit {
		if _, err := agent.ReserveExec(context.Background(), "allocation", execTestRequest("web", "sh")); err != nil {
			t.Fatalf("reserve session %d: %v", i, err)
		}
	}

	_, err := client.NewAgentClient("", nil).Exec(context.Background(), uuid.Nil, serveExecTestAgent(t, agent), "allocation", execTestRequest("web", "sh"))
	var httpErr *transport.HTTPError
	if !errors.As(err, &httpErr) || httpErr.Status != http.StatusTooManyRequests {
		t.Fatalf("exec error = %v, want 429", err)
	}
	if !strings.Contains(httpErr.Message(), "allocation allocation has 8 exec sessions (maximum 8)") {
		t.Fatalf("overload response is not actionable: %s", httpErr.Message())
	}
}

func TestFailedExecStartReleasesCapacity(t *testing.T) {
	rt := newExecTestRuntime()
	rt.startErr = errors.New("executable file not found")
	agent := newOperationTestAgent(t, rt)
	addExecTestTask(agent, "allocation-g1-web", "web", 1, "running")

	stream := openExecTestStream(t, serveExecTestAgent(t, agent), "allocation", execTestRequest("web", "missing"))
	stream.expectError("start exec in task web: executable file not found")
	waitForExec(t, "slot release", func() bool { return agent.execSlots() == 0 && len(agent.execSessionsByAllocation) == 0 })
}

func TestExecSessionGlobalLimitIsAtomicAndFailedKillsRetainCapacity(t *testing.T) {
	rt := newExecTestRuntime()
	agent := newOperationTestAgent(t, rt)
	shortenExecTiming(agent)
	for i := 0; i <= execSessionGlobalLimit/execSessionPerAllocationLimit; i++ {
		addExecTestAllocation(agent, fmt.Sprintf("allocation-%d", i))
	}

	results := make(chan error, execSessionGlobalLimit+1)
	reservations := make(chan *ExecReservation, execSessionGlobalLimit+1)
	var wg sync.WaitGroup
	for i := range execSessionGlobalLimit + 1 {
		allocation := fmt.Sprintf("allocation-%d", i/execSessionPerAllocationLimit)
		wg.Go(func() {
			reservation, err := agent.ReserveExec(context.Background(), allocation, execTestRequest("web", "sh"))
			if err == nil {
				reservations <- reservation
			}
			results <- err
		})
	}
	wg.Wait()
	close(results)
	close(reservations)
	rejected := 0
	for err := range results {
		if err == nil {
			continue
		}
		if !errors.Is(err, ErrExecSessionLimit) || !strings.Contains(err.Error(), "node has 64 exec sessions (maximum 64)") {
			t.Fatalf("unexpected concurrent reserve error: %v", err)
		}
		rejected++
	}
	if rejected != 1 || agent.execSlots() != execSessionGlobalLimit {
		t.Fatalf("rejected = %d, reserved = %d; want 1, 64", rejected, agent.execSlots())
	}
	for reservation := range reservations {
		agent.ReleaseExec(reservation)
	}

	// A process that survives its kill keeps its slot until it exits.
	address := serveExecTestAgent(t, agent)
	stream := openExecTestStream(t, address, "allocation-0", nodeapi.AgentExecRequest{ExecRequest: api.ExecRequest{Command: []string{"sh"}}, Epoch: 1})
	process := rt.process(t, "allocation-0-web")
	process.setKillErr(errors.New("persistent kill failure"))
	waitForExec(t, "session registration", func() bool { return agent.execSessionTotal() == 1 })
	_ = stream.conn.Close()
	waitForExec(t, "kill retries", func() bool { return process.killCount() >= 3 })
	if agent.execSlots() != 1 {
		t.Fatalf("reserved = %d after failed kills, want 1", agent.execSlots())
	}
	process.setKillErr(nil)
	waitForExec(t, "slot release", func() bool { return agent.execSlots() == 0 && len(agent.execSessionsByAllocation) == 0 })
}

func TestCloseExecSessionsEndsAndRefusesSessions(t *testing.T) {
	rt := newExecTestRuntime()
	agent := newOperationTestAgent(t, rt)
	addExecTestTask(agent, "allocation-g1-web", "web", 1, "running")
	stream := openExecTestStream(t, serveExecTestAgent(t, agent), "allocation", execTestRequest("web", "sh"))
	waitForExec(t, "session registration", func() bool { return agent.execSessionTotal() == 1 })

	agent.CloseExecSessions(context.Background())
	if rt.process(t, "allocation-g1-web").killCount() != 1 || agent.execSessionTotal() != 0 {
		t.Fatal("shutdown left exec sessions running")
	}
	stream.expectError("shutting down")
	if _, err := agent.ReserveExec(context.Background(), "allocation", execTestRequest("web", "sh")); !errors.Is(err, ErrAgentShuttingDown) {
		t.Fatalf("session after shutdown error = %v, want refusal", err)
	}
}

func TestExecRejectsRecordWhoseContainerStopped(t *testing.T) {
	rt := newExecTestRuntime()
	rt.status = runtime.StatusStopped
	agent := newOperationTestAgent(t, rt)
	addExecTestTask(agent, "allocation-g1-web", "web", 1, "running")

	if _, err := agent.ReserveExec(context.Background(), "allocation", execTestRequest("web", "true")); !errors.Is(err, ErrAllocationNotFound) {
		t.Fatalf("exec error = %v, want not found", err)
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

	reservation, err := agent.ReserveExec(context.Background(), "allocation", execTestRequest("", "true"))
	if err != nil {
		t.Fatal(err)
	}
	defer agent.ReleaseExec(reservation)
	if reservation.target.ID != "allocation-g1-web" {
		t.Fatalf("exec target = %q, want running generation", reservation.target.ID)
	}
}
