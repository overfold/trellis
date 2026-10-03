package runtime

// The injected runtime is a deliberately small, durable runtime used by the
// multi-process integration suite. Unlike a mock wired into the server, it runs
// behind the real agent HTTP API and persists its inventory across node
// restarts, which exercises adoption and ambiguous operation recovery.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/containerd/errdefs"
)

type injectedState struct {
	Containers map[string]ContainerInfo `json:"containers"`
}

// InjectedRuntime is a persistent test runtime with injectable failures.
type InjectedRuntime struct {
	mu        sync.Mutex
	path      string
	faultPath string
	state     injectedState
}

// InjectedFault is written atomically to the fault file by tests. Before
// returns an error without applying the operation; After applies it and then
// returns an error, modelling a lost/ambiguous response. Count is consumed and
// persisted so a process restart cannot replay a one-shot fault.
type InjectedFault struct {
	Operation string `json:"operation"`
	Timing    string `json:"timing"` // "before" or "after"
	Count     int    `json:"count"`
}

// NewInjectedRuntime opens or creates a persistent injected runtime.
func NewInjectedRuntime(path, faultPath string) (*InjectedRuntime, error) {
	r := &InjectedRuntime{path: path, faultPath: faultPath, state: injectedState{Containers: map[string]ContainerInfo{}}}
	b, err := os.ReadFile(path)
	if err == nil {
		if err := json.Unmarshal(b, &r.state); err != nil {
			return nil, fmt.Errorf("decode injected runtime state: %w", err)
		}
		if r.state.Containers == nil {
			r.state.Containers = map[string]ContainerInfo{}
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return r, nil
}

// Close releases the injected runtime.
func (r *InjectedRuntime) Close() error { return nil }

// Pull records an injected image-pull operation.
func (r *InjectedRuntime) Pull(context.Context, string) error { return r.operation("pull", nil) }

// Create records a container in the injected runtime.
func (r *InjectedRuntime) Create(_ context.Context, o CreateOptions) (string, error) {
	id := o.ID
	err := r.operation("create", func() error {
		if _, ok := r.state.Containers[id]; ok {
			return fmt.Errorf("container %s already exists", id)
		}
		r.state.Containers[id] = ContainerInfo{ID: id, Status: StatusCreated, Labels: cloneLabels(o.Labels)}
		return nil
	})
	return id, err
}

// Start marks a container as running.
func (r *InjectedRuntime) Start(_ context.Context, id string) error {
	return r.setStatus("start", id, StatusRunning)
}

// Restart marks a container as running after an injected restart.
func (r *InjectedRuntime) Restart(_ context.Context, id string) error {
	return r.setStatus("restart", id, StatusRunning)
}

// Stop marks a container as stopped.
func (r *InjectedRuntime) Stop(_ context.Context, id string) error {
	return r.setStatus("stop", id, StatusStopped)
}

// Pause marks a container as paused, modelling an out-of-band pause; Trellis
// itself never pauses containers.
func (r *InjectedRuntime) Pause(_ context.Context, id string) error {
	return r.setStatus("pause", id, StatusPaused)
}

// Remove deletes a container from injected state.
func (r *InjectedRuntime) Remove(_ context.Context, id string) error {
	return r.operation("remove", func() error {
		if _, ok := r.state.Containers[id]; !ok {
			return fmt.Errorf("container %s not found", id)
		}
		delete(r.state.Containers, id)
		return nil
	})
}
func (r *InjectedRuntime) setStatus(op, id string, status ContainerStatus) error {
	return r.operation(op, func() error {
		c, ok := r.state.Containers[id]
		if !ok {
			return fmt.Errorf("container %s not found", id)
		}
		c.Status = status
		r.state.Containers[id] = c
		return nil
	})
}

// Exec simulates a successful container command.
func (r *InjectedRuntime) Exec(context.Context, string, []string) (int, error) { return 0, nil }

// injectedExecProcess is an in-memory exec process. Its command selects a
// small scripted behaviour so streams can be tested without containerd:
//
//   - echo ARGS... writes ARGS and a newline to stdout and exits 0.
//   - stderr ARGS... writes ARGS and a newline to stderr (stdout with a TTY)
//     and exits 0.
//   - exit CODE exits with CODE.
//   - anything else copies stdin to stdout until stdin ends, then exits 0.
//
// With a TTY, each resize is reported on stdout as "[resize COLSxROWS]".
type injectedExecProcess struct {
	options ExecOptions
	done    chan struct{}
	once    sync.Once
	code    int
}

func (p *injectedExecProcess) Done() <-chan struct{} { return p.done }

func (p *injectedExecProcess) ExitCode() (int, error) {
	<-p.done
	return p.code, nil
}

func (p *injectedExecProcess) exit(code int) {
	p.once.Do(func() {
		p.code = code
		close(p.done)
	})
}

func (p *injectedExecProcess) Resize(_ context.Context, cols, rows uint32) error {
	if cols == 0 || rows == 0 {
		return fmt.Errorf("terminal dimensions must be greater than zero")
	}
	if p.options.TTY {
		_, _ = fmt.Fprintf(p.options.Stdout, "[resize %dx%d]", cols, rows)
	}
	return nil
}

func (p *injectedExecProcess) Kill(context.Context) error {
	p.exit(137)
	return nil
}

func (p *injectedExecProcess) run() {
	command := p.options.Command
	args := strings.Join(command[1:], " ")
	switch command[0] {
	case "echo":
		_, _ = io.WriteString(p.options.Stdout, args+"\n")
		p.exit(0)
	case "stderr":
		target := p.options.Stderr
		if p.options.TTY {
			target = p.options.Stdout
		}
		_, _ = io.WriteString(target, args+"\n")
		p.exit(0)
	case "exit":
		code, err := strconv.Atoi(args)
		if err != nil {
			code = 2
		}
		p.exit(code)
	default:
		if p.options.Stdin != nil {
			_, _ = io.Copy(p.options.Stdout, p.options.Stdin)
		}
		p.exit(0)
	}
}

// StartExec starts an in-memory scripted process for integration tests.
func (r *InjectedRuntime) StartExec(_ context.Context, id string, options ExecOptions) (ExecProcess, error) {
	if len(options.Command) == 0 {
		return nil, fmt.Errorf("exec command is required")
	}
	r.mu.Lock()
	_, ok := r.state.Containers[id]
	r.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("container %s not found", id)
	}
	if options.Stdout == nil {
		options.Stdout = io.Discard
	}
	if options.Stderr == nil {
		options.Stderr = io.Discard
	}
	process := &injectedExecProcess{options: options, done: make(chan struct{})}
	go process.run()
	return process, nil
}

// Metrics returns zero metrics for an injected container.
func (r *InjectedRuntime) Metrics(context.Context, string) (*ContainerMetrics, error) {
	return &ContainerMetrics{}, nil
}

// Inspect returns a copy of injected container state.
func (r *InjectedRuntime) Inspect(_ context.Context, id string) (*ContainerInfo, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.state.Containers[id]
	if !ok {
		return nil, fmt.Errorf("container %s: %w", id, errdefs.ErrNotFound)
	}
	result := c
	result.Labels = cloneLabels(c.Labels)
	return &result, nil
}

// Logs returns an empty log stream.
func (r *InjectedRuntime) Logs(context.Context, string, bool, int) (io.ReadCloser, error) {
	return io.NopCloser(&emptyReader{}), nil
}

// ListManaged lists injected containers owned by a cluster.
func (r *InjectedRuntime) ListManaged(_ context.Context, cluster string) ([]ContainerInfo, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := []ContainerInfo{}
	for _, c := range r.state.Containers {
		if cluster == "" || c.Labels["trellis.cluster"] == cluster {
			c.Labels = cloneLabels(c.Labels)
			out = append(out, c)
		}
	}
	return out, nil
}

type emptyReader struct{}

func (*emptyReader) Read([]byte) (int, error) { return 0, io.EOF }

func (r *InjectedRuntime) operation(op string, apply func() error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	f := r.consumeFault(op)
	if f == "before" {
		return fmt.Errorf("injected ambiguous %s failure before effect", op)
	}
	if apply != nil {
		if err := apply(); err != nil {
			return err
		}
	}
	if err := r.save(); err != nil {
		return err
	}
	if f == "after" {
		return fmt.Errorf("injected ambiguous %s failure after effect", op)
	}
	return nil
}
func (r *InjectedRuntime) consumeFault(op string) string {
	if r.faultPath == "" {
		return ""
	}
	b, err := os.ReadFile(r.faultPath)
	if err != nil {
		return ""
	}
	var f InjectedFault
	if json.Unmarshal(b, &f) != nil || f.Operation != op || f.Count <= 0 {
		return ""
	}
	f.Count--
	b, _ = json.Marshal(f)
	_ = os.WriteFile(r.faultPath, b, 0o600)
	return f.Timing
}
func (r *InjectedRuntime) save() error {
	if err := os.MkdirAll(filepath.Dir(r.path), 0o750); err != nil {
		return err
	}
	b, _ := json.Marshal(r.state)
	tmp := r.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, r.path)
}
func cloneLabels(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
