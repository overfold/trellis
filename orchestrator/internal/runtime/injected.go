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
	"sync"
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

// ExecOutput simulates a successful command and returns empty output.
func (r *InjectedRuntime) ExecOutput(context.Context, string, []string) ([]byte, []byte, int, error) {
	return nil, nil, 0, nil
}

type injectedTerminalSession struct {
	mu       sync.Mutex
	data     []byte
	exited   bool
	exitCode *int
}

func (s *injectedTerminalSession) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.exited {
		return 0, io.ErrClosedPipe
	}
	s.data = append(s.data, p...)
	return len(p), nil
}

func (s *injectedTerminalSession) Read(offset int64) ([]byte, int64, bool, *int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if offset < 0 {
		offset = 0
	}
	if offset > int64(len(s.data)) {
		offset = int64(len(s.data))
	}
	data := append([]byte(nil), s.data[int(offset):]...)
	var code *int
	if s.exitCode != nil {
		value := *s.exitCode
		code = &value
	}
	return data, int64(len(s.data)), s.exited, code, nil
}

func (s *injectedTerminalSession) Resize(context.Context, uint32, uint32) error { return nil }

func (s *injectedTerminalSession) Close(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.exited {
		value := 0
		s.exitCode = &value
		s.exited = true
	}
	return nil
}

// StartTerminal creates an in-memory interactive session for integration tests.
func (r *InjectedRuntime) StartTerminal(_ context.Context, id string, _ []string, _, _ uint32) (TerminalSession, error) {
	r.mu.Lock()
	_, ok := r.state.Containers[id]
	r.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("container %s not found", id)
	}
	return &injectedTerminalSession{}, nil
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
		return nil, fmt.Errorf("container %s not found", id)
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
	b, err := json.Marshal(r.state)
	if err != nil {
		return err
	}
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
