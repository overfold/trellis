package runtime

import (
	"context"
	"io"
)

// ContainerStatus describes the observed state of a container.
type ContainerStatus string

const (
	// StatusCreated indicates that a container has been created.
	StatusCreated ContainerStatus = "created"
	// StatusRunning indicates that a container is running.
	StatusRunning ContainerStatus = "running"
	// StatusStopped indicates that a container has stopped.
	StatusStopped ContainerStatus = "stopped"
	// StatusPaused indicates that a container's task exists but its processes
	// are frozen. Trellis never pauses containers itself.
	StatusPaused ContainerStatus = "paused"
	// StatusUnknown indicates that container state is unavailable.
	StatusUnknown ContainerStatus = "unknown"
)

// CreateOptions configures a new allocation container.
type CreateOptions struct {
	ID               string
	Image            string
	Env              map[string]string
	Mounts           []*Mount
	CPU              int
	Memory           int64
	PidsLimit        int64
	Runtime          string
	NetworkNamespace string
	DNSServers       []string
	ExtraHosts       map[string]string
	Labels           map[string]string
}

// ContainerInfo describes a managed container.
type ContainerInfo struct {
	ID     string            `json:"ID"`
	Status ContainerStatus   `json:"Status"`
	Labels map[string]string `json:"Labels"`
}

// ManagedRuntime is implemented by runtimes that can inventory Trellis-owned
// containers for restart adoption and confirmed orphan collection.
// ListManaged reports a container it cannot fully read with StatusUnknown
// rather than failing the listing or omitting it; such an entry may lack
// labels, in which case its cluster ownership is unknown.
type ManagedRuntime interface {
	ListManaged(ctx context.Context, cluster string) ([]ContainerInfo, error)
}

// ContainerMetrics holds a point-in-time resource usage snapshot for a container.
type ContainerMetrics struct {
	// CPUUsageNanoseconds is the cumulative CPU time consumed by the container.
	CPUUsageNanoseconds int64
	// MemoryUsageBytes is the current memory footprint of the container.
	MemoryUsageBytes int64
}

// ExecOptions configures a streamed exec process.
type ExecOptions struct {
	Command []string
	// TTY allocates a terminal; its output is written to Stdout.
	TTY bool
	// Term, when set, replaces TERM in a TTY process's environment.
	Term string
	// Cols and Rows are the initial size of a TTY. Zero keeps the default.
	Cols uint32
	Rows uint32
	// Stdin supplies process input until it returns io.EOF, which closes the
	// process's input. A nil Stdin gives the process no input.
	Stdin io.Reader
	// Stdout and Stderr receive process output. A write that blocks stalls
	// the process's output rather than buffering it. Stderr is unused with
	// TTY; a nil writer discards its stream.
	Stdout io.Writer
	Stderr io.Writer
}

// ExecProcess is a running process started by StartExec.
type ExecProcess interface {
	// Resize changes the terminal size of a TTY process.
	Resize(ctx context.Context, cols, rows uint32) error
	// Done is closed once the process has exited and its output has been
	// delivered, or its delivery abandoned after a bounded wait.
	Done() <-chan struct{}
	// ExitCode returns the process's exit status after Done is closed.
	ExitCode() (int, error)
	// Kill terminates the process. It is safe to call after the process
	// exited and more than once.
	Kill(ctx context.Context) error
}

// ContainerRuntime defines the operations required by an allocation runtime.
type ContainerRuntime interface {
	Pull(ctx context.Context, image string) error
	Create(ctx context.Context, options CreateOptions) (string, error)
	Start(ctx context.Context, containerID string) error
	Restart(ctx context.Context, containerID string) error
	Stop(ctx context.Context, containerID string) error
	Remove(ctx context.Context, containerID string) error
	Exec(ctx context.Context, containerID string, command []string) (int, error)
	// StartExec starts a process in a running container with the task's OCI
	// process context. The process outlives ctx; the caller ends it with Kill.
	StartExec(ctx context.Context, containerID string, options ExecOptions) (ExecProcess, error)
	// Metrics returns a point-in-time resource usage snapshot for a container.
	Metrics(ctx context.Context, containerID string) (*ContainerMetrics, error)
	Inspect(ctx context.Context, containerID string) (*ContainerInfo, error)
	Logs(ctx context.Context, containerID string, follow bool, tail int) (io.ReadCloser, error)
}
