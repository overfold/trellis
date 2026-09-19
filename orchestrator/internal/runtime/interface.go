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
	Runtime          string
	NetworkNamespace string
	DNSServers       []string
	ExtraHosts       map[string]string
	Labels           map[string]string
}

// ContainerInfo describes a managed container.
type ContainerInfo struct {
	ID     string
	Status ContainerStatus
	Labels map[string]string
}

// ManagedRuntime is implemented by runtimes that can inventory Trellis-owned
// containers for restart adoption and confirmed orphan collection.
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

// TerminalSession is a live TTY-backed process running inside a container.
// Implementations must support concurrent output reads and input writes.
type TerminalSession interface {
	Write([]byte) (int, error)
	Read(offset int64) (data []byte, nextOffset int64, exited bool, exitCode *int, err error)
	Resize(ctx context.Context, cols, rows uint32) error
	Close(ctx context.Context) error
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
	// ExecOutput runs a command in a container and returns its stdout, stderr,
	// and exit code. The command must not require a terminal.
	ExecOutput(ctx context.Context, containerID string, command []string) (stdout []byte, stderr []byte, exitCode int, err error)
	// StartTerminal starts an interactive TTY-backed process inside a container.
	StartTerminal(ctx context.Context, containerID string, command []string, term string, cols, rows uint32) (TerminalSession, error)
	// Metrics returns a point-in-time resource usage snapshot for a container.
	Metrics(ctx context.Context, containerID string) (*ContainerMetrics, error)
	Inspect(ctx context.Context, containerID string) (*ContainerInfo, error)
	Logs(ctx context.Context, containerID string, follow bool, tail int) (io.ReadCloser, error)
}
