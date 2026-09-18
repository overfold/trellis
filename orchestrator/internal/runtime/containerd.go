// Package runtime manages containers used for Trellis allocations.
package runtime

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/containerd/v2/pkg/oci"
	v1stats "github.com/containerd/cgroups/v3/cgroup1/stats"
	v2stats "github.com/containerd/cgroups/v3/cgroup2/stats"
	"github.com/containerd/errdefs"
	"github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/protobuf/proto"
)

const trellisNamespace = "trellis"
const gracePeriod = 10 * time.Second

// ContainerdRuntime implements container lifecycle operations with containerd.
type ContainerdRuntime struct {
	client *containerd.Client
	logDir string
}

// Port maps a host port to a container port.
type Port struct {
	HostPort      int
	ContainerPort int
}

// Mount describes a host path mounted into a container.
type Mount struct {
	HostPath      string
	ContainerPath string
	ReadOnly      bool
}

// NewContainerdRuntime connects to containerd at socketPath.
func NewContainerdRuntime(socketPath string) (*ContainerdRuntime, error) {
	client, err := containerd.New(socketPath)
	if err != nil {
		return nil, err
	}

	return &ContainerdRuntime{
		client: client,
		logDir: filepath.Join(os.TempDir(), "trellis-logs"),
	}, nil
}

// Close releases the containerd client.
func (c *ContainerdRuntime) Close() error {
	return c.client.Close()
}

// Pull downloads a container image.
func (c *ContainerdRuntime) Pull(ctx context.Context, image string) error {
	ctx = c.withNamespace(ctx)

	_, err := c.client.Pull(ctx, image, containerd.WithPullUnpack)
	if err != nil {
		return err
	}

	return nil
}

// Create creates a container from the supplied options.
func (c *ContainerdRuntime) Create(ctx context.Context, options CreateOptions) (string, error) {
	ctx = c.withNamespace(ctx)

	image, err := c.client.GetImage(ctx, options.Image)
	if err != nil {
		return "", fmt.Errorf("getting image %s: %w", options.Image, err)
	}

	allMounts := convertMounts(options.Mounts)
	if len(options.DNSServers) > 0 {
		resolvPath := filepath.Join(c.logDir, options.ID+"-resolv.conf")
		if err := writeDNSConfig(resolvPath, options.DNSServers); err != nil {
			return "", fmt.Errorf("write resolv.conf for %s: %w", options.ID, err)
		}
		allMounts = append(allMounts, specs.Mount{
			Source:      resolvPath,
			Destination: "/etc/resolv.conf",
			Type:        "bind",
			Options:     []string{"rbind", "ro"},
		})
	}
	ociSpecOpts := []oci.SpecOpts{
		oci.WithImageConfig(image),
		oci.WithEnv(convertEnv(options.Env)),
		oci.WithMounts(allMounts),
	}
	if options.NetworkNamespace != "" {
		ociSpecOpts = append(ociSpecOpts, oci.WithLinuxNamespace(specs.LinuxNamespace{
			Type: specs.NetworkNamespace, Path: options.NetworkNamespace,
		}))
	}
	if options.CPU > 0 {
		ociSpecOpts = append(ociSpecOpts, oci.WithCPUCFS(int64(options.CPU*100), 100000))
	}
	if options.Memory > 0 {
		ociSpecOpts = append(ociSpecOpts, oci.WithMemoryLimit(uint64(options.Memory)))
	}

	containerOpts := []containerd.NewContainerOpts{
		containerd.WithImage(image),
		containerd.WithNewSnapshot(options.ID, image),
		containerd.WithNewSpec(ociSpecOpts...),
	}
	if len(options.Labels) > 0 {
		containerOpts = append(containerOpts, containerd.WithContainerLabels(options.Labels))
	}
	if options.Runtime != "" {
		switch options.Runtime {
		case "runc":
			containerOpts = append(containerOpts, containerd.WithRuntime("io.containerd.runc.v2", nil))
		case "runsc":
			containerOpts = append(containerOpts, containerd.WithRuntime("io.containerd.runsc.v1", nil))
		default:
			return "", fmt.Errorf("unsupported runtime %q", options.Runtime)
		}
	}
	container, err := c.client.NewContainer(ctx, options.ID, containerOpts...)
	if err != nil {
		return "", fmt.Errorf("creating container %s: %w", options.ID, err)
	}

	return container.ID(), nil
}

// Start starts a created container.
func (c *ContainerdRuntime) Start(ctx context.Context, containerID string) error {
	ctx = c.withNamespace(ctx)

	container, err := c.client.LoadContainer(ctx, containerID)
	if err != nil {
		return fmt.Errorf("loading container %s: %w", containerID, err)
	}

	if err := os.MkdirAll(c.logDir, 0o750); err != nil {
		return fmt.Errorf("create log directory: %w", err)
	}
	task, err := container.NewTask(ctx, cio.LogFile(c.logPath(containerID)))
	if err != nil {
		return fmt.Errorf("creating task for %s: %w", containerID, err)
	}

	err = task.Start(ctx)
	if err != nil {
		_, _ = task.Delete(ctx)
		return fmt.Errorf("starting task for %s: %w", containerID, err)
	}

	return nil
}

func (c *ContainerdRuntime) logPath(containerID string) string {
	return filepath.Join(c.logDir, filepath.Base(containerID)+".log")
}

// Logs opens the log stream for a container.
func (c *ContainerdRuntime) Logs(ctx context.Context, containerID string, follow bool, tail int) (io.ReadCloser, error) {
	file, err := os.Open(c.logPath(containerID))
	if err != nil {
		return nil, fmt.Errorf("open logs for %s: %w", containerID, err)
	}
	return newLogReader(ctx, file, follow, tail)
}

// Restart stops and starts a container.
func (c *ContainerdRuntime) Restart(ctx context.Context, containerID string) error {
	err := c.Stop(ctx, containerID)
	if err != nil {
		return fmt.Errorf("stopping container %s: %w", containerID, err)
	}

	err = c.Start(ctx, containerID)
	if err != nil {
		return fmt.Errorf("starting container %s: %w", containerID, err)
	}

	return nil
}

// Stop stops a running container.
func (c *ContainerdRuntime) Stop(ctx context.Context, containerID string) error {
	ctx = c.withNamespace(ctx)

	container, err := c.client.LoadContainer(ctx, containerID)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("loading container %s: %w", containerID, err)
	}

	task, err := container.Task(ctx, nil)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("getting task for %s: %w", containerID, err)
	}

	rawStatus, err := task.Status(ctx)
	if err != nil {
		return fmt.Errorf("getting task status for %s: %w", containerID, err)
	}

	// Only signal and wait if the process is still running. A task whose
	// process has already exited (Stopped) must be deleted without signaling —
	// sending SIGTERM to a dead process returns a FailedPrecondition error
	// from containerd that is not errdefs.IsNotFound, which would cause Stop
	// to fail and leave the exited task un-deleted, blocking future restarts.
	if rawStatus.Status != containerd.Stopped {
		exitChannel, err := task.Wait(ctx)
		if err != nil {
			return fmt.Errorf("waiting on task for %s: %w", containerID, err)
		}

		err = task.Kill(ctx, syscall.SIGTERM)
		if err != nil && !errdefs.IsNotFound(err) {
			return fmt.Errorf("sending SIGTERM to %s: %w", containerID, err)
		}

		select {
		case <-exitChannel:

		case <-time.After(gracePeriod):
			err := task.Kill(ctx, syscall.SIGKILL)
			if err != nil && !errdefs.IsNotFound(err) {
				return fmt.Errorf("sending SIGKILL to %s: %w", containerID, err)
			}

			select {
			case <-exitChannel:
			case <-time.After(5 * time.Second):
				return fmt.Errorf("container %s did not exit after SIGKILL", containerID)
			}
		}
	}

	_, err = task.Delete(ctx, containerd.WithProcessKill)
	if err != nil && !errdefs.IsNotFound(err) {
		return fmt.Errorf("deleting task for %s: %w", containerID, err)
	}

	return nil
}

// Remove deletes a container and its resources.
func (c *ContainerdRuntime) Remove(ctx context.Context, containerID string) error {
	ctx = c.withNamespace(ctx)

	container, err := c.client.LoadContainer(ctx, containerID)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("loading container %s: %w", containerID, err)
	}

	err = container.Delete(ctx, containerd.WithSnapshotCleanup)
	if err != nil && !errdefs.IsNotFound(err) {
		return fmt.Errorf("deleting container %s: %w", containerID, err)
	}

	return nil
}

// Exec runs a command in a container and returns its exit code.
func (c *ContainerdRuntime) Exec(ctx context.Context, containerID string, command []string) (int, error) {
	ctx = c.withNamespace(ctx)

	container, err := c.client.LoadContainer(ctx, containerID)
	if err != nil {
		return 1, fmt.Errorf("loading container %s: %w", containerID, err)
	}

	task, err := container.Task(ctx, nil)
	if err != nil {
		return 1, fmt.Errorf("getting task for %s: %w", containerID, err)
	}

	execID := fmt.Sprintf("healthcheck-%d", time.Now().UnixNano())
	process := &specs.Process{
		Args: command,
		Cwd:  "/",
	}
	if containerSpec, specErr := container.Spec(ctx); specErr == nil && containerSpec.Process != nil {
		process.Env = containerSpec.Process.Env
		process.User = containerSpec.Process.User
		if containerSpec.Process.Cwd != "" {
			process.Cwd = containerSpec.Process.Cwd
		}
	}

	taskExec, err := task.Exec(ctx, execID, process, cio.NullIO)
	if err != nil {
		return 1, fmt.Errorf("constructing command %s: %w", command, err)
	}
	defer func() { _, _ = taskExec.Delete(ctx) }()

	exitChannel, err := taskExec.Wait(ctx)
	if err != nil {
		return 1, fmt.Errorf("waiting on command %s: %w", command, err)
	}

	err = taskExec.Start(ctx)
	if err != nil {
		return 1, fmt.Errorf("executing command %s: %w", command, err)
	}

	status := <-exitChannel
	code, _, err := status.Result()
	if err != nil {
		return 1, fmt.Errorf("extracting status %s: %w", command, err)
	}

	return int(code), nil
}

// Inspect returns the current state of a container.
func (c *ContainerdRuntime) Inspect(ctx context.Context, containerID string) (*ContainerInfo, error) {
	ctx = c.withNamespace(ctx)

	container, err := c.client.LoadContainer(ctx, containerID)
	if err != nil {
		return nil, fmt.Errorf("loading container %s: %w", containerID, err)
	}

	info, err := container.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting info for %s: %w", containerID, err)
	}

	result := &ContainerInfo{
		ID:     info.ID,
		Status: StatusUnknown,
		Labels: info.Labels,
	}

	task, err := container.Task(ctx, nil)
	if err != nil {
		if errdefs.IsNotFound(err) {
			result.Status = StatusStopped
			return result, nil
		}

		return nil, fmt.Errorf("getting task for %s: %w", containerID, err)
	}

	rawStatus, err := task.Status(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting task status for %s: %w", containerID, err)
	}

	switch rawStatus.Status {
	case containerd.Created:
		result.Status = StatusCreated
	case containerd.Running:
		result.Status = StatusRunning
	case containerd.Stopped:
		result.Status = StatusStopped
	default:
		result.Status = StatusUnknown
	}

	return result, nil
}

// ListManaged lists containers owned by a Trellis cluster.
func (c *ContainerdRuntime) ListManaged(ctx context.Context, cluster string) ([]ContainerInfo, error) {
	ctx = c.withNamespace(ctx)
	containers, err := c.client.Containers(ctx)
	if err != nil {
		return nil, fmt.Errorf("list containers: %w", err)
	}
	result := make([]ContainerInfo, 0, len(containers))
	for _, container := range containers {
		info, err := container.Info(ctx)
		if err != nil {
			return nil, fmt.Errorf("inspect container %s: %w", container.ID(), err)
		}
		if info.Labels["trellis.cluster"] != cluster {
			continue
		}
		observed, err := c.Inspect(ctx, container.ID())
		if err != nil {
			return nil, err
		}
		observed.Labels = info.Labels
		result = append(result, *observed)
	}
	return result, nil
}

func convertEnv(envMap map[string]string) []string {
	env := make([]string, 0, len(envMap))

	for k, v := range envMap {
		env = append(env, k+"="+v)
	}

	return env
}

func convertMounts(mounts []*Mount) []specs.Mount {
	result := make([]specs.Mount, len(mounts))

	for i, m := range mounts {

		mode := "rw"
		if m.ReadOnly {
			mode = "ro"
		}
		result[i] = specs.Mount{
			Source:      m.HostPath,
			Destination: m.ContainerPath,
			Type:        "bind",
			Options:     []string{"rbind", mode},
		}

	}

	return result
}

func writeDNSConfig(path string, servers []string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return fmt.Errorf("create DNS config directory: %w", err)
	}
	var content string
	for _, s := range servers {
		content += "nameserver " + s + "\n"
	}
	return os.WriteFile(path, []byte(content), 0o644)
}

// ExecOutput runs a command in a container and returns its captured output.
func (c *ContainerdRuntime) ExecOutput(ctx context.Context, containerID string, command []string) ([]byte, []byte, int, error) {
	ctx = c.withNamespace(ctx)

	container, err := c.client.LoadContainer(ctx, containerID)
	if err != nil {
		return nil, nil, 1, fmt.Errorf("loading container %s: %w", containerID, err)
	}

	task, err := container.Task(ctx, nil)
	if err != nil {
		return nil, nil, 1, fmt.Errorf("getting task for %s: %w", containerID, err)
	}

	execID := fmt.Sprintf("exec-%d", time.Now().UnixNano())
	process := &specs.Process{
		Args: command,
		Cwd:  "/",
	}

	var outBuf, errBuf bytes.Buffer
	creator := cio.NewCreator(cio.WithStreams(nil, &outBuf, &errBuf))
	taskExec, err := task.Exec(ctx, execID, process, creator)
	if err != nil {
		return nil, nil, 1, fmt.Errorf("constructing exec for %s: %w", containerID, err)
	}
	defer func() { _, _ = taskExec.Delete(ctx) }()

	exitCh, err := taskExec.Wait(ctx)
	if err != nil {
		return nil, nil, 1, fmt.Errorf("waiting on exec for %s: %w", containerID, err)
	}

	if err := taskExec.Start(ctx); err != nil {
		return nil, nil, 1, fmt.Errorf("starting exec for %s: %w", containerID, err)
	}

	status := <-exitCh
	code, _, err := status.Result()
	if err != nil {
		return nil, nil, 1, fmt.Errorf("extracting exec status for %s: %w", containerID, err)
	}

	return outBuf.Bytes(), errBuf.Bytes(), int(code), nil
}

const terminalOutputLimit = 2 * 1024 * 1024

type terminalOutputBuffer struct {
	mu   sync.Mutex
	base int64
	data []byte
}

func (b *terminalOutputBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.data = append(b.data, p...)
	if len(b.data) > terminalOutputLimit {
		drop := len(b.data) - terminalOutputLimit
		b.data = append([]byte(nil), b.data[drop:]...)
		b.base += int64(drop)
	}
	return len(p), nil
}

func (b *terminalOutputBuffer) read(offset int64) ([]byte, int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if offset < b.base {
		offset = b.base
	}
	end := b.base + int64(len(b.data))
	if offset > end {
		offset = end
	}
	start := int(offset - b.base)
	return append([]byte(nil), b.data[start:]...), end
}

type containerdTerminalSession struct {
	stdin   *io.PipeWriter
	output  *terminalOutputBuffer
	process containerd.Process

	mu       sync.RWMutex
	exited   bool
	exitCode *int
}

func (s *containerdTerminalSession) Write(p []byte) (int, error) {
	s.mu.RLock()
	exited := s.exited
	s.mu.RUnlock()
	if exited {
		return 0, io.ErrClosedPipe
	}
	return s.stdin.Write(p)
}

func (s *containerdTerminalSession) Read(offset int64) ([]byte, int64, bool, *int, error) {
	data, next := s.output.read(offset)
	s.mu.RLock()
	exited := s.exited
	var code *int
	if s.exitCode != nil {
		copyCode := *s.exitCode
		code = &copyCode
	}
	s.mu.RUnlock()
	return data, next, exited, code, nil
}

func (s *containerdTerminalSession) Resize(ctx context.Context, cols, rows uint32) error {
	if cols == 0 || rows == 0 {
		return fmt.Errorf("terminal dimensions must be greater than zero")
	}
	s.mu.RLock()
	exited := s.exited
	s.mu.RUnlock()
	if exited {
		return nil
	}
	return s.process.Resize(ctx, cols, rows)
}

func (s *containerdTerminalSession) Close(ctx context.Context) error {
	_ = s.stdin.Close()
	s.mu.RLock()
	exited := s.exited
	s.mu.RUnlock()
	if exited {
		return nil
	}
	if err := s.process.Kill(ctx, syscall.SIGKILL); err != nil && !errdefs.IsNotFound(err) {
		return err
	}
	return nil
}

// StartTerminal starts an interactive TTY-backed process in a container.
func (c *ContainerdRuntime) StartTerminal(ctx context.Context, containerID string, command []string, term string, cols, rows uint32) (TerminalSession, error) {
	if len(command) == 0 {
		return nil, fmt.Errorf("terminal command is required")
	}
	ctx = context.WithoutCancel(c.withNamespace(ctx))

	container, err := c.client.LoadContainer(ctx, containerID)
	if err != nil {
		return nil, fmt.Errorf("loading container %s: %w", containerID, err)
	}
	task, err := container.Task(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("getting task for %s: %w", containerID, err)
	}

	processSpec := &specs.Process{Args: command, Cwd: "/", Terminal: true}
	if containerSpec, specErr := container.Spec(ctx); specErr == nil && containerSpec.Process != nil {
		processSpec.Env = append([]string(nil), containerSpec.Process.Env...)
		processSpec.User = containerSpec.Process.User
		if containerSpec.Process.Cwd != "" {
			processSpec.Cwd = containerSpec.Process.Cwd
		}
	}
	if term != "" {
		env := make([]string, 0, len(processSpec.Env)+1)
		for _, value := range processSpec.Env {
			if len(value) >= 5 && value[:5] == "TERM=" {
				continue
			}
			env = append(env, value)
		}
		processSpec.Env = append(env, "TERM="+term)
	}

	stdinReader, stdinWriter := io.Pipe()
	output := &terminalOutputBuffer{}
	creator := cio.NewCreator(cio.WithStreams(stdinReader, output, nil), cio.WithTerminal)
	execID := fmt.Sprintf("terminal-%d", time.Now().UnixNano())
	process, err := task.Exec(ctx, execID, processSpec, creator)
	if err != nil {
		_ = stdinReader.Close()
		_ = stdinWriter.Close()
		return nil, fmt.Errorf("constructing terminal exec for %s: %w", containerID, err)
	}
	exitCh, err := process.Wait(ctx)
	if err != nil {
		_ = stdinReader.Close()
		_ = stdinWriter.Close()
		_, _ = process.Delete(ctx)
		return nil, fmt.Errorf("waiting on terminal exec for %s: %w", containerID, err)
	}
	if err := process.Start(ctx); err != nil {
		_ = stdinReader.Close()
		_ = stdinWriter.Close()
		_, _ = process.Delete(ctx)
		return nil, fmt.Errorf("starting terminal exec for %s: %w", containerID, err)
	}
	if cols > 0 && rows > 0 {
		if err := process.Resize(ctx, cols, rows); err != nil {
			_ = process.Kill(ctx, syscall.SIGKILL)
			_ = stdinReader.Close()
			_ = stdinWriter.Close()
			_, _ = process.Delete(ctx)
			return nil, fmt.Errorf("resize terminal exec for %s: %w", containerID, err)
		}
	}

	session := &containerdTerminalSession{
		stdin: stdinWriter, output: output, process: process,
	}
	go func() {
		status := <-exitCh
		code, _, resultErr := status.Result()
		session.mu.Lock()
		session.exited = true
		if resultErr == nil {
			value := int(code)
			session.exitCode = &value
		}
		session.mu.Unlock()
		_ = stdinWriter.Close()
		_ = stdinReader.Close()
		_, _ = process.Delete(ctx)
	}()

	return session, nil
}

// Metrics returns a point-in-time resource usage snapshot for a container.
func (c *ContainerdRuntime) Metrics(ctx context.Context, containerID string) (*ContainerMetrics, error) {
	ctx = c.withNamespace(ctx)

	container, err := c.client.LoadContainer(ctx, containerID)
	if err != nil {
		return nil, fmt.Errorf("loading container %s: %w", containerID, err)
	}

	task, err := container.Task(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("getting task for %s: %w", containerID, err)
	}

	metric, err := task.Metrics(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting metrics for %s: %w", containerID, err)
	}

	result := &ContainerMetrics{}
	if metric.Data != nil {
		switch metric.Data.TypeUrl {
		case "io.containerd.cgroups.v2.Metrics":
			var m v2stats.Metrics
			if proto.Unmarshal(metric.Data.Value, &m) == nil {
				if m.CPU != nil {
					result.CPUUsageNanoseconds = int64(m.CPU.UsageUsec) * 1000
				}
				if m.Memory != nil {
					result.MemoryUsageBytes = int64(m.Memory.Usage)
				}
			}
		default:
			// cgroup v1 and other runtimes
			var m v1stats.Metrics
			if proto.Unmarshal(metric.Data.Value, &m) == nil {
				if m.CPU != nil && m.CPU.Usage != nil {
					result.CPUUsageNanoseconds = int64(m.CPU.Usage.Total)
				}
				if m.Memory != nil && m.Memory.Usage != nil {
					result.MemoryUsageBytes = int64(m.Memory.Usage.Usage)
				}
			}
		}
	}

	return result, nil
}

func (c *ContainerdRuntime) withNamespace(ctx context.Context) context.Context {
	return namespaces.WithNamespace(ctx, trellisNamespace)
}
