// Package runtime manages containers used for Trellis allocations.
package runtime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	v1stats "github.com/containerd/cgroups/v3/cgroup1/stats"
	v2stats "github.com/containerd/cgroups/v3/cgroup2/stats"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/containerd/v2/pkg/oci"
	"github.com/containerd/errdefs"
	"github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/protobuf/proto"
)

const trellisNamespace = "trellis"
const gracePeriod = 10 * time.Second
const secretEnvContainerPath = "/run/trellis/env-secrets"
const healthProbeContainerPath = "/run/trellis/health-probe"

// ContainerdRuntime implements container lifecycle operations with containerd.
type ContainerdRuntime struct {
	client       *containerd.Client
	logDir       string
	legacyLogDir string
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
	// Secret marks a memory-backed mount whose source must be made readable
	// only by the image-configured process user before container creation.
	Secret bool
	// SecretEnv marks the private directory containing environment-secret
	// files. The runtime wraps the configured process so these values never
	// enter the container's persisted OCI environment.
	SecretEnv bool
}

func withoutRawSocketCapability() oci.SpecOpts {
	return oci.WithDroppedCapabilities([]string{"CAP_NET_RAW"})
}

func shouldDropRawSocketCapability(networkNamespace string) bool {
	return networkNamespace != "" && networkNamespace != "/proc/1/ns/net"
}

// NewContainerdRuntime connects to containerd at socketPath.
func NewContainerdRuntime(socketPath string) (*ContainerdRuntime, error) {
	client, err := containerd.New(socketPath)
	if err != nil {
		return nil, err
	}

	return &ContainerdRuntime{
		client:       client,
		logDir:       "/var/lib/trellis/runtime",
		legacyLogDir: filepath.Join(os.TempDir(), "trellis-logs"),
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
func (c *ContainerdRuntime) Create(ctx context.Context, options CreateOptions) (id string, err error) {
	resourceOpts, err := resourceSpecOpts(options, swapLimitApplies(cgroupRoot, options.Runtime))
	if err != nil {
		return "", err
	}
	ctx = c.withNamespace(ctx)
	if err := ensureRuntimeDir(c.logDir); err != nil {
		return "", fmt.Errorf("create runtime directory: %w", err)
	}

	image, err := c.client.GetImage(ctx, options.Image)
	if err != nil {
		return "", fmt.Errorf("getting image %s: %w", options.Image, err)
	}
	if err := reclaimStaleMountFiles(ctx, []string{
		filepath.Join(c.logDir, options.ID+"-resolv.conf"),
		filepath.Join(c.logDir, options.ID+"-hosts"),
	}, func(lookupCtx context.Context) error {
		_, loadErr := c.client.LoadContainer(lookupCtx, options.ID)
		return loadErr
	}); err != nil {
		return "", fmt.Errorf("reclaim mount files for %s: %w", options.ID, err)
	}

	allMounts := convertMounts(options.Mounts)
	var createdFiles []string
	creationAttempted := false
	defer func() {
		if err == nil || len(createdFiles) == 0 {
			return
		}
		if creationAttempted {
			// A failed response may still have created the container. Only
			// remove its mount sources when containerd confirms it is absent.
			err = errors.Join(err, removeCreateFilesAfterFailedCreate(ctx, createdFiles, func(cleanupCtx context.Context) error {
				_, loadErr := c.client.LoadContainer(cleanupCtx, options.ID)
				return loadErr
			}))
			return
		}
		err = errors.Join(err, removeRuntimeFiles(createdFiles...))
	}()
	if len(options.DNSServers) > 0 {
		resolvPath := filepath.Join(c.logDir, options.ID+"-resolv.conf")
		if err := writeDNSConfig(resolvPath, options.DNSServers); err != nil {
			return "", fmt.Errorf("write resolv.conf for %s: %w", options.ID, err)
		}
		createdFiles = append(createdFiles, resolvPath)
		allMounts = append(allMounts, specs.Mount{
			Source:      resolvPath,
			Destination: "/etc/resolv.conf",
			Type:        "bind",
			Options:     []string{"rbind", "ro"},
		})
	}
	if len(options.ExtraHosts) > 0 {
		hostsPath := filepath.Join(c.logDir, options.ID+"-hosts")
		if err := writeHostsConfig(hostsPath, options.ExtraHosts); err != nil {
			return "", fmt.Errorf("write hosts file for %s: %w", options.ID, err)
		}
		createdFiles = append(createdFiles, hostsPath)
		allMounts = append(allMounts, specs.Mount{
			Source:      hostsPath,
			Destination: "/etc/hosts",
			Type:        "bind",
			Options:     []string{"rbind", "ro"},
		})
	}
	ociSpecOpts := []oci.SpecOpts{
		oci.WithImageConfig(image),
		oci.WithEnv(convertEnv(options.Env)),
		oci.WithMounts(allMounts),
		withManagedSecretMounts(options.Mounts),
	}
	if options.NetworkNamespace != "" {
		ociSpecOpts = append(ociSpecOpts, oci.WithLinuxNamespace(specs.LinuxNamespace{
			Type: specs.NetworkNamespace, Path: options.NetworkNamespace,
		}))
		if shouldDropRawSocketCapability(options.NetworkNamespace) {
			ociSpecOpts = append(ociSpecOpts, withoutRawSocketCapability())
		}
	}
	ociSpecOpts = append(ociSpecOpts, resourceOpts...)

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
	creationAttempted = true
	container, err := c.client.NewContainer(ctx, options.ID, containerOpts...)
	if err != nil {
		return "", fmt.Errorf("creating container %s: %w", options.ID, err)
	}

	return container.ID(), nil
}

const cgroupRoot = "/sys/fs/cgroup"

// SwapUncapped reports whether the host has active swap that its memory
// cgroup visibly cannot account, so task memory limits may not cap swap.
func SwapUncapped() bool {
	return swapActive("/proc/swaps") && !swapAccountingDetected(cgroupRoot)
}

// PidsControllerDetected reports whether the host exposes the pids cgroup
// controller that task pids limits require.
func PidsControllerDetected() bool {
	return pidsControllerDetected(cgroupRoot)
}

func pidsControllerDetected(cgroupRoot string) bool {
	if data, err := os.ReadFile(filepath.Join(cgroupRoot, "cgroup.controllers")); err == nil {
		return slices.Contains(strings.Fields(string(data)), "pids")
	}
	_, err := os.Stat(filepath.Join(cgroupRoot, "pids"))
	return err == nil
}

func swapActive(procSwaps string) bool {
	data, err := os.ReadFile(procSwaps)
	if err != nil {
		return false
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	return len(lines) > 1
}

func cgroupV2(cgroupRoot string) bool {
	_, err := os.Stat(filepath.Join(cgroupRoot, "cgroup.controllers"))
	return err == nil
}

// swapAccountingDetected looks for swap accounting where containerd places
// tasks. cgroup v2 exposes memory.swap.max only in non-root cgroups whose
// parent enables the memory controller with swap accounting (or at the root
// of a nested cgroup namespace); cgroup v1 exposes memory.memsw.limit_in_bytes.
func swapAccountingDetected(cgroupRoot string) bool {
	if !cgroupV2(cgroupRoot) {
		_, err := os.Stat(filepath.Join(cgroupRoot, "memory", "memory.memsw.limit_in_bytes"))
		return err == nil
	}
	if _, err := os.Stat(filepath.Join(cgroupRoot, "memory.swap.max")); err == nil {
		return true
	}
	matches, _ := filepath.Glob(filepath.Join(cgroupRoot, "*", "memory.swap.max"))
	return len(matches) > 0
}

// swapLimitApplies reports whether a task's memory+swap limit can be set.
// cgroup v1 rejects memsw limits without swap accounting. On cgroup v2 runc
// silently skips a missing memory.swap.max when disabling swap, but runsc
// does not, so runsc requires detected swap accounting. An empty runtime uses
// the containerd client's default, io.containerd.runc.v2. It is probed per
// create because controllers can be enabled after startup.
func swapLimitApplies(cgroupRoot, taskRuntime string) bool {
	if cgroupV2(cgroupRoot) && taskRuntime != "runsc" {
		return true
	}
	return swapAccountingDetected(cgroupRoot)
}

// resourceSpecOpts converts task resource limits into cgroup settings. The
// OCI swap value is the combined memory and swap limit, so setting it equal to
// the memory limit denies swap on both cgroup v1 (memsw) and v2 (runc writes
// memory.swap.max as swap minus memory).
func resourceSpecOpts(options CreateOptions, limitSwap bool) ([]oci.SpecOpts, error) {
	var opts []oci.SpecOpts
	if options.CPU < 0 || options.Memory < 0 || options.PidsLimit < 0 {
		return nil, fmt.Errorf("resource limits for %s must not be negative: cpu=%d memory=%d pids=%d", options.ID, options.CPU, options.Memory, options.PidsLimit)
	}
	if options.CPU > 0 {
		cpuQuota := int64(options.CPU) * 100
		if cpuQuota/100 != int64(options.CPU) {
			return nil, fmt.Errorf("CPU request %d overflows CFS quota", options.CPU)
		}
		opts = append(opts, oci.WithCPUCFS(cpuQuota, 100000))
	}
	if options.Memory > 0 {
		opts = append(opts, oci.WithMemoryLimit(uint64(options.Memory)))
		if limitSwap {
			opts = append(opts, oci.WithMemorySwap(options.Memory))
		}
	}
	if options.PidsLimit > 0 {
		opts = append(opts, oci.WithPidsLimit(options.PidsLimit))
	}
	return opts, nil
}

// Start starts a created container.
func (c *ContainerdRuntime) Start(ctx context.Context, containerID string) error {
	ctx = c.withNamespace(ctx)

	container, err := c.client.LoadContainer(ctx, containerID)
	if err != nil {
		return fmt.Errorf("loading container %s: %w", containerID, err)
	}

	if err := ensureRuntimeDir(c.logDir); err != nil {
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

func ensureRuntimeDir(path string) error {
	return ensureOwnedDir(path, true)
}

func ensureOwnedDir(path string, private bool) error {
	if parent := filepath.Dir(path); parent != path {
		if err := ensureOwnedDir(parent, false); err != nil {
			return err
		}
	}
	if err := os.Mkdir(path, 0o750); err != nil && !os.IsExist(err) {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	return checkOwnedDir(path, info, 0, private)
}

func checkOwnedDir(path string, info os.FileInfo, uid uint32, private bool) error {
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", path)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uid || info.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("%s must be owned by UID %d and not writable by other users", path, uid)
	}
	if private && info.Mode().Perm()&0o007 != 0 {
		return fmt.Errorf("%s must not be accessible by other users", path)
	}
	return nil
}

// Logs opens the log stream for a container.
func (c *ContainerdRuntime) Logs(ctx context.Context, containerID string, follow bool, tail int) (io.ReadCloser, error) {
	file, err := os.Open(c.logPath(containerID))
	if os.IsNotExist(err) {
		file, err = c.openLegacyLog(filepath.Base(containerID) + ".log")
	}
	if err != nil {
		return nil, fmt.Errorf("open logs for %s: %w", containerID, err)
	}
	return newLogReader(ctx, file, follow, tail)
}

func (c *ContainerdRuntime) openLegacyLog(name string) (*os.File, error) {
	dir, err := os.OpenFile(c.legacyLogDir, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_DIRECTORY, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = dir.Close() }()
	info, err := dir.Stat()
	if err != nil {
		return nil, err
	}
	if err := checkOwnedDir(c.legacyLogDir, info, uint32(os.Geteuid()), true); err != nil {
		return nil, err
	}
	fd, err := syscall.Openat(int(dir.Fd()), name, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), filepath.Join(c.legacyLogDir, name))
	info, err = file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		_ = file.Close()
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("legacy log %s is not a regular file", name)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		_ = file.Close()
		return nil, fmt.Errorf("legacy log %s must be owned by the runtime user", name)
	}
	return file, nil
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

	// Only signal and wait for active tasks. Created tasks have not started
	// their user process, and Stopped tasks have already exited; both should
	// skip Trellis's graceful TERM/KILL sequence and go straight to containerd
	// task deletion below. WithProcessKill handles Created tasks whose shim PID
	// is already nonzero while remaining safe for Stopped tasks.
	if rawStatus.Status != containerd.Created && rawStatus.Status != containerd.Stopped {
		// A frozen process cannot handle SIGTERM, so thaw a paused task
		// first to give it the same graceful stop as a running one.
		if rawStatus.Status == containerd.Paused || rawStatus.Status == containerd.Pausing {
			err = task.Resume(ctx)
			if err != nil && !errdefs.IsNotFound(err) {
				return fmt.Errorf("resuming paused task for %s: %w", containerID, err)
			}
		}
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
		if !errdefs.IsNotFound(err) {
			return fmt.Errorf("loading container %s: %w", containerID, err)
		}
	} else {
		err = container.Delete(ctx, containerd.WithSnapshotCleanup)
		if err != nil && !errdefs.IsNotFound(err) {
			return fmt.Errorf("deleting container %s: %w", containerID, err)
		}
	}

	return c.removeAllocationFiles(containerID)
}

func (c *ContainerdRuntime) removeAllocationFiles(containerID string) error {
	name := filepath.Base(containerID)
	var paths []string
	for _, suffix := range []string{".log", "-resolv.conf", "-hosts"} {
		paths = append(paths, filepath.Join(c.logDir, name+suffix))
	}
	err := removeRuntimeFiles(paths...)
	// Old log locations may be controlled by local users. Cleanup there is best effort.
	if dir, openErr := os.OpenFile(c.legacyLogDir, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_DIRECTORY, 0); openErr == nil {
		removeLegacyAllocationFiles(dir, name)
		_ = dir.Close()
	}
	return err
}

func removeLegacyAllocationFiles(dir *os.File, name string) {
	info, err := dir.Stat()
	if err != nil || checkOwnedDir(dir.Name(), info, uint32(os.Geteuid()), true) != nil {
		return
	}
	for _, suffix := range []string{".log", "-resolv.conf", "-hosts"} {
		// Stay anchored to the checked directory even if a writable ancestor
		// of the legacy temp directory is renamed or replaced with a symlink.
		_ = syscall.Unlinkat(int(dir.Fd()), name+suffix)
	}
}

func removeRuntimeFiles(paths ...string) error {
	var errs []error
	for _, path := range paths {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			errs = append(errs, fmt.Errorf("remove runtime file %s: %w", path, err))
		}
	}
	return errors.Join(errs...)
}

func removeCreateFilesIfAbsent(paths []string, loadErr error) error {
	if !errdefs.IsNotFound(loadErr) {
		return nil
	}
	return removeRuntimeFiles(paths...)
}

func removeCreateFilesAfterFailedCreate(ctx context.Context, paths []string, load func(context.Context) error) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return removeCreateFilesIfAbsent(paths, load(cleanupCtx))
}

func reclaimStaleMountFiles(ctx context.Context, paths []string, load func(context.Context) error) error {
	for _, path := range paths {
		if _, err := os.Lstat(path); err == nil {
			lookupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			loadErr := load(lookupCtx)
			cancel()
			if loadErr == nil {
				return nil
			}
			if !errdefs.IsNotFound(loadErr) {
				return fmt.Errorf("check container before reclaiming mount files: %w", loadErr)
			}
			return removeRuntimeFiles(paths...)
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("inspect mount file %s: %w", path, err)
		}
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
	containerSpec, _ := container.Spec(ctx)
	process := execProcessSpec(containerSpec, command, false)

	taskExec, err := task.Exec(ctx, execID, process, cio.NullIO)
	if err != nil {
		return 1, fmt.Errorf("constructing command %s: %w", command, err)
	}
	status, err := runExecProcess(ctx, taskExec)
	if err != nil {
		return 1, fmt.Errorf("executing command %s: %w", command, err)
	}
	_ = deleteExecProcess(ctx, taskExec)
	code, _, err := status.Result()
	if err != nil {
		return 1, fmt.Errorf("extracting status %s: %w", command, err)
	}

	return int(code), nil
}

func execProcessSpec(containerSpec *specs.Spec, command []string, terminal bool) *specs.Process {
	process := &specs.Process{
		Args:     append([]string(nil), command...),
		Cwd:      "/",
		Terminal: terminal,
	}
	if containerSpec == nil || containerSpec.Process == nil {
		return process
	}
	process.Env = append([]string(nil), containerSpec.Process.Env...)
	process.User = containerSpec.Process.User
	if containerSpec.Process.Cwd != "" {
		process.Cwd = containerSpec.Process.Cwd
	}
	if hasSecretEnvironment(containerSpec) {
		process.Args = secretEnvironmentCommand(command)
	}
	return process
}

func hasSecretEnvironment(containerSpec *specs.Spec) bool {
	if containerSpec == nil {
		return false
	}
	for _, mount := range containerSpec.Mounts {
		if mount.Destination == secretEnvContainerPath {
			return true
		}
	}
	return false
}

func secretEnvironmentCommand(command []string) []string {
	wrapped := []string{healthProbeContainerPath, "env-exec", secretEnvContainerPath, "--"}
	return append(wrapped, command...)
}

func withManagedSecretMounts(mounts []*Mount) oci.SpecOpts {
	return func(_ context.Context, _ oci.Client, _ *containers.Container, spec *oci.Spec) error {
		if spec.Process == nil {
			return fmt.Errorf("container process is missing")
		}
		uid, gid := int(spec.Process.User.UID), int(spec.Process.User.GID)
		for _, mount := range mounts {
			if !mount.Secret {
				continue
			}
			if mount.SecretEnv {
				entries, err := os.ReadDir(mount.HostPath)
				if err != nil {
					return fmt.Errorf("list environment secrets: %w", err)
				}
				for _, entry := range entries {
					if err := os.Chown(filepath.Join(mount.HostPath, entry.Name()), uid, gid); err != nil {
						return fmt.Errorf("set environment secret ownership: %w", err)
					}
				}
			}
			if err := os.Chown(mount.HostPath, uid, gid); err != nil {
				return fmt.Errorf("set secret ownership: %w", err)
			}
		}
		if hasSecretEnvironment(spec) {
			spec.Process.Args = secretEnvironmentCommand(spec.Process.Args)
		}
		return nil
	}
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

	result.Status = containerStatus(rawStatus.Status)
	return result, nil
}

// containerStatus maps a containerd task status to a runtime status. A task
// that is paused or pausing still exists, so it is reported as paused rather
// than as unknown state.
func containerStatus(status containerd.ProcessStatus) ContainerStatus {
	switch status {
	case containerd.Created:
		return StatusCreated
	case containerd.Running:
		return StatusRunning
	case containerd.Stopped:
		return StatusStopped
	case containerd.Paused, containerd.Pausing:
		return StatusPaused
	default:
		return StatusUnknown
	}
}

// ListManaged lists containers owned by a Trellis cluster. One unreadable
// container does not abort the listing: it is reported with StatusUnknown so
// callers never mistake it for an absent container. When its metadata cannot
// be read, its cluster is unknown and it is reported without labels. A
// container without a task is reported stopped and one with a paused task
// is reported paused. Only containers whose metadata lookup reports them
// deleted are omitted.
func (c *ContainerdRuntime) ListManaged(ctx context.Context, cluster string) ([]ContainerInfo, error) {
	ctx = c.withNamespace(ctx)
	containers, err := c.client.Containers(ctx)
	if err != nil {
		return nil, fmt.Errorf("list containers: %w", err)
	}
	result := make([]ContainerInfo, 0, len(containers))
	for _, container := range containers {
		info, err := container.Info(ctx)
		if errdefs.IsNotFound(err) {
			continue
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, fmt.Errorf("list containers: %w", ctxErr)
		}
		if err != nil {
			result = append(result, ContainerInfo{ID: container.ID(), Status: StatusUnknown})
			continue
		}
		if info.Labels["trellis.cluster"] != cluster {
			continue
		}
		// Inspect can also report a missing task, so treat every error as
		// unknown state rather than as proof that the container is gone.
		observed, err := c.Inspect(ctx, container.ID())
		if err != nil {
			observed = &ContainerInfo{ID: container.ID(), Status: StatusUnknown}
		}
		observed.Labels = info.Labels
		result = append(result, *observed)
	}
	// A cancelled context turns late lookups into unknown entries, so the
	// listing is incomplete rather than authoritative.
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("list containers: %w", err)
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
	for _, server := range servers {
		if _, err := netip.ParseAddr(server); err != nil {
			return fmt.Errorf("DNS server %q must be an IP address without a port", server)
		}
		content += "nameserver " + server + "\n"
	}
	return writeRuntimeFile(path, content)
}

func writeHostsConfig(path string, hosts map[string]string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return fmt.Errorf("create hosts config directory: %w", err)
	}
	names := make([]string, 0, len(hosts))
	for name := range hosts {
		names = append(names, name)
	}
	sort.Strings(names)
	content := "127.0.0.1 localhost\n::1 localhost ip6-localhost ip6-loopback\n"
	for _, name := range names {
		content += hosts[name] + " " + name + "\n"
	}
	return writeRuntimeFile(path, content)
}

func writeRuntimeFile(path, content string) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o644)
	if err != nil {
		return err
	}
	_, err = io.WriteString(file, content)
	closeErr := file.Close()
	if err != nil {
		_ = os.Remove(path)
		return err
	}
	if closeErr != nil {
		_ = os.Remove(path)
	}
	return closeErr
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
	containerSpec, _ := container.Spec(ctx)
	process := execProcessSpec(containerSpec, command, false)

	var outBuf, errBuf lockedBuffer
	// Output a background child writes after this returns is not collected.
	defer outBuf.take()
	defer errBuf.take()
	creator := cio.NewCreator(cio.WithStreams(nil, &outBuf, &errBuf))
	taskExec, err := task.Exec(ctx, execID, process, creator)
	if err != nil {
		return nil, nil, 1, fmt.Errorf("constructing exec for %s: %w", containerID, err)
	}
	status, err := runExecProcess(ctx, taskExec)
	if err != nil {
		return nil, nil, 1, fmt.Errorf("exec in %s: %w", containerID, err)
	}
	// Deleting the process waits for its output copy to finish. The wait is
	// bounded because a background child can hold the output open after the
	// command exits; the output collected by then is returned.
	_ = deleteExecProcess(ctx, taskExec)
	code, _, err := status.Result()
	if err != nil {
		return nil, nil, 1, fmt.Errorf("extracting exec status for %s: %w", containerID, err)
	}

	return outBuf.take(), errBuf.take(), int(code), nil
}

const execCleanupTimeout = 5 * time.Second

// execKillRetryInterval is a variable so tests can shorten it.
var execKillRetryInterval = time.Second

// execProcess is the part of a containerd exec process used to run it to completion.
type execProcess interface {
	Wait(context.Context) (<-chan containerd.ExitStatus, error)
	Start(context.Context) error
	Kill(context.Context, syscall.Signal, ...containerd.KillOpts) error
	Delete(context.Context, ...containerd.ProcessDeleteOpts) (*containerd.ExitStatus, error)
}

// runExecProcess starts a created exec process and waits for it to exit. If
// ctx ends first, the process is killed and deleted in the background so it
// does not outlive the request, and ctx's error is returned. On any other
// error the process has been cleaned up; after a successful exit the caller
// deletes it.
func runExecProcess(ctx context.Context, process execProcess) (containerd.ExitStatus, error) {
	// Waiting must survive cancellation so cleanup can observe the kill.
	waitCtx, cancelWait := context.WithCancel(context.WithoutCancel(ctx))
	exitCh, err := process.Wait(waitCtx)
	if err != nil {
		cancelWait()
		_ = deleteExecProcess(ctx, process)
		return containerd.ExitStatus{}, fmt.Errorf("waiting on exec: %w", err)
	}
	// A cancelled Start can still launch the process, so a failed Start is
	// always followed by a kill.
	if err := process.Start(ctx); err != nil {
		killExecProcess(ctx, process, exitCh)
		cancelWait()
		return containerd.ExitStatus{}, fmt.Errorf("starting exec: %w", err)
	}
	select {
	case status := <-exitCh:
		cancelWait()
		return status, nil
	case <-ctx.Done():
		go func() {
			defer cancelWait()
			killExecProcess(ctx, process, exitCh)
		}()
		return containerd.ExitStatus{}, ctx.Err()
	}
}

// killExecProcess kills an exec process that may be running and deletes it
// once it has exited, within a bounded time that survives ctx's cancellation.
func killExecProcess(ctx context.Context, process execProcess, exitCh <-chan containerd.ExitStatus) {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), execCleanupTimeout)
	defer cancel()
	// Kill fails when the process already exited or never started; Delete
	// below reports whether it is still running.
	_ = process.Kill(cleanupCtx, syscall.SIGKILL)
	if err := deleteWithin(cleanupCtx, process); err == nil || !errdefs.IsFailedPrecondition(err) {
		return
	}
	// Repeat the kill in case the first attempt failed transiently.
	retry := time.NewTicker(execKillRetryInterval)
	defer retry.Stop()
	for {
		select {
		case <-exitCh:
			_ = deleteWithin(cleanupCtx, process)
			return
		case <-retry.C:
			_ = process.Kill(cleanupCtx, syscall.SIGKILL)
		case <-cleanupCtx.Done():
			return
		}
	}
}

// deleteExecProcess deletes an exited exec process, waiting for its output
// copy to finish, within a bounded time that survives ctx's cancellation.
func deleteExecProcess(ctx context.Context, process execProcess) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), execCleanupTimeout)
	defer cancel()
	return deleteWithin(cleanupCtx, process)
}

// deleteWithin deletes an exec process, giving up when ctx ends. containerd's
// Delete waits for the output copy without honouring its context, so the
// delete runs in the background.
func deleteWithin(ctx context.Context, process execProcess) error {
	done := make(chan error, 1)
	go func() {
		_, err := process.Delete(ctx)
		done <- err
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// execOutputLimit bounds the stdout and stderr each one-shot exec captures.
// Worst-case JSON escaping (6 bytes per byte) of both streams stays within
// the 64 MiB response limit of the agent and server clients.
const execOutputLimit = 4 * 1024 * 1024

// lockedBuffer collects exec output written by containerd's IO copy
// goroutines, keeping at most execOutputLimit bytes. Taking its contents
// detaches it: a background child can keep writing after the result is
// returned, and that output is discarded. Discarded writes still succeed so
// the command is never blocked or signalled by a full buffer.
type lockedBuffer struct {
	mu       sync.Mutex
	buf      bytes.Buffer
	detached bool
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.detached {
		return len(p), nil
	}
	if room := execOutputLimit - b.buf.Len(); len(p) > room {
		b.buf.Write(p[:room])
		return len(p), nil
	}
	return b.buf.Write(p)
}

// take returns the collected output and discards later writes.
func (b *lockedBuffer) take() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.detached = true
	data := b.buf.Bytes()
	b.buf = bytes.Buffer{}
	return data
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

	containerSpec, _ := container.Spec(ctx)
	processSpec := execProcessSpec(containerSpec, command, true)
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
		_ = deleteExecProcess(ctx, process)
		return nil, fmt.Errorf("waiting on terminal exec for %s: %w", containerID, err)
	}
	if err := process.Start(ctx); err != nil {
		_ = stdinReader.Close()
		_ = stdinWriter.Close()
		killExecProcess(ctx, process, exitCh)
		return nil, fmt.Errorf("starting terminal exec for %s: %w", containerID, err)
	}
	if cols > 0 && rows > 0 {
		if err := process.Resize(ctx, cols, rows); err != nil {
			_ = stdinReader.Close()
			_ = stdinWriter.Close()
			killExecProcess(ctx, process, exitCh)
			return nil, fmt.Errorf("resize terminal exec for %s: %w", containerID, err)
		}
	}

	session := &containerdTerminalSession{
		stdin: stdinWriter, output: output, process: process,
	}
	go func() {
		status := <-exitCh
		code, _, resultErr := status.Result()
		// Input can no longer be delivered, so writes fail instead of blocking.
		_ = stdinWriter.Close()
		_ = stdinReader.Close()
		// Deleting waits (boundedly) for the output copy, so readers do not
		// see the exit before the final output.
		_ = deleteExecProcess(ctx, process)
		session.mu.Lock()
		session.exited = true
		if resultErr == nil {
			value := int(code)
			session.exitCode = &value
		}
		session.mu.Unlock()
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
