//go:build containerd_e2e

package runtime_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/overfold/trellis/orchestrator/internal/agent"
	"github.com/overfold/trellis/orchestrator/internal/health"
	"github.com/overfold/trellis/orchestrator/internal/runtime"
	"github.com/overfold/trellis/orchestrator/internal/spec"
)

// This intentionally stays small: distributed behavior belongs in the
// injected-runtime process suite, while this test protects the real containerd
// boundary and the managed-allocation inventory used for restart adoption.
func TestContainerdRetainedLogsAfterRemoval(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("containerd E2E requires root")
	}
	socket := os.Getenv("CONTAINERD_ADDRESS")
	if socket == "" {
		socket = "/run/containerd/containerd.sock"
	}
	if _, err := os.Stat(socket); err != nil {
		t.Skipf("containerd unavailable: %v", err)
	}
	r, err := runtime.NewContainerdRuntime(socket)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	const image = "docker.io/library/nginx:1.27-alpine"
	if err := r.Pull(ctx, image); err != nil {
		t.Fatal(err)
	}
	const id = "trellis-e2e-retained-logs"
	_ = r.Stop(ctx, id)
	_ = r.Remove(ctx, id)
	if _, err := r.Create(ctx, runtime.CreateOptions{ID: id, Image: image}); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Stop(context.Background(), id); _ = r.Remove(context.Background(), id) }()
	if err := r.Start(ctx, id); err != nil {
		t.Fatal(err)
	}
	var output []byte
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		stream, err := r.Logs(ctx, id, false, 0)
		if err != nil {
			t.Fatal(err)
		}
		output, err = io.ReadAll(stream)
		_ = stream.Close()
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(output, []byte("nginx/")) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !bytes.Contains(output, []byte("nginx/")) {
		t.Fatalf("missing nginx startup output: %s", output)
	}
	if err := r.Stop(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := r.RemoveRetainingLogs(ctx, id); err != nil {
		t.Fatal(err)
	}
	stream, err := r.Logs(ctx, id, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	retained, err := io.ReadAll(stream)
	_ = stream.Close()
	if err != nil || !bytes.Contains(retained, output) {
		t.Fatalf("retained logs=%s error=%v", retained, err)
	}
	if err := r.RemoveRetainedLogs(id); err != nil {
		t.Fatal(err)
	}
	if stream, err := r.Logs(ctx, id, false, 0); err == nil {
		_ = stream.Close()
		t.Fatal("pruned logs still readable")
	}
}

func TestContainerdAllocationAdoption(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("containerd overlayfs E2E requires root; run this test with sudo")
	}
	socket := os.Getenv("CONTAINERD_ADDRESS")
	if socket == "" {
		socket = "/run/containerd/containerd.sock"
	}
	if _, err := os.Stat(socket); err != nil {
		t.Skipf("containerd unavailable: %v", err)
	}
	r, err := runtime.NewContainerdRuntime(socket)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	const image = "docker.io/library/alpine:3.20"
	if err := r.Pull(ctx, image); err != nil {
		t.Fatal(err)
	}
	id := "trellis-e2e-adoption"
	_ = r.Stop(ctx, id)
	if err := r.Remove(ctx, id); err != nil {
		t.Fatal(err)
	}
	// Simulate a process exit after writing mount sources but before container creation.
	if err := os.MkdirAll("/var/lib/trellis/runtime", 0o750); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"-resolv.conf", "-hosts"} {
		path := filepath.Join("/var/lib/trellis/runtime", id+suffix)
		if err := os.WriteFile(path, []byte("stale"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	options := runtime.CreateOptions{
		ID:         id,
		Image:      image,
		Labels:     map[string]string{"trellis.cluster": "containerd-e2e", "trellis.managed": "true"},
		DNSServers: []string{"198.18.0.53"},
		ExtraHosts: map[string]string{"trellis": "127.0.0.1"},
	}
	created, err := r.Create(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Stop(context.Background(), created); _ = r.Remove(context.Background(), created) }()
	if _, err := r.Create(ctx, options); err == nil {
		t.Fatal("retry unexpectedly created the existing container")
	}
	if _, err := r.Inspect(ctx, created); err != nil {
		t.Fatalf("inspect after create retry: %v", err)
	}
	for _, suffix := range []string{"-resolv.conf", "-hosts"} {
		path := filepath.Join("/var/lib/trellis/runtime", created+suffix)
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("mount source %s after create retry: %v", path, err)
		}
	}
	if err := r.Start(ctx, created); err != nil {
		t.Fatalf("start after create retry: %v", err)
	}
	managed, err := r.ListManaged(ctx, "containerd-e2e")
	if err != nil {
		t.Fatal(err)
	}
	if len(managed) != 1 || managed[0].ID != created {
		t.Fatalf("created allocation was not adoptable: %+v", managed)
	}
}

func TestContainerdStopsCreatedTask(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("containerd overlayfs E2E requires root; run this test with sudo")
	}
	socket := os.Getenv("CONTAINERD_ADDRESS")
	if socket == "" {
		socket = "/run/containerd/containerd.sock"
	}
	if _, err := os.Stat(socket); err != nil {
		t.Skipf("containerd unavailable: %v", err)
	}

	r, err := runtime.NewContainerdRuntime(socket)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	raw, err := containerd.New(socket)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = raw.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	const image = "docker.io/library/nginx:1.27-alpine"
	if err := r.Pull(ctx, image); err != nil {
		t.Fatal(err)
	}
	const id = "trellis-e2e-created-stop"
	_ = r.Stop(ctx, id)
	_ = r.Remove(ctx, id)
	created, err := r.Create(ctx, runtime.CreateOptions{ID: id, Image: image, Runtime: "runc"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Stop(context.Background(), created); _ = r.Remove(context.Background(), created) }()

	nsCtx := namespaces.WithNamespace(ctx, "trellis")
	container, err := raw.LoadContainer(nsCtx, created)
	if err != nil {
		t.Fatal(err)
	}
	task, err := container.NewTask(nsCtx, cio.LogFile(filepath.Join(t.TempDir(), "created.log")))
	if err != nil {
		t.Fatal(err)
	}
	status, err := task.Status(nsCtx)
	if err != nil {
		t.Fatal(err)
	}
	if status.Status != containerd.Created {
		t.Fatalf("task status before stop = %q, want created", status.Status)
	}

	if err := r.Stop(ctx, created); err != nil {
		t.Fatalf("stop created task: %v", err)
	}
	observed, err := r.Inspect(ctx, created)
	if err != nil {
		t.Fatal(err)
	}
	if observed.Status != runtime.StatusStopped {
		t.Fatalf("status after stopping created task = %q, want stopped", observed.Status)
	}

	// Deleting the Created task must leave the container reusable: Start should
	// create a fresh task rather than colliding with the interrupted one.
	if err := r.Start(ctx, created); err != nil {
		t.Fatalf("start after created-task cleanup: %v", err)
	}
}

// Staging pins the backing directory even if its pathname is replaced before
// containerd creates a new task during an in-place restart.
func TestContainerdRestartsTaskWithStagedVolume(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("containerd overlayfs E2E requires root; run this test with sudo")
	}
	socket := os.Getenv("CONTAINERD_ADDRESS")
	if socket == "" {
		socket = "/run/containerd/containerd.sock"
	}
	if _, err := os.Stat(socket); err != nil {
		t.Skipf("containerd unavailable: %v", err)
	}
	r, err := runtime.NewContainerdRuntime(socket)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	const image = "docker.io/library/nginx:1.27-alpine"
	if err := r.Pull(ctx, image); err != nil {
		t.Fatal(err)
	}
	for _, managed := range []bool{true, false} {
		t.Run(fmt.Sprintf("managed=%t", managed), func(t *testing.T) {
			id := fmt.Sprintf("trellis-e2e-volume-restart-%t", managed)
			_ = r.Stop(ctx, id)
			_ = r.Remove(ctx, id)

			dataRoot := t.TempDir()
			backing := filepath.Join(dataRoot, "volumes", "namespaces", "e2e", "data")
			hostPath := "@/data"
			if !managed {
				backing = filepath.Join(dataRoot, "absolute")
				hostPath = backing
				if err := os.Mkdir(backing, 0o750); err != nil {
					t.Fatal(err)
				}
			}
			volumes := agent.NewVolumeManager(dataRoot)
			if err := volumes.CleanupStaging(nil); err != nil {
				t.Fatal(err)
			}
			volume := spec.VolumeSpec{Name: "data", HostPath: hostPath, ContainerPath: "/data"}
			mount, err := volumes.Create("e2e", "job", id, volume)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = volumes.ReleaseStaging(id) }()
			created, err := r.Create(ctx, runtime.CreateOptions{ID: id, Image: image, Runtime: "runc", Mounts: []*runtime.Mount{mount}})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = r.Stop(context.Background(), created); _ = r.Remove(context.Background(), created) }()
			if err := r.Start(ctx, created); err != nil {
				t.Fatalf("start: %v", err)
			}
			original := backing + "-original"
			if err := os.Rename(backing, original); err != nil {
				t.Fatal(err)
			}
			// Use a disposable decoy rather than / so a regression cannot write to
			// the host root. A redirected mount would write into this directory.
			decoy := t.TempDir()
			if err := os.Symlink(decoy, backing); err != nil {
				t.Fatal(err)
			}
			if err := r.Restart(ctx, created); err != nil {
				t.Fatalf("restart with staged volume: %v", err)
			}
			if code, err := r.Exec(ctx, created, []string{"touch", "/data/after-restart"}); err != nil || code != 0 {
				t.Fatalf("write volume after restart: code = %d, error = %v", code, err)
			}
			if _, err := os.Stat(filepath.Join(original, "after-restart")); err != nil {
				t.Fatalf("restarted task did not mount the original volume: %v", err)
			}
			if _, err := os.Stat(filepath.Join(decoy, "after-restart")); !os.IsNotExist(err) {
				t.Fatalf("restarted task wrote into the replacement: %v", err)
			}
		})
	}
}

func TestContainerdHealthProbe(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("containerd overlayfs E2E requires root; run this test with sudo")
	}
	socket := os.Getenv("CONTAINERD_ADDRESS")
	if socket == "" {
		socket = "/run/containerd/containerd.sock"
	}
	if _, err := os.Stat(socket); err != nil {
		t.Skipf("containerd unavailable: %v", err)
	}
	probePath := os.Getenv("TRELLIS_HEALTH_PROBE")
	if probePath == "" {
		t.Fatal("TRELLIS_HEALTH_PROBE must name a statically built health probe")
	}
	if _, err := os.Stat(probePath); err != nil {
		t.Fatalf("health probe unavailable: %v", err)
	}

	r, err := runtime.NewContainerdRuntime(socket)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	const image = "docker.io/library/nginx:1.27-alpine"
	if err := r.Pull(ctx, image); err != nil {
		t.Fatal(err)
	}
	const id = "trellis-e2e-health-probe"
	_ = r.Stop(ctx, id)
	_ = r.Remove(ctx, id)
	created, err := r.Create(ctx, runtime.CreateOptions{
		ID:      id,
		Image:   image,
		Runtime: "runc",
		Mounts: []*runtime.Mount{{
			HostPath:      probePath,
			ContainerPath: health.ProbeContainerPath,
			ReadOnly:      true,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Stop(context.Background(), created); _ = r.Remove(context.Background(), created) }()
	if err := r.Start(ctx, created); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(15 * time.Second)
	for {
		code, execErr := r.Exec(ctx, created, []string{health.ProbeContainerPath, "http", "80", "/", "2s"})
		if execErr == nil && code == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("HTTP probe did not become healthy: exit code %d, error %v", code, execErr)
		}
		time.Sleep(250 * time.Millisecond)
	}
	code, err := r.Exec(ctx, created, []string{health.ProbeContainerPath, "tcp", "80", "2s"})
	if err != nil || code != 0 {
		t.Fatalf("TCP probe failed: exit code %d, error %v", code, err)
	}
}

// newListingE2E connects to containerd and starts a long-running managed
// container labelled for cluster, returning the runtime, a raw client for
// out-of-band manipulation, and the container ID.
func newListingE2E(ctx context.Context, t *testing.T, id, cluster string) (*runtime.ContainerdRuntime, *containerd.Client, string) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("containerd overlayfs E2E requires root; run this test with sudo")
	}
	socket := os.Getenv("CONTAINERD_ADDRESS")
	if socket == "" {
		socket = "/run/containerd/containerd.sock"
	}
	if _, err := os.Stat(socket); err != nil {
		t.Skipf("containerd unavailable: %v", err)
	}
	r, err := runtime.NewContainerdRuntime(socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	raw, err := containerd.New(socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	const image = "docker.io/library/nginx:1.27-alpine"
	if err := r.Pull(ctx, image); err != nil {
		t.Fatal(err)
	}
	_ = r.Stop(ctx, id)
	_ = r.Remove(ctx, id)
	created, err := r.Create(ctx, runtime.CreateOptions{
		ID:      id,
		Image:   image,
		Runtime: "runc",
		Labels:  map[string]string{"trellis.cluster": cluster, "trellis.managed": "true"},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Stop(context.Background(), created); _ = r.Remove(context.Background(), created) })
	if err := r.Start(ctx, created); err != nil {
		t.Fatalf("start: %v", err)
	}
	return r, raw, created
}

func listedContainer(ctx context.Context, t *testing.T, r *runtime.ContainerdRuntime, cluster, id string) runtime.ContainerInfo {
	t.Helper()
	managed, err := r.ListManaged(ctx, cluster)
	if err != nil {
		t.Fatalf("list managed containers: %v", err)
	}
	for _, container := range managed {
		if container.ID == id {
			if container.Labels["trellis.cluster"] != cluster {
				t.Fatalf("listed container %s labels = %v, want cluster %q", id, container.Labels, cluster)
			}
			return container
		}
	}
	t.Fatalf("container %s was dropped from the listing: %+v", id, managed)
	return runtime.ContainerInfo{}
}

// A paused task still exists, so the listing must report it as paused rather
// than drop it or report unknown state, and a stop must still complete.
func TestContainerdListsPausedContainer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	const cluster = "containerd-e2e-paused"
	r, raw, id := newListingE2E(ctx, t, "trellis-e2e-list-paused", cluster)

	// The image entrypoint is a shell script that execs nginx. A shell as
	// PID 1 ignores SIGTERM, so pause only once nginx is PID 1; otherwise the
	// graceful-stop assertion below measures the entrypoint, not Stop.
	deadline := time.Now().Add(15 * time.Second)
	for {
		comm, _, code, execErr := execOutput(ctx, r, id, runtime.ExecOptions{Command: []string{"cat", "/proc/1/comm"}})
		if execErr == nil && code == 0 && strings.TrimSpace(string(comm)) == "nginx" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("nginx did not become PID 1: comm %q, exit code %d, error %v", comm, code, execErr)
		}
		time.Sleep(100 * time.Millisecond)
	}

	nsCtx := namespaces.WithNamespace(ctx, "trellis")
	container, err := raw.LoadContainer(nsCtx, id)
	if err != nil {
		t.Fatal(err)
	}
	task, err := container.Task(nsCtx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Pause(nsCtx); err != nil {
		t.Fatalf("pause task: %v", err)
	}

	if listed := listedContainer(ctx, t, r, cluster, id); listed.Status != runtime.StatusPaused {
		t.Fatalf("listed paused container status = %q, want %q", listed.Status, runtime.StatusPaused)
	}
	observed, err := r.Inspect(ctx, id)
	if err != nil || observed.Status != runtime.StatusPaused {
		t.Fatalf("inspect paused container = %+v, %v; want paused", observed, err)
	}

	// The frozen process cannot handle SIGTERM until the task is resumed.
	// Stopping must thaw it so nginx exits on SIGTERM instead of waiting out
	// the 10s grace period for SIGKILL.
	stopStarted := time.Now()
	if err := r.Stop(ctx, id); err != nil {
		t.Fatalf("stop paused container: %v", err)
	}
	if elapsed := time.Since(stopStarted); elapsed >= 8*time.Second {
		t.Fatalf("stopping the paused container took %s; it was not stopped gracefully", elapsed)
	}
	if listed := listedContainer(ctx, t, r, cluster, id); listed.Status != runtime.StatusStopped {
		t.Fatalf("listed container status after stop = %q, want %q", listed.Status, runtime.StatusStopped)
	}
}

// A container whose task is deleted underneath it still exists, so the
// listing must keep it, report it stopped, and keep its labels.
func TestContainerdListsContainerWithDeletedTask(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	const cluster = "containerd-e2e-deleted-task"
	r, raw, id := newListingE2E(ctx, t, "trellis-e2e-list-deleted-task", cluster)

	nsCtx := namespaces.WithNamespace(ctx, "trellis")
	container, err := raw.LoadContainer(nsCtx, id)
	if err != nil {
		t.Fatal(err)
	}
	task, err := container.Task(nsCtx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := task.Delete(nsCtx, containerd.WithProcessKill); err != nil {
		t.Fatalf("delete task underneath container: %v", err)
	}
	if _, err := container.Task(nsCtx, nil); err == nil {
		t.Fatal("task still exists after deletion")
	}

	if listed := listedContainer(ctx, t, r, cluster, id); listed.Status != runtime.StatusStopped {
		t.Fatalf("listed container status without a task = %q, want %q", listed.Status, runtime.StatusStopped)
	}
	// The container can still be started again with a fresh task.
	if err := r.Start(ctx, id); err != nil {
		t.Fatalf("start after task deletion: %v", err)
	}
	if listed := listedContainer(ctx, t, r, cluster, id); listed.Status != runtime.StatusRunning {
		t.Fatalf("listed container status after restart = %q, want %q", listed.Status, runtime.StatusRunning)
	}
}

// lockedWriter lets a test read output while containerd's copy goroutine
// may still be writing it.
type lockedWriter struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *lockedWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

// execOutput runs a streamed exec process to completion and returns its
// captured stdout and stderr.
func execOutput(ctx context.Context, r *runtime.ContainerdRuntime, id string, options runtime.ExecOptions) (string, string, int, error) {
	var stdout, stderr lockedWriter
	options.Stdout, options.Stderr = &stdout, &stderr
	process, err := r.StartExec(ctx, id, options)
	if err != nil {
		return "", "", 0, err
	}
	select {
	case <-process.Done():
	case <-ctx.Done():
		_ = process.Kill(context.Background())
		return stdout.String(), stderr.String(), 0, ctx.Err()
	}
	code, err := process.ExitCode()
	return stdout.String(), stderr.String(), code, err
}

// A streamed exec delivers stdin until its end, separates stdout from
// stderr, reports the exit status, and runs a TTY process with the requested
// TERM; Kill ends a process that would otherwise run forever.
func TestContainerdExecStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	r, _, id := newListingE2E(ctx, t, "trellis-e2e-exec-stream", "containerd-e2e-exec-stream")

	stdout, stderr, code, err := execOutput(ctx, r, id, runtime.ExecOptions{
		Command: []string{"sh", "-c", "cat; echo problem >&2; exit 7"},
		Stdin:   strings.NewReader("hello from stdin\n"),
	})
	if err != nil || code != 7 || stdout != "hello from stdin\n" || stderr != "problem\n" {
		t.Fatalf("exec = stdout %q, stderr %q, code %d, err %v", stdout, stderr, code, err)
	}

	stdout, _, code, err = execOutput(ctx, r, id, runtime.ExecOptions{
		Command: []string{"sh", "-c", "echo $TERM; stty size"},
		TTY:     true,
		Term:    "screen",
		Cols:    91,
		Rows:    17,
	})
	if err != nil || code != 0 || !strings.Contains(stdout, "screen") || !strings.Contains(stdout, "17 91") {
		t.Fatalf("tty exec = stdout %q, code %d, err %v", stdout, code, err)
	}

	stdinReader, stdinWriter := io.Pipe()
	defer func() { _ = stdinWriter.Close() }()
	process, err := r.StartExec(ctx, id, runtime.ExecOptions{Command: []string{"sleep", "600"}, Stdin: stdinReader})
	if err != nil {
		t.Fatal(err)
	}
	if err := process.Kill(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-process.Done():
	case <-ctx.Done():
		t.Fatal("killed exec process did not exit")
	}
	if code, err := process.ExitCode(); err != nil || code != 137 {
		t.Fatalf("killed exec exit = %d, %v; want 137", code, err)
	}
}
