//go:build containerd_e2e

package runtime_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/clofour/trellis/internal/health"
	"github.com/clofour/trellis/internal/runtime"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/containerd/v2/pkg/namespaces"
)

// This intentionally stays small: distributed behavior belongs in the
// injected-runtime process suite, while this test protects the real containerd
// boundary and the managed-allocation inventory used for restart adoption.
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
	_ = r.Remove(ctx, id)
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
