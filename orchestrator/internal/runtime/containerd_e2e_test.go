//go:build containerd_e2e

package runtime

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
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
	r, err := NewContainerdRuntime(socket)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	const image = "docker.io/library/alpine:3.20"
	if err := r.Pull(ctx, image); err != nil {
		t.Fatal(err)
	}
	id := "trellis-e2e-adoption"
	_ = r.Stop(ctx, id)
	_ = r.Remove(ctx, id)
	created, err := r.Create(ctx, CreateOptions{ID: id, Image: image, Labels: map[string]string{"trellis.cluster": "containerd-e2e", "trellis.managed": "true"}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Stop(context.Background(), created); _ = r.Remove(context.Background(), created) }()
	managed, err := r.ListManaged(ctx, "containerd-e2e")
	if err != nil {
		t.Fatal(err)
	}
	if len(managed) != 1 || managed[0].ID != created {
		t.Fatalf("created allocation was not adoptable: %+v", managed)
	}
}

func TestContainerdIsolatedRuncLoopback(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("containerd namespace E2E requires root; run this test with sudo")
	}
	socket := os.Getenv("CONTAINERD_ADDRESS")
	if socket == "" {
		socket = "/run/containerd/containerd.sock"
	}
	if _, err := os.Stat(socket); err != nil {
		t.Skipf("containerd unavailable: %v", err)
	}
	r, err := NewContainerdRuntime(socket)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	const image = "docker.io/library/nginx:alpine"
	if err := r.Pull(ctx, image); err != nil {
		t.Fatal(err)
	}
	id := "trellis-e2e-isolated-loopback"
	_ = r.Stop(ctx, id)
	_ = r.Remove(ctx, id)
	created, err := r.Create(ctx, CreateOptions{ID: id, Image: image, Runtime: "runc"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Stop(context.Background(), created); _ = r.Remove(context.Background(), created) }()
	if err := r.Start(ctx, created); err != nil {
		t.Fatal(err)
	}
	stdout, stderr, exitCode, err := r.ExecOutput(ctx, created, []string{"cat", "/sys/class/net/lo/flags"})
	if err != nil {
		t.Fatal(err)
	}
	if exitCode != 0 {
		t.Fatalf("read lo flags exited %d: %s", exitCode, stderr)
	}
	flags, err := strconv.ParseUint(strings.TrimSpace(string(stdout)), 0, 32)
	if err != nil {
		t.Fatalf("parse lo flags %q: %v", stdout, err)
	}
	if flags&unix.IFF_UP == 0 {
		t.Fatalf("isolated runc loopback is down: flags = %#x", flags)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, stderr, exitCode, err = r.ExecOutput(ctx, created, []string{"wget", "-q", "-O", "/dev/null", "http://127.0.0.1/"})
		if err == nil && exitCode == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("HTTP request over isolated loopback failed: exit %d, stderr %s, error %v", exitCode, stderr, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
