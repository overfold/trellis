package runtime

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/containerd/v2/pkg/oci"
	"github.com/containerd/errdefs"
	"github.com/opencontainers/runtime-spec/specs-go"
)

func TestContainerSpecDropsRawSocketCapability(t *testing.T) {
	capabilities := []string{"CAP_CHOWN", "CAP_NET_RAW"}
	s := oci.Spec{Process: &specs.Process{Capabilities: &specs.LinuxCapabilities{
		Bounding:    append([]string(nil), capabilities...),
		Effective:   append([]string(nil), capabilities...),
		Permitted:   append([]string(nil), capabilities...),
		Inheritable: append([]string(nil), capabilities...),
	}}}
	if err := withoutRawSocketCapability()(context.Background(), nil, nil, &s); err != nil {
		t.Fatal(err)
	}
	for name, got := range map[string][]string{
		"bounding": s.Process.Capabilities.Bounding, "effective": s.Process.Capabilities.Effective,
		"permitted": s.Process.Capabilities.Permitted, "inheritable": s.Process.Capabilities.Inheritable,
	} {
		if !reflect.DeepEqual(got, []string{"CAP_CHOWN"}) {
			t.Errorf("%s capabilities = %v, want CAP_NET_RAW removed", name, got)
		}
	}
}

func TestRawSocketCapabilityDropIsLimitedToNamespaceNetworking(t *testing.T) {
	for _, tc := range []struct {
		name             string
		networkNamespace string
		want             bool
	}{
		{name: "isolated"},
		{name: "host", networkNamespace: "/proc/1/ns/net"},
		{name: "namespace", networkNamespace: "/var/run/netns/allocation", want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldDropRawSocketCapability(tc.networkNamespace); got != tc.want {
				t.Fatalf("shouldDropRawSocketCapability(%q) = %t, want %t", tc.networkNamespace, got, tc.want)
			}
		})
	}
}

func TestResourceSpecOptsLimitMemorySwapAndPids(t *testing.T) {
	opts, err := resourceSpecOpts(CreateOptions{CPU: 250, Memory: 256 << 20, PidsLimit: 4096}, true)
	if err != nil {
		t.Fatal(err)
	}
	s := oci.Spec{Linux: &specs.Linux{}}
	for _, opt := range opts {
		if err := opt(context.Background(), nil, nil, &s); err != nil {
			t.Fatal(err)
		}
	}
	resources := s.Linux.Resources
	if resources == nil || resources.CPU == nil || resources.Memory == nil || resources.Pids == nil {
		t.Fatalf("resources = %#v, want CPU, memory, and pids limits", resources)
	}
	if got := resources.CPU.Quota; got == nil || *got != 25000 {
		t.Fatalf("CPU quota = %v, want 25000", got)
	}
	if got := resources.Memory.Limit; got == nil || *got != 256<<20 {
		t.Fatalf("memory limit = %v, want %d", got, 256<<20)
	}
	if got := resources.Memory.Swap; got == nil || *got != 256<<20 {
		t.Fatalf("memory+swap limit = %v, want memory limit %d", got, 256<<20)
	}
	if got := resources.Pids.Limit; got == nil || *got != 4096 {
		t.Fatalf("pids limit = %v, want 4096", got)
	}
}

func TestResourceSpecOptsSkipsSwapWithoutSwapAccounting(t *testing.T) {
	opts, err := resourceSpecOpts(CreateOptions{Memory: 256 << 20}, false)
	if err != nil {
		t.Fatal(err)
	}
	s := oci.Spec{Linux: &specs.Linux{}}
	for _, opt := range opts {
		if err := opt(context.Background(), nil, nil, &s); err != nil {
			t.Fatal(err)
		}
	}
	if s.Linux.Resources == nil || s.Linux.Resources.Memory == nil || s.Linux.Resources.Memory.Limit == nil {
		t.Fatal("memory limit was not applied")
	}
	if s.Linux.Resources.Memory.Swap != nil {
		t.Fatalf("memory+swap limit = %d, want unset without swap accounting", *s.Linux.Resources.Memory.Swap)
	}
}

func TestSwapControllerAvailable(t *testing.T) {
	for _, tc := range []struct {
		name  string
		files []string
		want  bool
	}{
		{name: "v2 with swap", files: []string{"cgroup.controllers", "system.slice/memory.swap.max"}, want: true},
		{name: "v2 without swap accounting", files: []string{"cgroup.controllers", "system.slice/memory.max"}},
		{name: "v2 without memory controller", files: []string{"cgroup.controllers", "system.slice/cgroup.procs"}},
		{name: "v1 with memsw", files: []string{"memory/memory.memsw.limit_in_bytes"}, want: true},
		{name: "v1 without memsw", files: []string{"memory/memory.limit_in_bytes"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			for _, name := range tc.files {
				path := filepath.Join(root, name)
				if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, nil, 0o644); err != nil {
					t.Fatal(err)
				}
			}
			if got := swapControllerAvailable(root); got != tc.want {
				t.Fatalf("swapControllerAvailable = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestResourceSpecOptsOmitsUnsetLimits(t *testing.T) {
	opts, err := resourceSpecOpts(CreateOptions{}, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(opts) != 0 {
		t.Fatalf("resource opts = %d, want none for unset limits", len(opts))
	}
}

func TestResourceSpecOptsRejectsNegativeLimits(t *testing.T) {
	for name, options := range map[string]CreateOptions{
		"cpu":    {CPU: -1},
		"memory": {Memory: -1},
		"pids":   {PidsLimit: -1},
	} {
		if _, err := resourceSpecOpts(options, true); err == nil {
			t.Errorf("%s: expected negative limit to be rejected", name)
		}
	}
}

func TestContainerdLogsFallsBackToLegacyDirectory(t *testing.T) {
	dir := t.TempDir()
	r := &ContainerdRuntime{
		logDir:       filepath.Join(dir, "runtime"),
		legacyLogDir: filepath.Join(dir, "legacy"),
	}
	if err := os.MkdirAll(r.legacyLogDir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(r.legacyLogDir, "allocation.log"), []byte("legacy\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	readLogs := func(want string) {
		t.Helper()
		logs, err := r.Logs(context.Background(), "allocation", false, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = logs.Close() }()
		got, err := io.ReadAll(logs)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != want {
			t.Fatalf("logs = %q, want %q", got, want)
		}
	}
	readLogs("legacy\n")
	if err := os.MkdirAll(r.logDir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(r.logPath("allocation"), []byte("current\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	readLogs("current\n")
}

func TestContainerdLogsRejectsUntrustedLegacyPaths(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "private")
	if err := os.WriteFile(target, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	legacy := filepath.Join(dir, "legacy")
	if err := os.Mkdir(legacy, 0o700); err != nil {
		t.Fatal(err)
	}
	r := &ContainerdRuntime{logDir: filepath.Join(dir, "runtime"), legacyLogDir: legacy}
	logPath := filepath.Join(legacy, "allocation.log")
	if err := os.Symlink(target, logPath); err != nil {
		t.Fatal(err)
	}
	if logs, err := r.Logs(context.Background(), "allocation", false, 0); err == nil {
		_ = logs.Close()
		t.Fatal("accepted a symlinked legacy log")
	}
	if err := os.Remove(logPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(legacy, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logPath, []byte("untrusted"), 0o600); err != nil {
		t.Fatal(err)
	}
	if logs, err := r.Logs(context.Background(), "allocation", false, 0); err == nil {
		_ = logs.Close()
		t.Fatal("accepted a writable legacy directory")
	}
	if err := os.RemoveAll(legacy); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(dir, legacy); err != nil {
		t.Fatal(err)
	}
	if logs, err := r.Logs(context.Background(), "allocation", false, 0); err == nil {
		_ = logs.Close()
		t.Fatal("accepted a symlinked legacy directory")
	}
}

func TestRemoveAllocationFilesCleansCurrentAndLegacyFiles(t *testing.T) {
	dir := t.TempDir()
	r := &ContainerdRuntime{
		logDir:       filepath.Join(dir, "runtime"),
		legacyLogDir: filepath.Join(dir, "legacy"),
	}
	for _, folder := range []string{r.logDir, r.legacyLogDir} {
		if err := os.MkdirAll(folder, 0o750); err != nil {
			t.Fatal(err)
		}
		for _, suffix := range []string{".log", "-resolv.conf", "-hosts"} {
			path := filepath.Join(folder, "allocation"+suffix)
			if err := os.WriteFile(path, []byte("data"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(filepath.Join(folder, "other.log"), []byte("keep"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		if err := r.removeAllocationFiles("allocation"); err != nil {
			t.Fatal(err)
		}
	}
	for _, folder := range []string{r.logDir, r.legacyLogDir} {
		entries, err := os.ReadDir(folder)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 1 || entries[0].Name() != "other.log" {
			t.Fatalf("remaining files in %s: %v", folder, entries)
		}
	}
}

func TestRemoveLegacyAllocationFilesUsesOpenedDirectory(t *testing.T) {
	parent := t.TempDir()
	legacy := filepath.Join(parent, "legacy")
	target := filepath.Join(parent, "target")
	suffixes := []string{".log", "-resolv.conf", "-hosts"}
	for _, path := range []string{legacy, target} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
		for _, suffix := range suffixes {
			if err := os.WriteFile(filepath.Join(path, "allocation"+suffix), []byte("keep"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	dir, err := os.Open(legacy)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dir.Close() }()
	// Model a directory replacement after open, without timing-dependent races.
	moved := filepath.Join(parent, "moved")
	if err := os.Rename(legacy, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, legacy); err != nil {
		t.Fatal(err)
	}
	removeLegacyAllocationFiles(dir, "allocation")
	for _, suffix := range suffixes {
		if _, err := os.Lstat(filepath.Join(moved, "allocation"+suffix)); !os.IsNotExist(err) {
			t.Fatalf("original legacy file was not removed: %v", err)
		}
		if got, err := os.ReadFile(filepath.Join(target, "allocation"+suffix)); err != nil || string(got) != "keep" {
			t.Fatalf("replacement target changed: %q, %v", got, err)
		}
	}
}

func TestRemoveAllocationFilesIgnoresUnremovableLegacyEntry(t *testing.T) {
	dir := t.TempDir()
	r := &ContainerdRuntime{logDir: filepath.Join(dir, "runtime"), legacyLogDir: filepath.Join(dir, "legacy")}
	for _, path := range []string{r.logDir, r.legacyLogDir} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(r.logPath("allocation"), []byte("current"), 0o600); err != nil {
		t.Fatal(err)
	}
	legacyLog := filepath.Join(r.legacyLogDir, "allocation.log")
	if err := os.Mkdir(legacyLog, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(legacyLog, "blocker"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := r.removeAllocationFiles("allocation"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(r.logPath("allocation")); !os.IsNotExist(err) {
		t.Fatalf("current log still exists: %v", err)
	}
}

func TestWriteDNSConfigCreatesParentDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "logs", "allocation-resolv.conf")
	if err := writeDNSConfig(path, []string{"198.18.0.53"}); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if want := "nameserver 198.18.0.53\n"; string(got) != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestExecProcessSpecInheritsContainerContext(t *testing.T) {
	containerSpec := &specs.Spec{Process: &specs.Process{
		Env:  []string{"PATH=/usr/local/bin:/usr/bin", "TRELLIS_NAMESPACE=default"},
		Cwd:  "/app",
		User: specs.User{UID: 1000, GID: 1000},
	}}
	process := execProcessSpec(containerSpec, []string{"node", "-v"}, false)

	if !reflect.DeepEqual(process.Env, containerSpec.Process.Env) {
		t.Fatalf("env = %#v, want %#v", process.Env, containerSpec.Process.Env)
	}
	if process.Cwd != "/app" {
		t.Fatalf("cwd = %q, want /app", process.Cwd)
	}
	if process.User.UID != 1000 || process.User.GID != 1000 {
		t.Fatalf("user = %#v, want uid/gid 1000", process.User)
	}
	if process.Terminal {
		t.Fatal("non-interactive exec unexpectedly requested a terminal")
	}
}

func TestExecProcessSpecDefaultsWithoutContainerProcess(t *testing.T) {
	process := execProcessSpec(nil, []string{"/bin/true"}, true)
	if process.Cwd != "/" {
		t.Fatalf("cwd = %q, want /", process.Cwd)
	}
	if !process.Terminal {
		t.Fatal("terminal exec did not preserve terminal flag")
	}
}

func TestWriteHostsConfigAddsDeterministicAliases(t *testing.T) {
	path := filepath.Join(t.TempDir(), "logs", "allocation-hosts")
	if err := writeHostsConfig(path, map[string]string{"zeta": "10.0.0.2", "trellis": "127.0.0.1"}); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := "127.0.0.1 localhost\n::1 localhost ip6-localhost ip6-loopback\n127.0.0.1 trellis\n10.0.0.2 zeta\n"
	if string(got) != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestWriteDNSConfigRejectsNameserverPorts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "resolv.conf")
	if err := writeDNSConfig(path, []string{"127.0.0.1:8053"}); err == nil {
		t.Fatal("expected nameserver with port to be rejected")
	}
}

func TestRuntimeDirectoryRejectsUnsafeExistingPaths(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	uid := uint32(os.Geteuid())
	info, err := os.Lstat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkOwnedDir(dir, info, uid, true); err != nil {
		t.Fatal(err)
	}
	if err := checkOwnedDir(dir, info, uid+1, true); err == nil {
		t.Fatal("accepted a directory owned by another user")
	}
	if err := os.Chmod(dir, 0o777); err != nil {
		t.Fatal(err)
	}
	info, err = os.Lstat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkOwnedDir(dir, info, uid, true); err == nil {
		t.Fatal("accepted a writable directory")
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	info, err = os.Lstat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkOwnedDir(dir, info, uid, true); err == nil {
		t.Fatal("accepted a publicly accessible runtime directory")
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(dir, link); err != nil {
		t.Fatal(err)
	}
	info, err = os.Lstat(link)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkOwnedDir(link, info, uid, true); err == nil {
		t.Fatal("accepted a symlink")
	}
}

func TestWriteRuntimeFileRejectsSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	if err := os.WriteFile(target, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "resolv.conf")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if err := writeDNSConfig(link, []string{"198.18.0.53"}); err == nil {
		t.Fatal("followed a symlink")
	}
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "original" {
		t.Fatalf("target changed to %q", got)
	}
}

func TestWriteConfigPreservesExistingMountSources(t *testing.T) {
	dir := t.TempDir()
	dns := filepath.Join(dir, "allocation-resolv.conf")
	hosts := filepath.Join(dir, "allocation-hosts")
	for _, path := range []string{dns, hosts} {
		if err := os.WriteFile(path, []byte("existing"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := writeDNSConfig(dns, []string{"198.18.0.53"}); !os.IsExist(err) {
		t.Fatalf("DNS retry error = %v, want file exists", err)
	}
	if err := writeHostsConfig(hosts, map[string]string{"trellis": "127.0.0.1"}); !os.IsExist(err) {
		t.Fatalf("hosts retry error = %v, want file exists", err)
	}
	for _, path := range []string{dns, hosts} {
		got, err := os.ReadFile(path)
		if err != nil || string(got) != "existing" {
			t.Fatalf("mount source %s = %q, %v", path, got, err)
		}
	}
}

func TestReclaimStaleMountFilesAfterRestart(t *testing.T) {
	for _, staleSuffix := range []string{"-resolv.conf", "-hosts"} {
		t.Run(staleSuffix, func(t *testing.T) {
			dir := t.TempDir()
			dns := filepath.Join(dir, "allocation-resolv.conf")
			hosts := filepath.Join(dir, "allocation-hosts")
			stale := filepath.Join(dir, "allocation"+staleSuffix)
			if err := os.WriteFile(stale, []byte("left by previous process"), 0o600); err != nil {
				t.Fatal(err)
			}
			paths := []string{dns, hosts}
			if err := reclaimStaleMountFiles(context.Background(), paths, func(context.Context) error {
				return errdefs.ErrNotFound
			}); err != nil {
				t.Fatal(err)
			}
			if err := writeDNSConfig(dns, []string{"198.18.0.53"}); err != nil {
				t.Fatalf("retry DNS config for same allocation: %v", err)
			}
			if err := writeHostsConfig(hosts, map[string]string{"trellis": "127.0.0.1"}); err != nil {
				t.Fatalf("retry hosts config for same allocation: %v", err)
			}
		})
	}
}

func TestReclaimStaleMountFilesPreservesSourcesWithoutConfirmedAbsence(t *testing.T) {
	for _, tc := range []struct {
		name    string
		loadErr error
		wantErr bool
	}{
		{name: "container exists"},
		{name: "lookup failed", loadErr: errors.New("container state unknown"), wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "allocation-resolv.conf")
			if err := os.WriteFile(path, []byte("existing"), 0o600); err != nil {
				t.Fatal(err)
			}
			err := reclaimStaleMountFiles(context.Background(), []string{path}, func(context.Context) error {
				return tc.loadErr
			})
			if (err != nil) != tc.wantErr {
				t.Fatalf("reclaim error = %v, want error = %v", err, tc.wantErr)
			}
			if got, err := os.ReadFile(path); err != nil || string(got) != "existing" {
				t.Fatalf("mount source after lookup %v = %q, %v", tc.loadErr, got, err)
			}
		})
	}
}

func TestFailedCreateRemovesFilesWhenContainerIsAbsentForRetry(t *testing.T) {
	dir := t.TempDir()
	dns := filepath.Join(dir, "allocation-resolv.conf")
	hosts := filepath.Join(dir, "allocation-hosts")
	writeFiles := func() {
		t.Helper()
		if err := writeDNSConfig(dns, []string{"198.18.0.53"}); err != nil {
			t.Fatal(err)
		}
		if err := writeHostsConfig(hosts, map[string]string{"trellis": "127.0.0.1"}); err != nil {
			t.Fatal(err)
		}
	}
	writeFiles()
	if err := removeCreateFilesIfAbsent([]string{dns, hosts}, errdefs.ErrNotFound); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{dns, hosts} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("file %s remains after failed create: %v", path, err)
		}
	}
	writeFiles() // The same allocation ID can create both mount sources again.
	for _, loadErr := range []error{nil, errors.New("container state unknown")} {
		if err := removeCreateFilesIfAbsent([]string{dns, hosts}, loadErr); err != nil {
			t.Fatal(err)
		}
		for _, path := range []string{dns, hosts} {
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("file %s was removed without confirmed absence: %v", path, err)
			}
		}
	}
}

func TestFailedCreateWithCanceledContextAllowsRetry(t *testing.T) {
	dir := t.TempDir()
	dns := filepath.Join(dir, "allocation-resolv.conf")
	hosts := filepath.Join(dir, "allocation-hosts")
	if err := writeDNSConfig(dns, []string{"198.18.0.53"}); err != nil {
		t.Fatal(err)
	}
	if err := writeHostsConfig(hosts, map[string]string{"trellis": "127.0.0.1"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(namespaces.WithNamespace(context.Background(), trellisNamespace))
	cancel()
	if err := removeCreateFilesAfterFailedCreate(ctx, []string{dns, hosts}, func(cleanupCtx context.Context) error {
		if err := cleanupCtx.Err(); err != nil {
			t.Fatalf("container lookup inherited canceled context: %v", err)
		}
		if _, ok := cleanupCtx.Deadline(); !ok {
			t.Fatal("container lookup has no deadline")
		}
		if namespace, ok := namespaces.Namespace(cleanupCtx); !ok || namespace != trellisNamespace {
			t.Fatalf("container lookup namespace = %q, present = %v", namespace, ok)
		}
		return errdefs.ErrNotFound
	}); err != nil {
		t.Fatal(err)
	}
	if err := writeDNSConfig(dns, []string{"198.18.0.53"}); err != nil {
		t.Fatalf("retry DNS config for same allocation: %v", err)
	}
	if err := writeHostsConfig(hosts, map[string]string{"trellis": "127.0.0.1"}); err != nil {
		t.Fatalf("retry hosts config for same allocation: %v", err)
	}
}

func TestContainerStatusReportsPausedTasks(t *testing.T) {
	for raw, want := range map[containerd.ProcessStatus]ContainerStatus{
		containerd.Created: StatusCreated,
		containerd.Running: StatusRunning,
		containerd.Stopped: StatusStopped,
		containerd.Paused:  StatusPaused,
		containerd.Pausing: StatusPaused,
		containerd.Unknown: StatusUnknown,
	} {
		if got := containerStatus(raw); got != want {
			t.Errorf("containerStatus(%q) = %q, want %q", raw, got, want)
		}
	}
}
