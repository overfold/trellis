package runtime

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/containerd/v2/pkg/oci"
	"github.com/containerd/errdefs"
	"github.com/opencontainers/runtime-spec/specs-go"
)

func buildWorkloadSpec(t *testing.T, runtimeName string, appArmorSupported bool, mounts []specs.Mount) *oci.Spec {
	t.Helper()
	ctx := namespaces.WithNamespace(context.Background(), trellisNamespace)
	opts := append([]oci.SpecOpts{oci.WithMounts(mounts)}, workloadSecurityOpts(runtimeName, appArmorSupported)...)
	s, err := oci.GenerateSpecWithPlatform(ctx, nil, "linux/amd64", &containers.Container{ID: "allocation"}, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func stubAppArmorProfile(t *testing.T) *[]string {
	t.Helper()
	var loaded []string
	previous := defaultAppArmorProfile
	defaultAppArmorProfile = func(name string) oci.SpecOpts {
		return func(_ context.Context, _ oci.Client, _ *containers.Container, s *specs.Spec) error {
			loaded = append(loaded, name)
			s.Process.ApparmorProfile = name
			return nil
		}
	}
	t.Cleanup(func() { defaultAppArmorProfile = previous })
	return &loaded
}

func TestWorkloadSpecDropsMknodAndRawSocketCapabilities(t *testing.T) {
	for _, runtimeName := range []string{"", "runc", "runsc"} {
		s := buildWorkloadSpec(t, runtimeName, false, nil)
		capabilities := s.Process.Capabilities
		for name, got := range map[string][]string{
			"bounding": capabilities.Bounding, "effective": capabilities.Effective,
			"permitted": capabilities.Permitted, "inheritable": capabilities.Inheritable,
			"ambient": capabilities.Ambient,
		} {
			for _, dropped := range []string{"CAP_MKNOD", "CAP_NET_RAW"} {
				if slices.Contains(got, dropped) {
					t.Errorf("runtime %q %s capabilities = %v, want %s removed", runtimeName, name, got, dropped)
				}
			}
		}
		if !slices.Contains(capabilities.Bounding, "CAP_CHOWN") {
			t.Errorf("runtime %q bounding capabilities = %v, want other defaults kept", runtimeName, capabilities.Bounding)
		}
	}
}

func TestWorkloadSpecAppliesDefaultSeccompProfile(t *testing.T) {
	for _, runtimeName := range []string{"", "runc", "runsc"} {
		s := buildWorkloadSpec(t, runtimeName, false, nil)
		if s.Linux.Seccomp == nil || s.Linux.Seccomp.DefaultAction != specs.ActErrno || len(s.Linux.Seccomp.Syscalls) == 0 {
			t.Fatalf("runtime %q seccomp = %#v, want containerd default profile", runtimeName, s.Linux.Seccomp)
		}
		// The profile is derived from the final capability set, so it must not
		// allow syscalls gated on capabilities workloads are not granted.
		var allowsRead bool
		for _, rule := range s.Linux.Seccomp.Syscalls {
			if rule.Action != specs.ActAllow {
				continue
			}
			allowsRead = allowsRead || slices.Contains(rule.Names, "read")
			if slices.Contains(rule.Names, "mount") {
				t.Fatalf("runtime %q seccomp allows mount without CAP_SYS_ADMIN: %#v", runtimeName, rule)
			}
		}
		if !allowsRead {
			t.Fatalf("runtime %q seccomp does not allow ordinary syscalls", runtimeName)
		}
	}
}

func TestWorkloadSpecAppArmorProfileSelection(t *testing.T) {
	for _, tc := range []struct {
		name      string
		runtime   string
		supported bool
		want      string
	}{
		{name: "default runtime", supported: true, want: appArmorProfileName},
		{name: "runc", runtime: "runc", supported: true, want: appArmorProfileName},
		{name: "runsc ignores apparmor", runtime: "runsc", supported: true},
		{name: "unsupported host", runtime: "runc"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loaded := stubAppArmorProfile(t)
			s := buildWorkloadSpec(t, tc.runtime, tc.supported, nil)
			if s.Process.ApparmorProfile != tc.want {
				t.Fatalf("apparmor profile = %q, want %q", s.Process.ApparmorProfile, tc.want)
			}
			if tc.want == "" && len(*loaded) != 0 {
				t.Fatalf("loaded apparmor profiles %v, want none", *loaded)
			}
		})
	}
}

func TestConvertMountsAddsNosuidNodev(t *testing.T) {
	mounts := convertMounts([]*Mount{
		{HostPath: "/var/lib/trellis/volume-staging/a", ContainerPath: "/data"},
		{HostPath: "/var/lib/trellis/probe", ContainerPath: "/run/trellis/probe", ReadOnly: true},
	})
	want := [][]string{
		{"rbind", "rw", "nosuid", "nodev"},
		{"rbind", "ro", "nosuid", "nodev"},
	}
	for i, m := range mounts {
		if m.Type != "bind" || !reflect.DeepEqual(m.Options, want[i]) {
			t.Fatalf("mount %d = %#v, want bind with options %v", i, m, want[i])
		}
		if slices.Contains(m.Options, "noexec") {
			t.Fatalf("mount %d options %v must stay executable", i, m.Options)
		}
	}
	s := buildWorkloadSpec(t, "runc", false, mounts)
	for _, m := range s.Mounts {
		if m.Type == "bind" && (!slices.Contains(m.Options, "nosuid") || !slices.Contains(m.Options, "nodev")) {
			t.Fatalf("spec bind mount %#v lacks nosuid,nodev", m)
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

func TestExecProcessSpecInheritsSecurityContext(t *testing.T) {
	loaded := stubAppArmorProfile(t)
	containerSpec := buildWorkloadSpec(t, "runc", true, nil)
	if len(*loaded) != 1 {
		t.Fatalf("loaded apparmor profiles %v, want one", *loaded)
	}
	process := execProcessSpec(containerSpec, []string{"/bin/sh"}, true)

	if !process.NoNewPrivileges {
		t.Fatal("exec process dropped no_new_privs")
	}
	if process.ApparmorProfile != appArmorProfileName {
		t.Fatalf("apparmor profile = %q, want %q", process.ApparmorProfile, appArmorProfileName)
	}
	if !reflect.DeepEqual(process.Capabilities, containerSpec.Process.Capabilities) {
		t.Fatalf("capabilities = %#v, want %#v", process.Capabilities, containerSpec.Process.Capabilities)
	}
	process.Capabilities.Bounding[0] = "CAP_SYS_ADMIN"
	if slices.Contains(containerSpec.Process.Capabilities.Bounding, "CAP_SYS_ADMIN") {
		t.Fatal("exec capabilities alias the container spec")
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
