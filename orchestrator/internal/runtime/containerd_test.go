package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"syscall"
	"testing"

	v1stats "github.com/containerd/cgroups/v3/cgroup1/stats"
	v2stats "github.com/containerd/cgroups/v3/cgroup2/stats"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/containerd/v2/pkg/oci"
	"github.com/containerd/errdefs"
	"github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func buildWorkloadSpec(t *testing.T, runtimeName string, appArmorProfile func(string) oci.SpecOpts, mounts []specs.Mount) *oci.Spec {
	t.Helper()
	ctx := namespaces.WithNamespace(context.Background(), trellisNamespace)
	opts := append([]oci.SpecOpts{oci.WithMounts(mounts)}, workloadSecurityOpts(runtimeName, appArmorProfile)...)
	s, err := oci.GenerateSpecWithPlatform(ctx, nil, "linux/amd64", &containers.Container{ID: "allocation"}, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func stubAppArmorProfile(loaded *[]string) func(string) oci.SpecOpts {
	return func(name string) oci.SpecOpts {
		return func(_ context.Context, _ oci.Client, _ *containers.Container, s *specs.Spec) error {
			*loaded = append(*loaded, name)
			s.Process.ApparmorProfile = name
			return nil
		}
	}
}

func TestWorkloadSpecDropsMknodAndRawSocketCapabilities(t *testing.T) {
	for _, runtimeName := range []string{"", "runc", "runsc"} {
		s := buildWorkloadSpec(t, runtimeName, nil, nil)
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
		s := buildWorkloadSpec(t, runtimeName, nil, nil)
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
			var loaded []string
			var profile func(string) oci.SpecOpts
			if tc.supported {
				profile = stubAppArmorProfile(&loaded)
			}
			s := buildWorkloadSpec(t, tc.runtime, profile, nil)
			if s.Process.ApparmorProfile != tc.want {
				t.Fatalf("apparmor profile = %q, want %q", s.Process.ApparmorProfile, tc.want)
			}
			if tc.want == "" && len(loaded) != 0 {
				t.Fatalf("loaded apparmor profiles %v, want none", loaded)
			}
		})
	}
}

func TestEnsureAppArmorProfileIgnoresOtherProfiles(t *testing.T) {
	for _, s := range []*specs.Spec{
		{},
		{Process: &specs.Process{}},
		{Process: &specs.Process{ApparmorProfile: "unconfined"}},
	} {
		if err := ensureAppArmorProfile(s); err != nil {
			t.Fatalf("ensureAppArmorProfile(%#v) = %v, want nil", s.Process, err)
		}
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
	s := buildWorkloadSpec(t, "runc", nil, mounts)
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
		if err := r.removeAllocationFiles("allocation", ".log", "-resolv.conf", "-hosts"); err != nil {
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
	removeLegacyAllocationFiles(dir, "allocation", suffixes...)
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
	if err := r.removeAllocationFiles("allocation", ".log", "-resolv.conf", "-hosts"); err != nil {
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

func TestExecProcessSpecCopiesContainerProcess(t *testing.T) {
	umask := uint32(0o027)
	oomScoreAdj := 100
	containerProcess := &specs.Process{
		Terminal:    false,
		ConsoleSize: &specs.Box{Height: 24, Width: 80},
		User:        specs.User{UID: 1000, GID: 1000, Umask: &umask, AdditionalGids: []uint32{20, 44}, Username: "app"},
		Args:        []string{"/usr/bin/app", "serve"},
		CommandLine: "app serve",
		Env:         []string{"PATH=/usr/local/bin:/usr/bin", "TRELLIS_NAMESPACE=default"},
		Cwd:         "/app",
		Capabilities: &specs.LinuxCapabilities{
			Bounding:  []string{"CAP_CHOWN"},
			Effective: []string{"CAP_CHOWN"},
			Permitted: []string{"CAP_CHOWN"},
		},
		Rlimits:         []specs.POSIXRlimit{{Type: "RLIMIT_NOFILE", Hard: 1024, Soft: 1024}},
		NoNewPrivileges: true,
		ApparmorProfile: "trellis-default",
		OOMScoreAdj:     &oomScoreAdj,
		SelinuxLabel:    "system_u:system_r:container_t:s0",
	}
	containerSpec := &specs.Spec{Process: containerProcess}

	for _, terminal := range []bool{false, true} {
		process, err := execProcessSpec(containerSpec, []string{"node", "-v"}, terminal)
		if err != nil {
			t.Fatal(err)
		}

		want := *containerProcess
		want.Args = []string{"node", "-v"}
		want.CommandLine = ""
		want.Terminal = terminal
		want.ConsoleSize = nil
		if !reflect.DeepEqual(*process, want) {
			t.Fatalf("terminal=%v: process = %#v, want %#v", terminal, *process, want)
		}

		// The exec process must not alias the container's spec.
		process.Env[0] = "PATH=/tmp"
		process.User.AdditionalGids[0] = 0
		*process.User.Umask = 0
		process.Rlimits[0].Hard = 0
		*process.OOMScoreAdj = 0
		if containerProcess.Env[0] != "PATH=/usr/local/bin:/usr/bin" || containerProcess.User.AdditionalGids[0] != 20 ||
			*containerProcess.User.Umask != 0o027 || containerProcess.Rlimits[0].Hard != 1024 || *containerProcess.OOMScoreAdj != 100 ||
			containerProcess.Terminal || containerProcess.ConsoleSize == nil || containerProcess.CommandLine == "" || containerProcess.Args[0] != "/usr/bin/app" {
			t.Fatalf("exec process mutated container process: %#v", containerProcess)
		}
	}
}

func TestExecProcessSpecWrapsSecretEnvironment(t *testing.T) {
	containerSpec := &specs.Spec{
		Process: &specs.Process{User: specs.User{UID: 1000, GID: 1000}},
		Mounts:  []specs.Mount{{Destination: secretEnvContainerPath}},
	}
	process, err := execProcessSpec(containerSpec, []string{"/bin/check"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if want := secretEnvironmentCommand([]string{"/bin/check"}); !reflect.DeepEqual(process.Args, want) {
		t.Fatalf("args = %#v, want %#v", process.Args, want)
	}
	if process.User.UID != 1000 {
		t.Fatalf("uid = %d, want 1000", process.User.UID)
	}
}

func TestExecProcessSpecInheritsSecurityContext(t *testing.T) {
	var loaded []string
	containerSpec := buildWorkloadSpec(t, "runc", stubAppArmorProfile(&loaded), nil)
	if len(loaded) != 1 {
		t.Fatalf("loaded apparmor profiles %v, want one", loaded)
	}
	process, err := execProcessSpec(containerSpec, []string{"/bin/sh"}, true)
	if err != nil {
		t.Fatal(err)
	}

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

func TestManagedSecretsStayOutOfPersistedOCIEnvironmentAndUseProcessOwner(t *testing.T) {
	dir := t.TempDir()
	envDir := filepath.Join(dir, "env")
	if err := os.Mkdir(envDir, 0o700); err != nil {
		t.Fatal(err)
	}
	const sentinel = "persisted-oci-secret-sentinel"
	envFile := filepath.Join(envDir, "PASSWORD")
	fileSecret := filepath.Join(dir, "file-secret")
	for _, path := range []string{envFile, fileSecret} {
		if err := os.WriteFile(path, []byte(sentinel), 0o400); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chmod(envDir, 0o500); err != nil {
		t.Fatal(err)
	}
	uid, gid := os.Geteuid(), os.Getegid()
	targetUID, targetGID := uint32(uid), uint32(gid)
	if uid == 0 {
		targetUID, targetGID = 65534, 65534
	}
	t.Cleanup(func() {
		for _, path := range []string{envDir, envFile, fileSecret} {
			_ = os.Chown(path, uid, gid)
		}
		_ = os.Chmod(envDir, 0o700)
	})
	mounts := []*Mount{
		{HostPath: envDir, ContainerPath: secretEnvContainerPath, ReadOnly: true, Secret: true, SecretEnv: true},
		{HostPath: fileSecret, ContainerPath: "/run/trellis-secrets/token", ReadOnly: true, Secret: true},
	}
	spec := &oci.Spec{
		Process: &specs.Process{Args: []string{"/app", "serve"}, Env: []string{"PATH=/usr/bin"}, User: specs.User{UID: targetUID, GID: targetGID}},
		Mounts:  convertMounts(mounts),
	}
	if err := withManagedSecretMounts(mounts)(context.Background(), nil, nil, spec); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte(sentinel)) {
		t.Fatal("persisted OCI spec contains secret plaintext")
	}
	wantArgs := secretEnvironmentCommand([]string{"/app", "serve"})
	if !reflect.DeepEqual(spec.Process.Args, wantArgs) {
		t.Fatalf("process args = %#v, want %#v", spec.Process.Args, wantArgs)
	}
	for _, path := range []string{envDir, envFile, fileSecret} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		stat := info.Sys().(*syscall.Stat_t)
		if stat.Uid != targetUID || stat.Gid != targetGID {
			t.Fatalf("%s owner = %d:%d, want %d:%d", path, stat.Uid, stat.Gid, targetUID, targetGID)
		}
		if info.Mode().Perm()&0o077 != 0 {
			t.Fatalf("%s mode = %o, accessible beyond owner", path, info.Mode().Perm())
		}
		if path == envDir && info.Mode().Perm()&0o200 != 0 {
			t.Fatalf("environment secret directory mode = %o, writable by workload user", info.Mode().Perm())
		}
	}
}

func TestExecProcessSpecDefaultsEmptyCwd(t *testing.T) {
	process, err := execProcessSpec(&specs.Spec{Process: &specs.Process{User: specs.User{UID: 1000}}}, []string{"/bin/true"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if process.Cwd != "/" {
		t.Fatalf("cwd = %q, want /", process.Cwd)
	}
	if process.User.UID != 1000 {
		t.Fatalf("uid = %d, want 1000", process.User.UID)
	}
}

func TestExecProcessSpecFailsClosedWithoutContainerProcess(t *testing.T) {
	for name, containerSpec := range map[string]*specs.Spec{
		"nil spec":    nil,
		"nil process": {},
	} {
		t.Run(name, func(t *testing.T) {
			if process, err := execProcessSpec(containerSpec, []string{"/bin/true"}, true); err == nil {
				t.Fatalf("process = %#v, want error", process)
			}
		})
	}
}

type specContainer struct {
	containerd.Container
	spec *specs.Spec
	err  error
}

func (c specContainer) ID() string { return "container-1" }

func (c specContainer) Spec(context.Context) (*oci.Spec, error) { return c.spec, c.err }

func TestContainerExecProcessFailsClosed(t *testing.T) {
	specErr := errors.New("spec unavailable")
	if _, err := containerExecProcess(context.Background(), specContainer{err: specErr}, []string{"/bin/true"}, false); !errors.Is(err, specErr) {
		t.Fatalf("err = %v, want %v", err, specErr)
	}
	if _, err := containerExecProcess(context.Background(), specContainer{spec: &specs.Spec{}}, []string{"/bin/true"}, false); err == nil {
		t.Fatal("exec process without container process succeeded")
	}

	process, err := containerExecProcess(context.Background(), specContainer{spec: &specs.Spec{Process: &specs.Process{User: specs.User{UID: 1000, GID: 1000}}}}, []string{"/bin/true"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if process.User.UID != 1000 || process.User.GID != 1000 {
		t.Fatalf("user = %#v, want uid/gid 1000", process.User)
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

func TestSwapLimitApplies(t *testing.T) {
	for _, tc := range []struct {
		name    string
		files   []string
		runtime string
		want    bool
	}{
		{name: "v2 runc without detected swap accounting", files: []string{"cgroup.controllers"}, runtime: "runc", want: true},
		{name: "v2 default runtime is runc", files: []string{"cgroup.controllers"}, want: true},
		{name: "v2 runsc in nested cgroup namespace", files: []string{"cgroup.controllers", "memory.swap.max"}, runtime: "runsc", want: true},
		{name: "v2 runsc with swap", files: []string{"cgroup.controllers", "system.slice/memory.swap.max"}, runtime: "runsc", want: true},
		{name: "v2 runsc without swap accounting", files: []string{"cgroup.controllers", "system.slice/memory.max"}, runtime: "runsc"},
		{name: "v1 with memsw", files: []string{"memory/memory.memsw.limit_in_bytes"}, runtime: "runc", want: true},
		{name: "v1 without memsw", files: []string{"memory/memory.limit_in_bytes"}, runtime: "runc"},
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
			if got := swapLimitApplies(root, tc.runtime); got != tc.want {
				t.Fatalf("swapLimitApplies = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestPidsControllerDetected(t *testing.T) {
	for name, tc := range map[string]struct {
		files map[string]string
		want  bool
	}{
		"v2 with pids":    {files: map[string]string{"cgroup.controllers": "cpuset cpu io memory pids\n"}, want: true},
		"v2 without pids": {files: map[string]string{"cgroup.controllers": "cpuset cpu io memory\n"}},
		"v1 with pids":    {files: map[string]string{"pids/pids.max": "max"}, want: true},
		"v1 without pids": {files: map[string]string{"memory/memory.limit_in_bytes": "0"}},
	} {
		root := t.TempDir()
		for path, data := range tc.files {
			path = filepath.Join(root, path)
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		if got := pidsControllerDetected(root); got != tc.want {
			t.Errorf("%s: pidsControllerDetected = %t, want %t", name, got, tc.want)
		}
	}
}

func TestSwapActive(t *testing.T) {
	dir := t.TempDir()
	for name, tc := range map[string]struct {
		data string
		want bool
	}{
		"none":   {data: "Filename\t\t\t\tType\t\tSize\t\tUsed\t\tPriority\n"},
		"active": {data: "Filename\t\t\t\tType\t\tSize\t\tUsed\t\tPriority\n/swapfile file 1048572 0 -2\n", want: true},
	} {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(tc.data), 0o644); err != nil {
			t.Fatal(err)
		}
		if got := swapActive(path); got != tc.want {
			t.Errorf("%s: swapActive = %t, want %t", name, got, tc.want)
		}
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

func TestDecodeContainerMetricsBounds(t *testing.T) {
	for _, test := range []struct {
		name, version string
		cpu, memory   uint64
		wantCPU       int64
		wantErr       bool
	}{
		{"units", "v1", 23, 47, 23, false},
		{"units", "v2", 23, 47, 23000, false},
		{"CPU limit", "v1", math.MaxInt64, 47, math.MaxInt64, false},
		{"CPU overflow", "v1", uint64(math.MaxInt64) + 1, 47, 0, true},
		{"CPU limit", "v2", math.MaxInt64 / 1000, 47, 9223372036854775000, false},
		{"CPU overflow", "v2", math.MaxInt64/1000 + 1, 47, 0, true},
		{"memory limit", "v1", 23, math.MaxInt64, 23, false},
		{"memory overflow", "v1", 23, uint64(math.MaxInt64) + 1, 0, true},
		{"memory limit", "v2", 23, math.MaxInt64, 23000, false},
		{"memory overflow", "v2", 23, uint64(math.MaxInt64) + 1, 0, true},
	} {
		t.Run(test.version+"/"+test.name, func(t *testing.T) {
			var message proto.Message
			if test.version == "v2" {
				message = &v2stats.Metrics{CPU: &v2stats.CPUStat{UsageUsec: test.cpu}, Memory: &v2stats.MemoryStat{Usage: test.memory}}
			} else {
				message = &v1stats.Metrics{CPU: &v1stats.CPUStat{Usage: &v1stats.CPUUsage{Total: test.cpu}}, Memory: &v1stats.MemoryStat{Usage: &v1stats.MemoryEntry{Usage: test.memory}}}
			}
			raw, err := proto.Marshal(message)
			if err != nil {
				t.Fatal(err)
			}
			got, err := decodeContainerMetrics(&anypb.Any{TypeUrl: "io.containerd.cgroups." + test.version + ".Metrics", Value: raw})
			if test.wantErr {
				if err == nil {
					t.Fatalf("overflowing metric accepted: %+v", got)
				}
				return
			}
			if err != nil || got.CPUUsageNanoseconds != test.wantCPU || got.MemoryUsageBytes != int64(test.memory) {
				t.Fatalf("metrics = %+v, %v; want CPU %d and memory %d", got, err, test.wantCPU, test.memory)
			}
		})
	}
}

func TestResourceSpecMinimumCPUQuota(t *testing.T) {
	for _, cpu := range []int{0, 1, 9, 10, 11} {
		opts, err := resourceSpecOpts(CreateOptions{CPU: cpu}, true)
		if cpu > 0 && cpu < 10 {
			if err == nil {
				t.Fatalf("CPU %d accepted below the minimum CFS quota", cpu)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if cpu == 0 {
			continue
		}
		generated := specs.Spec{Linux: &specs.Linux{}}
		for _, opt := range opts {
			if err := opt(context.Background(), nil, nil, &generated); err != nil {
				t.Fatal(err)
			}
		}
		if got := *generated.Linux.Resources.CPU.Quota; got != int64(cpu)*100 {
			t.Fatalf("CPU %d quota=%d", cpu, got)
		}
	}
}
