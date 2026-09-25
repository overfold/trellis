package health

import (
	"context"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/clofour/trellis/internal/runtime"
)

type probeRuntime struct {
	containerID string
	command     []string
	exitCode    int
}

func (*probeRuntime) Pull(context.Context, string) error { return nil }
func (*probeRuntime) Create(context.Context, runtime.CreateOptions) (string, error) {
	return "", nil
}
func (*probeRuntime) Start(context.Context, string) error   { return nil }
func (*probeRuntime) Restart(context.Context, string) error { return nil }
func (*probeRuntime) Stop(context.Context, string) error    { return nil }
func (*probeRuntime) Remove(context.Context, string) error  { return nil }
func (r *probeRuntime) Exec(_ context.Context, containerID string, command []string) (int, error) {
	r.containerID = containerID
	r.command = append([]string(nil), command...)
	return r.exitCode, nil
}
func (*probeRuntime) ExecOutput(context.Context, string, []string) ([]byte, []byte, int, error) {
	return nil, nil, 0, nil
}
func (*probeRuntime) StartTerminal(context.Context, string, []string, string, uint32, uint32) (runtime.TerminalSession, error) {
	return nil, nil
}
func (*probeRuntime) Metrics(context.Context, string) (*runtime.ContainerMetrics, error) {
	return &runtime.ContainerMetrics{}, nil
}
func (*probeRuntime) Inspect(context.Context, string) (*runtime.ContainerInfo, error) {
	return &runtime.ContainerInfo{}, nil
}
func (*probeRuntime) Logs(context.Context, string, bool, int) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

func TestHTTPAndTCPChecksExecTaskLocalProbe(t *testing.T) {
	for _, test := range []struct {
		name    string
		check   func(context.Context, runtime.ContainerRuntime) (bool, error)
		command []string
	}{
		{
			name: "http",
			check: func(ctx context.Context, rt runtime.ContainerRuntime) (bool, error) {
				return CheckHTTP(ctx, rt, "container", 8080, "/ready", 1500*time.Millisecond)
			},
			command: []string{ProbeContainerPath, "http", "8080", "/ready", "1.5s"},
		},
		{
			name: "tcp",
			check: func(ctx context.Context, rt runtime.ContainerRuntime) (bool, error) {
				return CheckTCP(ctx, rt, "container", 5432, 2*time.Second)
			},
			command: []string{ProbeContainerPath, "tcp", "5432", "2s"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			rt := &probeRuntime{}
			healthy, err := test.check(context.Background(), rt)
			if err != nil || !healthy {
				t.Fatalf("check = %v, %v; want healthy", healthy, err)
			}
			if rt.containerID != "container" || !reflect.DeepEqual(rt.command, test.command) {
				t.Fatalf("exec = %q %#v, want container %#v", rt.containerID, rt.command, test.command)
			}
		})
	}
}

func TestScriptCheckExecutesUserCommandUnchanged(t *testing.T) {
	rt := &probeRuntime{}
	command := []string{"/bin/check", "--verbose"}
	if healthy, err := CheckScript(context.Background(), rt, "container", command); err != nil || !healthy {
		t.Fatalf("check = %v, %v; want healthy", healthy, err)
	}
	if !reflect.DeepEqual(rt.command, command) {
		t.Fatalf("exec command = %#v, want %#v", rt.command, command)
	}
}
