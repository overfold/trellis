package health

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/clofour/trellis/internal/runtime"
	"github.com/clofour/trellis/internal/spec"
)

type runscProbeRuntime struct {
	runtime.ContainerRuntime
	command []string
}

func (r *runscProbeRuntime) Exec(_ context.Context, _ string, command []string) (int, error) {
	r.command = command
	return 0, nil
}

func (r *runscProbeRuntime) NetworkNamespace(context.Context, string) (string, error) {
	return "", fmt.Errorf("runsc probe must not enter the Linux network namespace")
}

func TestIsolatedRunscNetworkChecksExecInsideSandbox(t *testing.T) {
	for _, kind := range []spec.HealthCheckType{"http", "tcp"} {
		t.Run(string(kind), func(t *testing.T) {
			rt := &runscProbeRuntime{}
			h := NewHealthManager(nil, rt, nil)
			config := newHealthConfig(&spec.HealthCheckSpec{Type: kind, Port: 8080, Path: "/health"})
			config.Isolated = true
			config.Runtime = "runsc"
			ok, err := h.runHealthCheck(context.Background(), &trackedTask{containerID: "task", config: config})
			if err != nil || !ok {
				t.Fatalf("runsc check = %v, %v", ok, err)
			}
			want := []string{ProbePath, "__health-probe", string(kind), "8080", "/health", defaultCheckTimeout.String()}
			if !slices.Equal(rt.command, want) {
				t.Fatalf("exec command = %q, want %q", rt.command, want)
			}
		})
	}
}

func TestNewHealthConfigUsesDefaults(t *testing.T) {
	config := newHealthConfig(&spec.HealthCheckSpec{Type: "tcp", Port: 8080})
	if config.Interval != defaultCheckInterval {
		t.Fatalf("interval = %s, want %s", config.Interval, defaultCheckInterval)
	}
	if config.Timeout != defaultCheckTimeout {
		t.Fatalf("timeout = %s, want %s", config.Timeout, defaultCheckTimeout)
	}
	if config.Threshold != defaultCheckThreshold {
		t.Fatalf("threshold = %d, want %d", config.Threshold, defaultCheckThreshold)
	}
}

func TestNetworkChecksUseProvidedDialer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	serverAddr := strings.TrimPrefix(server.URL, "http://")
	_, portText, err := net.SplitHostPort(serverAddr)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}
	var called atomic.Int32
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		called.Add(1)
		return (&net.Dialer{}).DialContext(ctx, network, serverAddr)
	}
	for _, check := range []func() (bool, error){
		func() (bool, error) { return checkHTTP(context.Background(), "127.0.0.1", port, "/health", dial) },
		func() (bool, error) { return checkTCP(context.Background(), "127.0.0.1", port, dial) },
	} {
		ok, err := check()
		if err != nil || !ok {
			t.Fatalf("check = %v, %v", ok, err)
		}
	}
	if called.Load() != 2 {
		t.Fatalf("dial calls = %d, want 2", called.Load())
	}
}

func TestRunProbeChecksSandboxLoopback(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	_, port, err := net.SplitHostPort(strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"http", "tcp"} {
		t.Run(kind, func(t *testing.T) {
			ok, err := RunProbe(context.Background(), []string{kind, port, "/health", time.Second.String()})
			if err != nil || !ok {
				t.Fatalf("probe = %v, %v", ok, err)
			}
		})
	}
}

func TestRunProbeTimesOutWhenHTTPServerDoesNotRespond(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(entered)
		<-release
	}))
	defer server.Close()
	defer close(release)
	_, port, err := net.SplitHostPort(strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}

	result := make(chan error, 1)
	go func() {
		ok, err := RunProbe(context.Background(), []string{"http", port, "/health", "100ms"})
		if ok {
			result <- fmt.Errorf("unresponsive endpoint reported healthy")
			return
		}
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("probe did not reach the server")
	}
	select {
	case err := <-result:
		if err == nil || !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
			t.Fatalf("probe error = %v, want deadline exceeded", err)
		}
	case <-time.After(time.Second):
		t.Fatal("probe did not time out")
	}
}

func TestNewHealthConfigUsesConfiguredValues(t *testing.T) {
	config := newHealthConfig(&spec.HealthCheckSpec{
		Type: "tcp", Port: 8080, Interval: 2 * time.Second, Timeout: time.Second, Threshold: 5,
	})
	if config.Interval != 2*time.Second || config.Timeout != time.Second || config.Threshold != 5 {
		t.Fatalf("unexpected health config: %#v", config)
	}
}
