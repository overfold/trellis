// Package health evaluates and tracks allocation health checks.
package health

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"

	"github.com/clofour/trellis/internal/runtime"
)

// ProbePath is the read-only Trellis executable mounted in isolated runsc tasks.
const ProbePath = "/trellis-health-probe"

// RunProbe executes an HTTP or TCP check from inside a runsc sandbox.
func RunProbe(ctx context.Context, args []string) (bool, error) {
	if len(args) != 3 {
		return false, fmt.Errorf("probe requires type, port, and path")
	}
	port, err := strconv.Atoi(args[1])
	if err != nil || port < 1 || port > 65535 {
		return false, fmt.Errorf("invalid probe port %q", args[1])
	}
	switch args[0] {
	case "http":
		return CheckHTTP(ctx, "127.0.0.1", port, args[2])
	case "tcp":
		return CheckTCP(ctx, "127.0.0.1", port)
	default:
		return false, fmt.Errorf("invalid probe type %q", args[0])
	}
}

// CheckHTTP runs an HTTP health check.
func CheckHTTP(ctx context.Context, addr string, port int, path string) (bool, error) {
	return checkHTTP(ctx, addr, port, path, nil)
}

func checkHTTP(ctx context.Context, addr string, port int, path string, dial func(context.Context, string, string) (net.Conn, error)) (bool, error) {
	transport := &http.Transport{DialContext: dial}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	url := fmt.Sprintf("http://%s:%d%s", addr, port, path)

	request, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return false, fmt.Errorf("constructing request %s: %w", url, err)
	}

	response, err := client.Do(request)
	if err != nil {
		return false, fmt.Errorf("executing request %s: %w", url, err)
	}
	defer func() {
		_ = response.Body.Close()
	}()

	return response.StatusCode >= 200 && response.StatusCode < 300, nil
}

// CheckTCP runs a TCP health check.
func CheckTCP(ctx context.Context, addr string, port int) (bool, error) {
	return checkTCP(ctx, addr, port, (&net.Dialer{}).DialContext)
}

func checkTCP(ctx context.Context, addr string, port int, dial func(context.Context, string, string) (net.Conn, error)) (bool, error) {
	url := net.JoinHostPort(addr, strconv.Itoa(port))

	conn, err := dial(ctx, "tcp", url)
	if err != nil {
		return false, fmt.Errorf("executing request %s: %w", url, err)
	}
	defer func() {
		_ = conn.Close()
	}()

	return true, nil
}

// CheckScript runs a command health check in a container.
func CheckScript(ctx context.Context, c runtime.ContainerRuntime, containerID string, command []string) (bool, error) {
	code, err := c.Exec(ctx, containerID, command)
	if err != nil {
		return false, fmt.Errorf("executing command %s: %w", command, err)
	}

	return code == 0, nil
}
