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
	return checkTCP(ctx, addr, port, nil)
}

func checkTCP(ctx context.Context, addr string, port int, dial func(context.Context, string, string) (net.Conn, error)) (bool, error) {
	url := net.JoinHostPort(addr, strconv.Itoa(port))

	dialer := net.Dialer{}
	if dial == nil {
		dial = dialer.DialContext
	}
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
