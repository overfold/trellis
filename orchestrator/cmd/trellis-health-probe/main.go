// trellis-health-probe performs task-local HTTP and TCP health checks.
package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/overfold/trellis/orchestrator/internal/probepath"
)

const (
	loopback     = "127.0.0.1"
	maxRedirects = 10
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) > 0 && args[0] == "env-exec" {
		return runEnvExec(args[1:])
	}
	if len(args) < 3 {
		return probepath.UsageExit
	}

	port, err := strconv.Atoi(args[1])
	if err != nil || port < 1 || port > 65535 {
		return probepath.UsageExit
	}
	timeout, err := time.ParseDuration(args[len(args)-1])
	if err != nil || timeout <= 0 {
		return probepath.UsageExit
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	switch args[0] {
	case "http":
		if len(args) != 4 {
			return probepath.UsageExit
		}
		target, err := httpTarget(port, args[2])
		if err != nil {
			return probepath.UsageExit
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
		if err != nil {
			return probepath.UsageExit
		}
		response, err := probeClient(port).Do(request)
		if err != nil {
			return 1
		}
		_ = response.Body.Close()
		if response.StatusCode < 200 || response.StatusCode >= 400 {
			return 1
		}
		return 0
	case "tcp":
		if len(args) != 3 {
			return probepath.UsageExit
		}
		conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort(loopback, strconv.Itoa(port)))
		if err != nil {
			return 1
		}
		_ = conn.Close()
		return 0
	default:
		return probepath.UsageExit
	}
}

// httpTarget builds the task-local loopback URL for a validated origin-form
// request path. The path never contributes to the scheme, host, or user
// information, and it is sent unchanged.
func httpTarget(port int, path string) (*url.URL, error) {
	if err := probepath.Validate(path); err != nil {
		return nil, err
	}
	if path == "" {
		path = "/"
	}
	target, err := url.ParseRequestURI(path)
	if err != nil {
		return nil, err
	}
	target.Scheme = "http"
	target.Host = net.JoinHostPort(loopback, strconv.Itoa(port))
	return target, nil
}

// probeClient never uses a proxy or resolves names, dials only 127.0.0.1, and
// follows at most maxRedirects redirects, only while they stay on 127.0.0.1
// (or localhost) and the probed port. Like a kubelet HTTP probe, it stops
// at a redirect anywhere else and treats that 3xx response as the result, so a
// check never leaves task-local loopback.
func probeClient(port int) *http.Client {
	dialer := &net.Dialer{}
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		host, targetPort, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		if !loopbackHost(host) {
			return nil, fmt.Errorf("refusing %s: not the probed loopback address", address)
		}
		return dialer.DialContext(ctx, network, net.JoinHostPort(loopback, targetPort))
	}
	return &http.Client{
		Transport: &http.Transport{Proxy: nil, DialContext: dial, DisableKeepAlives: true},
		CheckRedirect: func(request *http.Request, via []*http.Request) error {
			if !loopbackTarget(request.URL, port) {
				return http.ErrUseLastResponse
			}
			if len(via) > maxRedirects {
				return errors.New("too many redirects")
			}
			return nil
		},
	}
}

// loopbackTarget reports whether target is plain HTTP to the probed socket:
// 127.0.0.1 (or localhost) on port.
func loopbackTarget(target *url.URL, port int) bool {
	if target.Scheme != "http" {
		return false
	}
	targetPort := 80
	if target.Port() != "" {
		var err error
		if targetPort, err = strconv.Atoi(target.Port()); err != nil {
			return false
		}
	}
	return loopbackHost(target.Hostname()) && targetPort == port
}

// loopbackHost reports whether host is 127.0.0.1 or localhost. localhost is
// matched by name without consulting a resolver, and other spellings of the
// loopback address (including IPv4-mapped IPv6) are refused.
func loopbackHost(host string) bool {
	return host == loopback || strings.EqualFold(strings.TrimSuffix(host, "."), "localhost")
}

func runEnvExec(args []string) int {
	if len(args) < 3 || args[1] != "--" {
		return 2
	}
	values, err := readSecretEnvironment(args[0])
	if err != nil {
		return 1
	}
	for name, value := range values {
		if err := os.Setenv(name, string(value)); err != nil {
			clear(value)
			return 1
		}
		clear(value)
	}
	path, err := execPath(args[2], os.Getenv("PATH"))
	if err != nil {
		return 1
	}
	if err := syscall.Exec(path, args[2:], os.Environ()); err != nil {
		return 1
	}
	return 0
}

func readSecretEnvironment(dir string) (map[string][]byte, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	values := make(map[string][]byte)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		value, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			for _, previous := range values {
				clear(previous)
			}
			return nil, err
		}
		values[name] = value
	}
	return values, nil
}

func execPath(command, pathEnv string) (string, error) {
	if strings.ContainsRune(command, '/') {
		return command, nil
	}
	for _, dir := range filepath.SplitList(pathEnv) {
		candidate := filepath.Join(dir, command)
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() && info.Mode()&0o111 != 0 {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("executable %q not found", command)
}
