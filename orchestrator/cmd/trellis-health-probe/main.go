// trellis-health-probe performs task-local HTTP and TCP health checks.
package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const loopback = "127.0.0.1"

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) > 0 && args[0] == "env-exec" {
		return runEnvExec(args[1:])
	}
	if len(args) < 3 {
		return 2
	}

	port, err := strconv.Atoi(args[1])
	if err != nil || port < 1 || port > 65535 {
		return 2
	}
	timeout, err := time.ParseDuration(args[len(args)-1])
	if err != nil || timeout <= 0 {
		return 2
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	switch args[0] {
	case "http":
		if len(args) != 4 {
			return 2
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://%s:%d%s", loopback, port, args[2]), nil)
		if err != nil {
			return 1
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			return 1
		}
		_ = response.Body.Close()
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return 1
		}
		return 0
	case "tcp":
		if len(args) != 3 {
			return 2
		}
		conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort(loopback, strconv.Itoa(port)))
		if err != nil {
			return 1
		}
		_ = conn.Close()
		return 0
	default:
		return 2
	}
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
