// Package health evaluates and tracks allocation health checks.
package health

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/overfold/trellis/orchestrator/internal/probepath"
	"github.com/overfold/trellis/orchestrator/internal/runtime"
)

// ProbeContainerPath is the reserved path of the health probe inside tasks.
const ProbeContainerPath = "/run/trellis/health-probe"

// CheckHTTP runs an HTTP health check inside a task.
func CheckHTTP(ctx context.Context, c runtime.ContainerRuntime, containerID string, port int, path string, timeout time.Duration) (bool, error) {
	return checkProbe(ctx, c, containerID, "http", port, []string{ProbeContainerPath, "http", strconv.Itoa(port), path, timeout.String()})
}

// CheckTCP runs a TCP health check inside a task.
func CheckTCP(ctx context.Context, c runtime.ContainerRuntime, containerID string, port int, timeout time.Duration) (bool, error) {
	return checkProbe(ctx, c, containerID, "tcp", port, []string{ProbeContainerPath, "tcp", strconv.Itoa(port), timeout.String()})
}

func checkProbe(ctx context.Context, c runtime.ContainerRuntime, containerID, kind string, port int, command []string) (bool, error) {
	code, err := c.Exec(ctx, containerID, command)
	if err != nil {
		return false, fmt.Errorf("executing health probe: %w", err)
	}
	if code == probepath.UsageExit {
		// Name the check rather than echo its full argv.
		return false, fmt.Errorf("health probe in %s rejected its %s check configuration for port %d", containerID, kind, port)
	}
	return code == 0, nil
}

// CheckScript runs a command health check in a container.
func CheckScript(ctx context.Context, c runtime.ContainerRuntime, containerID string, command []string) (bool, error) {
	code, err := c.Exec(ctx, containerID, command)
	if err != nil {
		return false, fmt.Errorf("executing command %s: %w", command, err)
	}

	return code == 0, nil
}
