// trellis-health-probe performs task-local HTTP and TCP health checks.
package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"
)

const loopback = "127.0.0.1"

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
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
