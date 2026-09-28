// trellis-health-probe performs task-local HTTP and TCP health checks.
package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
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
		target, err := httpTarget(port, args[2])
		if err != nil {
			return 2
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
		if err != nil || request.URL.Hostname() != loopback {
			return 2
		}
		response, err := probeClient().Do(request)
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

// httpTarget builds the task-local loopback URL for an origin-form request
// path. The path never contributes to the scheme, host, or user information.
func httpTarget(port int, path string) (*url.URL, error) {
	if path == "" {
		path = "/"
	}
	if path[0] != '/' {
		return nil, errors.New("path must begin with /")
	}
	target, err := url.ParseRequestURI(path)
	if err != nil {
		return nil, err
	}
	if target.Scheme != "" || target.Host != "" || target.User != nil {
		return nil, errors.New("path must be an origin-form request target")
	}
	target.Scheme = "http"
	target.Host = net.JoinHostPort(loopback, strconv.Itoa(port))
	return target, nil
}

// probeClient never follows redirects and never uses a proxy, so a check
// cannot leave task-local loopback. A 3xx response is itself the probe result
// and counts as healthy: the task answered on its port.
func probeClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true},
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}
