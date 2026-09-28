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

	"github.com/clofour/trellis/internal/probepath"
)

const (
	loopback     = "127.0.0.1"
	maxRedirects = 10
)

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
		if err != nil {
			return 2
		}
		response, err := probeClient(target.Host).Do(request)
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

// probeClient never uses a proxy and follows at most maxRedirects redirects,
// only while they stay on the probed loopback host and port. Like a kubelet
// HTTP probe, it stops at a redirect anywhere else and treats that 3xx
// response as the result, so a check never leaves task-local loopback.
func probeClient(host string) *http.Client {
	return &http.Client{
		Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true},
		CheckRedirect: func(request *http.Request, via []*http.Request) error {
			if len(via) >= maxRedirects {
				return errors.New("too many redirects")
			}
			if request.URL.Scheme != "http" || canonicalHost(request.URL) != host {
				return http.ErrUseLastResponse
			}
			return nil
		},
	}
}

// canonicalHost returns host:port with the scheme's default port filled in.
func canonicalHost(target *url.URL) string {
	port := target.Port()
	if port == "" {
		port = "80"
	}
	return net.JoinHostPort(target.Hostname(), port)
}
