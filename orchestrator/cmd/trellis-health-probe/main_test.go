package main

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

func TestHTTPProbe(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ready" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	port := strings.TrimPrefix(server.URL, "http://127.0.0.1:")

	if code := run([]string{"http", port, "/ready", "1s"}); code != 0 {
		t.Fatalf("healthy HTTP probe exit code = %d, want 0", code)
	}
	if code := run([]string{"http", port, "/missing", "1s"}); code != 1 {
		t.Fatalf("unhealthy HTTP probe exit code = %d, want 1", code)
	}
}

func TestHTTPProbeKeepsRequestOnLoopback(t *testing.T) {
	var got []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = append(got, r.Host+" "+r.RequestURI)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	port := strings.TrimPrefix(server.URL, "http://127.0.0.1:")

	for _, path := range []string{"", "/@169.254.169.254/latest", "//169.254.169.254/latest", "/a%2Fb?x=1&y=%20"} {
		if code := run([]string{"http", port, path, "1s"}); code != 0 {
			t.Fatalf("probe of %q exit code = %d, want 0", path, code)
		}
	}
	want := []string{
		"127.0.0.1:" + port + " /",
		"127.0.0.1:" + port + " /@169.254.169.254/latest",
		"127.0.0.1:" + port + " //169.254.169.254/latest",
		"127.0.0.1:" + port + " /a%2Fb?x=1&y=%20",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("requests = %q, want %q", got, want)
	}
}

func TestHTTPProbeRejectsNonOriginPaths(t *testing.T) {
	for _, path := range []string{"@169.254.169.254/latest", "http://169.254.169.254/latest", "health", "/%zz"} {
		t.Run(path, func(t *testing.T) {
			if code := run([]string{"http", "8080", path, "1s"}); code != 2 {
				t.Fatalf("probe of %q exit code = %d, want 2", path, code)
			}
		})
	}
}

func TestHTTPProbeDoesNotFollowRedirects(t *testing.T) {
	var followed atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/target" {
			followed.Store(true)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		http.Redirect(w, r, "/target", http.StatusFound)
	}))
	defer server.Close()
	port := strings.TrimPrefix(server.URL, "http://127.0.0.1:")

	if code := run([]string{"http", port, "/health", "1s"}); code != 0 {
		t.Fatalf("redirecting HTTP probe exit code = %d, want 0 for the unfollowed 3xx", code)
	}
	if followed.Load() {
		t.Fatal("probe followed a redirect")
	}
}

func TestTCPProbe(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	port := strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)

	if code := run([]string{"tcp", port, "1s"}); code != 0 {
		t.Fatalf("healthy TCP probe exit code = %d, want 0", code)
	}
}

func TestProbeRejectsUnsupportedArguments(t *testing.T) {
	for _, args := range [][]string{
		{},
		{"udp", "80", "1s"},
		{"tcp", "0", "1s"},
		{"tcp", "80", "0s"},
		{"tcp", "80", "1s", "extra"},
		{"http", "80", "/"},
	} {
		t.Run(fmt.Sprint(args), func(t *testing.T) {
			if code := run(args); code != 2 {
				t.Fatalf("invalid arguments exit code = %d, want 2", code)
			}
		})
	}
}
