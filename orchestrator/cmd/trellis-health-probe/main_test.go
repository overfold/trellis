package main

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/clofour/trellis/internal/probepath"
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
	for _, path := range []string{"@169.254.169.254/latest", "http://169.254.169.254/latest", "health", "/%zz", "/a|b", "/héalth", "/a#b"} {
		t.Run(path, func(t *testing.T) {
			if code := run([]string{"http", "8080", path, "1s"}); code != probepath.UsageExit {
				t.Fatalf("probe of %q exit code = %d, want %d", path, code, probepath.UsageExit)
			}
		})
	}
}

func requestPort(r *http.Request) string {
	_, port, _ := net.SplitHostPort(r.Host)
	return port
}

func TestHTTPProbeFollowsOnlyLoopbackRedirects(t *testing.T) {
	var offHost atomic.Bool
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		offHost.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer other.Close()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ok":
			w.WriteHeader(http.StatusOK)
		case "/failing":
			w.WriteHeader(http.StatusServiceUnavailable)
		case "/to-ok":
			http.Redirect(w, r, "/ok", http.StatusFound)
		case "/to-failing":
			http.Redirect(w, r, "/failing", http.StatusMovedPermanently)
		case "/to-metadata":
			http.Redirect(w, r, "http://169.254.169.254/latest", http.StatusFound)
		case "/to-other-port":
			http.Redirect(w, r, other.URL+"/", http.StatusFound)
		case "/to-https":
			http.Redirect(w, r, "https://"+r.Host+"/ok", http.StatusFound)
		case "/to-localhost-failing":
			http.Redirect(w, r, "http://localhost:"+requestPort(r)+"/failing", http.StatusFound)
		case "/to-padded-port-failing":
			http.Redirect(w, r, "http://127.0.0.1:0"+requestPort(r)+"/failing", http.StatusFound)
		case "/to-external-after-hops":
			http.Redirect(w, r, "/hops-then-external/9", http.StatusFound)
		case "/to-localhost-dot-ok":
			http.Redirect(w, r, "http://localhost.:"+requestPort(r)+"/ok", http.StatusFound)
		case "/to-mapped-loopback":
			http.Redirect(w, r, "http://[::ffff:7f00:1]:"+requestPort(r)+"/failing", http.StatusFound)
		case "/to-ipv6-loopback":
			http.Redirect(w, r, "http://[::1]:"+requestPort(r)+"/ok", http.StatusFound)
		case "/loop":
			http.Redirect(w, r, "/loop", http.StatusFound)
		default:
			if hops, err := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/hops-then-external/")); err == nil {
				if hops == 0 {
					http.Redirect(w, r, "https://example.com/", http.StatusFound)
					return
				}
				http.Redirect(w, r, "/hops-then-external/"+strconv.Itoa(hops-1), http.StatusFound)
				return
			}
			if hops, err := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/hops/")); err == nil {
				if hops == 0 {
					w.WriteHeader(http.StatusOK)
					return
				}
				http.Redirect(w, r, "/hops/"+strconv.Itoa(hops-1), http.StatusFound)
			}
		}
	}))
	defer server.Close()
	port := strings.TrimPrefix(server.URL, "http://127.0.0.1:")

	for path, want := range map[string]int{
		"/to-ok":                  0,
		"/to-failing":             1,
		"/to-metadata":            0,
		"/to-other-port":          0,
		"/to-https":               0,
		"/loop":                   1,
		"/to-localhost-failing":   1,
		"/to-localhost-dot-ok":    0,
		"/to-padded-port-failing": 1,
		"/to-ipv6-loopback":       0, // a different socket: not followed
		"/to-mapped-loopback":     0,
		"/hops/10":                0,
		"/hops/11":                1,
		"/to-external-after-hops": 0,
	} {
		if code := run([]string{"http", port, path, "2s"}); code != want {
			t.Errorf("probe of %s exit code = %d, want %d", path, code, want)
		}
	}
	if offHost.Load() {
		t.Fatal("probe followed a redirect off the probed loopback port")
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
			if code := run(args); code != probepath.UsageExit {
				t.Fatalf("invalid arguments exit code = %d, want %d", code, probepath.UsageExit)
			}
		})
	}
}

func TestProbeClientDialsOnlyLoopback(t *testing.T) {
	_, err := probeClient(80).Get("http://192.0.2.1:80/")
	if err == nil || !strings.Contains(err.Error(), "not the probed loopback address") {
		t.Fatalf("non-loopback dial error = %v, want refusal", err)
	}
}

func TestReadSecretEnvironmentReadsOnlyMountedDirectoryFiles(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "PASSWORD"), []byte("secret-value"), 0o400); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "ignored"), 0o700); err != nil {
		t.Fatal(err)
	}
	values, err := readSecretEnvironment(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 1 || !bytes.Equal(values["PASSWORD"], []byte("secret-value")) {
		t.Fatalf("environment values = %#v", values)
	}
	clear(values["PASSWORD"])
}
