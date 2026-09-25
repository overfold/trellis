package main

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
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
