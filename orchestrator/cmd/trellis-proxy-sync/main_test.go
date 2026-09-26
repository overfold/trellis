package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/lifecycle"
)

func TestWriteConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "proxy.conf")
	if err := os.WriteFile(path, []byte("old"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := writeConfig(path, []byte("new")); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != "new" {
		t.Fatalf("config = %q, %v; want new", got, err)
	}

	blocked := filepath.Join(dir, "directory")
	if err := os.Mkdir(blocked, 0755); err != nil {
		t.Fatal(err)
	}
	if err := writeConfig(blocked, []byte("partial")); err == nil {
		t.Fatal("expected rename to fail")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("temporary file left behind: %v", entries)
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != "new" {
		t.Fatalf("config after failed write = %q, %v; want new", got, err)
	}
}

func TestWriteConfigPreservesMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "proxy.conf")
	if err := os.WriteFile(path, []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := writeConfig(path, []byte("new")); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0600 {
		t.Fatalf("config mode = %04o; want 0600", got)
	}
}

func TestWriteConfigFollowsSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "proxy.conf")
	link := filepath.Join(dir, "output.conf")
	if err := os.WriteFile(target, []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("proxy.conf", link); err != nil {
		t.Fatal(err)
	}
	if err := writeConfig(link, []byte("new")); err != nil {
		t.Fatal(err)
	}
	if got, err := os.Readlink(link); err != nil || got != "proxy.conf" {
		t.Fatalf("output symlink = %q, %v; want proxy.conf", got, err)
	}
	if got, err := os.ReadFile(target); err != nil || string(got) != "new" {
		t.Fatalf("target config = %q, %v; want new", got, err)
	}
	info, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0600 {
		t.Fatalf("target mode = %04o; want 0600", got)
	}
}

func TestWriteConfigFollowsDanglingSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "proxy.conf")
	link := filepath.Join(dir, "output.conf")
	if err := os.Symlink("proxy.conf", link); err != nil {
		t.Fatal(err)
	}
	if err := writeConfig(link, []byte("new")); err != nil {
		t.Fatal(err)
	}
	if got, err := os.Readlink(link); err != nil || got != "proxy.conf" {
		t.Fatalf("output symlink = %q, %v; want proxy.conf", got, err)
	}
	if got, err := os.ReadFile(target); err != nil || string(got) != "new" {
		t.Fatalf("target config = %q, %v; want new", got, err)
	}
}

func TestSelectUpstreamsExcludesStoppedHealthyAllocation(t *testing.T) {
	allocs := []api.AllocationResponse{
		{
			Phase:   lifecycle.PhaseStopped,
			Health:  lifecycle.HealthHealthy,
			Address: "10.0.0.1",
			Ports:   []api.PortMapping{{HostPort: 31000, ContainerPort: 8080}},
		},
		{
			Phase:   lifecycle.PhaseRunning,
			Health:  lifecycle.HealthHealthy,
			Address: "10.0.0.2",
			Ports:   []api.PortMapping{{HostPort: 32000, ContainerPort: 8080}},
		},
	}

	got := selectUpstreams(allocs, 8080)
	if len(got) != 1 || got[0] != (upstream{Address: "10.0.0.2", Port: 32000, Weight: 1}) {
		t.Fatalf("upstreams = %+v; want only the running healthy allocation", got)
	}
}

func TestSelectHostPort(t *testing.T) {
	ports := []api.PortMapping{
		{HostPort: 31000, ContainerPort: 8080},
		{HostPort: 32000, ContainerPort: 9090},
	}

	if got, ok := selectHostPort(ports, 0); !ok || got != 31000 {
		t.Fatalf("default port = %d, %v; want 31000, true", got, ok)
	}
	if got, ok := selectHostPort(ports, 9090); !ok || got != 32000 {
		t.Fatalf("selected port = %d, %v; want 32000, true", got, ok)
	}
	if got, ok := selectHostPort(ports, 7070); ok || got != 0 {
		t.Fatalf("missing port = %d, %v; want 0, false", got, ok)
	}
}
