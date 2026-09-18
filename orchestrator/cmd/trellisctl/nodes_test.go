package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/spec"
	"github.com/google/uuid"
)

func TestResolveNodeReference(t *testing.T) {
	first := api.NodeResponse{ID: uuid.MustParse("11111111-1111-1111-1111-111111111111"), Host: "node-a", Port: 8128}
	second := api.NodeResponse{ID: uuid.MustParse("22222222-2222-2222-2222-222222222222"), Host: "node-b", Port: 8128}
	nodes := api.NodeListResponse{first, second}

	for _, ref := range []string{"node-a", "node-a:8128", "11111111", first.ID.String()} {
		got, err := resolveNodeReference(nodes, ref)
		if err != nil {
			t.Fatalf("resolve %q: %v", ref, err)
		}
		if got.ID != first.ID {
			t.Fatalf("resolve %q = %s", ref, got.ID)
		}
	}
}

func TestResolveNodeReferenceRejectsAmbiguousPrefix(t *testing.T) {
	nodes := api.NodeListResponse{
		{ID: uuid.MustParse("11111111-1111-1111-1111-111111111111"), Host: "a", Port: 8128},
		{ID: uuid.MustParse("11112222-2222-2222-2222-222222222222"), Host: "b", Port: 8128},
	}
	_, err := resolveNodeReference(nodes, "1111")
	if err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("expected ambiguity, got %v", err)
	}
}

func TestFormatByteCount(t *testing.T) {
	for _, test := range []struct {
		bytes int64
		want  string
	}{
		{bytes: 512, want: "512 B"},
		{bytes: 1024, want: "1.0 KiB"},
		{bytes: 64 << 20, want: "64.0 MiB"},
		{bytes: 8 << 30, want: "8.0 GiB"},
	} {
		if got := formatByteCount(test.bytes); got != test.want {
			t.Fatalf("formatByteCount(%d) = %q, want %q", test.bytes, got, test.want)
		}
	}
}

func TestPrintNodeStatusShowsPlacementMetadata(t *testing.T) {
	node := api.NodeResponse{
		ID:            uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		Host:          "node-a",
		Port:          8128,
		Status:        api.StatusHealthy,
		LastHeartbeat: time.Date(2026, 9, 2, 15, 0, 0, 0, time.UTC),
		CPU:           4000,
		Memory:        8 << 30,
		OS:            "linux",
		Arch:          "amd64",
		Labels:        map[string]string{"zone": "a", "storage": "fast"},
		Volumes:       []string{"data", "cache"},
		Capabilities:  []spec.NodeCapability{spec.CapabilityRunsc},
		Version:       "v0.1.0",
	}
	var out bytes.Buffer
	if err := printNodeStatus(&out, node); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	for _, want := range []string{
		"Node: node-a:8128",
		"Platform: linux/amd64",
		"CPU: 4000m",
		"Memory: 8.0 GiB",
		"  storage=fast",
		"  zone=a",
		"Volume registrations:\n  cache\n  data",
		"Capabilities:\n  runtime.runsc",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("status output missing %q:\n%s", want, text)
		}
	}
}
