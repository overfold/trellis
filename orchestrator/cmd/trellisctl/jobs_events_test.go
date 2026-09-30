package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/overfold/trellis/orchestrator/api"
)

func TestJobsStatusObservationModesRegistered(t *testing.T) {
	config = CLIConfig{}
	root := newRootCmd()
	cmd, _, err := root.Find([]string{"jobs", "status"})
	if err != nil {
		t.Fatalf("find jobs status: %v", err)
	}
	for _, flag := range []string{"watch", "history", "allocation", "output"} {
		if cmd.Flags().Lookup(flag) == nil {
			t.Fatalf("jobs status is missing --%s", flag)
		}
	}
}

func TestPrintJobEvents(t *testing.T) {
	events := []jobAllocationEvent{
		{
			Allocation: "abcdef12-rest",
			Group:      "web",
			Phase:      api.PhaseRunning,
			At:         time.Date(2026, 9, 3, 12, 0, 2, 0, time.UTC),
		},
		{
			Allocation: "abcdef12-rest",
			Group:      "web",
			Phase:      api.PhaseFailed,
			Reason:     "image_pull_failed",
			Message:    "first line\nsecond line",
			At:         time.Date(2026, 9, 3, 12, 0, 1, 0, time.UTC),
		},
	}

	var output bytes.Buffer
	if err := printJobEvents(&output, events); err != nil {
		t.Fatalf("print events: %v", err)
	}
	text := output.String()
	for _, want := range []string{
		"Allocation",
		"Task group",
		"Lifecycle",
		"abcdef12",
		"failed",
		"image_pull_failed",
		"first line second line",
		"running",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("output %q does not contain %q", text, want)
		}
	}
}
