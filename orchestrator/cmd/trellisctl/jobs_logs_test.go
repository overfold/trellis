package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/overfold/trellis/orchestrator/api"
)

func TestJobsLogsAllocationTaskSelection(t *testing.T) {
	previousConfig := config
	t.Cleanup(func() { config = previousConfig })
	for _, tc := range []struct {
		name string
		args []string
		want []string
		err  string
	}{
		{name: "default prefers active", want: []string{"active/new"}},
		{name: "retained full ID", args: []string{"--allocation", "retained-one"}, want: []string{"retained-one/old", "retained-one/sidecar"}},
		{name: "retained unique prefix", args: []string{"--allocation", "retained-o", "--task", "old", "--follow"}, want: []string{"retained-one/old"}},
		{name: "retained ambiguous prefix", args: []string{"--allocation", "retained"}, err: "ambiguous"},
		{name: "removed group", args: []string{"--group", "removed"}, want: []string{"retained-two/removed-task"}},
		{name: "current task absent historically", args: []string{"--allocation", "retained-one", "--task", "new"}, err: "no task"},
		{name: "group mismatch", args: []string{"--allocation", "retained-one", "--group", "removed"}, err: "no allocation matches"},
		{name: "historical multi-task follow", args: []string{"--allocation", "retained-one", "--follow"}, err: "--follow requires exactly one task stream"},
		{name: "missing metadata explicit task", args: []string{"--allocation", "legacy", "--task", "old"}, want: []string{"legacy/old"}},
		{name: "missing metadata implicit task", args: []string{"--allocation", "legacy"}, want: []string{"legacy/"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/namespaces/default/jobs/web" {
					// The current spec deliberately disagrees with historical task inventories.
					status := api.JobStatusResponse{Spec: json.RawMessage(`{"name":"web","task_groups":[{"name":"frontend","tasks":[{"name":"new"}]}]}`), Allocations: []api.AllocationResponse{
						{ID: "active", Group: "frontend", Phase: api.PhaseRunning, Tasks: []string{"new"}},
						{ID: "retained-one", Group: "frontend", Phase: api.PhaseFailed, Tasks: []string{"old", "sidecar"}},
						{ID: "retained-two", Group: "removed", Phase: api.PhaseStopped, Tasks: []string{"removed-task"}},
						{ID: "legacy", Group: "frontend", Phase: api.PhaseLost},
					}}
					if err := json.NewEncoder(w).Encode(status); err != nil {
						t.Error(err)
					}
					return
				}
				id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/namespaces/default/allocations/"), "/logs")
				ref := id + "/" + r.URL.Query().Get("task")
				got = append(got, ref)
				if r.URL.Query().Get("tail") != "100" {
					t.Errorf("lost tail option: %s", r.URL)
				}
				if _, err := fmt.Fprintln(w, ref); err != nil {
					t.Error(err)
				}
			}))
			defer server.Close()
			config = CLIConfig{ServerAddr: server.URL, Namespace: "default"}
			cmd := NewJobsLogsCmd()
			cmd.SetArgs(append([]string{"web"}, tc.args...))
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&bytes.Buffer{})
			err := cmd.Execute()
			if tc.err != "" {
				if err == nil || !strings.Contains(err.Error(), tc.err) || len(got) != 0 {
					t.Fatalf("logs = %v, %v; want %q before opening logs", got, err, tc.err)
				}
				return
			}
			if err != nil || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("logs = %v, %v; want %v", got, err, tc.want)
			}
			for _, ref := range tc.want {
				if !strings.Contains(out.String(), ref+"\n") {
					t.Fatalf("output %q omits %s", out.String(), ref)
				}
			}
		})
	}
}
