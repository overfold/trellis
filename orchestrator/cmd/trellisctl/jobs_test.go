package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/api"
)

func TestApplyWaitReadinessAndSupersession(t *testing.T) {
	previousConfig := config
	t.Cleanup(func() { config = previousConfig })
	manifest := filepath.Join(t.TempDir(), "web.yaml")
	if err := os.WriteFile(manifest, []byte("name: web\nnamespace: default\ntask_groups:\n  - name: api\n    count: 2\n    tasks:\n      - name: server\n        image: app\n  - name: worker\n    count: 1\n    tasks:\n      - name: worker\n        image: app\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	canonical := json.RawMessage(`{"name":"web","namespace":"default","task_groups":[{"name":"api","count":2},{"name":"worker","count":1}]}`)
	for _, action := range []string{"update", "none"} {
		for _, kind := range []string{"ready", "group deficit", "old incarnation", "wrong namespace", "wrong job", "old revision", "draining", "duplicate", "missing detail", "new version", "new revision", "recreated", "deleted"} {
			t.Run(action+"/"+kind, func(t *testing.T) {
				polls := 0
				version := 5
				if action == "none" {
					version = 4
				}
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/v1/namespaces/default/jobs/plan":
						_ = json.NewEncoder(w).Encode(api.JobPlanResponse{Action: action, BaseIncarnation: "inc", BaseVersion: 4, BaseRevision: 2, Spec: canonical})
					case "/v1/namespaces/default/jobs":
						_ = json.NewEncoder(w).Encode(api.JobRegistrationResponse{Namespace: "default", Name: "web", Incarnation: "inc", Version: version, Revision: 2})
					case "/v1/namespaces/default/jobs/web":
						polls++
						status := api.JobStatusResponse{Name: "web", Incarnation: "inc", Version: version, Revision: 2, Desired: 3, Running: 3, Healthy: 3, Spec: canonical}
						for i, group := range []string{"api", "api", "worker"} {
							status.Allocations = append(status.Allocations, api.AllocationResponse{ID: group + string(rune('a'+i)), Namespace: "default", Job: "web", Group: group, JobIncarnation: "inc", JobRevision: 2, Phase: api.PhaseRunning, Health: api.HealthHealthy})
						}
						if polls == 1 {
							a := &status.Allocations[2]
							switch kind {
							case "group deficit":
								a.Group = "api"
							case "old incarnation":
								a.JobIncarnation = "old"
							case "wrong namespace":
								a.Namespace = "other"
							case "wrong job":
								a.Job = "other"
							case "old revision":
								a.JobRevision = 1
							case "draining":
								a.Draining = true
							case "duplicate":
								a.ID = status.Allocations[0].ID
							case "missing detail":
								status.Allocations = nil
							case "new version":
								status.Version++
							case "new revision":
								status.Revision++
							case "recreated":
								status.Incarnation = "new"
							case "deleted":
								http.Error(w, `{"message":"not found"}`, http.StatusNotFound)
								return
							}
						}
						_ = json.NewEncoder(w).Encode(status)
					default:
						t.Errorf("unexpected path %s", r.URL.Path)
					}
				}))
				defer server.Close()
				config = CLIConfig{ServerAddr: server.URL, Namespace: "default"}
				cmd := NewJobsApplyCmd()
				cmd.SetArgs([]string{"--file", manifest, "--wait", "--interval", "1ns", "--timeout", "1s"})
				var output bytes.Buffer
				cmd.SetOut(&output)
				cmd.SetErr(&bytes.Buffer{})
				err := cmd.Execute()
				superseded := kind == "new version" || kind == "new revision" || kind == "recreated" || kind == "deleted"
				if superseded {
					if err == nil || !strings.Contains(err.Error(), "superseded") || strings.Contains(output.String(), "Ready:") {
						t.Fatalf("superseded deployment: err=%v output=%s", err, &output)
					}
					return
				}
				wantPolls := 2
				if kind == "ready" {
					wantPolls = 1
				}
				if err != nil || polls != wantPolls || !strings.Contains(output.String(), "Ready:") {
					t.Fatalf("readiness err=%v polls=%d want=%d output=%s", err, polls, wantPolls, &output)
				}
			})
		}
	}
}

func TestJobsCommandSurface(t *testing.T) {
	previousConfig := config
	config = CLIConfig{}
	t.Cleanup(func() { config = previousConfig })

	root := newRootCmd()
	jobs, _, err := root.Find([]string{"jobs"})
	if err != nil {
		t.Fatalf("find jobs: %v", err)
	}
	want := map[string]bool{
		"apply":         true,
		"delete":        true,
		"list":          true,
		"logs":          true,
		"reset-backoff": true,
		"status":        true,
	}
	got := map[string]bool{}
	for _, command := range jobs.Commands() {
		if !command.Hidden {
			got[command.Name()] = true
		}
	}
	if len(got) != len(want) {
		t.Fatalf("jobs commands = %v, want %v", got, want)
	}
	for name := range want {
		if !got[name] {
			t.Fatalf("jobs command %q is missing: %v", name, got)
		}
	}

	apply, _, err := root.Find([]string{"jobs", "apply"})
	if err != nil {
		t.Fatalf("find jobs apply: %v", err)
	}
	for _, flag := range []string{"check", "dry-run", "wait"} {
		if apply.Flags().Lookup(flag) == nil {
			t.Fatalf("jobs apply is missing --%s", flag)
		}
	}
}

func TestJobsApplyRejectsSourceAndFile(t *testing.T) {
	cmd := NewJobsApplyCmd()
	cmd.SetArgs([]string{"github.com/overfold/example-app"})
	if err := cmd.Flags().Set("file", "trellis.yaml"); err != nil {
		t.Fatal(err)
	}
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "SOURCE and --file cannot be used together") {
		t.Fatalf("error = %v", err)
	}
}

func TestPrintJobPlanFormatsHumanDurations(t *testing.T) {
	previousOutput := config.Output
	config.Output = "table"
	defer func() { config.Output = previousOutput }()

	result := &api.JobPlanResponse{
		Action:       "update",
		Namespace:    "default",
		Job:          "web",
		BaseRevision: 7,
		Changes: []api.JobPlanChange{{
			Operation: "change",
			Path:      "task_groups[frontend].restart.window",
			Before:    json.Number("60000000000"),
			After:     json.Number("120000000000"),
		}},
	}
	var out strings.Builder
	if err := printJobPlan(&out, result); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "1m0s -> 2m0s") {
		t.Fatalf("plan output did not humanize duration: %q", out.String())
	}
	if got := formatChangeValue("task_groups[api].restart.window", json.Number("9007199254740993")); got != "2501h59m59.254740993s" {
		t.Fatalf("duration lost nanosecond precision: %s", got)
	}
	if got := formatChangeValue("task_groups[api].tasks[0].resources.memory", json.Number("9007199254740993")); got != "9007199254740993" {
		t.Fatalf("byte count lost precision: %s", got)
	}
}

func TestPrintJobStatusIncludesReplacementBackoff(t *testing.T) {
	next := time.Date(2026, 9, 27, 12, 0, 40, 0, time.UTC)
	status := &api.JobStatusResponse{
		Name:     "web",
		Revision: 1,
		Desired:  1,
		ReplacementBackoff: []api.ReplacementBackoffResponse{{
			Group:             "api",
			JobRevision:       1,
			Failures:          3,
			LastAllocationID:  "default-web-api-1234abcd",
			Reason:            "restart_budget_exhausted",
			NextReplacementAt: next,
		}},
	}
	var out strings.Builder
	if err := printJobStatus(&out, status); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	for _, want := range []string{"Replacement backoff:", "api", "3", "2026-09-27T12:00:40Z", "restart_budget_exhausted"} {
		if !strings.Contains(text, want) {
			t.Fatalf("status output %q does not contain %q", text, want)
		}
	}
}

func TestPrintJobStatusIncludesDiagnostics(t *testing.T) {
	status := &api.JobStatusResponse{
		Name:     "web",
		Revision: 2,
		Desired:  1,
		Running:  1,
		Healthy:  0,
		Allocations: []api.AllocationResponse{{
			ID:          "abcdef12-rest",
			Group:       "web",
			JobRevision: 2,
			Phase:       api.PhaseRunning,
			Health:      api.HealthUnhealthy,
			Reason:      "health_check_failed",
			Message:     "connection refused",
		}},
	}
	var out strings.Builder
	if err := printJobStatus(&out, status); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	for _, want := range []string{
		"Problems:",
		"health_check_failed",
		"connection refused",
		"trellisctl jobs status web --watch",
		"trellisctl jobs status web --history",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("status output %q does not contain %q", text, want)
		}
	}
}

func TestResolveAllocationPrefix(t *testing.T) {
	allocations := []api.AllocationResponse{{ID: "abcdef12-one"}, {ID: "12345678-two"}}
	got, err := resolveAllocationPrefix(allocations, "abcdef")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "abcdef12-one" {
		t.Fatalf("resolved %q", got.ID)
	}
	_, err = resolveAllocationPrefix(append(allocations, api.AllocationResponse{ID: "abcdef99-three"}), "abcdef")
	if err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("expected ambiguous error, got %v", err)
	}
}

func TestAllocationDisplayPreservesSelectionIDsAndNodeIdentity(t *testing.T) {
	allocations := []api.AllocationResponse{
		{ID: "default-hello-web-1234abcd", Group: "web", NodeID: uuid.MustParse("44562de6-36e1-4349-9947-043906ae8467"), Address: "10.64.0.27", Phase: api.PhaseRunning, Health: api.HealthHealthy},
		{ID: "default-hello-web-5678efab", Group: "web", NodeID: uuid.MustParse("44562de6-36e1-4349-9947-043906ae8467"), Phase: api.PhaseStarting},
		{ID: "default-hello-web-90abcdef", Group: "web", Phase: api.PhasePending},
	}
	var output strings.Builder
	if err := printJobStatus(&output, &api.JobStatusResponse{Name: "hello", Desired: 3, Allocations: allocations}); err != nil {
		t.Fatal(err)
	}
	for _, allocation := range allocations {
		if !strings.Contains(output.String(), allocation.ID) {
			t.Fatalf("status omits selection ID %s: %s", allocation.ID, &output)
		}
		resolved, err := resolveAllocationPrefix(allocations, allocation.ID)
		if err != nil || resolved.ID != allocation.ID {
			t.Fatalf("displayed ID %s cannot be selected: %v", allocation.ID, err)
		}
	}
	if strings.Contains(output.String(), "10.64.0.27") {
		t.Fatalf("status labels task address as node: %s", &output)
	}
	for i, want := range []string{"44562de6", "44562de6", "—"} {
		if got := allocationNode(allocations[i]); got != want {
			t.Fatalf("allocation %d node = %q, want %q", i, got, want)
		}
	}
	_, err := resolveAllocationPrefix(allocations, "default-")
	if err == nil {
		t.Fatal("expected ambiguous prefix")
	}
	streams := []jobLogStream{{allocation: allocations[0], task: "hello"}, {allocation: allocations[1], task: "hello"}}
	for _, allocation := range allocations[:2] {
		if !strings.Contains(err.Error(), allocation.ID) || !strings.Contains(logStreamRefs(streams), allocation.ID+"/web/hello") {
			t.Fatalf("selection diagnostics omit full ID %s", allocation.ID)
		}
	}
}

func TestJobStateSeparatesConvergingAndDegraded(t *testing.T) {
	converging := &api.JobStatusResponse{Desired: 2, Running: 1, Healthy: 1, Allocations: []api.AllocationResponse{{Phase: api.PhaseStarting, Health: api.HealthUnknown}}}
	if got := jobState(converging); got != "converging" {
		t.Fatalf("state = %q", got)
	}
	degraded := &api.JobStatusResponse{Desired: 2, Running: 2, Healthy: 1, Allocations: []api.AllocationResponse{{Phase: api.PhaseRunning, Health: api.HealthUnhealthy}}}
	if got := jobState(degraded); got != "degraded" {
		t.Fatalf("state = %q", got)
	}
	ready := &api.JobStatusResponse{Desired: 2, Running: 2, Healthy: 2}
	if got := jobState(ready); got != "ready" {
		t.Fatalf("state = %q", got)
	}
	overlap := &api.JobStatusResponse{
		Revision: 2,
		Desired:  2,
		Running:  3,
		Healthy:  3,
		Allocations: []api.AllocationResponse{
			{JobRevision: 1, Draining: true, Phase: api.PhaseRunning, Health: api.HealthHealthy},
			{JobRevision: 2, Phase: api.PhaseRunning, Health: api.HealthHealthy},
			{JobRevision: 2, Phase: api.PhaseStarting, Health: api.HealthUnknown},
		},
	}
	if got := jobState(overlap); got != "converging" {
		t.Fatalf("rolling overlap state = %q, want converging", got)
	}
}

func TestJobsResetBackoffCommand(t *testing.T) {
	previousConfig := config
	t.Cleanup(func() { config = previousConfig })
	var method, path string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method, path = r.Method, r.URL.Path
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	config = CLIConfig{ServerAddr: server.URL, Namespace: "payments"}
	cmd := NewJobsResetBackoffCmd()
	cmd.SetArgs([]string{"web", "api"})
	var stdout bytes.Buffer
	cmd.SetOut(&stdout)
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if method != http.MethodPost || path != "/v1/namespaces/payments/jobs/web/groups/api/replacement-backoff/reset" {
		t.Fatalf("request = %s %s", method, path)
	}
	if got := stdout.String(); got != "Reset replacement backoff of task group api in job web.\n" {
		t.Fatalf("output = %q", got)
	}
}

func TestJobsApplySendsPlannedVersionAndReportsConflict(t *testing.T) {
	previousConfig := config
	t.Cleanup(func() { config = previousConfig })
	manifest := filepath.Join(t.TempDir(), "web.yaml")
	if err := os.WriteFile(manifest, []byte("name: web\nnamespace: default\ntask_groups:\n  - name: api\n    count: 3\n    tasks:\n      - name: server\n        image: app:v1\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name       string
		status     int
		body       string
		wantOutput string
		wantErr    string
	}{
		{name: "applied", status: http.StatusAccepted, body: `{"namespace":"default","name":"web","version":5,"revision":2}`, wantOutput: "Applied job default/web: version 4 -> 5, revision 2 unchanged.\n"},
		{name: "conflict", status: http.StatusConflict, body: `{"message":"job version conflict: expected version 4 but the job is at version 5; plan the manifest again"}`, wantErr: "the job changed after it was planned (job version conflict: expected version 4 but the job is at version 5; plan the manifest again)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var expected *int
			var expectedIncarnation string
			pinned := "docker.io/library/app:v1@sha256:" + strings.Repeat("a", 64)
			resolvedSpec := json.RawMessage(`{"name":"web","namespace":"default","task_groups":[{"name":"api","count":3,"tasks":[{"name":"app","image":"app:v1","resources":{"cpu":250,"memory":134217728}}]}]}`)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/namespaces/default/jobs/plan":
					_ = json.NewEncoder(w).Encode(api.JobPlanResponse{Action: "update", Namespace: "default", Job: "web", BaseIncarnation: "inc-1", BaseVersion: 4, BaseRevision: 2, Spec: resolvedSpec, SettingsFingerprint: "settings-1", ResolvedImages: map[string]string{"app:v1": pinned}, Changes: []api.JobPlanChange{{Operation: "change", Path: "task_groups[api].count", Before: 1, After: 3}}})
				case "/v1/namespaces/default/jobs":
					var request api.JobRegistrationRequest
					decoder := json.NewDecoder(r.Body)
					decoder.DisallowUnknownFields()
					if err := decoder.Decode(&request); err != nil {
						t.Error(err)
					}
					expected, expectedIncarnation = request.ExpectedVersion, request.ExpectedIncarnation
					if request.ResolvedImages["app:v1"] != pinned {
						t.Errorf("apply lost the plan's image pin: %+v", request.ResolvedImages)
					}
					if string(request.Spec) != string(resolvedSpec) || request.ExpectedSettings != "settings-1" {
						t.Errorf("apply lost resolved defaults or settings fence: %+v", request)
					}
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(tc.status)
					_, _ = w.Write([]byte(tc.body))
				default:
					t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
				}
			}))
			defer server.Close()

			config = CLIConfig{ServerAddr: server.URL, Namespace: "default"}
			cmd := NewJobsApplyCmd()
			cmd.SetArgs([]string{"--file", manifest})
			var stdout bytes.Buffer
			cmd.SetOut(&stdout)
			cmd.SetErr(&bytes.Buffer{})
			err := cmd.Execute()
			if expected == nil || *expected != 4 || expectedIncarnation != "inc-1" {
				t.Fatalf("submitted expected_version = %v, expected_incarnation = %q, want 4 and inc-1", expected, expectedIncarnation)
			}
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := stdout.String(); got != tc.wantOutput {
				t.Fatalf("output = %q, want %q", got, tc.wantOutput)
			}
		})
	}
}

func TestJobsListRequiresExplicitNamespace(t *testing.T) {
	previousConfig := config
	t.Cleanup(func() { config = previousConfig })

	for _, tc := range []struct {
		name      string
		namespace string
		wantPath  string
		wantErr   string
	}{
		{name: "explicit namespace", namespace: "payments", wantPath: "/v1/namespaces/payments/jobs"},
		{name: "missing namespace", wantErr: "--namespace is required for namespace-scoped commands"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var paths []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				paths = append(paths, r.URL.Path)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte("[]"))
			}))
			defer server.Close()

			config = CLIConfig{ServerAddr: server.URL, Namespace: tc.namespace}
			cmd := NewJobsListCmd()
			cmd.SetArgs(nil)
			cmd.SetOut(&bytes.Buffer{})
			cmd.SetErr(&bytes.Buffer{})
			err := cmd.Execute()
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(paths) == 0 || paths[len(paths)-1] != tc.wantPath {
				t.Fatalf("requests = %v, want final %s", paths, tc.wantPath)
			}
		})
	}
}
