package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/distribution/reference"
	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/opencontainers/go-digest"
	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/internal/auth"
	"github.com/overfold/trellis/orchestrator/internal/spec"
)

func testImageResolver(_ context.Context, image string) (string, error) {
	named, err := reference.ParseNormalizedNamed(image)
	if err != nil {
		return "", err
	}
	named = reference.TagNameOnly(named)
	if _, ok := named.(reference.Digested); ok {
		return named.String(), nil
	}
	pinned, err := reference.WithDigest(named, digest.FromString(image))
	if err != nil {
		return "", err
	}
	return pinned.String(), nil
}

func imageTestSpec(image string, count int) *spec.JobSpec {
	job := versionTestSpec(image, count)
	job.TaskGroups[0].Tasks[0].Networking = &spec.TaskNetworkingSpec{Mode: spec.TaskNetworkHost}
	return canonicalTestSpec(job)
}

func TestImageApplyPinsPlanAndRetainedHistory(t *testing.T) {
	for _, tag := range []string{"main", "latest", "1.4.2"} {
		t.Run(tag, func(t *testing.T) {
			ctx := context.Background()
			store := memoryStore{}
			s := NewServer(slog.Default(), nil, NewStateController(store, "test"), store, "test", "")
			agent := newTestAgent()
			t.Cleanup(agent.server.Close)
			s.client = newTestAgentClient()
			node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy}
			addTestNode(s, node, s.now())
			image := "example/app:" + tag
			first := "docker.io/example/app:" + tag + "@sha256:" + strings.Repeat("a", 64)
			second := "docker.io/example/app:" + tag + "@sha256:" + strings.Repeat("b", 64)
			third := "docker.io/example/app:" + tag + "@sha256:" + strings.Repeat("c", 64)
			current, calls := first, 0
			s.SetImageResolver(func(context.Context, string) (string, error) {
				calls++
				return current, nil
			})
			if _, err := s.RegisterJob(ctx, "default", imageTestSpec(image, 1), nil, nil); err != nil {
				t.Fatal(err)
			}
			s.Reconcile(ctx)
			if len(s.allocations) != 1 || s.allocations[0].Tasks[0].Image != first {
				t.Fatalf("initial allocation did not use %s", first)
			}
			observeStarted(t, s, node.ID)
			unchanged, err := s.PlanJob(ctx, imageTestSpec(image, 1))
			if err != nil || unchanged.Action != "none" {
				t.Fatalf("unchanged plan = %+v, %v", unchanged, err)
			}
			unchangedApply, err := s.RegisterJob(ctx, "default", imageTestSpec(image, 1), nil, nil)
			if err != nil || unchangedApply.Version != 1 || unchangedApply.Revision != 1 {
				t.Fatalf("unchanged apply = %+v, %v", unchangedApply, err)
			}
			current = second
			planned, err := s.PlanJob(ctx, imageTestSpec(image, 1))
			if err != nil || planned.Action != "update" || len(planned.Changes) != 1 || planned.Changes[0].Before != first || planned.Changes[0].After != second {
				t.Fatalf("moved tag plan = %+v, %v", planned, err)
			}
			// Registry movement after planning must not change the accepted artifact.
			current = third
			beforeApply := calls
			applied, err := s.RegisterJob(ctx, "default", imageTestSpec(image, 1), expectVersion(planned.BaseVersion, planned.BaseIncarnation), planned.ResolvedImages)
			if err != nil || applied.Version != 2 || applied.Revision != 2 || calls != beforeApply {
				t.Fatalf("planned apply = %+v, %v; resolver calls = %d, want %d", applied, err, calls, beforeApply)
			}
			// Caller-owned maps must not mutate durable desired state.
			planned.ResolvedImages[image] = third
			s.Reconcile(ctx)
			// Recreate releases old execution in one pass and places in the next.
			s.Reconcile(ctx)
			if s.allocations[len(s.allocations)-1].Tasks[0].Image != second {
				t.Fatal("replacement did not use the reviewed image")
			}
			status, err := s.GetJob("default", "web")
			if err != nil || status.ResolvedImages[image] != second {
				t.Fatalf("status pins = %+v, %v", status, err)
			}
			for _, allocation := range status.Allocations {
				if allocation.JobIncarnation == "" || allocation.JobIncarnation != applied.Incarnation {
					t.Fatalf("allocation status lost canonical job identity: %+v", allocation)
				}
			}
			var authored spec.JobSpec
			if err := json.Unmarshal(status.Spec, &authored); err != nil || authored.TaskGroups[0].Tasks[0].Image != image {
				t.Fatal("authored tag was not preserved")
			}
			// A successor restores pins, and restart/scale use them with no registry I/O.
			successor := NewServer(slog.Default(), nil, NewStateController(store, "test"), store, "test", "")
			if err := successor.Reload(ctx); err != nil {
				t.Fatal(err)
			}
			restored := successor.jobs[jobKey("default", "web")]
			if restored.ResolvedImages[image] != second {
				t.Fatal("leader reload lost image pins")
			}
			if _, err := s.RegisterJob(ctx, "default", imageTestSpec(image, 2), nil, restored.ResolvedImages); err != nil {
				t.Fatal(err)
			}
			s.Reconcile(ctx)
			if s.allocations[len(s.allocations)-1].Tasks[0].Image != second || calls != beforeApply {
				t.Fatal("scale re-resolved the tag")
			}
			if err := s.RestartJob(ctx, "default", "web"); err != nil {
				t.Fatal(err)
			}
			if s.allocations[len(s.allocations)-1].Tasks[0].Image != second || calls != beforeApply {
				t.Fatal("restart re-resolved the tag")
			}
			history, err := s.ListJobVersions(ctx, "default", "web")
			if err != nil || history[0].ResolvedImages[image] != first {
				t.Fatalf("history lost original artifact: %+v, %v", history, err)
			}
			var rollback spec.JobSpec
			if err := json.Unmarshal(history[0].Spec, &rollback); err != nil {
				t.Fatal(err)
			}
			rolledBack, err := s.RegisterJob(ctx, "default", &rollback, nil, history[0].ResolvedImages)
			if err != nil || rolledBack.Revision != 3 || calls != beforeApply {
				t.Fatalf("rollback = %+v, %v", rolledBack, err)
			}
			s.Reconcile(ctx)
			if s.allocations[len(s.allocations)-1].Tasks[0].Image != first {
				t.Fatal("rollback used the tag's current content")
			}
			fresh, err := s.RegisterJob(ctx, "default", imageTestSpec(image, 1), nil, nil)
			if err != nil || fresh.Revision != 4 || s.jobs[jobKey("default", "web")].ResolvedImages[image] != third {
				t.Fatalf("direct apply did not resolve moved tag: %+v, %v", fresh, err)
			}
		})
	}
}

func TestImageResolutionFailuresDoNotMutateJob(t *testing.T) {
	s, agent := newTestServerWithAgent()
	t.Cleanup(agent.server.Close)
	ctx := context.Background()
	if _, err := s.RegisterJob(ctx, "default", imageTestSpec("app:main", 1), nil, nil); err != nil {
		t.Fatal(err)
	}
	original := s.jobs[jobKey("default", "web")]
	s.SetImageResolver(func(context.Context, string) (string, error) { return "", fmt.Errorf("registry unavailable") })
	if _, err := s.PlanJob(ctx, imageTestSpec("app:main", 1)); err == nil {
		t.Fatal("plan silently used stale image content")
	}
	if _, err := s.RegisterJob(ctx, "default", imageTestSpec("app:main", 1), nil, nil); err == nil {
		t.Fatal("apply silently used stale image content")
	}
	if s.jobs[jobKey("default", "web")] != original {
		t.Fatal("failed resolution changed desired state")
	}
	for name, pins := range map[string]map[string]string{
		"missing":          {},
		"unpinned":         {"app:main": "app:main"},
		"wrong repository": {"app:main": "other:main@sha256:" + strings.Repeat("a", 64)},
		"wrong tag":        {"app:main": "app:other@sha256:" + strings.Repeat("a", 64)},
		"extra":            {"app:main": original.ResolvedImages["app:main"], "other": original.ResolvedImages["app:main"]},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := s.RegisterJob(ctx, "default", imageTestSpec("app:main", 1), nil, pins); err == nil {
				t.Fatal("invalid image pins were accepted")
			}
		})
	}
}

func TestImagePinsDeduplicateAndPreserveExplicitDigest(t *testing.T) {
	s, agent := newTestServerWithAgent()
	t.Cleanup(agent.server.Close)
	job := imageTestSpec("app:main", 1)
	job.TaskGroups[0].Tasks = append(job.TaskGroups[0].Tasks, job.TaskGroups[0].Tasks[0])
	job.TaskGroups[0].Tasks[1].Name = "sidecar"
	calls := 0
	s.SetImageResolver(func(ctx context.Context, image string) (string, error) {
		calls++
		return testImageResolver(ctx, image)
	})
	if _, err := s.PlanJob(t.Context(), job); err != nil || calls != 1 {
		t.Fatalf("shared image resolved %d times, error %v", calls, err)
	}
	digestImage := "app@sha256:" + strings.Repeat("a", 64)
	if _, err := s.RegisterJob(t.Context(), "default", imageTestSpec(digestImage, 1), nil, map[string]string{digestImage: "app@sha256:" + strings.Repeat("b", 64)}); err == nil {
		t.Fatal("explicit digest was overridden")
	}
}

func TestRegistryResolutionAndHTTPApply(t *testing.T) {
	// Serve an OCI index: resolve its top-level digest, not a leader-specific
	// platform manifest. Workers select their platform from the pinned index.
	body := `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[]}`
	registryCalls := 0
	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		registryCalls++
		if r.URL.Path != "/v2/app/manifests/main" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/vnd.oci.image.index.v1+json")
		w.Header().Set("Content-Length", fmt.Sprint(len(body)))
		w.Header().Set("Docker-Content-Digest", digest.FromString(body).String())
		if r.Method != http.MethodHead {
			_, _ = w.Write([]byte(body))
		}
	}))
	t.Cleanup(registry.Close)
	image := strings.TrimPrefix(registry.URL, "http://") + "/app:main"
	pinned := image + "@" + digest.FromString(body).String()
	resolved, err := resolveRegistryImage(context.Background(), image)
	if err != nil || resolved != pinned || registryCalls == 0 {
		t.Fatalf("registry resolution = %q, %v; calls %d", resolved, err, registryCalls)
	}
	before := registryCalls
	if resolved, err := resolveRegistryImage(context.Background(), pinned); err != nil || resolved != pinned || registryCalls != before {
		t.Fatalf("digest resolution = %q, %v; calls %d", resolved, err, registryCalls)
	}
	s, agent := newTestServerWithAgent()
	t.Cleanup(agent.server.Close)
	s.SetImageResolver(resolveRegistryImage)
	handler := echo.New()
	NewHandler(s).Register(handler)
	raw, _ := json.Marshal(imageTestSpec(image, 1))
	requestBody, err := json.Marshal(api.JobRegistrationRequest{Spec: raw})
	if err != nil {
		t.Fatal(err)
	}
	planResponse := httptest.NewRecorder()
	handler.ServeHTTP(planResponse, scopedRequest(t, http.MethodPost, "/v1/namespaces/default/jobs/plan", string(requestBody), auth.AccessCluster, auth.AccessWrite))
	var planned api.JobPlanResponse
	if planResponse.Code != http.StatusOK || json.Unmarshal(planResponse.Body.Bytes(), &planned) != nil || planned.ResolvedImages[image] != pinned {
		t.Fatalf("HTTP plan = %d %s", planResponse.Code, planResponse.Body.String())
	}
	registry.Close()
	zero := 0
	requestBody, err = json.Marshal(api.JobRegistrationRequest{Spec: raw, ResolvedImages: planned.ResolvedImages, ExpectedVersion: &zero})
	if err != nil {
		t.Fatal(err)
	}
	applyResponse := httptest.NewRecorder()
	handler.ServeHTTP(applyResponse, scopedRequest(t, http.MethodPost, "/v1/namespaces/default/jobs", string(requestBody), auth.AccessCluster, auth.AccessWrite))
	if applyResponse.Code != http.StatusAccepted {
		t.Fatalf("planned apply contacted unavailable registry: %d %s", applyResponse.Code, applyResponse.Body.String())
	}
}
