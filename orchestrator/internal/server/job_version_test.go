package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/overfold/trellis/internal/api"
	"github.com/overfold/trellis/internal/auth"
	"github.com/overfold/trellis/internal/spec"
)

func versionTestSpec(image string, count int) *spec.JobSpec {
	return &spec.JobSpec{
		Namespace: "default", Name: "web",
		TaskGroups: []spec.TaskGroupSpec{{
			Name: "api", Count: count,
			Tasks: []spec.TaskSpec{{Name: "server", Image: image}},
		}},
	}
}

func expectVersion(version int) *int { return &version }

func TestRegisterJobConcurrentAppliesAtSameVersionConflict(t *testing.T) {
	s, _ := newTestServerWithAgent()
	ctx := context.Background()
	if _, err := s.RegisterJob(ctx, "default", versionTestSpec("app:v1", 1), expectVersion(0)); err != nil {
		t.Fatal(err)
	}

	const appliers = 8
	var wg sync.WaitGroup
	errs := make([]error, appliers)
	for i := range appliers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = s.RegisterJob(ctx, "default", versionTestSpec(fmt.Sprintf("app:v%d", i+2), 1), expectVersion(1))
		}()
	}
	wg.Wait()

	succeeded := 0
	for _, err := range errs {
		switch {
		case err == nil:
			succeeded++
		case !errors.Is(err, ErrJobVersionConflict):
			t.Fatalf("unexpected apply error: %v", err)
		}
	}
	if succeeded != 1 {
		t.Fatalf("successful applies = %d, want exactly 1", succeeded)
	}
	job := s.jobs[jobKey("default", "web")]
	if job.Version != 2 || job.Revision != 2 {
		t.Fatalf("job version/revision = %d/%d, want 2/2", job.Version, job.Revision)
	}
}

func TestRegisterJobExpectedVersionPreconditions(t *testing.T) {
	s, _ := newTestServerWithAgent()
	ctx := context.Background()

	if _, err := s.RegisterJob(ctx, "default", versionTestSpec("app:v1", 1), expectVersion(1)); !errors.Is(err, ErrJobVersionConflict) {
		t.Fatalf("update of a missing job error = %v, want version conflict", err)
	}
	created, err := s.RegisterJob(ctx, "default", versionTestSpec("app:v1", 1), expectVersion(0))
	if err != nil {
		t.Fatal(err)
	}
	if created.Version != 1 || created.Revision != 1 {
		t.Fatalf("created job = %+v, want version 1 revision 1", created)
	}
	var conflict *JobVersionConflictError
	if _, err := s.RegisterJob(ctx, "default", versionTestSpec("app:v2", 1), expectVersion(0)); !errors.As(err, &conflict) || !conflict.Exists || conflict.Current != 1 {
		t.Fatalf("create of an existing job error = %v, want conflict at version 1", err)
	}
	if _, err := s.RegisterJob(ctx, "default", versionTestSpec("app:v2", 1), expectVersion(-1)); err == nil || errors.Is(err, ErrJobVersionConflict) {
		t.Fatalf("negative expected version error = %v, want validation error", err)
	}
	// An unconditional apply still succeeds.
	if _, err := s.RegisterJob(ctx, "default", versionTestSpec("app:v2", 1), nil); err != nil {
		t.Fatal(err)
	}
	// Stale version is rejected even when the spec already matches.
	if _, err := s.RegisterJob(ctx, "default", versionTestSpec("app:v2", 1), expectVersion(1)); !errors.Is(err, ErrJobVersionConflict) {
		t.Fatalf("stale no-op apply error = %v, want version conflict", err)
	}
}

func TestRegisterJobScaleChangeAdvancesVersionAndHistory(t *testing.T) {
	s, _ := newTestServerWithAgent()
	ctx := context.Background()
	if _, err := s.RegisterJob(ctx, "default", versionTestSpec("app:v1", 1), nil); err != nil {
		t.Fatal(err)
	}
	s.events = newEventBus()
	events, ok := s.events.subscribe("default")
	if !ok {
		t.Fatal("subscribe to events")
	}
	scaled, err := s.RegisterJob(ctx, "default", versionTestSpec("app:v1", 3), expectVersion(1))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-events:
		if event.Type != api.EventJobRegistered || event.Version != 2 || event.Revision != 1 {
			t.Fatalf("scale event = %+v, want job.registered at version 2 revision 1", event)
		}
	default:
		t.Fatal("scale change published no job.registered event")
	}
	if scaled.Version != 2 || scaled.Revision != 1 {
		t.Fatalf("scaled job = %+v, want version 2 at unchanged revision 1", scaled)
	}
	unchanged, err := s.RegisterJob(ctx, "default", versionTestSpec("app:v1", 3), expectVersion(2))
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.Version != 2 || unchanged.Revision != 1 {
		t.Fatalf("identical apply = %+v, want version 2 revision 1", unchanged)
	}
	updated, err := s.RegisterJob(ctx, "default", versionTestSpec("app:v2", 3), expectVersion(2))
	if err != nil {
		t.Fatal(err)
	}
	if updated.Version != 3 || updated.Revision != 2 {
		t.Fatalf("image change = %+v, want version 3 revision 2", updated)
	}

	history, err := s.ListJobVersions(ctx, "default", "web")
	if err != nil {
		t.Fatal(err)
	}
	type entry struct{ version, revision, count int }
	want := []entry{{1, 1, 1}, {2, 1, 3}, {3, 2, 3}}
	if len(history) != len(want) {
		t.Fatalf("history = %+v, want %d entries", history, len(want))
	}
	for i, got := range history {
		if got.Version != want[i].version || got.Revision != want[i].revision || got.Spec.TaskGroups[0].Count != want[i].count {
			t.Fatalf("history[%d] = version %d revision %d count %d, want %+v", i, got.Version, got.Revision, got.Spec.TaskGroups[0].Count, want[i])
		}
	}
	status, ok := s.GetJob("default", "web")
	if !ok || status.Version != 3 || status.Revision != 2 {
		t.Fatalf("job status = %+v, want version 3 revision 2", status)
	}
}

func TestRegisterJobHistoryIsBoundedAndClearedByDelete(t *testing.T) {
	s, _ := newTestServerWithAgent()
	ctx := context.Background()
	for count := 1; count <= jobRevisionRetention+2; count++ {
		if _, err := s.RegisterJob(ctx, "default", versionTestSpec("app:v1", count), nil); err != nil {
			t.Fatal(err)
		}
	}
	history, err := s.ListJobVersions(ctx, "default", "web")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != jobRevisionRetention || history[0].Version != 3 || history[len(history)-1].Version != jobRevisionRetention+2 {
		t.Fatalf("history versions = %d..%d (%d entries), want 3..%d", history[0].Version, history[len(history)-1].Version, len(history), jobRevisionRetention+2)
	}

	if err := s.DeleteJob(ctx, "default", "web"); err != nil {
		t.Fatal(err)
	}
	recreated, err := s.RegisterJob(ctx, "default", versionTestSpec("app:v9", 1), expectVersion(0))
	if err != nil {
		t.Fatal(err)
	}
	if recreated.Version != 1 || recreated.Revision != 1 {
		t.Fatalf("recreated job = %+v, want version 1 revision 1", recreated)
	}
	history, err = s.ListJobVersions(ctx, "default", "web")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 || history[0].Version != 1 || history[0].Spec.TaskGroups[0].Tasks[0].Image != "app:v9" {
		t.Fatalf("recreated history = %+v, want only the new version 1", history)
	}
}

func TestRegisterJobHandlerReportsVersionConflict(t *testing.T) {
	s, _ := newTestServerWithAgent()
	e := echo.New()
	NewHandler(s).Register(e)
	body := func(image string, expected int) string {
		return fmt.Sprintf(`{"spec":{"name":"web","namespace":"default","task_groups":[{"name":"api","count":1,"tasks":[{"name":"server","image":%q}]}]},"expected_version":%d}`, image, expected)
	}

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, scopedRequest(t, http.MethodPost, "/v1/jobs", body("app:v1", 0), auth.AccessNamespace, auth.AccessWrite, "default"))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("create status = %d, body: %s", rec.Code, rec.Body.String())
	}
	var created api.JobRegistrationResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created != (api.JobRegistrationResponse{Namespace: "default", Name: "web", Version: 1, Revision: 1}) {
		t.Fatalf("create response = %+v", created)
	}

	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, scopedRequest(t, http.MethodPost, "/v1/jobs", body("app:v2", 0), auth.AccessNamespace, auth.AccessWrite, "default"))
	if rec.Code != http.StatusConflict {
		t.Fatalf("conflicting create status = %d, want 409; body: %s", rec.Code, rec.Body.String())
	}
	var failure struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &failure); err != nil || failure.Message == "" {
		t.Fatalf("conflict body = %s, want a message", rec.Body.String())
	}

	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, scopedRequest(t, http.MethodPost, "/v1/jobs/plan", body("app:v2", 0), auth.AccessNamespace, auth.AccessWrite, "default"))
	var planned struct {
		BaseVersion  int `json:"base_version"`
		BaseRevision int `json:"base_revision"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &planned); err != nil || planned.BaseVersion != 1 || planned.BaseRevision != 1 {
		t.Fatalf("plan = %s, want base version and revision 1", rec.Body.String())
	}
}
