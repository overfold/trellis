package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/internal/auth"
	"github.com/overfold/trellis/orchestrator/internal/state"
)

func TestDottedDiscoveryIdentitiesRejectedBeforePersistence(t *testing.T) {
	for _, field := range []string{"namespace", "job", "group"} {
		t.Run(field, func(t *testing.T) {
			job := persistedTestSpec("web")
			switch field {
			case "namespace":
				job.Namespace = "team.prod"
			case "job":
				job.Name = "web.v1"
			case "group":
				job.TaskGroups[0].Name = "api.v1"
			}
			store := memoryStore{}
			s := &Server{state: NewStateController(store, "test"), jobs: map[string]*Job{}}
			if _, err := s.RegisterJob(t.Context(), job.Namespace, job, nil, nil); err == nil {
				t.Fatal("direct admission accepted dotted identity")
			}
			e := echo.New()
			NewHandler(s).Register(e)
			raw, err := json.Marshal(job)
			if err != nil {
				t.Fatal(err)
			}
			body, err := json.Marshal(api.JobRegistrationRequest{Spec: raw})
			if err != nil {
				t.Fatal(err)
			}
			for _, suffix := range []string{"", "/plan"} {
				rec := httptest.NewRecorder()
				e.ServeHTTP(rec, scopedRequest(t, http.MethodPost, "/v1/namespaces/"+job.Namespace+"/jobs"+suffix, string(body), auth.AccessCluster, auth.AccessWrite))
				want := http.StatusUnprocessableEntity
				if field == "namespace" {
					want = http.StatusBadRequest
				}
				if rec.Code != want || !strings.Contains(rec.Body.String(), "dots") {
					t.Fatalf("status=%d body=%s, want %d dot diagnostic", rec.Code, rec.Body.String(), want)
				}
			}
			if len(store) != 0 || len(s.jobs) != 0 {
				t.Fatal("rejected identity was persisted")
			}
		})
	}
}

func TestJobPathsRejectDottedNames(t *testing.T) {
	e := echo.New()
	NewHandler(&Server{}).Register(e)
	for _, tc := range []struct{ method, path string }{
		{http.MethodGet, "/v1/namespaces/default/jobs/web.v1"},
		{http.MethodDelete, "/v1/namespaces/default/jobs/web.v1"},
		{http.MethodPost, "/v1/namespaces/default/jobs/web.v1/restart"},
		{http.MethodGet, "/v1/namespaces/default/jobs/web.v1/versions"},
		{http.MethodPost, "/v1/namespaces/default/jobs/web/groups/api.v1/replacement-backoff/reset"},
	} {
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, scopedRequest(t, tc.method, tc.path, "", auth.AccessCluster, auth.AccessWrite))
		if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "dots") {
			t.Fatalf("%s: status=%d body=%s", tc.path, rec.Code, rec.Body.String())
		}
	}
}

func TestLegacyDottedJobReadsAndRestoreFailClosed(t *testing.T) {
	for _, field := range []string{"namespace", "job", "group", "nil spec"} {
		t.Run(field, func(t *testing.T) {
			job := persistedTestSpec("web")
			switch field {
			case "namespace":
				job.Namespace = "team.prod"
			case "job":
				job.Name = "web.v1"
			case "group":
				job.TaskGroups[0].Name = "api.v1"
			case "nil spec":
				job = nil
			}
			store := memoryStore{}
			controller := NewStateController(store, "test")
			identity := jobKey("default", "web")
			if job != nil {
				identity = jobKey(job.Namespace, job.Name)
			}
			// Low-level writes deliberately emulate the old persisted format.
			if err := controller.PutJob(t.Context(), identity, &Job{Spec: job, Revision: 1, Version: 1}); err != nil {
				t.Fatal(err)
			}
			if jobs, err := controller.ListJobs(t.Context()); err == nil || jobs != nil {
				t.Fatalf("legacy jobs exposed: %v, %v", jobs, err)
			}
			revision := &JobRevisionRecord{Spec: job, Revision: 1, Version: 1, CreatedAt: time.Unix(1, 0).UTC()}
			raw, err := json.Marshal(revision)
			if err != nil {
				t.Fatal(err)
			}
			key := url.QueryEscape(identity) + "/1"
			store["trellis/test/job-revisions/"+key] = raw
			if revisions, err := controller.ListJobRevisions(t.Context(), identity); err == nil || revisions != nil {
				t.Fatalf("legacy revisions exposed: %v, %v", revisions, err)
			}
			for _, snapshot := range []*state.DesiredSnapshot{
				{Jobs: map[string][]byte{url.QueryEscape(identity): store["trellis/test/jobs/"+url.QueryEscape(identity)]}},
				{JobRevisions: map[string][]byte{key: raw}},
			} {
				if err := state.ValidateDesiredSnapshot(snapshot, nil); err == nil {
					t.Fatal("restore accepted legacy identity")
				}
			}
		})
	}
}
