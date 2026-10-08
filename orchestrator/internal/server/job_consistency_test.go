package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/client"
	"github.com/overfold/trellis/orchestrator/internal/auth"
	"github.com/overfold/trellis/orchestrator/internal/spec"
	"github.com/overfold/trellis/orchestrator/internal/state"
)

func TestPersistedContentHashesRebuiltWithoutExecutionRevision(t *testing.T) {
	for _, legacy := range []string{"current", "missing", "binary", "incorrect"} {
		t.Run(legacy, func(t *testing.T) {
			store, err := state.NewBoltStore(filepath.Join(t.TempDir(), "state.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			controller := NewStateController(store, "test")
			s := NewServer(slog.Default(), nil, controller, store, "test", "")
			s.SetImageResolver(testImageResolver)
			if _, err := s.RegisterJob(t.Context(), "default", versionTestSpec("app:v1", 1), nil, nil); err != nil {
				t.Fatal(err)
			}
			original := s.jobs[jobKey("default", "web")]
			hash := original.ContentHashes["api"]
			if len(hash) != 64 {
				t.Fatalf("digest is not hex SHA-256: %q", hash)
			}
			encoded, _ := json.Marshal(original)
			var roundTrip Job
			if err := json.Unmarshal(encoded, &roundTrip); err != nil || roundTrip.ContentHashes["api"] != hash {
				t.Fatal("JSON persistence corrupted digest")
			}
			switch legacy {
			case "current":
			case "missing":
				roundTrip.ContentHashes = nil
			case "binary":
				roundTrip.ContentHashes["api"] = string([]byte{0xff, 0xfe, 0x80})
			case "incorrect":
				roundTrip.ContentHashes["api"] = strings.Repeat("0", 64)
			}
			if err := controller.PutJob(t.Context(), jobKey("default", "web"), &roundTrip); err != nil {
				t.Fatal(err)
			}
			successor := NewServer(slog.Default(), nil, controller, store, "test", "")
			successor.SetImageResolver(testImageResolver)
			if err := successor.Reload(t.Context()); err != nil {
				t.Fatal(err)
			}
			if successor.jobs[jobKey("default", "web")].ContentHashes["api"] != hash {
				t.Fatal("reload did not rebuild the pinned execution hash")
			}
			if legacy == "binary" {
				// Backups retain the damaged stored cache. Restore must rebuild it
				// from authoritative pins rather than carrying it into revisions.
				s.backupStore = &restoreBoltStore{store}
				_, public := encodedAdministratorPublicKey(t)
				if err := s.Init(t.Context(), ClusterBootstrap{AdministratorPublicKey: public, Settings: DefaultClusterSettings()}); err != nil {
					t.Fatal(err)
				}
				backup := mustBackup(t, s)
				target, err := state.NewBoltStore(filepath.Join(t.TempDir(), "restored.db"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = target.Close() })
				successor = NewServer(slog.Default(), nil, NewStateController(target, "test"), &restoreBoltStore{target}, "test", "")
				successor.SetImageResolver(testImageResolver)
				if err := successor.Init(t.Context(), ClusterBootstrap{AdministratorPublicKey: public, Settings: DefaultClusterSettings()}); err != nil {
					t.Fatal(err)
				}
				if err := successor.Restore(t.Context(), backup); err != nil {
					t.Fatal(err)
				}
				if successor.jobs[jobKey("default", "web")].ContentHashes["api"] != hash {
					t.Fatal("restore lost the rebuilt execution hash")
				}
			}
			for i := range 3 {
				desired := versionTestSpec("app:v1", 1)
				desired.TaskGroups[0].Labels = map[string]string{"route": "new"}
				if i >= 1 {
					desired.TaskGroups[0].Count = 2
				}
				if i == 2 {
					desired.TaskGroups[0].Update = &spec.UpdateSpec{Strategy: spec.UpdateRolling, MaxParallel: 2}
				}
				result, err := successor.RegisterJob(t.Context(), "default", desired, nil, original.ResolvedImages)
				if err != nil || result.Revision != 1 || result.Version != i+2 {
					t.Fatalf("metadata/scale/policy apply = %+v, %v", result, err)
				}
				if err := successor.Reload(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			result, err := successor.RegisterJob(t.Context(), "default", versionTestSpec("app:v2", 2), nil, nil)
			if err != nil || result.Revision != 2 || result.Version != 5 {
				t.Fatalf("execution change after reload = %+v, %v", result, err)
			}
		})
	}
}

func TestPublicClientAppliesResolvedPlanAndFencesSettings(t *testing.T) {
	store := memoryStore{}
	s := newSettingsTestServer(t, store)
	_, public := encodedAdministratorPublicKey(t)
	if err := s.Init(t.Context(), ClusterBootstrap{AdministratorPublicKey: public, Settings: DefaultClusterSettings()}); err != nil {
		t.Fatal(err)
	}
	s.SetImageResolver(testImageResolver)
	httpServer := httptest.NewServer(authenticatedHandler(s, auth.AccessWrite))
	t.Cleanup(httpServer.Close)
	operator, err := client.New(client.Config{Address: httpServer.URL, Token: "token", Namespace: "default"})
	if err != nil {
		t.Fatal(err)
	}
	authored, _ := json.Marshal(versionTestSpec("app:v1", 1))
	planned, err := operator.PlanJob(t.Context(), authored)
	if err != nil {
		t.Fatal(err)
	}
	var resolved spec.JobSpec
	if err := json.Unmarshal(planned.Spec, &resolved); err != nil || spec.ValidateCanonical(&resolved) != nil || resolved.TaskGroups[0].Tasks[0].Resources.CPU != 100 {
		t.Fatalf("plan did not resolve defaults: %s, %v", planned.Spec, err)
	}
	request := &api.JobRegistrationRequest{Spec: planned.Spec, ResolvedImages: planned.ResolvedImages, ExpectedVersion: new(0), ExpectedSettings: planned.SettingsFingerprint}
	limits := spec.DefaultLimits()
	limits.DefaultTaskCPU = 250
	if _, err := s.UpdateJobLimits(t.Context(), limits); err != nil {
		t.Fatal(err)
	}
	if _, err := operator.ApplyJob(t.Context(), request); err == nil {
		t.Fatal("stale settings plan accepted")
	} else {
		var httpErr *client.HTTPError
		if !errors.As(err, &httpErr) || httpErr.Status != http.StatusConflict {
			t.Fatalf("stale settings returned %v, want 409", err)
		}
	}
	if len(s.jobs) != 0 {
		t.Fatal("stale plan mutated desired state")
	}
	planned, err = operator.PlanJob(t.Context(), authored)
	if err != nil {
		t.Fatal(err)
	}
	request.Spec, request.ResolvedImages, request.ExpectedSettings = authored, planned.ResolvedImages, planned.SettingsFingerprint
	if _, err := operator.ApplyJob(t.Context(), request); err == nil {
		t.Fatal("settings-fenced apply accepted unresolved defaults")
	} else {
		var httpErr *client.HTTPError
		if !errors.As(err, &httpErr) || httpErr.Status != http.StatusUnprocessableEntity {
			t.Fatalf("unresolved planned spec returned %v, want 422", err)
		}
	}
	request.Spec = planned.Spec
	if _, err := operator.ApplyJob(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if s.jobs[jobKey("default", "web")].Spec.TaskGroups[0].Tasks[0].Resources.CPU != 250 {
		t.Fatal("apply did not use reviewed defaults")
	}
	// Reconciliation changes also invalidate the settings fence, including
	// an otherwise no-op apply: checking after the no-op return is too late.
	planned, err = operator.PlanJob(t.Context(), authored)
	if err != nil {
		t.Fatal(err)
	}
	reconciliation := DefaultReconciliationSettings()
	reconciliation.TerminalAllocationRetention++
	if _, err := s.UpdateReconciliationSettings(t.Context(), reconciliation); err != nil {
		t.Fatal(err)
	}
	request.Spec, request.ResolvedImages, request.ExpectedSettings = planned.Spec, planned.ResolvedImages, planned.SettingsFingerprint
	request.ExpectedVersion, request.ExpectedIncarnation = &planned.BaseVersion, planned.BaseIncarnation
	if _, err := operator.ApplyJob(t.Context(), request); err == nil {
		t.Fatal("no-op apply bypassed settings fence")
	}
}

func TestSettingsChangeDuringResolutionFencesPlanAndApply(t *testing.T) {
	for _, operation := range []string{"plan", "apply"} {
		t.Run(operation, func(t *testing.T) {
			store := memoryStore{}
			s := newSettingsTestServer(t, store)
			_, public := encodedAdministratorPublicKey(t)
			if err := s.Init(t.Context(), ClusterBootstrap{AdministratorPublicKey: public, Settings: DefaultClusterSettings()}); err != nil {
				t.Fatal(err)
			}
			fingerprint := settingsFingerprint(s.ClusterSettings())
			s.SetImageResolver(func(ctx context.Context, image string) (string, error) {
				limits := spec.DefaultLimits()
				limits.DefaultTaskMemory *= 2
				if _, err := s.UpdateJobLimits(ctx, limits); err != nil {
					return "", err
				}
				return testImageResolver(ctx, image)
			})
			var err error
			if operation == "plan" {
				_, err = s.PlanJob(t.Context(), versionTestSpec("app:v1", 1))
			} else {
				_, err = s.RegisterJob(t.Context(), "default", canonicalTestSpec(versionTestSpec("app:v1", 1)), &JobPreconditions{Settings: fingerprint}, nil)
			}
			if !errors.Is(err, ErrJobVersionConflict) || len(s.jobs) != 0 {
				t.Fatalf("settings changed during %s: %v, jobs=%d", operation, err, len(s.jobs))
			}
		})
	}
}
