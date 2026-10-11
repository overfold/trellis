package server

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/internal/agent"
	"github.com/overfold/trellis/orchestrator/internal/client"
	"github.com/overfold/trellis/orchestrator/internal/health"
	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	"github.com/overfold/trellis/orchestrator/internal/runtime"
	"github.com/overfold/trellis/orchestrator/internal/spec"
	"github.com/overfold/trellis/orchestrator/internal/storage"
)

// Exercise real background reconciliation and heartbeat HTTP serialization,
// server observation application, and discovery. Only container execution is
// injected; no synthetic health reports are supplied to the server.
func TestAgentStoppedExecutionLeavesCatalogDuringRecoveryFailure(t *testing.T) {
	for _, accounting := range []bool{false, true} {
		t.Run(map[bool]string{false: "restart", true: "accounting"}[accounting], func(t *testing.T) {
			s, stub := newTestServerWithAgent()
			defer stub.server.Close()
			node := &Node{ID: uuid.New(), Host: "127.0.0.1", Status: NodeStatusHealthy}
			addTestNode(s, node, s.now())
			job := canonicalTestSpec(&spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "web", Count: 1, Restart: &spec.RestartPolicySpec{MaxRestarts: 100, Window: time.Hour}, Tasks: []spec.TaskSpec{{Name: "app", Image: "app", Networking: &spec.TaskNetworkingSpec{Mode: spec.TaskNetworkHost}}}}}})
			group := job.TaskGroups[0]
			s.jobs[jobKey("default", "web")] = &Job{Spec: job, Revision: 1}
			allocation := &Allocation{ID: "web-1", Namespace: "default", JobName: "web", TaskGroupName: "web", Node: node, Tasks: group.Tasks, Generation: 1, JobRevision: 1, Phase: lifecycle.PhaseStarting, Health: lifecycle.HealthUnknown}
			s.allocations = []*Allocation{allocation}
			type report struct {
				status   nodeapi.AllocationStatus
				services int
				err      error
			}
			reports := make(chan report, 8)
			leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if strings.HasSuffix(req.URL.Path, "/heartbeat") {
					var hb nodeapi.HeartbeatRequest
					if err := json.NewDecoder(req.Body).Decode(&hb); err != nil {
						reports <- report{err: err}
						w.WriteHeader(400)
						return
					}
					err := s.Heartbeat(req.Context(), node.ID, hb.Allocations, "test", nil, nil, nodeResourceObservation{})
					applyTestObservations(s)
					if len(hb.Allocations) == 1 {
						reports <- report{status: hb.Allocations[0], services: len(s.ListServices("default", nil)), err: err}
					}
				}
				_, _ = io.WriteString(w, `{}`)
			}))
			defer leader.Close()
			root := t.TempDir()
			faultPath := filepath.Join(root, "fault.json")
			rt, err := runtime.NewInjectedRuntime(filepath.Join(root, "runtime.json"), faultPath)
			if err != nil {
				t.Fatal(err)
			}
			log := slog.New(slog.NewTextHandler(io.Discard, nil))
			r := agent.NewAllocationReconciler(rt, nil)
			a := agent.NewAgent(log, rt, health.NewHealthManager(log, rt, nil), r, agent.NewPortManager(rt, 0, 0), agent.NewVolumeManager(root), client.NewServerClient("test", leader.URL, nil), node.ID)
			local := storage.NewLocalStorage(root)
			if err := local.Init(); err != nil {
				t.Fatal(err)
			}
			a.ConfigureDurability(local, "test")
			ctx := t.Context()
			if err := a.Init(ctx); err != nil {
				t.Fatal(err)
			}
			request := &nodeapi.AllocationRequest{AllocationID: allocation.ID, Generation: 1, JobRevision: 1, ExecutionHash: "hash", Epoch: 1, Namespace: "default", JobName: "web", GroupName: "web", Runtime: string(group.Runtime), Restart: group.Restart, Tasks: group.Tasks}
			if err := a.StartGroup(ctx, request); err != nil {
				t.Fatal(err)
			}
			check := func(want lifecycle.Health, services int) {
				select {
				case got := <-reports:
					if got.err != nil || got.status.Phase != lifecycle.PhaseRunning || got.status.Health != want || got.services != services {
						t.Fatalf("heartbeat/catalog = %+v; want running/%s, services=%d", got, want, services)
					}
					t.Logf("heartbeat=%s/%s catalog=%d", got.status.Phase, got.status.Health, got.services)
				case <-time.After(20 * time.Second):
					t.Fatal("heartbeat did not arrive")
				}
			}
			check(lifecycle.HealthHealthy, 1)
			if accounting {
				if err := os.Rename(filepath.Join(root, "agent", "allocations"), filepath.Join(root, "saved-allocations")); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(root, "agent", "allocations"), []byte("blocked"), 0o600); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.WriteFile(faultPath, []byte(`{"operation":"restart","timing":"before","count":100}`), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if err := rt.Stop(ctx, "web-1-g1-app"); err != nil {
				t.Fatal(err)
			}
			check(lifecycle.HealthUnhealthy, 0)
			if accounting {
				if err := os.Remove(filepath.Join(root, "agent", "allocations")); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(filepath.Join(root, "saved-allocations"), filepath.Join(root, "agent", "allocations")); err != nil {
					t.Fatal(err)
				}
			} else if err := os.Remove(faultPath); err != nil {
				t.Fatal(err)
			}
			check(lifecycle.HealthHealthy, 1)
		})
	}
}
