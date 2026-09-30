package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/overfold/trellis/internal/api"
	"github.com/overfold/trellis/internal/auth"
	"github.com/overfold/trellis/internal/spec"
	"github.com/overfold/trellis/internal/storage"
)

func newSettingsTestServer(t *testing.T, store memoryStore) *Server {
	t.Helper()
	local := storage.NewLocalStorage(t.TempDir())
	if err := local.Init(); err != nil {
		t.Fatal(err)
	}
	return NewServer(slog.Default(), local, NewStateController(store, "test"), store, "test", "node:8128")
}

func strictClusterSettings() ClusterSettings {
	settings := DefaultClusterSettings()
	settings.JobLimits.MaxReplicasPerTaskGroup = 2
	settings.WireGuardPool = netip.MustParsePrefix("10.128.0.0/10")
	settings.WireGuardPortCount = 64
	return settings
}

func replicasJob(count int) *spec.JobSpec {
	return &spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "api", Count: count, Tasks: []spec.TaskSpec{{Name: "app", Image: "app"}}}}}
}

// Every member uses the settings of the node that created the cluster, so a
// leader configured differently admits jobs, derives namespace subnets, and
// registers nodes exactly like the original leader.
func TestMembersUseReplicatedClusterSettingsInsteadOfLocalConfiguration(t *testing.T) {
	ctx := context.Background()
	store := memoryStore{}
	_, encoded := encodedAdministratorPublicKey(t)
	created := strictClusterSettings()
	first := newSettingsTestServer(t, store)
	if err := first.Init(ctx, ClusterBootstrap{AdministratorPublicKey: encoded, Settings: created}); err != nil {
		t.Fatal(err)
	}

	// The second member's configuration asks for permissive defaults and a
	// different pool and port count.
	second := newSettingsTestServer(t, store)
	if err := second.Init(ctx, ClusterBootstrap{Settings: DefaultClusterSettings()}); err != nil {
		t.Fatal(err)
	}
	if err := second.AcquireLeadership(ctx); err != nil {
		t.Fatal(err)
	}
	for name, member := range map[string]*Server{"creator": first, "failover leader": second} {
		if got := member.ClusterSettings(); got != created {
			t.Fatalf("%s settings = %+v, want %+v", name, got, created)
		}
		if err := member.CanonicalizeJob(replicasJob(3)); err == nil {
			t.Fatalf("%s admitted a job above the replicated replica limit", name)
		}
		if err := member.CanonicalizeJob(replicasJob(2)); err != nil {
			t.Fatalf("%s refused a job within the replicated limits: %v", name, err)
		}
	}

	nodeID := uuid.New()
	if a, b := networkSubnet(first.networkPool, 5), networkSubnet(second.networkPool, 5); a != b || !created.WireGuardPool.Contains(b.Addr()) {
		t.Fatalf("namespace subnets differ across leaders or leave the replicated pool: %s, %s", a, b)
	}

	registration := func(count int) *NodeRegistration {
		return &NodeRegistration{ID: nodeID, Host: "node-b", Port: 8127, CPUCapacity: 1000, MemoryCapacity: 1 << 30, CPUAllocatable: 1000, MemoryAllocatable: 1 << 30, WireGuardPublicKey: "key", WireGuardEndpoint: "node-b:51820", WireGuardPortBase: 51820, WireGuardPortCount: count}
	}
	if err := second.RegisterNode(ctx, registration(DefaultWireGuardPortCount)); err == nil {
		t.Fatal("failover leader accepted a node whose port count differs from the replicated count")
	}
	if err := second.RegisterNode(ctx, registration(created.WireGuardPortCount)); err != nil {
		t.Fatalf("failover leader rejected a node matching the replicated port count: %v", err)
	}
}

func TestInitRefusesClusterRecordWithoutSettings(t *testing.T) {
	ctx := context.Background()
	store := memoryStore{}
	_, encoded := encodedAdministratorPublicKey(t)
	if err := NewStateController(store, "test").PutCluster(ctx, &Cluster{AdministratorPublicKey: encoded}); err != nil {
		t.Fatal(err)
	}
	if err := newSettingsTestServer(t, store).Init(ctx, ClusterBootstrap{Settings: DefaultClusterSettings()}); err == nil {
		t.Fatal("initialized from a cluster record without settings")
	}
}

func TestInitRefusesInvalidBootstrapSettings(t *testing.T) {
	_, encoded := encodedAdministratorPublicKey(t)
	settings := DefaultClusterSettings()
	settings.WireGuardPortCount = 0
	if err := newSettingsTestServer(t, memoryStore{}).Init(context.Background(), ClusterBootstrap{AdministratorPublicKey: encoded, Settings: settings}); err == nil {
		t.Fatal("created a cluster with an invalid WireGuard port count")
	}
}

func TestUpdateJobLimitsIsReplicatedToLaterLeaders(t *testing.T) {
	ctx := context.Background()
	store := memoryStore{}
	_, encoded := encodedAdministratorPublicKey(t)
	leader := newSettingsTestServer(t, store)
	if err := leader.Init(ctx, ClusterBootstrap{AdministratorPublicKey: encoded, Settings: DefaultClusterSettings()}); err != nil {
		t.Fatal(err)
	}
	follower := newSettingsTestServer(t, store)
	if err := follower.Init(ctx, ClusterBootstrap{}); err != nil {
		t.Fatal(err)
	}
	if err := leader.AcquireLeadership(ctx); err != nil {
		t.Fatal(err)
	}

	limits := spec.DefaultLimits()
	limits.MaxReplicasPerTaskGroup = 1000
	if _, err := leader.UpdateJobLimits(ctx, limits); err != nil {
		t.Fatal(err)
	}
	if err := leader.AcquireLeadership(ctx); err != nil {
		t.Fatal(err)
	}
	if err := follower.AcquireLeadership(ctx); err != nil {
		t.Fatal(err)
	}
	if got := follower.ClusterSettings().JobLimits; got != limits {
		t.Fatalf("later leader job limits = %+v, want %+v", got, limits)
	}
	persisted, err := NewStateController(store, "test").GetCluster(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.ControlEpoch != 3 || persisted.Settings.JobLimits != limits || persisted.AdministratorPublicKey != encoded {
		t.Fatalf("persisted cluster = %+v", persisted)
	}
}

func TestUpdateJobLimitsRefusesLimitsThatWouldStopDesiredJobs(t *testing.T) {
	ctx := context.Background()
	store := memoryStore{}
	_, encoded := encodedAdministratorPublicKey(t)
	s := newSettingsTestServer(t, store)
	if err := s.Init(ctx, ClusterBootstrap{AdministratorPublicKey: encoded, Settings: DefaultClusterSettings()}); err != nil {
		t.Fatal(err)
	}
	job := replicasJob(3)
	if err := s.CanonicalizeJob(job); err != nil {
		t.Fatal(err)
	}
	s.jobs[jobKey("default", "web")] = &Job{Spec: canonicalTestSpec(job), Revision: 1}

	limits := spec.DefaultLimits()
	limits.MaxReplicasPerTaskGroup = 2
	if _, err := s.UpdateJobLimits(ctx, limits); !errors.Is(err, ErrClusterSettingsConflict) {
		t.Fatalf("lowering limits below a desired job: err = %v, want conflict", err)
	}
	limits = spec.DefaultLimits()
	limits.MaxDesiredAllocationsPerNamespace = 2
	if _, err := s.UpdateJobLimits(ctx, limits); !errors.Is(err, ErrClusterSettingsConflict) {
		t.Fatalf("lowering the namespace limit below desired allocations: err = %v, want conflict", err)
	}
	limits = spec.DefaultLimits()
	limits.DefaultTaskCPU = limits.MaxTaskCPU + 1
	if _, err := s.UpdateJobLimits(ctx, limits); !errors.Is(err, ErrInvalidClusterSettings) {
		t.Fatalf("invalid limits: err = %v, want invalid", err)
	}
	if got := s.ClusterSettings().JobLimits; got != spec.DefaultLimits() {
		t.Fatalf("refused updates changed job limits to %+v", got)
	}
	limits = spec.DefaultLimits()
	limits.MaxReplicasPerTaskGroup = 3
	if _, err := s.UpdateJobLimits(ctx, limits); err != nil {
		t.Fatalf("limits that still admit every desired job: %v", err)
	}
}

func TestRegisterJobUsesJobLimitsCurrentAtCommit(t *testing.T) {
	ctx := context.Background()
	store := memoryStore{}
	_, encoded := encodedAdministratorPublicKey(t)
	s := newSettingsTestServer(t, store)
	if err := s.Init(ctx, ClusterBootstrap{AdministratorPublicKey: encoded, Settings: DefaultClusterSettings()}); err != nil {
		t.Fatal(err)
	}
	job := replicasJob(3)
	if err := s.CanonicalizeJob(job); err != nil {
		t.Fatal(err)
	}
	// Model limits lowered between the handler's canonicalization and commit.
	limits := spec.DefaultLimits()
	limits.MaxReplicasPerTaskGroup = 2
	if _, err := s.UpdateJobLimits(ctx, limits); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterJob(ctx, "default", job, nil); err == nil || !strings.Contains(err.Error(), "exceeds operator limit of 2 replicas") {
		t.Fatalf("registering a job above the job limits current at commit: err = %v", err)
	}
}

func TestClusterSettingsEndpointsAuthorization(t *testing.T) {
	ctx := context.Background()
	store := memoryStore{}
	_, encoded := encodedAdministratorPublicKey(t)
	control := newSettingsTestServer(t, store)
	if err := control.Init(ctx, ClusterBootstrap{AdministratorPublicKey: encoded, Settings: DefaultClusterSettings()}); err != nil {
		t.Fatal(err)
	}
	e := echo.New()
	NewHandler(control).Register(e)
	serve := func(req *http.Request) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		return rec
	}
	admin := func(req *http.Request) *http.Request {
		return req.WithContext(context.WithValue(req.Context(), AdminContextKey, true))
	}

	if rec := serve(scopedRequest(t, http.MethodGet, "/v1/cluster/settings", "", auth.AccessCluster, auth.AccessRead, "")); rec.Code != http.StatusOK {
		t.Fatalf("cluster/read settings status = %d: %s", rec.Code, rec.Body.String())
	}
	if rec := serve(scopedRequest(t, http.MethodGet, "/v1/cluster/settings", "", auth.AccessNamespace, auth.AccessWrite, "team")); rec.Code != http.StatusForbidden {
		t.Fatalf("namespace credential settings status = %d, want 403", rec.Code)
	}
	body := `{"max_replicas_per_task_group":900,"max_task_groups_per_job":64,"max_tasks_per_task_group":32,"max_desired_allocations":1000,"max_desired_allocations_per_namespace":10000,"default_task_cpu":100,"default_task_memory":134217728,"max_task_cpu":1000000,"max_task_memory":1099511627776}`
	if rec := serve(scopedRequest(t, http.MethodPut, "/v1/cluster/settings/job-limits", body, auth.AccessCluster, auth.AccessWrite, "")); rec.Code != http.StatusForbidden {
		t.Fatalf("cluster/write job limits status = %d, want 403", rec.Code)
	}
	if rec := serve(admin(scopedRequest(t, http.MethodPut, "/v1/cluster/settings/job-limits", `{"max_replicas_per_task_group":900,"unknown":1}`, "", "", ""))); rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown field status = %d, want 400", rec.Code)
	}
	if rec := serve(admin(scopedRequest(t, http.MethodPut, "/v1/cluster/settings/job-limits", `{"max_replicas_per_task_group":900}`, "", "", ""))); rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("incomplete limits status = %d, want 422", rec.Code)
	}
	if rec := serve(admin(scopedRequest(t, http.MethodPut, "/v1/cluster/settings/job-limits", body, "", "", ""))); rec.Code != http.StatusOK {
		t.Fatalf("administrator job limits status = %d: %s", rec.Code, rec.Body.String())
	}
	if got := control.ClusterSettings().JobLimits.MaxReplicasPerTaskGroup; got != 900 {
		t.Fatalf("max replicas per task group = %d, want 900", got)
	}
}

func TestUpdateReconciliationSettingsIsReplicatedToLaterLeaders(t *testing.T) {
	ctx := context.Background()
	store := memoryStore{}
	_, encoded := encodedAdministratorPublicKey(t)
	leader := newSettingsTestServer(t, store)
	if err := leader.Init(ctx, ClusterBootstrap{AdministratorPublicKey: encoded, Settings: DefaultClusterSettings()}); err != nil {
		t.Fatal(err)
	}
	follower := newSettingsTestServer(t, store)
	if err := follower.Init(ctx, ClusterBootstrap{}); err != nil {
		t.Fatal(err)
	}
	if err := leader.AcquireLeadership(ctx); err != nil {
		t.Fatal(err)
	}

	reconciliation := DefaultReconciliationSettings()
	reconciliation.AllocationLossTimeout = 5 * time.Minute
	reconciliation.ReplacementBackoffBase = time.Minute
	reconciliation.ReplacementBackoffMax = time.Hour
	reconciliation.TerminalAllocationRetention = 20
	if _, err := leader.UpdateReconciliationSettings(ctx, reconciliation); err != nil {
		t.Fatal(err)
	}
	invalid := reconciliation
	invalid.AllocationLossTimeout = time.Second
	if _, err := leader.UpdateReconciliationSettings(ctx, invalid); !errors.Is(err, ErrInvalidClusterSettings) {
		t.Fatalf("invalid settings error = %v, want ErrInvalidClusterSettings", err)
	}
	if err := follower.AcquireLeadership(ctx); err != nil {
		t.Fatal(err)
	}
	if got := follower.ClusterSettings().Reconciliation; got != reconciliation {
		t.Fatalf("later leader reconciliation = %+v, want %+v", got, reconciliation)
	}
	follower.mu.Lock()
	input, _ := follower.reconcilePlanInputLocked(follower.now(), nil, nil, nil)
	follower.mu.Unlock()
	if input.AllocationLossTimeout != reconciliation.AllocationLossTimeout || input.Policy != reconciliation.replacementPolicy() {
		t.Fatalf("planner input = %s/%+v, want the replicated settings", input.AllocationLossTimeout, input.Policy)
	}
}

func TestReconciliationSettingsEndpoint(t *testing.T) {
	ctx := context.Background()
	store := memoryStore{}
	_, encoded := encodedAdministratorPublicKey(t)
	control := newSettingsTestServer(t, store)
	if err := control.Init(ctx, ClusterBootstrap{AdministratorPublicKey: encoded, Settings: DefaultClusterSettings()}); err != nil {
		t.Fatal(err)
	}
	e := echo.New()
	NewHandler(control).Register(e)
	serve := func(req *http.Request) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		return rec
	}
	admin := func(req *http.Request) *http.Request {
		return req.WithContext(context.WithValue(req.Context(), AdminContextKey, true))
	}
	const path = "/v1/cluster/settings/reconciliation"
	body := `{"allocation_loss_timeout":120000000000,"replacement_backoff_base":10000000000,"replacement_backoff_max":300000000000,"replacement_stable_after":600000000000,"terminal_allocation_retention":8}`
	if rec := serve(scopedRequest(t, http.MethodPut, path, body, auth.AccessCluster, auth.AccessWrite, "")); rec.Code != http.StatusForbidden {
		t.Fatalf("cluster/write status = %d, want 403", rec.Code)
	}
	if rec := serve(admin(scopedRequest(t, http.MethodPut, path, `{"allocation_loss_timeout":120000000000,"unknown":1}`, "", "", ""))); rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown field status = %d, want 400", rec.Code)
	}
	if rec := serve(admin(scopedRequest(t, http.MethodPut, path, `{"allocation_loss_timeout":120000000000}`, "", "", ""))); rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("incomplete settings status = %d, want 422", rec.Code)
	}
	rec := serve(admin(scopedRequest(t, http.MethodPut, path, body, "", "", "")))
	if rec.Code != http.StatusOK {
		t.Fatalf("administrator status = %d: %s", rec.Code, rec.Body.String())
	}
	var response api.ClusterSettings
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Reconciliation.AllocationLossTimeout != 2*time.Minute || response.Reconciliation.TerminalAllocationRetention != 8 {
		t.Fatalf("response reconciliation = %+v", response.Reconciliation)
	}
	if got := control.ClusterSettings().Reconciliation.AllocationLossTimeout; got != 2*time.Minute {
		t.Fatalf("allocation loss timeout = %s, want 2m", got)
	}
}
