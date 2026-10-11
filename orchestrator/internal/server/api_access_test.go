package server

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/internal/auth"
	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	secretstore "github.com/overfold/trellis/orchestrator/internal/secrets"
	"github.com/overfold/trellis/orchestrator/internal/spec"
	"github.com/overfold/trellis/orchestrator/internal/state"
	"github.com/overfold/trellis/orchestrator/internal/storage"
)

type apiAccessStore map[string][]byte

func (m apiAccessStore) Get(_ context.Context, key string) ([]byte, error) { return m[key], nil }
func (m apiAccessStore) List(_ context.Context, prefix string) (map[string][]byte, error) {
	result := map[string][]byte{}
	for key, value := range m {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			result[key] = value
		}
	}
	return result, nil
}
func (m apiAccessStore) Put(_ context.Context, key string, value []byte) error {
	m[key] = value
	return nil
}
func (m apiAccessStore) Delete(_ context.Context, key string) error { delete(m, key); return nil }
func (m apiAccessStore) Batch(_ context.Context, mutations []state.Mutation) error {
	for _, mutation := range mutations {
		if mutation.Value == nil {
			delete(m, mutation.Key)
			continue
		}
		m[mutation.Key] = mutation.Value
	}
	return nil
}

func newAPIAccessServer(t *testing.T) (*Server, apiAccessStore) {
	t.Helper()
	store := apiAccessStore{}
	sealer, err := secretstore.NewStore(store, "test", "k1", bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}
	return &Server{log: slog.Default(), tokenManager: auth.NewTokenManager(store, "test"), secrets: sealer, jobs: map[string]*Job{}}, store
}

func TestAPIAccessTokenClusterRead(t *testing.T) {
	s, _ := newAPIAccessServer(t)
	requested := &spec.APIAccessSpec{Access: spec.APIAccessRead}
	request := &nodeapi.AllocationRequest{AllocationID: "alloc-1", Generation: 1, Namespace: "default", JobName: "web", GroupName: "app"}
	token, err := s.apiAccessToken(context.Background(), requested, request, "app")
	if err != nil {
		t.Fatalf("cluster/read api access: %v", err)
	}
	stored, err := s.tokenManager.ValidateToken(context.Background(), token)
	if err != nil || stored == nil {
		t.Fatalf("validate generated token: %v", err)
	}
	if stored.Kind != auth.CredentialWorkload || stored.Access != auth.AccessRead {
		t.Fatalf("unexpected generated principal: %#v", stored)
	}
	if stored.Subject == nil || *stored.Subject != (auth.CredentialSubject{Namespace: "default", Job: "web", TaskGroup: "app", Task: "app"}) {
		t.Fatalf("generated principal subject = %#v", stored.Subject)
	}
}

func TestAPIAccessTokenRequiresSecretsKey(t *testing.T) {
	s := &Server{tokenManager: auth.NewTokenManager(apiAccessStore{}, "test")}
	requested := &spec.APIAccessSpec{Access: spec.APIAccessRead}
	request := &nodeapi.AllocationRequest{AllocationID: "alloc-1", Generation: 1, Namespace: "default", JobName: "web", GroupName: "app"}
	if _, err := s.apiAccessToken(context.Background(), requested, request, "app"); err == nil || !strings.Contains(err.Error(), "secrets encryption key") {
		t.Fatalf("api access without secrets key error = %v", err)
	}
}

func TestStartRetryRedeliversWorkloadTokenWithStableExecutionHash(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	apiServer, store := newAPIAccessServer(t)
	s.tokenManager, s.secrets = apiServer.tokenManager, apiServer.secrets
	s.serverAddr = "10.0.0.1:8128"
	s.storage = storage.NewLocalStorage(t.TempDir())
	node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy}
	s.nodes[node.ID] = node
	task := spec.TaskSpec{Name: "app", Image: "app", APIAccess: &spec.APIAccessSpec{Access: spec.APIAccessWrite}}
	sibling := spec.TaskSpec{Name: "sibling", Image: "app", APIAccess: &spec.APIAccessSpec{Access: spec.APIAccessRead}}
	helper := spec.TaskSpec{Name: "helper", Image: "app"}
	group := spec.TaskGroupSpec{Name: "app", Tasks: []spec.TaskSpec{task, sibling, helper}}
	s.jobs[jobKey("default", "web")] = &Job{Spec: canonicalTestSpec(&spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{group}}), Revision: 1}
	alloc := &Allocation{ID: "allocation", Namespace: "default", JobName: "web", TaskGroupName: "app", Tasks: group.Tasks, Node: node, Generation: 1, JobRevision: 1, Phase: lifecycle.PhasePlaced}
	s.allocations = []*Allocation{alloc}
	start := &Action{Type: ActionStart, Allocation: alloc}
	for range 2 {
		if err := s.Execute(context.Background(), start); err != nil {
			t.Fatal(err)
		}
	}
	alloc.Generation = 2
	if err := s.Execute(context.Background(), start); err != nil {
		t.Fatal(err)
	}
	calls := agent.recordedCalls()
	if len(calls) != 3 {
		t.Fatalf("start calls = %d, want 3", len(calls))
	}
	requests := make([]nodeapi.AllocationRequest, len(calls))
	for i := range calls {
		if err := json.Unmarshal(calls[i].body, &requests[i]); err != nil {
			t.Fatal(err)
		}
	}
	first, retry, next := requests[0], requests[1], requests[2]
	for _, request := range requests {
		if len(request.EnvOverrides) != 2 || request.EnvOverrides["helper"] != nil {
			t.Fatal("API environment was injected into a task without api_access")
		}
		appToken, siblingToken := request.EnvOverrides["app"]["TRELLIS_TOKEN"], request.EnvOverrides["sibling"]["TRELLIS_TOKEN"]
		if appToken == "" || siblingToken == "" || appToken == siblingToken {
			t.Fatal("sibling tasks did not receive distinct workload tokens")
		}
	}
	for name, access := range map[string]auth.AccessLevel{"app": auth.AccessWrite, "sibling": auth.AccessRead} {
		principal, err := s.tokenManager.ValidateToken(context.Background(), next.EnvOverrides[name]["TRELLIS_TOKEN"])
		if err != nil || principal == nil || principal.Subject == nil || principal.Subject.Task != name || principal.Access != access {
			t.Fatalf("task %s credential has incorrect identity or access", name)
		}
		if first.EnvOverrides[name]["TRELLIS_TOKEN"] != retry.EnvOverrides[name]["TRELLIS_TOKEN"] {
			t.Fatalf("task %s credential changed on retry", name)
		}
	}
	token := first.EnvOverrides["app"]["TRELLIS_TOKEN"]
	if token == "" || retry.EnvOverrides["app"]["TRELLIS_TOKEN"] != token {
		t.Fatal("start retry did not re-deliver the same workload token")
	}
	if first.ExecutionHash == "" || first.ExecutionHash != retry.ExecutionHash {
		t.Fatalf("execution hash changed across start retries: %q, %q", first.ExecutionHash, retry.ExecutionHash)
	}
	if next.EnvOverrides["app"]["TRELLIS_TOKEN"] == token {
		t.Fatal("new allocation generation reused the previous workload token")
	}
	if principal, _ := s.tokenManager.ValidateToken(context.Background(), token); principal != nil {
		t.Fatal("previous generation's workload token remains valid")
	}
	for key, value := range store {
		if bytes.Contains(value, []byte(next.EnvOverrides["app"]["TRELLIS_TOKEN"])) {
			t.Fatalf("state key %q contains the plaintext workload token", key)
		}
	}
}

func TestRevokeStaleWorkloadCredentials(t *testing.T) {
	s, _ := newAPIAccessServer(t)
	ctx := context.Background()
	access := &spec.APIAccessSpec{Access: spec.APIAccessRead}
	s.jobs[jobKey("default", "web")] = &Job{Spec: canonicalTestSpec(&spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "app", Tasks: []spec.TaskSpec{{Name: "app", Image: "app", APIAccess: access}}}}})}
	issue := func(id string) string {
		t.Helper()
		token, err := s.apiAccessToken(ctx, access, &nodeapi.AllocationRequest{AllocationID: id, Generation: 1, Namespace: "default", JobName: "web", GroupName: "app"}, "app")
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	valid := func(token string) bool {
		principal, err := s.tokenManager.ValidateToken(ctx, token)
		return err == nil && principal != nil
	}
	kept, pruned := issue("kept"), issue("pruned")
	s.allocations = []*Allocation{{ID: "kept", Namespace: "default", JobName: "web", TaskGroupName: "app"}}

	s.revokeStaleWorkloadCredentials(ctx)
	if !valid(kept) || valid(pruned) {
		t.Fatalf("after pruning an allocation: kept valid=%t pruned valid=%t", valid(kept), valid(pruned))
	}

	s.jobs[jobKey("default", "web")].Spec.TaskGroups[0].Tasks[0].APIAccess = &spec.APIAccessSpec{Access: spec.APIAccessWrite}
	s.revokeStaleWorkloadCredentials(ctx)
	if !valid(kept) {
		t.Fatal("widening api_access revoked a running allocation's workload token")
	}

	writer, err := s.apiAccessToken(ctx, &spec.APIAccessSpec{Access: spec.APIAccessWrite}, &nodeapi.AllocationRequest{AllocationID: "kept", Generation: 2, Namespace: "default", JobName: "web", GroupName: "app"}, "app")
	if err != nil {
		t.Fatal(err)
	}
	s.jobs[jobKey("default", "web")].Spec.TaskGroups[0].Tasks[0].APIAccess = &spec.APIAccessSpec{Access: spec.APIAccessRead}
	s.revokeStaleWorkloadCredentials(ctx)
	if valid(writer) {
		t.Fatal("narrowing api_access left a broader workload token valid")
	}

	s.jobs[jobKey("default", "web")].Spec.TaskGroups[0].Tasks[0].APIAccess = nil
	kept = issue("kept")
	s.revokeStaleWorkloadCredentials(ctx)
	if valid(kept) {
		t.Fatal("removing api_access left the workload token valid")
	}

	s.jobs[jobKey("default", "web")].Spec.TaskGroups[0].Tasks[0].APIAccess = access
	kept = issue("kept")
	s.jobs[jobKey("default", "web")].Incarnation = "recreated"
	s.revokeStaleWorkloadCredentials(ctx)
	if valid(kept) {
		t.Fatal("recreating the job left the previous incarnation's workload token valid")
	}

	s.jobs[jobKey("default", "web")].Incarnation = ""
	kept = issue("kept")
	delete(s.jobs, jobKey("default", "web"))
	s.revokeStaleWorkloadCredentials(ctx)
	if valid(kept) {
		t.Fatal("deleting the job left its workload token valid")
	}
}

func TestTaskAccessChangesOnlyRevokeThatTaskCredential(t *testing.T) {
	for _, change := range []string{"narrow", "remove access", "delete task"} {
		t.Run(change, func(t *testing.T) {
			s, store := newAPIAccessServer(t)
			s.state = NewStateController(store, "test")
			now := time.Now()
			s.now = func() time.Time { return now }
			s.leaderSince = now
			node := &Node{ID: uuid.New()}
			s.nodes = map[uuid.UUID]*Node{node.ID: node}
			access := &spec.APIAccessSpec{Access: spec.APIAccessWrite}
			job := &Job{Spec: canonicalTestSpec(&spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "app", Tasks: []spec.TaskSpec{{Name: "app", Image: "app", APIAccess: access}, {Name: "sibling", Image: "app", APIAccess: access}}}}})}
			s.jobs[jobKey("default", "web")] = job
			s.allocations = []*Allocation{{ID: "active", Generation: 1, Namespace: "default", JobName: "web", TaskGroupName: "app", Phase: lifecycle.PhaseRunning, Node: node}}
			request := &nodeapi.AllocationRequest{AllocationID: "active", Generation: 1, Namespace: "default", JobName: "web", GroupName: "app"}
			tokens := map[string]string{}
			for _, name := range []string{"app", "sibling"} {
				var err error
				tokens[name], err = s.apiAccessToken(t.Context(), access, request, name)
				if err != nil {
					t.Fatal(err)
				}
			}
			s.tokenManager.SetWorkloadAuthorizer(s.authorizeWorkloadCredential)
			check := func(name string, want bool) {
				t.Helper()
				principal, err := s.tokenManager.ValidateToken(t.Context(), tokens[name])
				if err != nil || (principal != nil) != want {
					t.Fatalf("task %s authenticated=%t, want %t, err=%v", name, principal != nil, want, err)
				}
			}
			check("app", true)
			check("sibling", true)
			switch change {
			case "narrow":
				job.Spec.TaskGroups[0].Tasks[0].APIAccess = &spec.APIAccessSpec{Access: spec.APIAccessRead}
			case "remove access":
				job.Spec.TaskGroups[0].Tasks[0].APIAccess = nil
			case "delete task":
				job.Spec.TaskGroups[0].Tasks = job.Spec.TaskGroups[0].Tasks[1:]
			}
			check("app", false)
			check("sibling", true)
			s.revokeStaleWorkloadCredentials(t.Context())
			// Disable use-time authorization to verify durable revocation too.
			s.tokenManager.SetWorkloadAuthorizer(nil)
			check("app", false)
			check("sibling", true)
		})
	}
}

func TestAPIAccessTokenNone(t *testing.T) {
	s := &Server{}
	token, err := s.apiAccessToken(context.Background(), nil, &nodeapi.AllocationRequest{Namespace: "default"}, "app")
	if err != nil {
		t.Fatalf("disabled api access: %v", err)
	}
	if token != "" {
		t.Fatalf("disabled api access returned token %q", token)
	}
}

func TestRetainedHistoryDoesNotRetainWorkloadAuthority(t *testing.T) {
	for _, phase := range []lifecycle.Phase{lifecycle.PhaseRunning, lifecycle.PhaseStopping, lifecycle.PhaseStopped, lifecycle.PhaseFailed, lifecycle.PhaseLost} {
		t.Run(string(phase), func(t *testing.T) {
			s, store := newAPIAccessServer(t)
			s.state = NewStateController(store, "test")
			node := &Node{ID: uuid.New()}
			s.nodes = map[uuid.UUID]*Node{node.ID: node}
			access := &spec.APIAccessSpec{Access: spec.APIAccessRead}
			s.jobs[jobKey("default", "web")] = &Job{Spec: canonicalTestSpec(&spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "app", Tasks: []spec.TaskSpec{{Name: "app", Image: "app", APIAccess: access}}}}})}
			s.allocations = []*Allocation{{ID: "retained", Namespace: "default", JobName: "web", TaskGroupName: "app", Phase: phase, Node: node}}
			token, err := s.apiAccessToken(context.Background(), access, &nodeapi.AllocationRequest{AllocationID: "retained", Generation: 1, Namespace: "default", JobName: "web", GroupName: "app"}, "app")
			if err != nil {
				t.Fatal(err)
			}
			s.revokeStaleWorkloadCredentials(context.Background())
			principal, err := s.tokenManager.ValidateToken(context.Background(), token)
			wantValid := phase == lifecycle.PhaseRunning || phase == lifecycle.PhaseStopping
			if err != nil || (principal != nil) != wantValid || len(s.allocations) != 1 {
				t.Fatalf("phase=%s principal=%v history=%d err=%v", phase, principal, len(s.allocations), err)
			}
			if wantValid {
				if err := s.state.PutNodeTombstone(context.Background(), node.ID.String(), NodeTombstone{}); err != nil {
					t.Fatal(err)
				}
				s.revokeStaleWorkloadCredentials(context.Background())
				if principal, _ := s.tokenManager.ValidateToken(context.Background(), token); principal != nil {
					t.Fatal("tombstoned node retained workload authority")
				}
			}
		})
	}
}

func TestWorkloadAuthenticationPartitionDeadlineWithoutReconciliation(t *testing.T) {
	s, store := newAPIAccessServer(t)
	s.state = NewStateController(store, "test")
	now := time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	s.leaderSince = now.Add(-time.Hour)
	s.tokenManager.SetWorkloadAuthorizer(s.authorizeWorkloadCredential)
	node := &Node{ID: uuid.New()}
	s.nodes = map[uuid.UUID]*Node{node.ID: node}
	s.liveness.register(node.ID, now)
	access := &spec.APIAccessSpec{Access: spec.APIAccessRead}
	s.jobs[jobKey("default", "web")] = &Job{Spec: canonicalTestSpec(&spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "app", Tasks: []spec.TaskSpec{{Name: "app", Image: "app", APIAccess: access}}}}})}
	s.allocations = []*Allocation{{ID: "active", Generation: 1, Namespace: "default", JobName: "web", TaskGroupName: "app", Phase: lifecycle.PhaseRunning, Node: node}}
	token, err := s.apiAccessToken(context.Background(), access, &nodeapi.AllocationRequest{AllocationID: "active", Generation: 1, Namespace: "default", JobName: "web", GroupName: "app"}, "app")
	if err != nil {
		t.Fatal(err)
	}
	valid := func() bool {
		principal, err := s.tokenManager.ValidateToken(context.Background(), token)
		if err != nil {
			t.Fatal(err)
		}
		return principal != nil
	}
	now = now.Add(DefaultAllocationLossTimeout - time.Nanosecond)
	if !valid() {
		t.Fatal("temporary partition prematurely revoked authority")
	}
	now = now.Add(time.Nanosecond)
	if valid() {
		t.Fatal("partition retained authority at the loss deadline without reconciliation")
	}
	s.liveness.stamp(node.ID, now)
	if !valid() {
		t.Fatal("renewed observation did not restore nonterminal authority")
	}
	s.leaderSince = now
	s.liveness.startTerm()
	s.reconciliation = DefaultReconciliationSettings()
	s.reconciliation.AllocationLossTimeout = MinAllocationLossTimeout
	now = now.Add(leaderRecoveryGrace - time.Nanosecond)
	if !valid() {
		t.Fatal("new-term recovery grace prematurely rejected authority")
	}
	now = now.Add(time.Nanosecond)
	if valid() {
		t.Fatal("new-term grace deadline did not bound silent workload authority")
	}
	s.liveness.stamp(node.ID, now)
	s.allocations[0].Phase = lifecycle.PhaseLost
	if valid() {
		t.Fatal("retained lost history authenticated after heartbeat")
	}
	s.allocations[0].Phase = lifecycle.PhaseRunning
	if err := s.state.PutNodeTombstone(context.Background(), node.ID.String(), NodeTombstone{}); err != nil {
		t.Fatal(err)
	}
	if valid() {
		t.Fatal("tombstone did not reject credential before durable token deletion")
	}
}
