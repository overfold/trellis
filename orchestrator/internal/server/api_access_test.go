package server

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

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
	requested := &spec.APIAccessSpec{Scope: spec.APIAccessCluster, Access: spec.APIAccessRead}
	request := &nodeapi.AllocationRequest{AllocationID: "alloc-1", Generation: 1, Namespace: "default", JobName: "web", GroupName: "app"}
	token, err := s.apiAccessToken(context.Background(), requested, request)
	if err != nil {
		t.Fatalf("cluster/read api access: %v", err)
	}
	stored, err := s.tokenManager.ValidateToken(context.Background(), token)
	if err != nil || stored == nil {
		t.Fatalf("validate generated token: %v", err)
	}
	if stored.Kind != auth.CredentialWorkload || stored.Scope != auth.AccessCluster || stored.Access != auth.AccessRead || stored.Namespace != "" {
		t.Fatalf("unexpected generated principal: %#v", stored)
	}
	if stored.Subject == nil || *stored.Subject != (auth.CredentialSubject{Namespace: "default", Job: "web", TaskGroup: "app"}) {
		t.Fatalf("generated principal subject = %#v", stored.Subject)
	}
}

func TestAPIAccessTokenRequiresSecretsKey(t *testing.T) {
	s := &Server{tokenManager: auth.NewTokenManager(apiAccessStore{}, "test")}
	requested := &spec.APIAccessSpec{Scope: spec.APIAccessNamespace, Access: spec.APIAccessRead}
	request := &nodeapi.AllocationRequest{AllocationID: "alloc-1", Generation: 1, Namespace: "default", JobName: "web", GroupName: "app"}
	if _, err := s.apiAccessToken(context.Background(), requested, request); err == nil || !strings.Contains(err.Error(), "secrets encryption key") {
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
	task := spec.TaskSpec{Name: "app", Image: "app"}
	group := spec.TaskGroupSpec{Name: "app", APIAccess: &spec.APIAccessSpec{Scope: spec.APIAccessNamespace, Access: spec.APIAccessWrite}, Tasks: []spec.TaskSpec{task}}
	s.jobs[jobKey("default", "web")] = &Job{Spec: canonicalTestSpec(&spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{group}}), Revision: 1}
	alloc := &Allocation{ID: "allocation", Namespace: "default", JobName: "web", TaskGroupName: "app", Tasks: []spec.TaskSpec{task}, Node: node, Generation: 1, JobRevision: 1, Phase: lifecycle.PhasePlaced}
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
	token := first.EnvOverrides["TRELLIS_TOKEN"]
	if token == "" || retry.EnvOverrides["TRELLIS_TOKEN"] != token {
		t.Fatal("start retry did not re-deliver the same workload token")
	}
	if first.ExecutionHash == "" || first.ExecutionHash != retry.ExecutionHash {
		t.Fatalf("execution hash changed across start retries: %q, %q", first.ExecutionHash, retry.ExecutionHash)
	}
	if next.EnvOverrides["TRELLIS_TOKEN"] == token {
		t.Fatal("new allocation generation reused the previous workload token")
	}
	if principal, _ := s.tokenManager.ValidateToken(context.Background(), token); principal != nil {
		t.Fatal("previous generation's workload token remains valid")
	}
	for key, value := range store {
		if bytes.Contains(value, []byte(next.EnvOverrides["TRELLIS_TOKEN"])) {
			t.Fatalf("state key %q contains the plaintext workload token", key)
		}
	}
}

func TestRevokeStaleWorkloadCredentials(t *testing.T) {
	s, _ := newAPIAccessServer(t)
	ctx := context.Background()
	access := &spec.APIAccessSpec{Scope: spec.APIAccessNamespace, Access: spec.APIAccessRead}
	s.jobs[jobKey("default", "web")] = &Job{Spec: canonicalTestSpec(&spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "app", APIAccess: access}}})}
	issue := func(id string) string {
		t.Helper()
		token, err := s.apiAccessToken(ctx, access, &nodeapi.AllocationRequest{AllocationID: id, Generation: 1, Namespace: "default", JobName: "web", GroupName: "app"})
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

	s.jobs[jobKey("default", "web")].Spec.TaskGroups[0].APIAccess = &spec.APIAccessSpec{Scope: spec.APIAccessCluster, Access: spec.APIAccessWrite}
	s.revokeStaleWorkloadCredentials(ctx)
	if !valid(kept) {
		t.Fatal("widening api_access revoked a running allocation's workload token")
	}

	writer, err := s.apiAccessToken(ctx, &spec.APIAccessSpec{Scope: spec.APIAccessCluster, Access: spec.APIAccessWrite}, &nodeapi.AllocationRequest{AllocationID: "kept", Generation: 2, Namespace: "default", JobName: "web", GroupName: "app"})
	if err != nil {
		t.Fatal(err)
	}
	s.jobs[jobKey("default", "web")].Spec.TaskGroups[0].APIAccess = &spec.APIAccessSpec{Scope: spec.APIAccessCluster, Access: spec.APIAccessRead}
	s.revokeStaleWorkloadCredentials(ctx)
	if valid(writer) {
		t.Fatal("narrowing api_access left a broader workload token valid")
	}

	s.jobs[jobKey("default", "web")].Spec.TaskGroups[0].APIAccess = nil
	kept = issue("kept")
	s.revokeStaleWorkloadCredentials(ctx)
	if valid(kept) {
		t.Fatal("removing api_access left the workload token valid")
	}

	s.jobs[jobKey("default", "web")].Spec.TaskGroups[0].APIAccess = access
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

func TestAPIAccessTokenNone(t *testing.T) {
	s := &Server{}
	token, err := s.apiAccessToken(context.Background(), nil, &nodeapi.AllocationRequest{Namespace: "default"})
	if err != nil {
		t.Fatalf("disabled api access: %v", err)
	}
	if token != "" {
		t.Fatalf("disabled api access returned token %q", token)
	}
}
