package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	"github.com/overfold/trellis/orchestrator/internal/spec"
	"github.com/overfold/trellis/orchestrator/internal/state"
)

type auditStore struct {
	memoryStore
	mu           sync.Mutex
	failBatch    bool
	failPutAfter int
	puts         int
	blockPut     chan struct{}
	putStarted   chan struct{}
	startOnce    sync.Once
	blockBatch   chan struct{}
	batchStarted chan struct{}
	batchOnce    sync.Once
	batchSizes   []int
}

func (s *auditStore) Put(ctx context.Context, key string, value []byte) error {
	s.mu.Lock()
	s.puts++
	put := s.puts
	block, started := s.blockPut, s.putStarted
	s.mu.Unlock()
	if block != nil {
		s.startOnce.Do(func() { close(started) })
		select {
		case <-block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failPutAfter > 0 && put >= s.failPutAfter {
		return errors.New("storage unavailable")
	}
	return s.memoryStore.Put(ctx, key, value)
}

func (s *auditStore) Batch(ctx context.Context, mutations []state.Mutation) error {
	s.mu.Lock()
	s.batchSizes = append(s.batchSizes, len(mutations))
	block, started := s.blockBatch, s.batchStarted
	s.mu.Unlock()
	if block != nil {
		s.batchOnce.Do(func() { close(started) })
		select {
		case <-block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failBatch {
		return errors.New("storage unavailable")
	}
	return s.memoryStore.Batch(ctx, mutations)
}

func (s *auditStore) Get(ctx context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.memoryStore.Get(ctx, key)
}

func (s *auditStore) List(ctx context.Context, prefix string) (map[string][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.memoryStore.List(ctx, prefix)
}

func (s *auditStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.memoryStore.Delete(ctx, key)
}

func TestExecutePersistenceFailuresDoNotAdvanceMemory(t *testing.T) {
	for _, tc := range []struct {
		name      string
		failAfter int
		failRun   bool
		wantPhase lifecycle.Phase
	}{
		{name: "initial transition", failAfter: 2, wantPhase: lifecycle.PhasePlaced},
		{name: "agent failure transition", failAfter: 3, failRun: true, wantPhase: lifecycle.PhaseStarting},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, agent := newTestServerWithAgent()
			defer agent.server.Close()
			node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy}
			s.nodes[node.ID] = node
			jobSpec := &spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "api", Tasks: []spec.TaskSpec{{Name: "server", Image: "app"}}}}}
			s.jobs[jobKey("default", "web")] = &Job{Spec: canonicalTestSpec(jobSpec), Revision: 1}
			allocation := &Allocation{ID: "web-1", Namespace: "default", JobName: "web", TaskGroupName: "api", Tasks: jobSpec.TaskGroups[0].Tasks, Node: node, Generation: 1, JobRevision: 1, Phase: lifecycle.PhasePlaced, Health: lifecycle.HealthUnknown}
			s.allocations = []*Allocation{allocation}
			store := &auditStore{memoryStore: memoryStore{}, failPutAfter: tc.failAfter}
			s.state = NewStateController(store, "test")
			if err := s.state.PutAllocation(context.Background(), allocation); err != nil {
				t.Fatal(err)
			}
			agent.mu.Lock()
			agent.failRun = tc.failRun
			agent.mu.Unlock()

			err := s.Execute(context.Background(), &Action{Type: ActionStart, Allocation: allocation})
			if err == nil || !strings.Contains(err.Error(), "storage unavailable") {
				t.Fatalf("Execute error = %v, want persistence failure", err)
			}
			persisted, listErr := s.state.ListAllocations(context.Background())
			if listErr != nil {
				t.Fatal(listErr)
			}
			if allocation.Phase != tc.wantPhase || persisted[allocation.ID].Phase != tc.wantPhase {
				t.Fatalf("phase after failed write: memory=%s durable=%s, want %s", allocation.Phase, persisted[allocation.ID].Phase, tc.wantPhase)
			}
		})
	}
}

func TestHeartbeatBatchFailureLeavesMemoryAndDurableStateUnchanged(t *testing.T) {
	node := &Node{ID: uuid.New(), Host: "node-a", Status: NodeStatusHealthy, Version: "old"}
	allocation := &Allocation{ID: "web-1", Node: node, Tasks: []spec.TaskSpec{{Name: "app"}}, Generation: 1, Phase: lifecycle.PhaseStarting, Health: lifecycle.HealthUnknown}
	store := &auditStore{memoryStore: memoryStore{}}
	s := &Server{now: time.Now, state: NewStateController(store, "test"), nodes: map[uuid.UUID]*Node{node.ID: node}, allocations: []*Allocation{allocation}, catalog: newNopCatalog()}
	if err := s.state.PutNode(context.Background(), node.ID.String(), nodeSummary(node)); err != nil {
		t.Fatal(err)
	}
	if err := s.state.PutAllocation(context.Background(), allocation); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.failBatch = true
	store.mu.Unlock()
	actual := []nodeapi.AllocationStatus{{ID: allocation.ID, Generation: 1, Task: "app", Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy}}
	if err := s.Heartbeat(context.Background(), node.ID, actual, "new", nil, nil, nodeResourceObservation{}); err == nil {
		t.Fatal("heartbeat succeeded despite failed atomic write")
	}
	if node.Version != "old" || allocation.Phase != lifecycle.PhaseStarting || allocation.Health != lifecycle.HealthUnknown {
		t.Fatalf("memory advanced after failed heartbeat: node=%q phase=%s health=%s", node.Version, allocation.Phase, allocation.Health)
	}
	nodes, _ := s.state.ListNodes(context.Background())
	allocations, _ := s.state.ListAllocations(context.Background())
	if nodes[node.ID.String()].Version != "old" || allocations[allocation.ID].Phase != lifecycle.PhaseStarting {
		t.Fatalf("durable state advanced after failed heartbeat: node=%#v allocation=%#v", nodes[node.ID.String()], allocations[allocation.ID])
	}
}

func TestUnchangedHeartbeatDoesNotWriteRaft(t *testing.T) {
	node := &Node{ID: uuid.New(), Host: "node-a", Status: NodeStatusHealthy, Version: "test", CPUCapacity: 4000, CPUAllocatable: 4000}
	allocation := &Allocation{ID: "web-1", Node: node, Tasks: []spec.TaskSpec{{Name: "app"}}, Generation: 1, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy,
		Endpoints: []api.AllocationEndpoint{{Task: "app"}}}
	store := &auditStore{memoryStore: memoryStore{}}
	clock := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	s := &Server{now: func() time.Time { return clock }, state: NewStateController(store, "test"), nodes: map[uuid.UUID]*Node{node.ID: node}, allocations: []*Allocation{allocation}, catalog: newNopCatalog()}
	status := []nodeapi.AllocationStatus{{ID: allocation.ID, Generation: 1, Task: "app", Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy}}

	for i := 0; i < 3; i++ {
		clock = clock.Add(heartbeatInterval)
		usage, used := float64(i)/10, int64(i)<<20
		metricsAt := clock
		resources := nodeResourceObservation{CPUCapacity: 4000, CPUAllocatable: 4000, CPUUsage: &usage, MemoryUsed: &used, MetricsAt: &metricsAt}
		if err := s.Heartbeat(context.Background(), node.ID, status, "test", nil, nil, resources); err != nil {
			t.Fatal(err)
		}
		if !node.LastHeartbeat.Equal(clock) || node.MetricsAt == nil || !node.MetricsAt.Equal(clock) || len(node.observedAllocations) != 1 {
			t.Fatalf("heartbeat %d observations not kept in memory: last=%s metrics=%v observed=%v", i, node.LastHeartbeat, node.MetricsAt, node.observedAllocations)
		}
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.batchSizes) != 0 || store.puts != 0 {
		t.Fatalf("unchanged heartbeats wrote to Raft: batches=%v puts=%d", store.batchSizes, store.puts)
	}
}

func TestHeartbeatPersistsOnlyDurableChanges(t *testing.T) {
	node := &Node{ID: uuid.New(), Host: "node-a", Status: NodeStatusUnhealthy, Version: "old"}
	allocation := &Allocation{ID: "web-1", Node: node, Tasks: []spec.TaskSpec{{Name: "app"}}, Generation: 1, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy,
		Endpoints: []api.AllocationEndpoint{{Task: "app"}}}
	store := &auditStore{memoryStore: memoryStore{}}
	s := &Server{now: time.Now, state: NewStateController(store, "test"), nodes: map[uuid.UUID]*Node{node.ID: node}, allocations: []*Allocation{allocation}, catalog: newNopCatalog()}
	status := []nodeapi.AllocationStatus{{ID: allocation.ID, Generation: 1, Task: "app", Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy}}

	// Liveness returning is an observation, not a durable fact.
	if err := s.Heartbeat(context.Background(), node.ID, status, "old", nil, nil, nodeResourceObservation{}); err != nil {
		t.Fatal(err)
	}
	if node.Status != NodeStatusHealthy {
		t.Fatalf("node status = %s, want healthy", node.Status)
	}
	// A new agent version is a durable node fact.
	if err := s.Heartbeat(context.Background(), node.ID, status, "new", nil, nil, nodeResourceObservation{}); err != nil {
		t.Fatal(err)
	}
	// An allocation phase change is durable lifecycle state.
	status[0].Phase, status[0].Health = lifecycle.PhaseFailed, lifecycle.HealthUnhealthy
	if err := s.Heartbeat(context.Background(), node.ID, status, "new", nil, nil, nodeResourceObservation{}); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	batches := append([]int(nil), store.batchSizes...)
	store.mu.Unlock()
	if len(batches) != 2 || batches[0] != 1 || batches[1] != 1 {
		t.Fatalf("heartbeat batches = %v, want one node write then one allocation write", batches)
	}
	nodes, err := s.state.ListNodes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	allocations, err := s.state.ListAllocations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if nodes[node.ID.String()].Version != "new" || allocations[allocation.ID].Phase != lifecycle.PhaseFailed {
		t.Fatalf("durable state = node %#v allocation phase %s", nodes[node.ID.String()], allocations[allocation.ID].Phase)
	}
}

func TestAllocationRecordStoresNodeIDWithoutObservations(t *testing.T) {
	usage, used := 0.5, int64(1<<30)
	metricsAt := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	node := &Node{ID: uuid.New(), Host: "node-a", Port: 8127, Status: NodeStatusHealthy, LastHeartbeat: metricsAt, CPUUsage: &usage, MemoryUsed: &used, MetricsAt: &metricsAt,
		Labels: map[string]string{"zone": "a"}, Volumes: []string{"data"}, Version: "v1"}
	allocation := &Allocation{ID: "web-1", Node: node, Generation: 1, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy}
	store := memoryStore{}
	controller := NewStateController(store, "test")
	if err := controller.PutNode(context.Background(), node.ID.String(), nodeSummary(node)); err != nil {
		t.Fatal(err)
	}
	if err := controller.PutAllocation(context.Background(), allocation); err != nil {
		t.Fatal(err)
	}
	raw, err := store.Get(context.Background(), controller.allocationKey(allocation.ID))
	if err != nil {
		t.Fatal(err)
	}
	var record map[string]json.RawMessage
	if err := json.Unmarshal(raw, &record); err != nil {
		t.Fatal(err)
	}
	if _, ok := record["Node"]; ok {
		t.Fatalf("allocation record embeds its node: %s", raw)
	}
	for _, observation := range []string{"CPUUsage", "MemoryUsed", "MetricsAt", "LastHeartbeat", "zone"} {
		if bytes.Contains(raw, []byte(observation)) {
			t.Fatalf("allocation record carries node data %q: %s", observation, raw)
		}
	}
	if got := string(record["node_id"]); got != `"`+node.ID.String()+`"` {
		t.Fatalf("allocation node_id = %s, want %s", got, node.ID)
	}
	rawNode, err := store.Get(context.Background(), fmt.Sprintf("trellis/test/nodes/%s", node.ID))
	if err != nil {
		t.Fatal(err)
	}
	for _, observation := range []string{"CPUUsage", "MemoryUsed", "MetricsAt", "LastHeartbeat", "Status"} {
		if bytes.Contains(rawNode, []byte(observation)) {
			t.Fatalf("node record carries observation %q: %s", observation, rawNode)
		}
	}

	s := &Server{now: time.Now, state: controller}
	if err := s.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	reloaded := s.allocations[0]
	if reloaded.Node == nil || reloaded.Node != s.nodes[node.ID] {
		t.Fatalf("reloaded allocation node = %p, want canonical node %p", reloaded.Node, s.nodes[node.ID])
	}
	if reloaded.Node.Status != NodeStatusUnhealthy || !reloaded.Node.LastHeartbeat.IsZero() || reloaded.Node.MetricsAt != nil || reloaded.Node.Version != "v1" {
		t.Fatalf("reloaded node = %#v, want durable facts without observations", reloaded.Node)
	}
}

func TestHeartbeatDoesNotLockAllocationsAssignedToOtherNodes(t *testing.T) {
	node, otherNode := &Node{ID: uuid.New(), Status: NodeStatusHealthy}, &Node{ID: uuid.New(), Status: NodeStatusHealthy}
	assigned := &Allocation{ID: "assigned", Node: node, Generation: 1, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy}
	unrelated := &Allocation{ID: "unrelated", Node: otherNode, Generation: 1, Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy}
	s := &Server{now: time.Now, state: NewStateController(memoryStore{}, "test"), nodes: map[uuid.UUID]*Node{node.ID: node, otherNode.ID: otherNode}, allocations: []*Allocation{assigned, unrelated}, catalog: newNopCatalog()}
	unrelated.mu.Lock()
	defer unrelated.mu.Unlock()
	done := make(chan error, 1)
	go func() {
		done <- s.Heartbeat(context.Background(), node.ID, nil, "test", nil, nil, nodeResourceObservation{})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("heartbeat inspected an allocation assigned to another node")
	}
}

func TestUndrainBatchFailureLeavesMemoryAndDurableStateDraining(t *testing.T) {
	s, agent, node, allocation := newDrainedNodeFixture(t)
	defer agent.server.Close()
	store := &auditStore{memoryStore: memoryStore{}}
	s.state = NewStateController(store, "test")
	if err := s.state.PutNode(context.Background(), node.ID.String(), nodeSummary(node)); err != nil {
		t.Fatal(err)
	}
	if err := s.state.PutAllocation(context.Background(), allocation); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.failBatch = true
	store.mu.Unlock()

	if err := s.UndrainNode(context.Background(), node.ID); err == nil {
		t.Fatal("undrain succeeded despite failed atomic write")
	}
	if node.Status != NodeStatusDraining || !allocation.Draining || allocation.DrainSequence != 1 {
		t.Fatalf("memory after failed undrain: node=%s draining=%t sequence=%d", node.Status, allocation.Draining, allocation.DrainSequence)
	}
	nodes, _ := s.state.ListNodes(context.Background())
	allocations, _ := s.state.ListAllocations(context.Background())
	if !nodes[node.ID.String()].Draining || !allocations[allocation.ID].Draining || allocations[allocation.ID].DrainSequence != 1 {
		t.Fatalf("durable state after failed undrain: node=%#v allocation=%#v", nodes[node.ID.String()], allocations[allocation.ID])
	}
	if requests := resumeCalls(agent, allocation.ID); len(requests) != 0 {
		t.Fatalf("resume delivered after failed undrain: %#v", requests)
	}
}

func TestDrainWriteFailureLeavesMemoryAndDurableStateHealthy(t *testing.T) {
	store := &auditStore{memoryStore: memoryStore{}, failPutAfter: 2}
	s := NewServer(slog.Default(), nil, NewStateController(store, "test"), store, "test", "")
	node := &Node{ID: uuid.New(), Host: "node-a", Status: NodeStatusHealthy}
	s.nodes[node.ID] = node
	if err := s.state.PutNode(context.Background(), node.ID.String(), nodeSummary(node)); err != nil {
		t.Fatal(err)
	}

	if err := s.DrainNode(context.Background(), node.ID); err == nil {
		t.Fatal("drain succeeded despite failed node write")
	}
	nodes, err := s.state.ListNodes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if node.Status != NodeStatusHealthy || nodes[node.ID.String()].Draining {
		t.Fatalf("state after failed drain: memory=%s durable draining=%t", node.Status, nodes[node.ID.String()].Draining)
	}
}

func TestRegisterAndDrainSerializeDurableNodeSnapshots(t *testing.T) {
	store := &auditStore{memoryStore: memoryStore{}, blockPut: make(chan struct{}), putStarted: make(chan struct{})}
	s := NewServer(slog.Default(), nil, NewStateController(store, "test"), store, "test", "")
	nodeID := uuid.New()
	node := &Node{ID: nodeID, Host: "old", Status: NodeStatusHealthy}
	s.nodes[nodeID] = node
	store.blockPut = nil
	if err := s.state.PutNode(context.Background(), nodeID.String(), nodeSummary(node)); err != nil {
		t.Fatal(err)
	}
	store.blockPut = make(chan struct{})
	store.putStarted = make(chan struct{})
	store.startOnce = sync.Once{}
	registerDone := make(chan error, 1)
	go func() {
		registerDone <- s.RegisterNode(context.Background(), &NodeRegistration{ID: nodeID, Host: "new"})
	}()
	<-store.putStarted
	drainDone := make(chan error, 1)
	go func() { drainDone <- s.DrainNode(context.Background(), nodeID) }()
	close(store.blockPut)
	if err := <-registerDone; err != nil {
		t.Fatal(err)
	}
	if err := <-drainDone; err != nil {
		t.Fatal(err)
	}
	nodes, err := s.state.ListNodes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if node.Host != "new" || node.Status != NodeStatusDraining || nodes[nodeID.String()].Host != "new" || !nodes[nodeID.String()].Draining {
		t.Fatalf("register/drain result: memory=%#v durable=%#v", node, nodes[nodeID.String()])
	}
}

func TestHeartbeatAndDrainSerializeDurableNodeSnapshots(t *testing.T) {
	store := &auditStore{memoryStore: memoryStore{}}
	s := NewServer(slog.Default(), nil, NewStateController(store, "test"), store, "test", "")
	nodeID := uuid.New()
	node := &Node{ID: nodeID, Host: "node-a", Status: NodeStatusHealthy, Version: "old"}
	s.nodes[nodeID] = node
	if err := s.state.PutNode(context.Background(), nodeID.String(), nodeSummary(node)); err != nil {
		t.Fatal(err)
	}
	store.blockBatch = make(chan struct{})
	store.batchStarted = make(chan struct{})
	heartbeatDone := make(chan error, 1)
	go func() {
		heartbeatDone <- s.Heartbeat(context.Background(), nodeID, nil, "new", nil, nil, nodeResourceObservation{})
	}()
	<-store.batchStarted
	drainDone := make(chan error, 1)
	go func() { drainDone <- s.DrainNode(context.Background(), nodeID) }()
	close(store.blockBatch)
	if err := <-heartbeatDone; err != nil {
		t.Fatal(err)
	}
	if err := <-drainDone; err != nil {
		t.Fatal(err)
	}
	nodes, err := s.state.ListNodes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if node.Version != "new" || node.Status != NodeStatusDraining || nodes[nodeID.String()].Version != "new" || !nodes[nodeID.String()].Draining {
		t.Fatalf("heartbeat/drain result: memory=%#v durable=%#v", node, nodes[nodeID.String()])
	}
}
