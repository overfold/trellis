package server

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/clofour/trellis/internal/api"
	"github.com/clofour/trellis/internal/lifecycle"
	"github.com/clofour/trellis/internal/spec"
	"github.com/clofour/trellis/internal/state"
	"github.com/google/uuid"
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
		{name: "running transition", failAfter: 3, wantPhase: lifecycle.PhaseStarting},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, agent := newTestServerWithAgent()
			defer agent.server.Close()
			node := &Node{ID: uuid.New(), Host: agent.host, Port: agent.port, Status: NodeStatusHealthy}
			s.nodes[node.ID] = node
			jobSpec := &spec.JobSpec{Namespace: "default", Name: "web", TaskGroups: []spec.TaskGroupSpec{{Name: "api", Tasks: []spec.TaskSpec{{Name: "server", Image: "app"}}}}}
			s.jobs[jobKey("default", "web")] = &Job{Spec: jobSpec, Revision: 1}
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
	s := &Server{state: NewStateController(store, "test"), nodes: map[uuid.UUID]*Node{node.ID: node}, allocations: []*Allocation{allocation}, catalog: newNopCatalog()}
	if err := s.state.PutNode(context.Background(), node.ID.String(), nodeSummary(node)); err != nil {
		t.Fatal(err)
	}
	if err := s.state.PutAllocation(context.Background(), allocation); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.failBatch = true
	store.mu.Unlock()
	actual := []api.AllocationStatus{{ID: allocation.ID, Generation: 1, Task: "app", Phase: lifecycle.PhaseRunning, Health: lifecycle.HealthHealthy}}
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
	if nodes[node.ID.String()].Status != NodeStatusDraining || !allocations[allocation.ID].Draining || allocations[allocation.ID].DrainSequence != 1 {
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
	if node.Status != NodeStatusHealthy || nodes[node.ID.String()].Status != NodeStatusHealthy {
		t.Fatalf("state after failed drain: memory=%s durable=%s", node.Status, nodes[node.ID.String()].Status)
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
	if node.Host != "new" || node.Status != NodeStatusDraining || nodes[nodeID.String()].Host != "new" || nodes[nodeID.String()].Status != NodeStatusDraining {
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
	if node.Version != "new" || node.Status != NodeStatusDraining || nodes[nodeID.String()].Version != "new" || nodes[nodeID.String()].Status != NodeStatusDraining {
		t.Fatalf("heartbeat/drain result: memory=%#v durable=%#v", node, nodes[nodeID.String()])
	}
}
