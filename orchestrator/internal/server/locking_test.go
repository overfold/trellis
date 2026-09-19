package server

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/clofour/trellis/internal/lifecycle"
	"github.com/clofour/trellis/internal/spec"
	"github.com/google/uuid"
)

func TestExecuteAcquiresServerLockBeforeAllocationLock(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()

	node := &Node{
		ID:            uuid.New(),
		Host:          agent.host,
		Port:          agent.port,
		Status:        NodeStatusHealthy,
		LastHeartbeat: s.now(),
	}
	s.nodes[node.ID] = node

	jobSpec := &spec.JobSpec{
		Namespace: "default",
		Name:      "web",
		TaskGroups: []spec.TaskGroupSpec{{
			Name:  "api",
			Count: 1,
			Tasks: []spec.TaskSpec{{Name: "server", Image: "app:v1"}},
		}},
	}
	s.jobs[jobKey("default", "web")] = &Job{Spec: jobSpec, Revision: 1}

	alloc := &Allocation{
		ID:            "default-web-api-deadbeef",
		Namespace:     "default",
		JobName:       "web",
		TaskGroupName: "api",
		Tasks:         jobSpec.TaskGroups[0].Tasks,
		Node:          node,
		Generation:    1,
		JobRevision:   1,
		Phase:         lifecycle.PhasePlaced,
		Health:        lifecycle.HealthUnknown,
		Diagnostic: lifecycle.Diagnostic{
			CreatedAt:      s.now(),
			TransitionedAt: s.now(),
		},
	}

	// Block the server read lock. Execute must wait here before taking
	// allocation.mu; taking allocation.mu first recreates the production
	// deadlock with metrics/listing readers and a queued heartbeat writer.
	s.mu.Lock()
	serverLocked := true
	defer func() {
		if serverLocked {
			s.mu.Unlock()
		}
	}()

	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		done <- s.Execute(context.Background(), &Action{Type: ActionStart, Allocation: alloc})
	}()
	<-started

	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		runtime.Gosched()
		if !alloc.mu.TryLock() {
			s.mu.Unlock()
			serverLocked = false
			select {
			case <-done:
			case <-time.After(time.Second):
			}
			t.Fatal("Execute acquired allocation.mu while waiting for server mu")
		}
		alloc.mu.Unlock()
	}

	s.mu.Unlock()
	serverLocked = false

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Execute failed after server lock was released: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Execute did not complete after server lock was released")
	}
}
