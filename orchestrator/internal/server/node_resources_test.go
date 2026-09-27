package server

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestRegisterNodeRejectsAllocatableResourcesAboveCapacity(t *testing.T) {
	s := &Server{
		state: NewStateController(memoryStore{}, "test"),
		nodes: map[uuid.UUID]*Node{},
		now:   time.Now,
	}
	err := s.RegisterNode(context.Background(), &NodeRegistration{
		ID: uuid.New(), CPUCapacity: 1000, CPUAllocatable: 1001,
		MemoryCapacity: 1 << 30, MemoryAllocatable: 1 << 30,
	})
	if err == nil {
		t.Fatal("expected allocatable CPU above capacity to be rejected")
	}
}
