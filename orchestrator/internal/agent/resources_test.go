package agent

import (
	"testing"

	"github.com/clofour/trellis/internal/nodecapacity"
)

func TestSetResourcesOwnsCapacityAndAllocatableValues(t *testing.T) {
	reservedCPU := 500
	reservedMemory := int64(1 << 30)
	t.Cleanup(func() { _ = nodecapacity.ConfigureReserve(nil, nil) })
	if err := nodecapacity.ConfigureReserve(&reservedCPU, &reservedMemory); err != nil {
		t.Fatal(err)
	}

	agent := &Agent{}
	if err := agent.SetResources(8000, 32<<30, "linux", "amd64"); err != nil {
		t.Fatal(err)
	}
	if agent.nodeInfo.CPUCapacity != 8000 || agent.nodeInfo.MemoryCapacity != 32<<30 {
		t.Fatalf("capacity = %dm/%d", agent.nodeInfo.CPUCapacity, agent.nodeInfo.MemoryCapacity)
	}
	if agent.nodeInfo.CPUAllocatable != 7500 || agent.nodeInfo.MemoryAllocatable != 31<<30 {
		t.Fatalf("allocatable = %dm/%d", agent.nodeInfo.CPUAllocatable, agent.nodeInfo.MemoryAllocatable)
	}
}
