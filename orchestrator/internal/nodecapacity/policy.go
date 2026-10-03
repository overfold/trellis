// Package nodecapacity resolves the portion of a node's physical resources
// that Trellis may schedule onto workloads.
package nodecapacity

import (
	"fmt"
	"sync"
)

const (
	minDefaultCPUReserve      = 100
	maxDefaultCPUReserve      = 1000
	minDefaultMemoryReserve   = int64(256 << 20)
	maxDefaultMemoryReserve   = int64(2 << 30)
	defaultReserveDenominator = 20 // 5%
)

var reserveOverride struct {
	sync.RWMutex
	cpu    *int
	memory *int64
}

// ConfigureReserve overrides Trellis's automatically selected host reserve.
// A nil resource keeps the built-in default for that resource.
func ConfigureReserve(cpu *int, memory *int64) error {
	if cpu != nil && *cpu < 0 {
		return fmt.Errorf("reserved CPU must be non-negative")
	}
	if memory != nil && *memory < 0 {
		return fmt.Errorf("reserved memory must be non-negative")
	}

	reserveOverride.Lock()
	defer reserveOverride.Unlock()
	reserveOverride.cpu = cloneInt(cpu)
	reserveOverride.memory = cloneInt64(memory)
	return nil
}

// Resolve returns the CPU millicores and memory bytes that Trellis may offer
// to the scheduler after keeping host resources in reserve.
func Resolve(cpu int, memory int64) (int, int64, error) {
	if cpu < 0 || memory < 0 {
		return 0, 0, fmt.Errorf("node capacity must be non-negative")
	}

	reservedCPU, reservedMemory := defaultReserve(cpu, memory)
	reserveOverride.RLock()
	if reserveOverride.cpu != nil {
		reservedCPU = *reserveOverride.cpu
	}
	if reserveOverride.memory != nil {
		reservedMemory = *reserveOverride.memory
	}
	reserveOverride.RUnlock()

	if reservedCPU > cpu {
		return 0, 0, fmt.Errorf("reserved CPU %dm exceeds node capacity %dm", reservedCPU, cpu)
	}
	if reservedMemory > memory {
		return 0, 0, fmt.Errorf("reserved memory %d bytes exceeds node capacity %d bytes", reservedMemory, memory)
	}
	return cpu - reservedCPU, memory - reservedMemory, nil
}

func defaultReserve(cpu int, memory int64) (int, int64) {
	reservedCPU := min(max(cpu/defaultReserveDenominator, minDefaultCPUReserve), maxDefaultCPUReserve)
	if cpu <= 0 {
		reservedCPU = 0
	} else if reservedCPU > cpu/2 {
		reservedCPU = cpu / 2
	}

	reservedMemory := min(max(memory/defaultReserveDenominator, minDefaultMemoryReserve), maxDefaultMemoryReserve)
	if memory <= 0 {
		reservedMemory = 0
	} else if reservedMemory > memory/2 {
		reservedMemory = memory / 2
	}

	return reservedCPU, reservedMemory
}

func cloneInt(value *int) *int {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
