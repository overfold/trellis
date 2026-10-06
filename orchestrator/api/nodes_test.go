package api

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestNodeResourceSerializationIsIndependentBetweenMessages(t *testing.T) {
	usage := 0.42
	used, available := int64(8<<30), int64(24<<30)
	logBytes := int64(1 << 30)
	at := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	first := NodeResponse{
		ID: uuid.New(), CPU: 7500, Memory: 31 << 30,
		CPUCapacity: 8000, MemoryCapacity: 32 << 30,
		CPUAllocatable: 7500, MemoryAllocatable: 31 << 30,
		CPUUsage: &usage, MemoryUsed: &used, MemoryAvailable: &available, MetricsAt: &at,
		TaskLogBytes: &logBytes, TaskLogFilesystemAvailable: &available, TaskLogFilesystemCapacity: &available,
	}
	if _, err := json.Marshal(first); err != nil {
		t.Fatal(err)
	}

	second := NodeResponse{
		ID: uuid.New(), CPU: 1900, Memory: 7 << 30,
		CPUCapacity: 2000, MemoryCapacity: 8 << 30,
		CPUAllocatable: 1900, MemoryAllocatable: 7 << 30,
	}
	raw, err := json.Marshal(second)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if fields["cpu_capacity"] != float64(2000) || fields["cpu_allocatable"] != float64(1900) {
		t.Fatalf("unexpected second-node CPU fields: %#v", fields)
	}
	for _, field := range []string{"cpu_usage", "memory_used", "memory_available", "metrics_at", "task_log_bytes", "task_log_filesystem_available", "task_log_filesystem_capacity"} {
		if _, ok := fields[field]; ok {
			t.Fatalf("second node inherited %q from first node: %#v", field, fields)
		}
	}
}
