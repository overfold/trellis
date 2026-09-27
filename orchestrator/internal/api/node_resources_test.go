package api

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestNodeRegistrationSerializationIsPure(t *testing.T) {
	id := uuid.New()
	request := NodeRegistrationRequest{
		ID: id, CPU: 7500, Memory: 31 << 30,
		CPUCapacity: 8000, MemoryCapacity: 32 << 30,
		CPUAllocatable: 7500, MemoryAllocatable: 31 << 30,
	}
	first, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("same registration encoded differently:\n%s\n%s", first, second)
	}
	var fields map[string]any
	if err := json.Unmarshal(first, &fields); err != nil {
		t.Fatal(err)
	}
	if fields["cpu"] != float64(7500) || fields["cpu_capacity"] != float64(8000) || fields["cpu_allocatable"] != float64(7500) {
		t.Fatalf("unexpected CPU fields: %#v", fields)
	}
	if fields["memory"] != float64(31<<30) || fields["memory_capacity"] != float64(32<<30) || fields["memory_allocatable"] != float64(31<<30) {
		t.Fatalf("unexpected memory fields: %#v", fields)
	}

	var decoded NodeRegistrationRequest
	if err := json.Unmarshal(first, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, request) {
		t.Fatalf("decoded registration = %#v, want %#v", decoded, request)
	}
}

func TestNodeResourceSerializationIsIndependentBetweenMessages(t *testing.T) {
	usage := 0.42
	used, available := int64(8<<30), int64(24<<30)
	at := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	first := NodeResponse{
		ID: uuid.New(), CPU: 7500, Memory: 31 << 30,
		CPUCapacity: 8000, MemoryCapacity: 32 << 30,
		CPUAllocatable: 7500, MemoryAllocatable: 31 << 30,
		CPUUsage: &usage, MemoryUsed: &used, MemoryAvailable: &available, MetricsAt: &at,
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
	for _, field := range []string{"cpu_usage", "memory_used", "memory_available", "metrics_at"} {
		if _, ok := fields[field]; ok {
			t.Fatalf("second node inherited %q from first node: %#v", field, fields)
		}
	}
}

func TestHeartbeatSerializationDoesNotSampleOrRetainHostMetrics(t *testing.T) {
	usage := 0.17
	withMetrics := HeartbeatRequest{NodeID: uuid.New(), CPUUsage: &usage}
	if _, err := json.Marshal(withMetrics); err != nil {
		t.Fatal(err)
	}

	withoutMetrics := HeartbeatRequest{NodeID: withMetrics.NodeID}
	first, err := json.Marshal(withoutMetrics)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	second, err := json.Marshal(withoutMetrics)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("heartbeat serialization depends on time or environment:\n%s\n%s", first, second)
	}
	var fields map[string]any
	if err := json.Unmarshal(first, &fields); err != nil {
		t.Fatal(err)
	}
	if _, ok := fields["cpu_usage"]; ok {
		t.Fatalf("heartbeat inherited metrics from another message: %#v", fields)
	}
}
