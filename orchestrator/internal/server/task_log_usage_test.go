package server

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
)

func TestHeartbeatReportsTaskLogUsage(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	registry := prometheus.NewRegistry()
	RegisterMetrics(s, registry)
	node := &Node{ID: uuid.New(), Status: NodeStatusHealthy}
	addTestNode(s, node, s.now())
	logBytes, available, capacity := int64(3<<30), int64(10<<30), int64(50<<30)
	resources := nodeResourceObservation{taskLogUsage: taskLogUsage{
		TaskLogBytes: &logBytes, TaskLogFilesystemAvailable: &available, TaskLogFilesystemCapacity: &capacity,
	}}
	if err := heartbeatAndApply(t, s, node.ID, nil, "test", resources); err != nil {
		t.Fatal(err)
	}

	response := NewHandler(s).convertNode(&NodeView{Node: *node.Clone()})
	if response.TaskLogBytes == nil || *response.TaskLogBytes != logBytes ||
		response.TaskLogFilesystemAvailable == nil || *response.TaskLogFilesystemAvailable != available ||
		response.TaskLogFilesystemCapacity == nil || *response.TaskLogFilesystemCapacity != capacity {
		t.Fatalf("node response task log usage = %v, %v, %v", response.TaskLogBytes, response.TaskLogFilesystemAvailable, response.TaskLogFilesystemCapacity)
	}
	want := map[string]float64{
		"trellis_node_task_log_bytes":                      float64(logBytes),
		"trellis_node_task_log_filesystem_available_bytes": float64(available),
		"trellis_node_task_log_filesystem_capacity_bytes":  float64(capacity),
	}
	for name, value := range want {
		if got, ok := nodeGauge(t, registry, name, node.ID); !ok || got != value {
			t.Fatalf("%s = %v (reported %v), want %v", name, got, ok, value)
		}
	}

	// A heartbeat whose runtime cannot measure log usage clears the stale
	// observation rather than keeping the previous value.
	if err := heartbeatAndApply(t, s, node.ID, nil, "test", nodeResourceObservation{}); err != nil {
		t.Fatal(err)
	}
	if node.TaskLogBytes != nil || node.TaskLogFilesystemAvailable != nil || node.TaskLogFilesystemCapacity != nil {
		t.Fatalf("stale task log usage kept: %+v", node.taskLogUsage)
	}
	for name := range want {
		if _, ok := nodeGauge(t, registry, name, node.ID); ok {
			t.Fatalf("%s reported without an observation", name)
		}
	}
}

func TestHeartbeatRejectsNegativeTaskLogUsage(t *testing.T) {
	s, agent := newTestServerWithAgent()
	defer agent.server.Close()
	node := &Node{ID: uuid.New(), Status: NodeStatusHealthy}
	addTestNode(s, node, time.Time{})
	negative := int64(-1)
	for _, usage := range []taskLogUsage{
		{TaskLogBytes: &negative},
		{TaskLogFilesystemAvailable: &negative},
		{TaskLogFilesystemCapacity: &negative},
	} {
		err := heartbeatAndApply(t, s, node.ID, nil, "test", nodeResourceObservation{taskLogUsage: usage})
		if err == nil || !strings.Contains(err.Error(), "task log") {
			t.Fatalf("negative task log usage %+v: error = %v", usage, err)
		}
	}
}

func nodeGauge(t *testing.T, registry *prometheus.Registry, name string, nodeID uuid.UUID) (float64, bool) {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "node_id" && label.GetValue() == nodeID.String() {
					return metric.GetGauge().GetValue(), true
				}
			}
		}
	}
	return 0, false
}
