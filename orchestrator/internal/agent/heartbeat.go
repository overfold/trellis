package agent

import (
	"context"
	"time"

	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/internal/client"
	"github.com/overfold/trellis/orchestrator/internal/lifecycle"
	"github.com/overfold/trellis/orchestrator/internal/nodeapi"
	"github.com/overfold/trellis/orchestrator/internal/nodecapacity"
)

const heartbeatInterval = 10 * time.Second

func (a *Agent) runHeartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	registered := false

	for {
		if !registered {
			if a.server.Ready() && a.ready() {
				a.nodeInfo.Volumes = a.volumes.AvailableHostVolumes()
				if _, err := a.server.RegisterNode(ctx, &a.nodeInfo); err != nil {
					a.log.Error("register node failed", "error", err)
				} else {
					registered = true
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !a.server.Ready() || !a.ready() {
				continue
			}
			if !registered {
				continue
			}
			heartbeat := &client.Heartbeat{
				NodeID:            a.nodeID,
				Timestamp:         time.Now(),
				Allocations:       a.allocationStatuses(),
				Volumes:           a.volumes.AvailableHostVolumes(),
				Capabilities:      a.nodeInfo.Capabilities,
				Version:           a.version,
				CPUCapacity:       a.nodeInfo.CPUCapacity,
				MemoryCapacity:    a.nodeInfo.MemoryCapacity,
				CPUAllocatable:    a.nodeInfo.CPUAllocatable,
				MemoryAllocatable: a.nodeInfo.MemoryAllocatable,
			}
			if a.raftAppliedIndex != nil {
				heartbeat.RaftAppliedIndex = a.raftAppliedIndex()
			}
			if metrics, ok := nodecapacity.SampleHostMetrics(); ok {
				if metrics.CPUValid {
					heartbeat.CPUUsage = &metrics.CPUUsage
				}
				if metrics.MemoryValid {
					heartbeat.MemoryUsed = &metrics.MemoryUsed
					heartbeat.MemoryAvailable = &metrics.MemoryAvailable
				}
				heartbeat.MetricsAt = &metrics.CollectedAt
			}
			a.addTaskLogUsage(heartbeat)
			err := a.server.SendHeartbeat(ctx, a.nodeID, heartbeat)
			if err != nil {
				a.log.Error("send heartbeat failed", "error", err)
				registered = false
			}
		}
	}
}

func (a *Agent) allocationStatuses() []nodeapi.AllocationStatus {
	a.mu.RLock()
	defer a.mu.RUnlock()
	actual := make([]nodeapi.AllocationStatus, 0, len(a.allocations)+len(a.retainedLogs))
	for _, alloc := range a.allocations {
		ports := make([]api.PortMapping, 0, len(alloc.Ports))
		for _, p := range alloc.Ports {
			ports = append(ports, api.PortMapping{HostPort: p.HostPort, ContainerPort: p.ContainerPort})
		}
		var reason nodeapi.OperationCode
		if alloc.Status == "failed" && alloc.RestartExhausted {
			reason = nodeapi.OperationRestartExhausted
		}
		actual = append(actual, nodeapi.AllocationStatus{ID: alloc.AllocationID, Generation: alloc.Generation, Task: alloc.TaskName, Address: allocationNetworkAddress(alloc), Phase: lifecycle.Phase(alloc.Status), Health: lifecycle.Health(reportedHealth(alloc)), Reason: reason, Ports: ports})
	}
	for _, record := range a.retainedLogs {
		if a.allocations[record.ContainerID] != nil {
			continue
		}
		actual = append(actual, nodeapi.AllocationStatus{ID: record.AllocationID, Generation: record.Generation, Task: record.TaskName, RetainedLogs: true, Phase: lifecycle.PhaseStopped, Health: lifecycle.HealthUnknown})
	}
	return append(actual, a.startingStatusesLocked()...)
}
