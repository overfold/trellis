package agent

import (
	"errors"
	"testing"

	"github.com/overfold/trellis/orchestrator/internal/client"
	"github.com/overfold/trellis/orchestrator/internal/runtime"
)

type logUsageRuntime struct {
	*listingRecoveryRuntime
	usage runtime.LogUsage
	err   error
}

func (r *logUsageRuntime) LogUsage() (runtime.LogUsage, error) { return r.usage, r.err }

func TestHeartbeatReportsTaskLogUsage(t *testing.T) {
	base := func() *listingRecoveryRuntime {
		return &listingRecoveryRuntime{reconcilerRuntime: &reconcilerRuntime{}}
	}
	usage := runtime.LogUsage{Bytes: 512, FilesystemAvailable: 4096, FilesystemCapacity: 8192}
	agent, _ := newRecoveryTestAgent(t, &logUsageRuntime{listingRecoveryRuntime: base(), usage: usage})
	heartbeat := &client.Heartbeat{}
	agent.addTaskLogUsage(heartbeat)
	if heartbeat.TaskLogBytes == nil || *heartbeat.TaskLogBytes != 512 ||
		heartbeat.TaskLogFilesystemAvailable == nil || *heartbeat.TaskLogFilesystemAvailable != 4096 ||
		heartbeat.TaskLogFilesystemCapacity == nil || *heartbeat.TaskLogFilesystemCapacity != 8192 {
		t.Fatalf("heartbeat task log usage = %v, %v, %v", heartbeat.TaskLogBytes, heartbeat.TaskLogFilesystemAvailable, heartbeat.TaskLogFilesystemCapacity)
	}

	for name, rt := range map[string]runtime.ContainerRuntime{
		"measurement fails":      &logUsageRuntime{listingRecoveryRuntime: base(), usage: usage, err: errors.New("statfs failed")},
		"runtime cannot measure": base(),
	} {
		agent, _ := newRecoveryTestAgent(t, rt)
		heartbeat := &client.Heartbeat{}
		agent.addTaskLogUsage(heartbeat)
		if heartbeat.TaskLogBytes != nil || heartbeat.TaskLogFilesystemAvailable != nil || heartbeat.TaskLogFilesystemCapacity != nil {
			t.Fatalf("%s: heartbeat reported task log usage %+v", name, heartbeat)
		}
	}
}
