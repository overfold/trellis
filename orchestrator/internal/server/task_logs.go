package server

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/clofour/trellis/internal/spec"
)

// ErrTaskSelection indicates that a log request needs a valid task selector.
var ErrTaskSelection = errors.New("invalid task selection")

// AllocationTaskLogsForNamespace opens one task's logs after namespace validation.
func (s *Server) AllocationTaskLogsForNamespace(ctx context.Context, namespace, id, task string, follow bool, tail int) (io.ReadCloser, error) {
	s.mu.RLock()
	var found *Allocation
	for _, allocation := range s.allocations {
		if allocation.ID == id && allocation.Namespace == namespace {
			found = allocation
			break
		}
	}
	if found == nil || found.Node == nil {
		s.mu.RUnlock()
		return nil, fmt.Errorf("allocation not found")
	}
	nodeID := found.Node.ID
	address := fmt.Sprintf("%s:%d", found.Node.Host, found.Node.Port)
	tasks := append([]spec.TaskSpec(nil), found.Tasks...)
	s.mu.RUnlock()

	if task == "" {
		switch len(tasks) {
		case 1:
			task = tasks[0].Name
		case 0:
			// Agent recovery can reconstruct an allocation before its task specs are
			// available locally. Let the agent resolve the only matching task.
		default:
			return nil, fmt.Errorf("%w: allocation %s has multiple tasks; specify task", ErrTaskSelection, id)
		}
	} else if len(tasks) > 0 {
		foundTask := false
		for _, candidate := range tasks {
			if candidate.Name == task {
				foundTask = true
				break
			}
		}
		if !foundTask {
			return nil, fmt.Errorf("%w: allocation %s has no task %q", ErrTaskSelection, id, task)
		}
	}

	return s.client.TaskLogs(ctx, nodeID, address, id, task, follow, tail)
}
