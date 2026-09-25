package health

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/clofour/trellis/internal/runtime"
	"github.com/clofour/trellis/internal/spec"
)

const (
	defaultCheckInterval  = 10 * time.Second
	defaultCheckTimeout   = 5 * time.Second
	defaultCheckThreshold = 3
)

// HealthSubscriber receives allocation health changes.
//
//nolint:revive // The established name emphasizes that this type belongs to health checking.
type HealthSubscriber interface {
	OnHealthy(ctx context.Context, allocID string) error
	OnUnhealthy(ctx context.Context, allocID string) error
}

// HealthConfig tracks the health-check configuration for an allocation.
//
//nolint:revive // The established name emphasizes that this type belongs to health checking.
type HealthConfig struct {
	Type      string
	Port      int
	Path      string
	Command   []string
	Interval  time.Duration
	Timeout   time.Duration
	Threshold int
}

type trackedTask struct {
	allocID     string
	containerID string

	config HealthConfig
	health *TaskHealth
	cancel context.CancelFunc
}

// HealthManager schedules health checks and publishes status changes.
//
//nolint:revive // The established name emphasizes that this type belongs to health checking.
type HealthManager struct {
	log        *slog.Logger
	runtime    runtime.ContainerRuntime
	Subscriber HealthSubscriber

	mu    sync.Mutex
	tasks map[string]*trackedTask
	ctx   context.Context
}

// NewHealthManager creates a health-check manager.
func NewHealthManager(log *slog.Logger, runtime runtime.ContainerRuntime, subscriber HealthSubscriber) *HealthManager {
	return &HealthManager{
		log:        log,
		runtime:    runtime,
		tasks:      make(map[string]*trackedTask),
		Subscriber: subscriber,
		ctx:        context.Background(),
	}
}

// SetContext replaces the context used for health-check workers.
func (h *HealthManager) SetContext(ctx context.Context) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ctx = ctx
}

// RegisterTask starts health checking an allocation task.
func (h *HealthManager) RegisterTask(allocID string, containerID string, spec *spec.HealthCheckSpec) {
	h.mu.Lock()
	defer h.mu.Unlock()

	ctx, cancel := context.WithCancel(h.ctx)

	existingTrackedTask, ok := h.tasks[allocID]
	if ok {
		existingTrackedTask.cancel()
		delete(h.tasks, allocID)
	}

	config := newHealthConfig(spec)

	newTrackedTask := &trackedTask{
		allocID:     allocID,
		containerID: containerID,
		config:      config,
		health:      NewTaskHealth(config.Threshold),
		cancel:      cancel,
	}
	h.tasks[allocID] = newTrackedTask

	go h.runHealthCheckLoop(ctx, newTrackedTask)
}

func newHealthConfig(spec *spec.HealthCheckSpec) HealthConfig {
	config := HealthConfig{
		Type:      string(spec.Type),
		Port:      spec.Port,
		Path:      spec.Path,
		Command:   spec.Command,
		Interval:  defaultCheckInterval,
		Timeout:   defaultCheckTimeout,
		Threshold: defaultCheckThreshold,
	}
	if spec.Interval != 0 {
		config.Interval = spec.Interval
	}
	if spec.Timeout != 0 {
		config.Timeout = spec.Timeout
	}
	if spec.Threshold != 0 {
		config.Threshold = spec.Threshold
	}
	return config
}

// DeregisterTask stops health checking an allocation.
func (h *HealthManager) DeregisterTask(allocID string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	trackedTask, ok := h.tasks[allocID]
	if ok {
		trackedTask.cancel()
		delete(h.tasks, allocID)
	}
}

func (h *HealthManager) runHealthCheckLoop(ctx context.Context, trackedTask *trackedTask) {
	ticker := time.NewTicker(trackedTask.config.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			result, err := h.runHealthCheck(ctx, trackedTask)
			if err != nil {
				h.log.Error("health check failed", "error", err)
				result = false
			}

			h.mu.Lock()
			change, status := trackedTask.health.RecordResult(result)
			h.mu.Unlock()

			if change {
				var err error
				switch status {
				case StatusHealthy:
					err = h.Subscriber.OnHealthy(ctx, trackedTask.allocID)
				case StatusUnhealthy:
					err = h.Subscriber.OnUnhealthy(ctx, trackedTask.allocID)
				}
				if err != nil {
					h.log.Error("health status callback failed", "status", status, "error", err)
				}
			}
		}
	}
}

func (h *HealthManager) runHealthCheck(ctx context.Context, trackedTask *trackedTask) (bool, error) {
	config := trackedTask.config

	ctx, cancel := context.WithTimeout(ctx, config.Timeout)
	defer cancel()

	switch config.Type {
	case "http":
		return CheckHTTP(ctx, h.runtime, trackedTask.containerID, config.Port, config.Path, config.Timeout)
	case "tcp":
		return CheckTCP(ctx, h.runtime, trackedTask.containerID, config.Port, config.Timeout)
	case "script":
		return CheckScript(ctx, h.runtime, trackedTask.containerID, config.Command)
	default:
		return false, fmt.Errorf("unknown check type %s", config.Type)
	}
}
