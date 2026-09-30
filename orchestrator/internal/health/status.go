package health

// HealthStatus describes the observed health of a task.
//
//nolint:revive // The established name emphasizes that this type belongs to health checking.
type HealthStatus string

const (
	// StatusInitializing indicates that a task has not met its health threshold.
	StatusInitializing HealthStatus = "initializing"
	// StatusHealthy indicates that a task met its health threshold.
	StatusHealthy HealthStatus = "healthy"
	// StatusUnhealthy indicates that a task failed its health threshold.
	StatusUnhealthy HealthStatus = "unhealthy"
)

// TaskHealth tracks consecutive health-check results.
type TaskHealth struct {
	Status          HealthStatus
	ConsecutivePass int
	ConsecutiveFail int
	threshold       int
}

// NewTaskHealth creates a tracker with the supplied canonical, positive
// consecutive-result threshold.
func NewTaskHealth(threshold int) *TaskHealth {
	return &TaskHealth{
		Status:          StatusInitializing,
		ConsecutivePass: 0,
		ConsecutiveFail: 0,
		threshold:       threshold,
	}
}

// RecordResult updates health state and reports whether it changed.
func (t *TaskHealth) RecordResult(pass bool) (bool, HealthStatus) {
	if pass {
		t.ConsecutivePass++
		t.ConsecutiveFail = 0
	} else {
		t.ConsecutiveFail++
		t.ConsecutivePass = 0
	}

	switch t.Status {
	case StatusInitializing:
		if t.ConsecutivePass >= t.threshold {
			t.Status = StatusHealthy
			return true, t.Status
		}
		if t.ConsecutiveFail >= t.threshold {
			t.Status = StatusUnhealthy
			return true, t.Status
		}

	case StatusUnhealthy:
		if t.ConsecutivePass >= t.threshold {
			t.Status = StatusHealthy
			return true, t.Status
		}

	case StatusHealthy:
		if t.ConsecutiveFail >= t.threshold {
			t.Status = StatusUnhealthy
			return true, t.Status
		}
	}

	return false, t.Status
}
