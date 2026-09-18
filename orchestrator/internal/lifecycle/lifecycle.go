package lifecycle

import (
	"fmt"
	"time"
)

// Phase describes allocation execution. Health is intentionally orthogonal.
type Phase string

const (
	// PhasePending describes an allocation waiting for a compatible node.
	PhasePending Phase = "pending"
	// PhasePlaced describes an allocation accepted for placement.
	PhasePlaced Phase = "placed"
	// PhaseStarting and the following values describe subsequent execution states.
	PhaseStarting Phase = "starting"
	// PhaseRunning indicates that the allocation is running.
	PhaseRunning Phase = "running"
	// PhaseStopping indicates that the allocation is stopping.
	PhaseStopping Phase = "stopping"
	// PhaseStopped indicates that the allocation has stopped.
	PhaseStopped Phase = "stopped"
	// PhaseFailed indicates that the allocation failed.
	PhaseFailed Phase = "failed"
	// PhaseLost indicates that the allocation was lost.
	PhaseLost Phase = "lost"
)

// Health describes the health-check state of an allocation.
type Health string

const (
	// HealthUnknown indicates that health is not yet known.
	HealthUnknown Health = "unknown"
	// HealthHealthy and HealthUnhealthy describe completed health checks.
	HealthHealthy Health = "healthy"
	// HealthUnhealthy indicates that a health check failed.
	HealthUnhealthy Health = "unhealthy"
)

var transitions = map[Phase]map[Phase]bool{
	PhasePending:  {PhasePlaced: true, PhaseStopping: true},
	PhasePlaced:   {PhaseStarting: true, PhaseStopping: true, PhaseFailed: true, PhaseLost: true},
	PhaseStarting: {PhaseRunning: true, PhaseStopping: true, PhaseFailed: true, PhaseLost: true},
	PhaseRunning:  {PhaseStopping: true, PhaseFailed: true, PhaseLost: true},
	PhaseStopping: {PhaseStopped: true, PhaseFailed: true, PhaseLost: true},
	PhaseStopped:  {PhaseStarting: true},
	PhaseFailed:   {PhaseStarting: true, PhaseStopping: true, PhaseLost: true},
	PhaseLost:     {PhaseStarting: true, PhaseStopping: true, PhaseStopped: true},
}

// Valid reports whether p is a recognized phase.
func (p Phase) Valid() bool {
	_, ok := transitions[p]
	return ok
}

// Valid reports whether h is a recognized health state.
func (h Health) Valid() bool {
	return h == HealthUnknown || h == HealthHealthy || h == HealthUnhealthy
}

// CanTransition reports whether an allocation can move between two phases.
func CanTransition(from, to Phase) bool {
	return from == to || transitions[from][to]
}

// Transition validates an allocation phase change.
func Transition(from, to Phase) error {
	if !from.Valid() || !to.Valid() {
		return fmt.Errorf("unknown allocation phase transition %q -> %q", from, to)
	}
	if !CanTransition(from, to) {
		return fmt.Errorf("invalid allocation phase transition %q -> %q", from, to)
	}
	return nil
}

// Diagnostic records details about the latest allocation transition.
type Diagnostic struct {
	CreatedAt      time.Time  `json:"created_at"`
	TransitionedAt time.Time  `json:"last_transition_at"`
	Reason         string     `json:"reason,omitempty"`
	Message        string     `json:"message,omitempty"`
	Attempt        int        `json:"attempt"`
	NextRetryAt    *time.Time `json:"next_retry_at,omitempty"`
}
