package lifecycle

import (
	"testing"

	"github.com/overfold/trellis/orchestrator/api"
)

func TestTransitions(t *testing.T) {
	phases := []Phase{PhasePending, PhasePlaced, PhaseStarting, PhaseRunning, PhaseStopping, PhaseStopped, PhaseFailed, PhaseLost}
	allowed := map[Phase]map[Phase]bool{
		PhasePending:  {PhasePlaced: true, PhaseStopping: true},
		PhasePlaced:   {PhaseStarting: true, PhaseStopping: true, PhaseFailed: true, PhaseLost: true},
		PhaseStarting: {PhaseRunning: true, PhaseStopping: true, PhaseFailed: true, PhaseLost: true},
		PhaseRunning:  {PhaseStarting: true, PhaseStopping: true, PhaseFailed: true, PhaseLost: true},
		PhaseStopping: {PhaseStopped: true, PhaseFailed: true, PhaseLost: true},
		PhaseStopped:  {PhaseStarting: true},
		PhaseFailed:   {PhaseStarting: true, PhaseStopping: true, PhaseLost: true},
		PhaseLost:     {PhaseStarting: true, PhaseStopping: true, PhaseStopped: true},
	}
	for _, from := range phases {
		for _, to := range phases {
			want := from == to || allowed[from][to]
			if got := Transition(from, to) == nil; got != want {
				t.Errorf("Transition(%q, %q) success = %v, want %v", from, to, got, want)
			}
		}
	}
}

func TestTransitionRejectsUnknownPhases(t *testing.T) {
	for _, test := range []struct{ from, to Phase }{{"", PhasePlaced}, {PhasePlaced, "unknown"}, {"unknown", "unknown"}} {
		if err := Transition(test.from, test.to); err == nil {
			t.Errorf("Transition(%q, %q) unexpectedly succeeded", test.from, test.to)
		}
	}
}

func TestCanObserveKeepsTerminalPhasesAndStopsAuthoritative(t *testing.T) {
	for _, tc := range []struct {
		from, to Phase
		want     bool
	}{
		{PhaseRunning, PhaseFailed, true},
		{PhaseStarting, PhaseFailed, true},
		{PhaseRunning, PhaseStarting, true},
		{PhaseFailed, PhaseStopping, false},
		{PhaseFailed, PhaseStarting, false},
		{PhaseStopping, PhaseFailed, false},
		{PhaseLost, PhaseStarting, false},
		{PhaseLost, PhaseStopping, false},
		{PhaseLost, PhaseStopped, false},
		{PhaseStopped, PhaseStarting, false},
		{PhaseStopping, PhaseStopped, true},
	} {
		if got := CanObserve(tc.from, tc.to); got != tc.want {
			t.Errorf("CanObserve(%s, %s) = %v, want %v", tc.from, tc.to, got, tc.want)
		}
	}
}

// TestStatesMatchPublicAPI keeps the public API's phase and health values
// in step with the lifecycle the control plane reports.
func TestStatesMatchPublicAPI(t *testing.T) {
	phases := map[Phase]api.AllocationPhase{
		PhasePending: api.PhasePending, PhasePlaced: api.PhasePlaced, PhaseStarting: api.PhaseStarting,
		PhaseRunning: api.PhaseRunning, PhaseStopping: api.PhaseStopping, PhaseStopped: api.PhaseStopped,
		PhaseFailed: api.PhaseFailed, PhaseLost: api.PhaseLost,
	}
	if len(phases) != len(transitions) {
		t.Fatalf("public API maps %d phases, lifecycle defines %d", len(phases), len(transitions))
	}
	for phase, public := range phases {
		if !phase.Valid() || string(phase) != string(public) {
			t.Errorf("phase %q is published as %q", phase, public)
		}
	}
	for health, public := range map[Health]api.AllocationHealth{HealthUnknown: api.HealthUnknown, HealthHealthy: api.HealthHealthy, HealthUnhealthy: api.HealthUnhealthy} {
		if !health.Valid() || string(health) != string(public) {
			t.Errorf("health %q is published as %q", health, public)
		}
	}
}
