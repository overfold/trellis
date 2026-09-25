package lifecycle

import "testing"

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
