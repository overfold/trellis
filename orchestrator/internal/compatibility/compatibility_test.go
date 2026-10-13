package compatibility

import "testing"

func TestAdjacentReleaseWindow(t *testing.T) {
	for _, test := range []struct {
		binary, active uint64
		want           bool
	}{
		{0, 0, true}, {0, 1, false}, {1, 0, false}, {2, 0, false},
		{1, 1, true}, {2, 1, true}, {3, 1, false},
		{1, 2, false}, {2, 2, true}, {3, 2, true}, {4, 2, false},
		{2, 3, false}, {3, 3, true}, {4, 3, true},
	} {
		if got := Supports(test.binary, test.active); got != test.want {
			t.Errorf("binary %d at level %d: supported=%v, want %v", test.binary, test.active, got, test.want)
		}
	}
}
