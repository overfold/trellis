package lifecycle

import (
	"testing"
	"time"
)

func TestRingBufferEmpty(t *testing.T) {
	var r RingBuffer
	if entries := r.Entries(); entries != nil {
		t.Fatalf("expected nil from empty buffer, got %v", entries)
	}
}

func TestRingBufferOrder(t *testing.T) {
	var r RingBuffer
	phases := []Phase{PhasePlaced, PhaseStarting, PhaseRunning}
	now := time.Now()
	for i, p := range phases {
		r.Append(Event{Phase: p, At: now.Add(time.Duration(i) * time.Second)})
	}
	entries := r.Entries()
	if len(entries) != len(phases) {
		t.Fatalf("expected %d entries, got %d", len(phases), len(entries))
	}
	for i, e := range entries {
		if e.Phase != phases[i] {
			t.Errorf("entry %d: expected phase %s, got %s", i, phases[i], e.Phase)
		}
	}
}

func TestRingBufferWraps(t *testing.T) {
	var r RingBuffer
	now := time.Now()
	// Write more than EventRingSize entries.
	total := EventRingSize + 10
	for i := range total {
		r.Append(Event{Phase: PhaseRunning, Reason: string(rune('a' + i%26)), At: now.Add(time.Duration(i) * time.Second)})
	}
	entries := r.Entries()
	if len(entries) != EventRingSize {
		t.Fatalf("expected %d entries after wrap, got %d", EventRingSize, len(entries))
	}
	// The oldest retained entry should be the (total-EventRingSize)th write.
	firstExpected := string(rune('a' + (total-EventRingSize)%26))
	if entries[0].Reason != firstExpected {
		t.Errorf("oldest entry reason: expected %q, got %q", firstExpected, entries[0].Reason)
	}
	// The newest entry should be the last write.
	lastExpected := string(rune('a' + (total-1)%26))
	if entries[EventRingSize-1].Reason != lastExpected {
		t.Errorf("newest entry reason: expected %q, got %q", lastExpected, entries[EventRingSize-1].Reason)
	}
}

func TestRingBufferCloneIsIndependent(t *testing.T) {
	var original RingBuffer
	original.Append(Event{Phase: PhasePlaced})
	clone := original.Clone()
	clone.Append(Event{Phase: PhaseStarting})
	original.Append(Event{Phase: PhaseFailed})
	if got := original.Entries(); len(got) != 2 || got[1].Phase != PhaseFailed {
		t.Fatalf("original entries = %v", got)
	}
	if got := clone.Entries(); len(got) != 2 || got[1].Phase != PhaseStarting {
		t.Fatalf("clone entries = %v", got)
	}
	if (*RingBuffer)(nil).Clone() != nil {
		t.Fatal("nil buffer cloned to non-nil")
	}
}
