// Package lifecycle defines allocation execution and health states.
package lifecycle

import (
	"sync"
	"time"
)

// EventRingSize is the number of lifecycle events retained per allocation.
const EventRingSize = 64

// Event records a single phase transition.
type Event struct {
	Phase   Phase     `json:"phase"`
	Reason  string    `json:"reason,omitempty"`
	Message string    `json:"message,omitempty"`
	At      time.Time `json:"at"`
}

// RingBuffer is a fixed-size in-memory circular buffer of lifecycle events.
// It is not persisted and is scoped to a single allocation's lifetime in the
// control plane's memory.
type RingBuffer struct {
	mu      sync.Mutex
	entries [EventRingSize]Event
	head    int
	count   int
}

// Append records a new event, evicting the oldest entry when full.
func (r *RingBuffer) Append(e Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries[r.head] = e
	r.head = (r.head + 1) % EventRingSize
	if r.count < EventRingSize {
		r.count++
	}
}

// Clone returns an independent copy of the buffer. A nil buffer clones to nil.
func (r *RingBuffer) Clone() *RingBuffer {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return &RingBuffer{entries: r.entries, head: r.head, count: r.count}
}

// Entries returns a snapshot of recorded events in chronological order.
func (r *RingBuffer) Entries() []Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.count == 0 {
		return nil
	}
	out := make([]Event, r.count)
	start := (r.head - r.count + EventRingSize) % EventRingSize
	for i := 0; i < r.count; i++ {
		out[i] = r.entries[(start+i)%EventRingSize]
	}
	return out
}
