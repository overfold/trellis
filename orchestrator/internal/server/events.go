package server

import (
	"sync"

	"github.com/overfold/trellis/internal/api"
)

const defaultMaxEventSubscribers = 256

// EventBus distributes cluster events to SSE subscribers.
type EventBus struct {
	mu          sync.Mutex
	subscribers map[chan api.ClusterEvent]string
	limit       int
}

func newEventBus() *EventBus {
	return newEventBusWithLimit(defaultMaxEventSubscribers)
}

func newEventBusWithLimit(limit int) *EventBus {
	return &EventBus{subscribers: make(map[chan api.ClusterEvent]string), limit: limit}
}

// subscribe registers a subscriber for namespace. An empty namespace receives
// events for the entire cluster.
func (b *EventBus) subscribe(namespace string) (chan api.ClusterEvent, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.subscribers) >= b.limit {
		return nil, false
	}
	ch := make(chan api.ClusterEvent, 64)
	b.subscribers[ch] = namespace
	return ch, true
}

func (b *EventBus) unsubscribe(ch chan api.ClusterEvent) {
	b.mu.Lock()
	delete(b.subscribers, ch)
	b.mu.Unlock()
}

func (b *EventBus) publish(event api.ClusterEvent) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch, namespace := range b.subscribers {
		if namespace != "" && event.Namespace != namespace {
			continue
		}
		select {
		case ch <- event:
		default:
		}
	}
}
