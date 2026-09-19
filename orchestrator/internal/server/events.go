package server

import (
	"sync"

	"github.com/clofour/trellis/internal/api"
)

// EventBus distributes cluster events to SSE subscribers.
type EventBus struct {
	mu          sync.Mutex
	subscribers map[chan api.ClusterEvent]string
}

func newEventBus() *EventBus {
	return &EventBus{subscribers: make(map[chan api.ClusterEvent]string)}
}

// subscribe registers a subscriber for namespace. An empty namespace receives
// events for the entire cluster.
func (b *EventBus) subscribe(namespace string) chan api.ClusterEvent {
	ch := make(chan api.ClusterEvent, 64)
	b.mu.Lock()
	b.subscribers[ch] = namespace
	b.mu.Unlock()
	return ch
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
