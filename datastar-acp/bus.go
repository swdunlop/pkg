package chat

import (
	"sync"

	"github.com/swdunlop/html-go/datastar"
)

// EventBus carries datastar patches to connected pages.  A datastar page has exactly one SSE feed, so a host
// that already publishes its own patches supplies its bus via Bus; the component then never mounts a feed of its
// own.  When no bus is supplied the component uses NewBus and serves its subscribers under the base path.
type EventBus interface {
	// Publish delivers one pre-rendered datastar event (typically a datastar.Batch) to every connected page.
	// It must not block on slow consumers.
	Publish(datastar.Event)
}

// NewBus returns the default in-process bus: single-topic fan-out where every subscriber gets every publish,
// pre-rendered once by the publisher.  Slow subscribers drop events rather than stalling Publish — single-user
// posture, where the next repaint supersedes anything dropped.
func NewBus() *DefaultBus {
	return &DefaultBus{subs: make(map[*Subscriber]struct{})}
}

// DefaultBus implements EventBus and adds the Subscribe side used by the component's SSE feed.
type DefaultBus struct {
	mu   sync.Mutex
	subs map[*Subscriber]struct{}
}

const subscriberBuffer = 8

// Publish delivers ev to every current subscriber, dropping (never blocking) on a subscriber whose buffer is
// full.
func (b *DefaultBus) Publish(ev datastar.Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for sub := range b.subs {
		select {
		case sub.ch <- ev:
		default: // subscriber too slow; drop rather than block the publisher
		}
	}
}

// Subscribe registers a new subscriber.  Callers must defer Close to unregister, and must Subscribe before
// sending any initial render so events landing mid-render queue rather than vanish.
func (b *DefaultBus) Subscribe() *Subscriber {
	sub := &Subscriber{ch: make(chan datastar.Event, subscriberBuffer), b: b}
	b.mu.Lock()
	b.subs[sub] = struct{}{}
	b.mu.Unlock()
	return sub
}

// Subscriber holds one SSE connection's inbox.
type Subscriber struct {
	ch chan datastar.Event
	b  *DefaultBus
}

// Events returns the channel to receive published events from.
func (s *Subscriber) Events() <-chan datastar.Event { return s.ch }

// Close unregisters the subscriber; safe to call once.
func (s *Subscriber) Close() {
	s.b.mu.Lock()
	delete(s.b.subs, s)
	s.b.mu.Unlock()
}
