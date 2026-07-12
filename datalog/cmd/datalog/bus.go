package main

import (
	"sync"

	"github.com/swdunlop/html-go/datastar"
)

// bus is a small in-process pub/sub for Transform-completed notifications
// (doc/notes/datastar.md §8). Single-user, single-topic ("session
// changed"): every subscriber gets every publish, so one channel is enough
// — no per-topic map, unlike the general Bus sketch in §8's Pre-Rendered
// Fan-Out example. Publish pre-renders once (the caller passes a
// datastar.Event built from html.Content, e.g. via datastar.Batch) and
// hands the same value to every subscriber, avoiding N re-renders of the
// same fragment.
type bus struct {
	mu   sync.Mutex
	subs map[*subscriber]struct{}
}

// subscriber holds one /events connection's inbox. The channel is buffered
// so a slow reader doesn't stall Publish; a full buffer means the
// subscriber drops the event rather than blocking the publisher (single-
// user, so a dropped notification just means the next one repaints
// everything current anyway).
type subscriber struct {
	ch chan datastar.Event
	b  *bus
}

const subscriberBuffer = 8

// newBus constructs an empty bus.
func newBus() *bus {
	return &bus{subs: make(map[*subscriber]struct{})}
}

// Subscribe registers a new subscriber. Callers must defer Close to
// unregister and release the channel. Per doc/notes/datastar.md §8's
// subscribe-before-render trap, callers must Subscribe before sending any
// initial render so events landing mid-render are queued rather than lost.
func (b *bus) Subscribe() *subscriber {
	sub := &subscriber{ch: make(chan datastar.Event, subscriberBuffer), b: b}
	b.mu.Lock()
	b.subs[sub] = struct{}{}
	b.mu.Unlock()
	return sub
}

// Events returns the channel to receive published events from.
func (s *subscriber) Events() <-chan datastar.Event { return s.ch }

// Close unregisters the subscriber. Safe to call once; the bus stops
// delivering to it immediately (Publish holds the lock across the whole
// fan-out, so there's no race between Close and an in-flight Publish
// beyond that lock).
func (s *subscriber) Close() {
	s.b.mu.Lock()
	delete(s.b.subs, s)
	s.b.mu.Unlock()
}

// Publish delivers ev to every current subscriber, dropping (never
// blocking) on a subscriber whose buffer is full.
func (b *bus) Publish(ev datastar.Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for sub := range b.subs {
		select {
		case sub.ch <- ev:
		default: // subscriber too slow; drop rather than block the publisher
		}
	}
}
