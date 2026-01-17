// Package pubsub provides a simple publish/subscribe mechanism for a generic type using a radix tree of topics.
package pubsub

// New creates a new pubsub instance for a generic type.
func New[T any]() Interface[T] {
	return &config[T]{
		publish:     make(chan publish[T]),
		subscribe:   make(chan subscribe[T]),
		unsubscribe: make(chan unsubscribe[T]),
	}
}

// Interface is a simple publish/subscribe mechanism for a generic type using a radix tree of topics.
type Interface[T any] interface {
	// Process starts the pubsub instance.  It will block until the exit channel is closed.
	Process(exit <-chan struct{})

	// Publish sends a value to all subscribers for any published value starting with the given topic.  Any
	// subscribers that are not ready to receive the value will be skipped.
	Publish(v T, topic ...string)

	// Subscribe adds a subscriber channel for any published value starting with the given topic.  Returns
	// DuplicateSubscription if the channel is already subscribed to the topic.
	Subscribe(ch chan<- T, topic ...string) error

	// Unsubscribe removes a subscriber channel for any published value starting with the given topic.  This
	// should match the original channel passed to Subscribe.  Does nothing if there is no subscriber.
	// The channel will be closed when it is unsubscribed.
	Unsubscribe(ch chan<- T, topic ...string)
}

type config[T any] struct {
	publish     chan publish[T]
	subscribe   chan subscribe[T]
	unsubscribe chan unsubscribe[T]
}

func (cfg *config[T]) Process(exit <-chan struct{}) {
	rr := router[T]{
		subscribers: make(map[chan<- T]struct{}),
		topics:      make(map[string]router[T]),
	}
	for {
		select {
		case p := <-cfg.publish:
			rr.publish(p.v, p.topic...)
			close(p.done)
		case s := <-cfg.subscribe:
			err := rr.subscribe(s.ch, s.topic...)
			s.done <- err
		case u := <-cfg.unsubscribe:
			rr.unsubscribe(u.ch, u.topic...)
			close(u.done)
		case <-exit:
			rr.close(make(map[chan<- T]struct{}))
			return
		}
	}
}

func (cfg *config[T]) Publish(v T, topic ...string) {
	done := make(chan struct{})
	cfg.publish <- publish[T]{topic, v, done}
	<-done
}

func (cfg *config[T]) Subscribe(ch chan<- T, topic ...string) error {
	done := make(chan error)
	cfg.subscribe <- subscribe[T]{topic, ch, done}
	return <-done
}

func (cfg *config[T]) Unsubscribe(ch chan<- T, topic ...string) {
	done := make(chan struct{})
	cfg.unsubscribe <- unsubscribe[T]{topic, ch, done}
	<-done
}

type publish[T any] struct {
	topic []string
	v     T
	done  chan struct{}
}

type subscribe[T any] struct {
	topic []string
	ch    chan<- T
	done  chan error
}

type unsubscribe[T any] struct {
	topic []string
	ch    chan<- T
	done  chan struct{}
}

type router[T any] struct {
	subscribers map[chan<- T]struct{}
	topics      map[string]router[T]
}

func (r *router[T]) subscribe(ch chan<- T, topic ...string) error {
	if len(topic) == 0 {
		_, dup := r.subscribers[ch]
		if dup {
			return DuplicateSubscription{}
		}
		r.subscribers[ch] = struct{}{}
		return nil
	}
	r2, ok := r.topics[topic[0]]
	if !ok {
		r2 = router[T]{
			subscribers: make(map[chan<- T]struct{}),
			topics:      make(map[string]router[T]),
		}
		r.topics[topic[0]] = r2
	}
	return r2.subscribe(ch, topic[1:]...)
}

func (r *router[T]) unsubscribe(ch chan<- T, topics ...string) {
	if len(topics) == 0 {
		delete(r.subscribers, ch)
		return
	}
	r2, ok := r.topics[topics[0]]
	if !ok {
		return
	}
	r2.unsubscribe(ch, topics[1:]...)
}

func (r *router[T]) publish(v T, topics ...string) {
	for ch := range r.subscribers {
		select {
		case ch <- v:
		default:
		}
	}
	if len(topics) == 0 {
		return
	}
	r2, ok := r.topics[topics[0]]
	if !ok {
		return
	}
	r2.publish(v, topics[1:]...)
}

func (r *router[T]) close(closed map[chan<- T]struct{}) {
	for ch := range r.subscribers {
		_, dup := closed[ch]
		if dup {
			continue
		}
		closed[ch] = struct{}{}
		close(ch)
	}
	for _, r2 := range r.topics {
		r2.close(closed)
	}
}

// DuplicateSubscription is an error returned when a channel is already subscribed.
type DuplicateSubscription struct{}

// Error implements the error interface.
func (DuplicateSubscription) Error() string {
	return "duplicate subscription"
}
