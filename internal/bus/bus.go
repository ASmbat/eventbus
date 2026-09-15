// Package bus implements a small in-process, topic-based event bus: each
// topic has its own bounded queue and worker pool, so one slow topic can't
// starve another, and publishers get an explicit signal (an error, not
// silent loss) when a topic's queue is full.
package bus

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/ASmbat/eventbus/internal/metrics"
)

var (
	ErrNoSubscribers = errors.New("no subscribers for topic")
	ErrQueueFull     = errors.New("topic queue is full")
	// ErrClosed is returned by a PolicyBlock Publish that was unblocked by
	// the topic's subscription context being cancelled (e.g. shutdown)
	// rather than by the event actually being enqueued.
	ErrClosed = errors.New("topic subscription closed")
)

// Policy controls what Publish does when a topic's queue is full.
type Policy int

const (
	// PolicyDrop rejects the event immediately (Publish returns
	// ErrQueueFull) rather than blocking the publisher. Appropriate when
	// producers must never be slowed down by a struggling consumer.
	PolicyDrop Policy = iota
	// PolicyBlock applies backpressure: Publish blocks until the queue
	// has room. Appropriate when losing events is worse than a slow
	// publisher.
	PolicyBlock
)

// Event is a single message published to a topic.
type Event struct {
	Topic     string
	Payload   string
	Timestamp time.Time
}

// Handler processes one event. A non-nil error is counted as a failure but
// does not stop the worker or requeue the event — retry policy is
// intentionally left to the caller (e.g. a handler that publishes to a
// dead-letter topic on failure).
type Handler func(ctx context.Context, e Event) error

type subscription struct {
	ctx     context.Context
	queue   chan Event
	workers int
}

type Bus struct {
	mu      sync.RWMutex
	subs    map[string]*subscription
	metrics *metrics.Metrics
	policy  Policy
}

func New(m *metrics.Metrics, policy Policy) *Bus {
	return &Bus{
		subs:    make(map[string]*subscription),
		metrics: m,
		policy:  policy,
	}
}

// Subscribe registers a handler for topic, backed by `workers` goroutines
// consuming from a queue of size `queueSize`. It must be called before any
// Publish calls for that topic (a Publish to an unknown topic returns
// ErrNoSubscribers rather than silently succeeding). Workers stop when ctx
// is cancelled.
func (b *Bus) Subscribe(ctx context.Context, topic string, workers, queueSize int, handler Handler) {
	sub := &subscription{
		ctx:     ctx,
		queue:   make(chan Event, queueSize),
		workers: workers,
	}

	b.mu.Lock()
	b.subs[topic] = sub
	b.mu.Unlock()

	for i := 0; i < workers; i++ {
		go b.worker(ctx, sub, handler)
	}
}

func (b *Bus) worker(ctx context.Context, sub *subscription, handler Handler) {
	for {
		select {
		case <-ctx.Done():
			return
		case e, ok := <-sub.queue:
			if !ok {
				return
			}
			if err := b.invoke(ctx, handler, e); err != nil {
				b.metrics.IncFailed()
				continue
			}
			b.metrics.IncProcessed()
		}
	}
}

// invoke calls handler for a single event, converting a panic into an error
// instead of letting it unwind past the worker goroutine. An unrecovered
// panic in any goroutine crashes the entire process, so without this a
// single bad event (or a bug triggered only by certain payloads) in one
// handler would take down every topic on the bus, not just the one that
// misbehaved.
func (b *Bus) invoke(ctx context.Context, handler Handler, e Event) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("eventbus: recovered panic in handler for topic %q: %v", e.Topic, r)
			err = fmt.Errorf("handler panicked: %v", r)
		}
	}()
	return handler(ctx, e)
}

// Publish sends an event to topic, honoring the bus's backpressure Policy.
func (b *Bus) Publish(topic, payload string) error {
	b.mu.RLock()
	sub, ok := b.subs[topic]
	b.mu.RUnlock()
	if !ok {
		return ErrNoSubscribers
	}

	event := Event{Topic: topic, Payload: payload, Timestamp: time.Now()}

	switch b.policy {
	case PolicyBlock:
		// Without also selecting on the subscription's context, a publisher
		// that's blocked here when the topic shuts down (workers stop
		// draining the queue) would block forever — a permanent goroutine
		// leak, and in an HTTP handler, a request that never completes.
		select {
		case sub.queue <- event:
			b.metrics.IncPublished()
			return nil
		case <-sub.ctx.Done():
			return ErrClosed
		}
	default: // PolicyDrop
		select {
		case sub.queue <- event:
			b.metrics.IncPublished()
			return nil
		default:
			b.metrics.IncDropped()
			return ErrQueueFull
		}
	}
}
