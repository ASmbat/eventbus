// Package bus implements a small in-process, topic-based event bus.
// Each topic has its own bounded queue and worker pool, so one slow topic
// can't starve another, and publishers get an explicit error (not silent
// loss) when a queue is full.
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
	ErrNoSubscribers     = errors.New("no subscribers for topic")
	ErrQueueFull         = errors.New("topic queue is full")
	ErrClosed            = errors.New("bus or subscription closed")
	ErrAlreadySubscribed = errors.New("topic already has a subscriber")
)

// Policy controls what Publish does when a topic's queue is full.
type Policy int

const (
	// PolicyDrop rejects the event right away with ErrQueueFull.
	PolicyDrop Policy = iota
	// PolicyBlock waits until the queue has room (backpressure).
	PolicyBlock
)

// Event is a single message published to a topic.
type Event struct {
	Topic     string
	Payload   string
	Timestamp time.Time
}

// Handler processes one event. A returned error is counted as a failure;
// the event is not retried. Retry or dead-letter logic belongs to the handler.
type Handler func(ctx context.Context, e Event) error

type subscription struct {
	ctx   context.Context
	queue chan Event
}

type Bus struct {
	mu     sync.RWMutex // guards subs and closed; never held while sending
	subs   map[string]*subscription
	closed bool

	closing  chan struct{}  // closed when Close starts; releases blocked publishers
	inflight sync.WaitGroup // publishers that may still send to a queue
	workers  sync.WaitGroup
	once     sync.Once

	metrics *metrics.Metrics
	policy  Policy
}

func New(m *metrics.Metrics, policy Policy) *Bus {
	return &Bus{
		subs:    make(map[string]*subscription),
		closing: make(chan struct{}),
		metrics: m,
		policy:  policy,
	}
}

// Subscribe registers handler for topic, with `workers` goroutines reading
// from a queue of size queueSize. A topic has one active subscription; it
// can be subscribed again after the previous subscription's ctx is cancelled.
//
// Cancelling ctx stops the workers at once, and events still in the queue
// are dropped. Use Close for a graceful stop that processes them first.
func (b *Bus) Subscribe(ctx context.Context, topic string, workers, queueSize int, handler Handler) error {
	if workers < 1 {
		return fmt.Errorf("workers must be >= 1, got %d", workers)
	}
	if queueSize < 0 {
		return fmt.Errorf("queueSize must be >= 0, got %d", queueSize)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return ErrClosed
	}
	if old, exists := b.subs[topic]; exists && old.ctx.Err() == nil {
		return fmt.Errorf("%w: %q", ErrAlreadySubscribed, topic)
	}

	sub := &subscription{ctx: ctx, queue: make(chan Event, queueSize)}
	b.subs[topic] = sub

	b.workers.Add(workers)
	for i := 0; i < workers; i++ {
		go b.worker(sub, handler)
	}
	return nil
}

func (b *Bus) worker(sub *subscription, handler Handler) {
	defer b.workers.Done()
	for {
		select {
		case <-sub.ctx.Done():
			return
		case e, ok := <-sub.queue:
			if !ok {
				return // queue closed by Close and fully drained
			}
			if err := b.invoke(sub.ctx, handler, e); err != nil {
				b.metrics.IncFailed()
				continue
			}
			b.metrics.IncProcessed()
		}
	}
}

// invoke runs the handler and turns a panic into an error, so one bad
// event can't crash the whole process.
func (b *Bus) invoke(ctx context.Context, handler Handler, e Event) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("eventbus: recovered panic in handler for topic %q: %v", e.Topic, r)
			err = fmt.Errorf("handler panicked: %v", r)
		}
	}()
	return handler(ctx, e)
}

// Publish sends an event to topic, following the bus's Policy.
func (b *Bus) Publish(topic, payload string) error {
	sub, err := b.acquire(topic)
	if err != nil {
		return err
	}
	defer b.inflight.Done()

	event := Event{Topic: topic, Payload: payload, Timestamp: time.Now()}

	if b.policy == PolicyBlock {
		select {
		case sub.queue <- event:
			b.metrics.IncPublished()
			return nil
		case <-sub.ctx.Done():
			return ErrClosed
		case <-b.closing:
			return ErrClosed
		}
	}

	select {
	case sub.queue <- event:
		b.metrics.IncPublished()
		return nil
	default:
		b.metrics.IncDropped()
		return ErrQueueFull
	}
}

// acquire looks up the topic and registers the caller as an in-flight
// publisher. The lock is held only for the lookup, not while sending, so
// a publisher blocked on one topic never holds up other topics or
// Subscribe. The caller must call b.inflight.Done when it has finished.
func (b *Bus) acquire(topic string) (*subscription, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.closed {
		return nil, ErrClosed
	}
	sub, ok := b.subs[topic]
	if !ok {
		return nil, ErrNoSubscribers
	}
	if sub.ctx.Err() != nil {
		return nil, ErrClosed // workers are gone; the event would never be handled
	}
	b.inflight.Add(1)
	return sub, nil
}

// Close stops accepting new events, lets workers finish everything already
// queued, and waits for them until ctx is done. It is safe to call more
// than once. If a handler never returns, Close returns ctx's error and
// that worker keeps running.
func (b *Bus) Close(ctx context.Context) error {
	b.once.Do(func() {
		b.mu.Lock()
		b.closed = true // from now on acquire fails, so inflight can only go down
		b.mu.Unlock()

		close(b.closing)  // release publishers blocked under PolicyBlock
		b.inflight.Wait() // no goroutine is sending after this

		b.mu.RLock()
		for _, sub := range b.subs {
			close(sub.queue) // safe: no senders left; workers drain, then exit
		}
		b.mu.RUnlock()
	})

	done := make(chan struct{})
	go func() {
		b.workers.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("waiting for workers: %w", ctx.Err())
	}
}
