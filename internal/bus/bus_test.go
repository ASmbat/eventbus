package bus

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ASmbat/eventbus/internal/metrics"
)

func TestPublish_NoSubscribers(t *testing.T) {
	b := New(metrics.New(), PolicyDrop)
	if err := b.Publish("nowhere", "x"); !errors.Is(err, ErrNoSubscribers) {
		t.Fatalf("expected ErrNoSubscribers, got %v", err)
	}
}

func TestSubscribe_RejectsDuplicateTopic(t *testing.T) {
	b := New(metrics.New(), PolicyDrop)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	noop := func(context.Context, Event) error { return nil }
	mustSubscribe(t, b, ctx, "orders", 1, 1, noop)

	if err := b.Subscribe(ctx, "orders", 1, 1, noop); !errors.Is(err, ErrAlreadySubscribed) {
		t.Fatalf("expected ErrAlreadySubscribed, got %v", err)
	}
}

func TestSubscribe_RejectsInvalidSizes(t *testing.T) {
	b := New(metrics.New(), PolicyDrop)
	noop := func(context.Context, Event) error { return nil }
	if err := b.Subscribe(context.Background(), "a", 0, 1, noop); err == nil {
		t.Error("expected error for 0 workers")
	}
	if err := b.Subscribe(context.Background(), "b", 1, -1, noop); err == nil {
		t.Error("expected error for negative queue size")
	}
}

func TestPublish_ProcessesEvents(t *testing.T) {
	m := metrics.New()
	b := New(m, PolicyDrop)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var processed int64
	var wg sync.WaitGroup
	wg.Add(5)
	mustSubscribe(t, b, ctx, "orders", 2, 10, func(context.Context, Event) error {
		atomic.AddInt64(&processed, 1)
		wg.Done()
		return nil
	})

	for i := 0; i < 5; i++ {
		if err := b.Publish("orders", "payload"); err != nil {
			t.Fatalf("unexpected publish error: %v", err)
		}
	}
	waitOrTimeout(t, &wg)

	if got := atomic.LoadInt64(&processed); got != 5 {
		t.Errorf("expected 5 processed events, got %d", got)
	}
	if got := m.Snapshot().Published; got != 5 {
		t.Errorf("expected 5 published, got %d", got)
	}
}

func TestPublish_DropPolicy_RejectsWhenFull(t *testing.T) {
	m := metrics.New()
	b := New(m, PolicyDrop)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)
	// One worker, queue of 1: event 1 is in the worker, event 2 fills the
	// queue, event 3 must be dropped.
	mustSubscribe(t, b, ctx, "slow", 1, 1, blockingHandler(started, release))

	if err := b.Publish("slow", "1"); err != nil {
		t.Fatalf("first publish: %v", err)
	}
	<-started // worker is now busy with event 1

	if err := b.Publish("slow", "2"); err != nil {
		t.Fatalf("second publish should fill the queue: %v", err)
	}
	if err := b.Publish("slow", "3"); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}
	if got := m.Snapshot().Dropped; got != 1 {
		t.Errorf("expected 1 dropped event, got %d", got)
	}
}

func TestPublish_FailedHandlerCountsAsFailed(t *testing.T) {
	m := metrics.New()
	b := New(m, PolicyDrop)
	mustSubscribe(t, b, context.Background(), "flaky", 1, 5, func(context.Context, Event) error {
		return errors.New("boom")
	})

	if err := b.Publish("flaky", "x"); err != nil {
		t.Fatalf("unexpected publish error: %v", err)
	}
	closeOrFail(t, b) // waits until the worker has recorded the result

	if got := m.Snapshot().Failed; got != 1 {
		t.Errorf("expected 1 failed event, got %d", got)
	}
}

func TestPublish_HandlerPanicIsRecovered(t *testing.T) {
	m := metrics.New()
	b := New(m, PolicyDrop)
	mustSubscribe(t, b, context.Background(), "flaky", 1, 5, func(_ context.Context, e Event) error {
		if e.Payload == "bad" {
			panic("bad payload")
		}
		return nil
	})

	_ = b.Publish("flaky", "bad")
	_ = b.Publish("flaky", "good")
	closeOrFail(t, b) // an unrecovered panic would crash the test binary instead

	snap := m.Snapshot()
	if snap.Failed != 1 || snap.Processed != 1 {
		t.Errorf("expected 1 failed and 1 processed, got %+v", snap)
	}
}

func TestPublish_BlockPolicy_UnblocksOnContextCancel(t *testing.T) {
	b := New(metrics.New(), PolicyBlock)
	ctx, cancel := context.WithCancel(context.Background())

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)
	mustSubscribe(t, b, ctx, "slow", 1, 0, blockingHandler(started, release))

	if err := b.Publish("slow", "1"); err != nil {
		t.Fatalf("first publish: %v", err)
	}
	<-started

	done := make(chan error, 1)
	go func() { done <- b.Publish("slow", "2") }() // blocks: worker busy, queue size 0
	cancel()

	expectErr(t, done, ErrClosed)
}

func TestClose_DrainsQueuedEvents(t *testing.T) {
	m := metrics.New()
	b := New(m, PolicyDrop)

	var processed int64
	mustSubscribe(t, b, context.Background(), "orders", 2, 100, func(context.Context, Event) error {
		atomic.AddInt64(&processed, 1)
		return nil
	})
	for i := 0; i < 50; i++ {
		if err := b.Publish("orders", "x"); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := b.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := atomic.LoadInt64(&processed); got != 50 {
		t.Errorf("expected all 50 queued events processed before Close returned, got %d", got)
	}
	if err := b.Publish("orders", "late"); !errors.Is(err, ErrClosed) {
		t.Errorf("expected ErrClosed after Close, got %v", err)
	}
	if err := b.Close(ctx); err != nil {
		t.Errorf("second Close should be a no-op, got %v", err)
	}
}

func TestClose_UnblocksBlockedPublisher(t *testing.T) {
	b := New(metrics.New(), PolicyBlock)

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	mustSubscribe(t, b, context.Background(), "slow", 1, 0, blockingHandler(started, release))

	if err := b.Publish("slow", "1"); err != nil {
		t.Fatalf("first publish: %v", err)
	}
	<-started

	done := make(chan error, 1)
	go func() { done <- b.Publish("slow", "2") }()

	closed := make(chan error, 1)
	go func() { closed <- b.Close(context.Background()) }()

	expectErr(t, done, ErrClosed)
	close(release) // let the busy worker finish so Close can return
	expectErr(t, closed, nil)
}

func TestClose_TimesOutOnStuckHandler(t *testing.T) {
	b := New(metrics.New(), PolicyDrop)

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)
	mustSubscribe(t, b, context.Background(), "stuck", 1, 1, blockingHandler(started, release))
	_ = b.Publish("stuck", "x")
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := b.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
}

func TestSubscribe_AfterCloseFails(t *testing.T) {
	b := New(metrics.New(), PolicyDrop)
	_ = b.Close(context.Background())
	err := b.Subscribe(context.Background(), "x", 1, 1, func(context.Context, Event) error { return nil })
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestPublish_BlockedTopicDoesNotFreezeOthers(t *testing.T) {
	b := New(metrics.New(), PolicyBlock)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)
	mustSubscribe(t, b, context.Background(), "slow", 1, 0, blockingHandler(started, release))
	mustSubscribe(t, b, context.Background(), "fast", 1, 10, func(context.Context, Event) error { return nil })

	_ = b.Publish("slow", "1")
	<-started
	go func() { _ = b.Publish("slow", "2") }() // blocks: worker busy, queue size 0
	time.Sleep(20 * time.Millisecond)          // give it time to block (a shorter wait only weakens the test)

	done := make(chan error, 2)
	go func() {
		done <- b.Subscribe(context.Background(), "other", 1, 1, func(context.Context, Event) error { return nil })
		done <- b.Publish("fast", "x")
	}()
	expectErr(t, done, nil) // Subscribe must not wait for the blocked publisher
	expectErr(t, done, nil) // nor must a publish to another topic
}

func TestPublish_AfterSubscriptionCancelledIsRejected(t *testing.T) {
	m := metrics.New()
	b := New(m, PolicyDrop)
	ctx, cancel := context.WithCancel(context.Background())
	mustSubscribe(t, b, ctx, "t", 1, 5, func(context.Context, Event) error { return nil })
	cancel()

	if err := b.Publish("t", "x"); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed (no worker would ever handle it), got %v", err)
	}
	if got := m.Snapshot().Published; got != 0 {
		t.Errorf("rejected event must not count as published, got %d", got)
	}
}

func TestSubscribe_AllowedAgainAfterCancel(t *testing.T) {
	b := New(metrics.New(), PolicyDrop)
	ctx, cancel := context.WithCancel(context.Background())
	noop := func(context.Context, Event) error { return nil }
	mustSubscribe(t, b, ctx, "t", 1, 1, noop)
	cancel()

	mustSubscribe(t, b, context.Background(), "t", 1, 1, noop)
	if err := b.Publish("t", "x"); err != nil {
		t.Fatalf("publish to the new subscription: %v", err)
	}
	closeOrFail(t, b)
}

func TestClose_LeavesNoGoroutines(t *testing.T) {
	before := runtime.NumGoroutine()

	b := New(metrics.New(), PolicyBlock)
	mustSubscribe(t, b, context.Background(), "a", 8, 16, func(context.Context, Event) error { return nil })
	mustSubscribe(t, b, context.Background(), "b", 8, 16, func(context.Context, Event) error { return nil })
	for i := 0; i < 100; i++ {
		_ = b.Publish("a", "x")
		_ = b.Publish("b", "x")
	}
	closeOrFail(t, b)

	waitForGoroutines(t, before)
}

func TestCancel_LeavesNoGoroutines(t *testing.T) {
	before := runtime.NumGoroutine()

	b := New(metrics.New(), PolicyDrop)
	ctx, cancel := context.WithCancel(context.Background())
	mustSubscribe(t, b, ctx, "a", 8, 16, func(context.Context, Event) error { return nil })
	cancel()

	waitForGoroutines(t, before)
}

// waitForGoroutines fails if the goroutine count doesn't return to want.
func waitForGoroutines(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for runtime.NumGoroutine() > want {
		if time.Now().After(deadline) {
			t.Fatalf("goroutine leak: %d running, want %d", runtime.NumGoroutine(), want)
		}
		runtime.Gosched()
		time.Sleep(time.Millisecond)
	}
}

// blockingHandler signals on started when it picks up an event, then waits
// for release. It lets tests control timing without time.Sleep.
func blockingHandler(started chan<- struct{}, release <-chan struct{}) Handler {
	return func(context.Context, Event) error {
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		return nil
	}
}

func mustSubscribe(t *testing.T, b *Bus, ctx context.Context, topic string, workers, size int, h Handler) {
	t.Helper()
	if err := b.Subscribe(ctx, topic, workers, size, h); err != nil {
		t.Fatalf("subscribe %q: %v", topic, err)
	}
}

func closeOrFail(t *testing.T, b *Bus) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := b.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func expectErr(t *testing.T, ch <-chan error, want error) {
	t.Helper()
	select {
	case err := <-ch:
		if !errors.Is(err, want) {
			t.Fatalf("expected %v, got %v", want, err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for result")
	}
}

func waitOrTimeout(t *testing.T, wg *sync.WaitGroup) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for events to be processed")
	}
}
