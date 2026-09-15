package bus

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ASmbat/eventbus/internal/metrics"
)

func TestPublish_NoSubscribers(t *testing.T) {
	b := New(metrics.New(), PolicyDrop)
	if err := b.Publish("nowhere", "x"); err != ErrNoSubscribers {
		t.Fatalf("expected ErrNoSubscribers, got %v", err)
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
	b.Subscribe(ctx, "orders", 2, 10, func(_ context.Context, e Event) error {
		atomic.AddInt64(&processed, 1)
		wg.Done()
		return nil
	})

	for i := 0; i < 5; i++ {
		if err := b.Publish("orders", "payload"); err != nil {
			t.Fatalf("unexpected publish error: %v", err)
		}
	}

	waitOrTimeout(t, &wg, time.Second)

	if got := atomic.LoadInt64(&processed); got != 5 {
		t.Errorf("expected 5 processed events, got %d", got)
	}
	if m.Snapshot().Published != 5 {
		t.Errorf("expected 5 published, got %d", m.Snapshot().Published)
	}
}

func TestPublish_DropPolicy_RejectsWhenFull(t *testing.T) {
	m := metrics.New()
	b := New(m, PolicyDrop)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	block := make(chan struct{})
	// Single slow worker, tiny queue: first event occupies the worker,
	// second fills the queue, third should be dropped.
	b.Subscribe(ctx, "slow", 1, 1, func(_ context.Context, e Event) error {
		<-block
		return nil
	})
	defer close(block)

	if err := b.Publish("slow", "1"); err != nil {
		t.Fatalf("unexpected error on first publish: %v", err)
	}
	// Give the worker a moment to pick up event 1.
	time.Sleep(20 * time.Millisecond)

	if err := b.Publish("slow", "2"); err != nil {
		t.Fatalf("unexpected error on second publish (should fill queue): %v", err)
	}

	if err := b.Publish("slow", "3"); err != ErrQueueFull {
		t.Fatalf("expected ErrQueueFull on third publish, got %v", err)
	}
	if m.Snapshot().Dropped != 1 {
		t.Errorf("expected 1 dropped event, got %d", m.Snapshot().Dropped)
	}
}

func TestPublish_FailedHandlerCountsAsFailed(t *testing.T) {
	m := metrics.New()
	b := New(m, PolicyDrop)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	b.Subscribe(ctx, "flaky", 1, 5, func(_ context.Context, e Event) error {
		defer wg.Done()
		return errBoom
	})

	if err := b.Publish("flaky", "x"); err != nil {
		t.Fatalf("unexpected publish error: %v", err)
	}
	waitOrTimeout(t, &wg, time.Second)

	if m.Snapshot().Failed != 1 {
		t.Errorf("expected 1 failed event, got %d", m.Snapshot().Failed)
	}
}

func TestPublish_HandlerPanicIsRecovered(t *testing.T) {
	m := metrics.New()
	b := New(m, PolicyDrop)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)
	b.Subscribe(ctx, "flaky", 1, 5, func(_ context.Context, e Event) error {
		defer wg.Done()
		if e.Payload == "bad" {
			var arr []int
			_ = arr[5] // panics
		}
		return nil
	})

	if err := b.Publish("flaky", "bad"); err != nil {
		t.Fatalf("unexpected publish error: %v", err)
	}
	if err := b.Publish("flaky", "good"); err != nil {
		t.Fatalf("unexpected publish error: %v", err)
	}

	// If the panic isn't recovered, this test process crashes outright
	// rather than failing normally — so getting here at all is part of
	// what's being verified.
	waitOrTimeout(t, &wg, time.Second)

	snap := m.Snapshot()
	if snap.Failed != 1 {
		t.Errorf("expected panicking event to count as 1 failed, got %d", snap.Failed)
	}
	if snap.Processed != 1 {
		t.Errorf("expected the second (good) event to still be processed by the same worker, got %d", snap.Processed)
	}
}

func TestPublish_BlockPolicy_UnblocksOnContextCancel(t *testing.T) {
	m := metrics.New()
	b := New(m, PolicyBlock)
	ctx, cancel := context.WithCancel(context.Background())

	block := make(chan struct{})
	// Single slow worker, zero-size queue: the first publish occupies the
	// worker, so a second blocking publish has nowhere to go until the
	// worker frees up or the subscription's context is cancelled.
	b.Subscribe(ctx, "slow", 1, 0, func(_ context.Context, e Event) error {
		<-block
		return nil
	})
	defer close(block)

	if err := b.Publish("slow", "1"); err != nil {
		t.Fatalf("unexpected error on first publish: %v", err)
	}
	time.Sleep(20 * time.Millisecond) // let the worker pick up event 1

	done := make(chan error, 1)
	go func() { done <- b.Publish("slow", "2") }()

	time.Sleep(20 * time.Millisecond) // let the second publish start blocking
	cancel()                          // simulate shutdown while a publisher is blocked

	select {
	case err := <-done:
		if err != ErrClosed {
			t.Fatalf("expected ErrClosed once the subscription context is cancelled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Publish under PolicyBlock did not unblock when its context was cancelled")
	}
}

var errBoom = &testError{"boom"}

type testError struct{ msg string }

func (e *testError) Error() string { return e.msg }

func waitOrTimeout(t *testing.T, wg *sync.WaitGroup, d time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatal("timed out waiting for events to be processed")
	}
}
