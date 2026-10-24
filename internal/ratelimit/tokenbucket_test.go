package ratelimit

import (
	"testing"
	"time"
)

// fakeClock is a manually advanced clock, so tests don't need time.Sleep.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestBucket(capacity, refill float64) (*TokenBucket, *fakeClock) {
	c := &fakeClock{t: time.Unix(0, 0)}
	return newTokenBucket(capacity, refill, c.now), c
}

func TestTokenBucket_AllowsBurstUpToCapacity(t *testing.T) {
	tb, _ := newTestBucket(3, 1)

	for i := 0; i < 3; i++ {
		if !tb.Allow() {
			t.Fatalf("expected token %d to be allowed", i)
		}
	}
	if tb.Allow() {
		t.Fatal("expected 4th call to be denied")
	}
}

func TestTokenBucket_RefillsOverTime(t *testing.T) {
	tb, clock := newTestBucket(1, 10) // 1 token per 100ms

	if !tb.Allow() {
		t.Fatal("expected first call to be allowed")
	}
	clock.advance(50 * time.Millisecond)
	if tb.Allow() {
		t.Fatal("expected denial after only half a token of refill")
	}
	clock.advance(50 * time.Millisecond)
	if !tb.Allow() {
		t.Fatal("expected call to be allowed after a full token refilled")
	}
}

func TestTokenBucket_CapsRefillAtCapacity(t *testing.T) {
	tb, clock := newTestBucket(2, 100)
	tb.Allow()
	tb.Allow()

	clock.advance(10 * time.Second) // would be 1000 tokens without the cap

	for i := 0; i < 2; i++ {
		if !tb.Allow() {
			t.Fatalf("expected token %d after refill", i)
		}
	}
	if tb.Allow() {
		t.Fatal("expected refill to be capped at capacity")
	}
}
