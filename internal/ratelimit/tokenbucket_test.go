package ratelimit

import (
	"testing"
	"time"
)

func TestTokenBucket_AllowsBurstUpToCapacity(t *testing.T) {
	tb := NewTokenBucket(3, 1) // 3 tokens, refills slowly

	for i := 0; i < 3; i++ {
		if !tb.Allow() {
			t.Fatalf("expected token %d to be allowed", i)
		}
	}
	if tb.Allow() {
		t.Fatal("expected 4th immediate call to be denied once capacity is exhausted")
	}
}

func TestTokenBucket_CapsRefillAtCapacity(t *testing.T) {
	tb := NewTokenBucket(2, 100) // fast refill, small capacity
	// Drain it, then simulate a long idle period so refill would
	// overshoot capacity if not clamped.
	tb.Allow()
	tb.Allow()
	tb.mu.Lock()
	tb.last = time.Now().Add(-10 * time.Second)
	tb.mu.Unlock()

	// Should allow exactly 2 (capacity), not more, even though 100/sec
	// refill over 10s would imply 1000 tokens.
	if !tb.Allow() {
		t.Fatal("expected token to be available after refill")
	}
	if !tb.Allow() {
		t.Fatal("expected second token to be available after refill")
	}
	if tb.Allow() {
		t.Fatal("expected refill to be capped at bucket capacity")
	}
}

func TestTokenBucket_RefillsOverTime(t *testing.T) {
	tb := NewTokenBucket(1, 10) // 1 capacity, refills fast

	if !tb.Allow() {
		t.Fatal("expected first call to be allowed")
	}
	if tb.Allow() {
		t.Fatal("expected immediate second call to be denied")
	}

	time.Sleep(150 * time.Millisecond) // ~1.5 tokens worth of refill at 10/sec
	if !tb.Allow() {
		t.Fatal("expected call to be allowed after refill window")
	}
}
