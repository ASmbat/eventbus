// Package ratelimit provides a small token-bucket limiter, used to protect
// the publish endpoint from bursty producers.
package ratelimit

import (
	"sync"
	"time"
)

type TokenBucket struct {
	mu           sync.Mutex
	capacity     float64
	tokens       float64
	refillPerSec float64
	last         time.Time
	now          func() time.Time // replaceable in tests
}

// NewTokenBucket creates a full bucket of capacity tokens that refills at
// refillPerSec tokens per second.
func NewTokenBucket(capacity, refillPerSec float64) *TokenBucket {
	return newTokenBucket(capacity, refillPerSec, time.Now)
}

func newTokenBucket(capacity, refillPerSec float64, now func() time.Time) *TokenBucket {
	return &TokenBucket{
		capacity:     capacity,
		tokens:       capacity,
		refillPerSec: refillPerSec,
		last:         now(),
		now:          now,
	}
}

// Allow reports whether one request may proceed now, using one token if so.
func (t *TokenBucket) Allow() bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.now()
	t.tokens += now.Sub(t.last).Seconds() * t.refillPerSec
	t.last = now
	if t.tokens > t.capacity {
		t.tokens = t.capacity
	}

	if t.tokens >= 1 {
		t.tokens--
		return true
	}
	return false
}
