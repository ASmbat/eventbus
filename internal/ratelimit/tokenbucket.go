// Package ratelimit provides a small token-bucket limiter, used here to
// protect the publish endpoint from bursty producers.
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
}

// NewTokenBucket creates a bucket holding up to capacity tokens, refilled
// at refillPerSec tokens per second. It starts full.
func NewTokenBucket(capacity, refillPerSec float64) *TokenBucket {
	return &TokenBucket{
		capacity:     capacity,
		tokens:       capacity,
		refillPerSec: refillPerSec,
		last:         time.Now(),
	}
}

// Allow reports whether a single unit of work may proceed right now,
// consuming one token if so.
func (t *TokenBucket) Allow() bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(t.last).Seconds()
	t.last = now

	t.tokens += elapsed * t.refillPerSec
	if t.tokens > t.capacity {
		t.tokens = t.capacity
	}

	if t.tokens >= 1 {
		t.tokens--
		return true
	}
	return false
}
