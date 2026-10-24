package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ASmbat/eventbus/internal/bus"
	"github.com/ASmbat/eventbus/internal/metrics"
	"github.com/ASmbat/eventbus/internal/ratelimit"
)

func newTestHandler(t *testing.T) *Handler {
	t.Helper()
	m := metrics.New()
	b := bus.New(m, bus.PolicyDrop)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	if err := b.Subscribe(ctx, "orders", 1, 10, func(context.Context, bus.Event) error { return nil }); err != nil {
		t.Fatal(err)
	}

	limiter := ratelimit.NewTokenBucket(100, 100) // generous, so tests aren't flaky
	return New(b, m, limiter)
}

func TestPublish_Accepted(t *testing.T) {
	h := newTestHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/publish/orders", strings.NewReader("hello"))
	rr := httptest.NewRecorder()
	h.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestPublish_UnknownTopic(t *testing.T) {
	h := newTestHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/publish/nope", strings.NewReader("x"))
	rr := httptest.NewRecorder()
	h.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
}

func TestPublish_RateLimited(t *testing.T) {
	m := metrics.New()
	b := bus.New(m, bus.PolicyDrop)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := b.Subscribe(ctx, "orders", 1, 10, func(context.Context, bus.Event) error { return nil }); err != nil {
		t.Fatal(err)
	}

	limiter := ratelimit.NewTokenBucket(1, 0.001) // capacity 1, effectively no refill
	h := New(b, m, limiter)

	first := httptest.NewRequest(http.MethodPost, "/publish/orders", strings.NewReader("a"))
	rr1 := httptest.NewRecorder()
	h.Routes().ServeHTTP(rr1, first)
	if rr1.Code != http.StatusAccepted {
		t.Fatalf("expected first request accepted, got %d", rr1.Code)
	}

	second := httptest.NewRequest(http.MethodPost, "/publish/orders", strings.NewReader("b"))
	rr2 := httptest.NewRecorder()
	h.Routes().ServeHTTP(rr2, second)
	if rr2.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 on second immediate request, got %d", rr2.Code)
	}
}

func TestPublish_BodyOverLimitIsRejected(t *testing.T) {
	h := newTestHandler(t)

	oversized := strings.NewReader(strings.Repeat("x", (1<<20)+1)) // 1MB + 1 byte
	req := httptest.NewRequest(http.MethodPost, "/publish/orders", oversized)
	rr := httptest.NewRecorder()
	h.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 for an oversized body, got %d: %s", rr.Code, rr.Body.String())
	}
	if h.metrics.Snapshot().Published != 0 {
		t.Errorf("an oversized, rejected body should not be counted as published")
	}
}

func TestMetricsEndpoint(t *testing.T) {
	h := newTestHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/publish/orders", strings.NewReader("x"))
	h.Routes().ServeHTTP(httptest.NewRecorder(), req)

	mreq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rr := httptest.NewRecorder()
	h.Routes().ServeHTTP(rr, mreq)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), `"published":1`) {
		t.Errorf("expected published count of 1, got %s", rr.Body.String())
	}
}

func TestPublish_AfterCloseReturns503(t *testing.T) {
	h := newTestHandler(t)
	if err := h.bus.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/publish/orders", strings.NewReader("x"))
	rr := httptest.NewRecorder()
	h.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 after shutdown, got %d", rr.Code)
	}
}

func TestHealthz(t *testing.T) {
	h := newTestHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()
	h.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
}
