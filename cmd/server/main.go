// Command server runs a demo of the event bus: one topic with a worker
// pool, a rate-limited publish endpoint, a metrics endpoint, and graceful
// shutdown that finishes queued events before exiting.
package main

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ASmbat/eventbus/internal/bus"
	"github.com/ASmbat/eventbus/internal/handler"
	"github.com/ASmbat/eventbus/internal/metrics"
	"github.com/ASmbat/eventbus/internal/ratelimit"
)

var errSimulated = errors.New("simulated processing failure")

func main() {
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}

	m := metrics.New()
	b := bus.New(m, bus.PolicyDrop)

	// Workers use a context that is NOT cancelled by the signal, so on
	// shutdown they can finish queued events (see b.Close below).
	// "orders": 3 workers, queue of 20, ~20ms per event, ~5% failures.
	err := b.Subscribe(context.Background(), "orders", 3, 20, func(context.Context, bus.Event) error {
		time.Sleep(20 * time.Millisecond) // simulate work
		if rand.Intn(20) == 0 {
			return errSimulated
		}
		return nil
	})
	if err != nil {
		log.Fatalf("subscribe: %v", err)
	}

	limiter := ratelimit.NewTokenBucket(20, 10) // burst of 20, refills at 10/sec
	h := handler.New(b, m, limiter)

	srv := &http.Server{
		Addr:         addr,
		Handler:      h.Routes(),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server error: %v", err)
		}
	}()

	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-sigCtx.Done()
	log.Println("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Stop taking HTTP requests first, then let the bus drain its queues.
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("http shutdown: %v", err)
	}
	if err := b.Close(shutdownCtx); err != nil {
		log.Printf("bus close: %v", err)
	}
	log.Printf("shutdown complete, final metrics: %+v", m.Snapshot())
}
