// Command server runs a small demo of the event bus: a couple of topics
// with worker pools, a rate-limited publish endpoint, and a metrics
// endpoint to watch backpressure happen in real time.
package main

import (
	"context"
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

func main() {
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}

	m := metrics.New()
	b := bus.New(m, bus.PolicyDrop)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// "orders" simulates a moderately loaded consumer: a small worker pool
	// with a bounded queue, so a burst of publishes will start hitting
	// ErrQueueFull once the queue fills — that's the backpressure signal
	// a real producer would react to (retry, shed load, alert).
	b.Subscribe(ctx, "orders", 3, 20, func(_ context.Context, e bus.Event) error {
		time.Sleep(20 * time.Millisecond) // simulate work
		if rand.Intn(20) == 0 {
			return errSimulated
		}
		return nil
	})

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
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("graceful shutdown failed: %v", err)
	}
	log.Println("shutdown complete")
}

var errSimulated = &simulatedError{}

type simulatedError struct{}

func (e *simulatedError) Error() string { return "simulated processing failure" }
