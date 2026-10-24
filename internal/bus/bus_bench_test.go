package bus

import (
	"context"
	"fmt"
	"testing"

	"github.com/ASmbat/eventbus/internal/metrics"
)

// BenchmarkPublish measures the cost of Publish with a fast consumer.
// Run with: go test ./internal/bus -bench . -benchmem
func BenchmarkPublish(b *testing.B) {
	for _, workers := range []int{1, 4} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			bus := New(metrics.New(), PolicyBlock)
			noop := func(context.Context, Event) error { return nil }
			if err := bus.Subscribe(context.Background(), "t", workers, 1024, noop); err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := bus.Publish("t", "payload"); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			_ = bus.Close(context.Background())
		})
	}
}

// BenchmarkPublishParallel measures Publish from many goroutines at once,
// which exercises the read lock and the shared queue.
func BenchmarkPublishParallel(b *testing.B) {
	bus := New(metrics.New(), PolicyBlock)
	noop := func(context.Context, Event) error { return nil }
	if err := bus.Subscribe(context.Background(), "t", 4, 1024, noop); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := bus.Publish("t", "payload"); err != nil {
				b.Error(err)
				return
			}
		}
	})
	b.StopTimer()
	_ = bus.Close(context.Background())
}

// BenchmarkPublishDropFull measures the rejection path when the queue is
// full, which is the hot path under overload.
func BenchmarkPublishDropFull(b *testing.B) {
	bus := New(metrics.New(), PolicyDrop)
	release := make(chan struct{})
	block := func(context.Context, Event) error { <-release; return nil }
	if err := bus.Subscribe(context.Background(), "t", 1, 0, block); err != nil {
		b.Fatal(err)
	}
	_ = bus.Publish("t", "x") // may or may not be taken by the worker; queue stays full either way

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = bus.Publish("t", "payload")
	}
	b.StopTimer()
	close(release)
	_ = bus.Close(context.Background())
}
