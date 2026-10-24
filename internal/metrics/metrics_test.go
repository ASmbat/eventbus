package metrics

import (
	"sync"
	"testing"
)

func TestMetrics_ConcurrentIncrements(t *testing.T) {
	m := New()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.IncPublished()
			m.IncProcessed()
			m.IncDropped()
			m.IncFailed()
		}()
	}
	wg.Wait()

	want := Snapshot{Published: 100, Processed: 100, Dropped: 100, Failed: 100}
	if got := m.Snapshot(); got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}
