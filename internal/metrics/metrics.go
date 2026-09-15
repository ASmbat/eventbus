// Package metrics holds simple, concurrency-safe counters for the bus.
package metrics

import "sync/atomic"

type Metrics struct {
	published int64
	processed int64
	dropped   int64
	failed    int64
}

func New() *Metrics { return &Metrics{} }

func (m *Metrics) IncPublished() { atomic.AddInt64(&m.published, 1) }
func (m *Metrics) IncProcessed() { atomic.AddInt64(&m.processed, 1) }
func (m *Metrics) IncDropped()   { atomic.AddInt64(&m.dropped, 1) }
func (m *Metrics) IncFailed()    { atomic.AddInt64(&m.failed, 1) }

// Snapshot is a point-in-time, JSON-serializable read of all counters.
type Snapshot struct {
	Published int64 `json:"published"`
	Processed int64 `json:"processed"`
	Dropped   int64 `json:"dropped"`
	Failed    int64 `json:"failed"`
}

func (m *Metrics) Snapshot() Snapshot {
	return Snapshot{
		Published: atomic.LoadInt64(&m.published),
		Processed: atomic.LoadInt64(&m.processed),
		Dropped:   atomic.LoadInt64(&m.dropped),
		Failed:    atomic.LoadInt64(&m.failed),
	}
}
