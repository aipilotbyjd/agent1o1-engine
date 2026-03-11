package engine

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const latencyBufferSize = 1000

type Metrics struct {
	TasksAdded      atomic.Int64
	TasksDispatched atomic.Int64
	PollersWaiting  atomic.Int64
	TasksFailed     atomic.Int64
	TasksTimedOut   atomic.Int64
	TasksDLQ        atomic.Int64
	TasksRejected   atomic.Int64

	QueueDepth    atomic.Int64
	InFlightCount atomic.Int64
	PollerCount   atomic.Int64

	latencies    []time.Duration
	latencyIndex int
	mu           sync.Mutex
}

// MetricsSnapshot is a point-in-time snapshot of queue metrics for monitoring.
type MetricsSnapshot struct {
	TasksAdded      int64
	TasksDispatched int64
	TasksFailed     int64
	TasksTimedOut   int64
	TasksDLQ        int64
	TasksRejected   int64
	QueueDepth      int64
	InFlightCount   int64
	PollerCount     int64
	P50Latency      time.Duration
	P95Latency      time.Duration
	P99Latency      time.Duration
}

func NewMetrics() *Metrics {
	return &Metrics{
		latencies: make([]time.Duration, latencyBufferSize),
	}
}

func (m *Metrics) TaskAdded() {
	m.TasksAdded.Add(1)
}

func (m *Metrics) TaskDispatched() {
	m.TasksDispatched.Add(1)
}

func (m *Metrics) TaskFailed() {
	m.TasksFailed.Add(1)
}

func (m *Metrics) TaskTimedOut() {
	m.TasksTimedOut.Add(1)
}

func (m *Metrics) TaskSentToDLQ() {
	m.TasksDLQ.Add(1)
}

func (m *Metrics) TaskRejected() {
	m.TasksRejected.Add(1)
}

func (m *Metrics) SetQueueDepth(n int64) {
	m.QueueDepth.Store(n)
}

func (m *Metrics) SetInFlightCount(n int64) {
	m.InFlightCount.Store(n)
}

func (m *Metrics) SetPollerCount(n int64) {
	m.PollerCount.Store(n)
}

func (m *Metrics) RecordLatency(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.latencies[m.latencyIndex] = d
	m.latencyIndex = (m.latencyIndex + 1) % latencyBufferSize
}

func (m *Metrics) GetLatencies() []time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]time.Duration, latencyBufferSize)
	copy(result, m.latencies)
	return result
}

// Snapshot returns a point-in-time snapshot of all metrics.
func (m *Metrics) Snapshot() MetricsSnapshot {
	p50, p95, p99 := m.computePercentiles()
	return MetricsSnapshot{
		TasksAdded:      m.TasksAdded.Load(),
		TasksDispatched: m.TasksDispatched.Load(),
		TasksFailed:     m.TasksFailed.Load(),
		TasksTimedOut:   m.TasksTimedOut.Load(),
		TasksDLQ:        m.TasksDLQ.Load(),
		TasksRejected:   m.TasksRejected.Load(),
		QueueDepth:      m.QueueDepth.Load(),
		InFlightCount:   m.InFlightCount.Load(),
		PollerCount:     m.PollerCount.Load(),
		P50Latency:      p50,
		P95Latency:      p95,
		P99Latency:      p99,
	}
}

func (m *Metrics) computePercentiles() (p50, p95, p99 time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Collect non-zero latencies
	nonZero := make([]time.Duration, 0, latencyBufferSize)
	for _, d := range m.latencies {
		if d > 0 {
			nonZero = append(nonZero, d)
		}
	}

	if len(nonZero) == 0 {
		return 0, 0, 0
	}

	sort.Slice(nonZero, func(i, j int) bool { return nonZero[i] < nonZero[j] })

	percentile := func(pct float64) time.Duration {
		idx := int(float64(len(nonZero)-1) * pct)
		return nonZero[idx]
	}

	return percentile(0.50), percentile(0.95), percentile(0.99)
}
