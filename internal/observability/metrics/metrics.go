package metrics

import (
	"math"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// MetricType represents the type of metric.
type MetricType int

const (
	MetricTypeCounter MetricType = iota
	MetricTypeGauge
	MetricTypeHistogram
)

// Labels represents metric labels.
type Labels map[string]string

// Metric is the base interface for all metrics.
type Metric interface {
	Name() string
	Type() MetricType
	Labels() Labels
}

// Counter is a monotonically increasing counter.
type Counter struct {
	name   string
	labels Labels
	value  int64
}

// NewCounter creates a new counter.
func NewCounter(name string, labels Labels) *Counter {
	return &Counter{
		name:   name,
		labels: labels,
	}
}

func (c *Counter) Name() string     { return c.name }
func (c *Counter) Type() MetricType { return MetricTypeCounter }
func (c *Counter) Labels() Labels   { return c.labels }
func (c *Counter) Value() int64     { return atomic.LoadInt64(&c.value) }

// Inc increments the counter by 1.
func (c *Counter) Inc() {
	atomic.AddInt64(&c.value, 1)
}

// Add adds the given value to the counter.
func (c *Counter) Add(delta int64) {
	atomic.AddInt64(&c.value, delta)
}

// Gauge is a metric that can go up and down.
type Gauge struct {
	name   string
	labels Labels
	value  uint64 // Stored as uint64, represents float64 bits
}

// NewGauge creates a new gauge.
func NewGauge(name string, labels Labels) *Gauge {
	return &Gauge{
		name:   name,
		labels: labels,
	}
}

func (g *Gauge) Name() string     { return g.name }
func (g *Gauge) Type() MetricType { return MetricTypeGauge }
func (g *Gauge) Labels() Labels   { return g.labels }
func (g *Gauge) Value() float64   { return math.Float64frombits(atomic.LoadUint64(&g.value)) }

// Set sets the gauge to the given value.
func (g *Gauge) Set(value float64) {
	atomic.StoreUint64(&g.value, math.Float64bits(value))
}

// Inc increments the gauge by 1.
func (g *Gauge) Inc() {
	g.Add(1)
}

// Dec decrements the gauge by 1.
func (g *Gauge) Dec() {
	g.Add(-1)
}

// Add adds the given value to the gauge using atomic compare-and-swap.
func (g *Gauge) Add(delta float64) {
	for {
		old := atomic.LoadUint64(&g.value)
		newVal := math.Float64frombits(old) + delta
		if atomic.CompareAndSwapUint64(&g.value, old, math.Float64bits(newVal)) {
			return
		}
	}
}

// Histogram tracks the distribution of values.
type Histogram struct {
	name    string
	labels  Labels
	buckets []float64
	counts  []int64
	sum     int64
	count   int64
	mu      sync.RWMutex
}

// DefaultBuckets are the default histogram buckets (in milliseconds).
var DefaultBuckets = []float64{5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000}

// NewHistogram creates a new histogram.
func NewHistogram(name string, labels Labels, buckets []float64) *Histogram {
	if buckets == nil {
		buckets = DefaultBuckets
	}
	return &Histogram{
		name:    name,
		labels:  labels,
		buckets: buckets,
		counts:  make([]int64, len(buckets)+1),
	}
}

func (h *Histogram) Name() string     { return h.name }
func (h *Histogram) Type() MetricType { return MetricTypeHistogram }
func (h *Histogram) Labels() Labels   { return h.labels }

// Observe records a value in the histogram.
func (h *Histogram) Observe(value float64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Find the bucket
	bucketIdx := len(h.buckets)
	for i, bound := range h.buckets {
		if value <= bound {
			bucketIdx = i
			break
		}
	}

	h.counts[bucketIdx]++
	h.sum += int64(value * 1000) // Store sum as microseconds for precision
	h.count++
}

// ObserveDuration records a duration in milliseconds.
func (h *Histogram) ObserveDuration(d time.Duration) {
	h.Observe(float64(d.Milliseconds()))
}

// Sum returns the sum of all observed values.
func (h *Histogram) Sum() float64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return float64(h.sum) / 1000
}

// Count returns the count of observations.
func (h *Histogram) Count() int64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.count
}

// Buckets returns the bucket counts.
func (h *Histogram) Buckets() map[float64]int64 {
	h.mu.RLock()
	defer h.mu.RUnlock()

	result := make(map[float64]int64, len(h.buckets))
	for i, bound := range h.buckets {
		result[bound] = h.counts[i]
	}
	return result
}

// Registry stores and manages metrics.
type Registry struct {
	counters   map[string]*Counter
	gauges     map[string]*Gauge
	histograms map[string]*Histogram
	mu         sync.RWMutex
}

// NewRegistry creates a new metrics registry.
func NewRegistry() *Registry {
	return &Registry{
		counters:   make(map[string]*Counter),
		gauges:     make(map[string]*Gauge),
		histograms: make(map[string]*Histogram),
	}
}

// DefaultRegistry is the default global metrics registry.
var DefaultRegistry = NewRegistry()

// Counter gets or creates a counter.
func (r *Registry) Counter(name string, labels Labels) *Counter {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := makeKey(name, labels)
	if c, exists := r.counters[key]; exists {
		return c
	}

	c := NewCounter(name, labels)
	r.counters[key] = c
	return c
}

// Gauge gets or creates a gauge.
func (r *Registry) Gauge(name string, labels Labels) *Gauge {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := makeKey(name, labels)
	if g, exists := r.gauges[key]; exists {
		return g
	}

	g := NewGauge(name, labels)
	r.gauges[key] = g
	return g
}

// Histogram gets or creates a histogram.
func (r *Registry) Histogram(name string, labels Labels, buckets []float64) *Histogram {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := makeKey(name, labels)
	if h, exists := r.histograms[key]; exists {
		return h
	}

	h := NewHistogram(name, labels, buckets)
	r.histograms[key] = h
	return h
}

// Handler returns an HTTP handler for metrics (Prometheus-compatible).
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.mu.RLock()
		defer r.mu.RUnlock()

		w.Header().Set("Content-Type", "text/plain; version=0.0.4")

		// Write counters
		for _, c := range r.counters {
			writeMetricLine(w, c.name, c.labels, float64(c.Value()), "counter")
		}

		// Write gauges
		for _, g := range r.gauges {
			writeMetricLine(w, g.name, g.labels, g.Value(), "gauge")
		}

		// Write histograms
		for _, h := range r.histograms {
			writeHistogramLines(w, h)
		}
	})
}

func makeKey(name string, labels Labels) string {
	if len(labels) == 0 {
		return name
	}

	// Sort label keys for consistent key generation
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	key := name
	for _, k := range keys {
		key += "," + k + "=" + labels[k]
	}
	return key
}

func writeMetricLine(w http.ResponseWriter, name string, labels Labels, value float64, metricType string) {
	// Write type comment
	w.Write([]byte("# TYPE " + name + " " + metricType + "\n"))

	// Write metric line
	line := name
	if len(labels) > 0 {
		line += "{"
		first := true
		for k, v := range labels {
			if !first {
				line += ","
			}
			line += k + "=\"" + v + "\""
			first = false
		}
		line += "}"
	}
	line += " " + formatFloat(value) + "\n"
	w.Write([]byte(line))
}

func writeHistogramLines(w http.ResponseWriter, h *Histogram) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	name := h.name
	labels := h.labels

	// Write type comment
	w.Write([]byte("# TYPE " + name + " histogram\n"))

	// Write bucket lines
	cumulative := int64(0)
	for i, bound := range h.buckets {
		cumulative += h.counts[i]
		bucketLabels := copyLabels(labels)
		bucketLabels["le"] = formatFloat(bound)
		writeMetricLineNoType(w, name+"_bucket", bucketLabels, float64(cumulative))
	}

	// +Inf bucket
	cumulative += h.counts[len(h.buckets)]
	infLabels := copyLabels(labels)
	infLabels["le"] = "+Inf"
	writeMetricLineNoType(w, name+"_bucket", infLabels, float64(cumulative))

	// Sum and count
	writeMetricLineNoType(w, name+"_sum", labels, float64(h.sum)/1000)
	writeMetricLineNoType(w, name+"_count", labels, float64(h.count))
}

func writeMetricLineNoType(w http.ResponseWriter, name string, labels Labels, value float64) {
	line := name
	if len(labels) > 0 {
		line += "{"
		first := true
		for k, v := range labels {
			if !first {
				line += ","
			}
			line += k + "=\"" + v + "\""
			first = false
		}
		line += "}"
	}
	line += " " + formatFloat(value) + "\n"
	w.Write([]byte(line))
}

func copyLabels(labels Labels) Labels {
	copy := make(Labels, len(labels))
	for k, v := range labels {
		copy[k] = v
	}
	return copy
}

func formatFloat(v float64) string {
	if v == float64(int64(v)) {
		return string(append([]byte(nil), []byte(intToStr(int64(v)))...))
	}
	return floatToStr(v)
}

func intToStr(v int64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	negative := v < 0
	if negative {
		v = -v
	}
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if negative {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func floatToStr(v float64) string {
	// Simple float to string (6 decimal places)
	negative := v < 0
	if negative {
		v = -v
	}
	intPart := int64(v)
	fracPart := int64((v - float64(intPart)) * 1000000)

	result := intToStr(intPart) + "."
	fracStr := intToStr(fracPart)
	// Pad with zeros
	for len(fracStr) < 6 {
		fracStr = "0" + fracStr
	}
	result += fracStr

	// Trim trailing zeros
	for len(result) > 1 && result[len(result)-1] == '0' {
		result = result[:len(result)-1]
	}
	if result[len(result)-1] == '.' {
		result = result[:len(result)-1]
	}

	if negative {
		return "-" + result
	}
	return result
}
