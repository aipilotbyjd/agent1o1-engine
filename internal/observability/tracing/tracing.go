package tracing

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

// SpanContext contains the identifiers for a trace.
type SpanContext struct {
	TraceID      string
	SpanID       string
	ParentSpanID string
	Sampled      bool
}

// IsValid returns whether the span context is valid.
func (sc SpanContext) IsValid() bool {
	return sc.TraceID != "" && sc.SpanID != ""
}

// Span represents a unit of work within a trace.
type Span struct {
	Name       string
	Context    SpanContext
	StartTime  time.Time
	EndTime    time.Time
	Status     SpanStatus
	StatusMsg  string
	Attributes map[string]interface{}
	Events     []SpanEvent
	mu         sync.Mutex
	tracer     *Tracer
}

// SpanStatus represents the status of a span.
type SpanStatus int

const (
	SpanStatusUnset SpanStatus = iota
	SpanStatusOK
	SpanStatusError
)

// SpanEvent represents an event within a span.
type SpanEvent struct {
	Name       string
	Timestamp  time.Time
	Attributes map[string]interface{}
}

// End finishes the span.
func (s *Span) End() {
	s.mu.Lock()
	s.EndTime = time.Now()
	s.mu.Unlock()

	if s.tracer != nil && s.tracer.exporter != nil {
		s.tracer.exporter.Export(s)
	}
}

// SetStatus sets the span status.
func (s *Span) SetStatus(status SpanStatus, message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Status = status
	s.StatusMsg = message
}

// SetAttribute sets an attribute on the span.
func (s *Span) SetAttribute(key string, value interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Attributes == nil {
		s.Attributes = make(map[string]interface{})
	}
	s.Attributes[key] = value
}

// SetAttributes sets multiple attributes on the span.
func (s *Span) SetAttributes(attrs map[string]interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Attributes == nil {
		s.Attributes = make(map[string]interface{})
	}
	for k, v := range attrs {
		s.Attributes[k] = v
	}
}

// AddEvent adds an event to the span.
func (s *Span) AddEvent(name string, attrs map[string]interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Events = append(s.Events, SpanEvent{
		Name:       name,
		Timestamp:  time.Now(),
		Attributes: attrs,
	})
}

// RecordError records an error in the span.
func (s *Span) RecordError(err error) {
	if err == nil {
		return
	}
	s.AddEvent("exception", map[string]interface{}{
		"exception.type":    "error",
		"exception.message": err.Error(),
	})
	s.SetStatus(SpanStatusError, err.Error())
}

// Duration returns the span duration.
func (s *Span) Duration() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.EndTime.IsZero() {
		return time.Since(s.StartTime)
	}
	return s.EndTime.Sub(s.StartTime)
}

// SpanExporter exports spans to an external system.
type SpanExporter interface {
	Export(span *Span)
	Shutdown(ctx context.Context) error
}

// Tracer creates spans and manages trace context.
type Tracer struct {
	name     string
	exporter SpanExporter
	sampler  Sampler
}

// Sampler determines whether a trace should be sampled.
type Sampler interface {
	ShouldSample(parentContext SpanContext, traceID string, name string) bool
}

// AlwaysSampler samples all traces.
type AlwaysSampler struct{}

func (s AlwaysSampler) ShouldSample(parentContext SpanContext, traceID, name string) bool {
	return true
}

// NeverSampler never samples traces.
type NeverSampler struct{}

func (s NeverSampler) ShouldSample(parentContext SpanContext, traceID, name string) bool {
	return false
}

// RatioSampler samples a fraction of traces.
type RatioSampler struct {
	ratio float64
}

func NewRatioSampler(ratio float64) *RatioSampler {
	if ratio < 0 {
		ratio = 0
	}
	if ratio > 1 {
		ratio = 1
	}
	return &RatioSampler{ratio: ratio}
}

func (s *RatioSampler) ShouldSample(parentContext SpanContext, traceID, name string) bool {
	// If parent is sampled, sample this span too
	if parentContext.Sampled {
		return true
	}
	// Use the trace ID's last 2 bytes as the random value
	if len(traceID) >= 4 {
		hashBytes, _ := hex.DecodeString(traceID[len(traceID)-4:])
		if len(hashBytes) == 2 {
			value := float64(int(hashBytes[0])<<8|int(hashBytes[1])) / 65535.0
			return value < s.ratio
		}
	}
	return false
}

// TracerConfig holds tracer configuration.
type TracerConfig struct {
	Name     string
	Exporter SpanExporter
	Sampler  Sampler
}

// NewTracer creates a new tracer.
func NewTracer(config TracerConfig) *Tracer {
	sampler := config.Sampler
	if sampler == nil {
		sampler = AlwaysSampler{}
	}
	return &Tracer{
		name:     config.Name,
		exporter: config.Exporter,
		sampler:  sampler,
	}
}

// Start creates and starts a new span.
func (t *Tracer) Start(ctx context.Context, name string) (context.Context, *Span) {
	parentCtx := SpanFromContext(ctx)

	var traceID, parentSpanID string
	if parentCtx != nil && parentCtx.Context.IsValid() {
		traceID = parentCtx.Context.TraceID
		parentSpanID = parentCtx.Context.SpanID
	} else {
		traceID = generateTraceID()
	}

	spanID := generateSpanID()
	sampled := t.sampler.ShouldSample(
		SpanContext{TraceID: traceID, Sampled: parentCtx != nil && parentCtx.Context.Sampled},
		traceID,
		name,
	)

	span := &Span{
		Name: name,
		Context: SpanContext{
			TraceID:      traceID,
			SpanID:       spanID,
			ParentSpanID: parentSpanID,
			Sampled:      sampled,
		},
		StartTime:  time.Now(),
		Attributes: make(map[string]interface{}),
		tracer:     t,
	}

	return ContextWithSpan(ctx, span), span
}

// contextKey is used for storing span in context.
type contextKey struct{}

// ContextWithSpan returns a new context with the span attached.
func ContextWithSpan(ctx context.Context, span *Span) context.Context {
	return context.WithValue(ctx, contextKey{}, span)
}

// SpanFromContext returns the span from the context, or nil if not found.
func SpanFromContext(ctx context.Context) *Span {
	if span, ok := ctx.Value(contextKey{}).(*Span); ok {
		return span
	}
	return nil
}

// TraceIDFromContext returns the trace ID from the context.
func TraceIDFromContext(ctx context.Context) string {
	if span := SpanFromContext(ctx); span != nil {
		return span.Context.TraceID
	}
	return ""
}

// generateTraceID generates a random 16-byte trace ID.
func generateTraceID() string {
	buf := make([]byte, 16)
	rand.Read(buf)
	return hex.EncodeToString(buf)
}

// generateSpanID generates a random 8-byte span ID.
func generateSpanID() string {
	buf := make([]byte, 8)
	rand.Read(buf)
	return hex.EncodeToString(buf)
}

// NoopExporter is an exporter that does nothing.
type NoopExporter struct{}

func (e *NoopExporter) Export(span *Span)                  {}
func (e *NoopExporter) Shutdown(ctx context.Context) error { return nil }

// InMemoryExporter stores spans in memory (for testing).
type InMemoryExporter struct {
	spans []*Span
	mu    sync.Mutex
}

func NewInMemoryExporter() *InMemoryExporter {
	return &InMemoryExporter{
		spans: make([]*Span, 0),
	}
}

func (e *InMemoryExporter) Export(span *Span) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.spans = append(e.spans, span)
}

func (e *InMemoryExporter) Shutdown(ctx context.Context) error {
	return nil
}

func (e *InMemoryExporter) Spans() []*Span {
	e.mu.Lock()
	defer e.mu.Unlock()
	result := make([]*Span, len(e.spans))
	copy(result, e.spans)
	return result
}

func (e *InMemoryExporter) Clear() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.spans = e.spans[:0]
}

// LogExporter logs spans (for development).
type LogExporter struct {
	logger func(string, ...interface{})
}

func NewLogExporter(logger func(string, ...interface{})) *LogExporter {
	return &LogExporter{logger: logger}
}

func (e *LogExporter) Export(span *Span) {
	e.logger("Span completed: name=%s trace_id=%s span_id=%s duration=%v status=%d",
		span.Name,
		span.Context.TraceID,
		span.Context.SpanID,
		span.Duration(),
		span.Status,
	)
}

func (e *LogExporter) Shutdown(ctx context.Context) error {
	return nil
}

// GlobalTracer is the default global tracer.
var GlobalTracer = NewTracer(TracerConfig{
	Name:     "linkflow",
	Exporter: &NoopExporter{},
	Sampler:  AlwaysSampler{},
})

// SetGlobalTracer sets the global tracer.
func SetGlobalTracer(tracer *Tracer) {
	GlobalTracer = tracer
}

// StartSpan starts a new span using the global tracer.
func StartSpan(ctx context.Context, name string) (context.Context, *Span) {
	return GlobalTracer.Start(ctx, name)
}
