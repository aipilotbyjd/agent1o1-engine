package audit

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// EventType represents audit event types.
type EventType string

const (
	EventTypeAuthentication EventType = "authentication"
	EventTypeAuthorization  EventType = "authorization"
	EventTypeWorkflow       EventType = "workflow"
	EventTypeExecution      EventType = "execution"
	EventTypeCredential     EventType = "credential"
	EventTypeAdmin          EventType = "admin"
	EventTypeSystem         EventType = "system"
)

// Action represents audit actions.
type Action string

const (
	ActionCreate  Action = "create"
	ActionRead    Action = "read"
	ActionUpdate  Action = "update"
	ActionDelete  Action = "delete"
	ActionExecute Action = "execute"
	ActionLogin   Action = "login"
	ActionLogout  Action = "logout"
	ActionGrant   Action = "grant"
	ActionRevoke  Action = "revoke"
	ActionExport  Action = "export"
)

// Outcome represents the outcome of an action.
type Outcome string

const (
	OutcomeSuccess Outcome = "success"
	OutcomeFailure Outcome = "failure"
	OutcomeDenied  Outcome = "denied"
)

// Event represents an audit event.
type Event struct {
	ID        string    `json:"id"`
	Timestamp time.Time `json:"timestamp"`
	EventType EventType `json:"event_type"`
	Action    Action    `json:"action"`
	Outcome   Outcome   `json:"outcome"`

	// Actor information
	ActorID   string `json:"actor_id,omitempty"`
	ActorType string `json:"actor_type,omitempty"` // user, service, system
	ActorIP   string `json:"actor_ip,omitempty"`

	// Target information
	ResourceType string `json:"resource_type,omitempty"`
	ResourceID   string `json:"resource_id,omitempty"`
	ResourceName string `json:"resource_name,omitempty"`

	// Context
	WorkspaceID string `json:"workspace_id,omitempty"`
	SessionID   string `json:"session_id,omitempty"`
	RequestID   string `json:"request_id,omitempty"`

	// Details
	Details      map[string]interface{} `json:"details,omitempty"`
	ErrorMessage string                 `json:"error_message,omitempty"`

	// Compliance
	Sensitive     bool `json:"sensitive,omitempty"`
	RetentionDays int  `json:"retention_days,omitempty"`
}

// Logger is the audit logger.
type Logger struct {
	sinks   []Sink
	sinksMu sync.RWMutex

	// Configuration
	enabled      bool
	minLevel     EventType
	redactFields []string

	// Buffering
	buffer     chan *Event
	bufferSize int

	// Base logger for fallback
	baseLogger *slog.Logger
}

// Sink is the interface for audit log destinations.
type Sink interface {
	Write(ctx context.Context, event *Event) error
	Close() error
}

// Config holds audit logger configuration.
type Config struct {
	Enabled      bool
	BufferSize   int
	RedactFields []string
}

// DefaultConfig returns default audit config.
func DefaultConfig() Config {
	return Config{
		Enabled:    true,
		BufferSize: 1000,
		RedactFields: []string{
			"password", "secret", "token", "api_key",
			"credit_card", "ssn", "private_key",
		},
	}
}

// NewLogger creates a new audit logger.
func NewLogger(config Config, baseLogger *slog.Logger) *Logger {
	if baseLogger == nil {
		baseLogger = slog.Default()
	}

	logger := &Logger{
		sinks:        make([]Sink, 0),
		enabled:      config.Enabled,
		redactFields: config.RedactFields,
		buffer:       make(chan *Event, config.BufferSize),
		bufferSize:   config.BufferSize,
		baseLogger:   baseLogger,
	}

	// Start background worker
	go logger.worker()

	return logger
}

// AddSink adds an audit sink.
func (l *Logger) AddSink(sink Sink) {
	l.sinksMu.Lock()
	defer l.sinksMu.Unlock()
	l.sinks = append(l.sinks, sink)
}

// Log logs an audit event.
func (l *Logger) Log(ctx context.Context, event *Event) {
	if !l.enabled {
		return
	}

	// Set defaults
	if event.ID == "" {
		event.ID = generateEventID()
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	// Redact sensitive fields
	l.redactEvent(event)

	// Queue for async writing
	select {
	case l.buffer <- event:
	default:
		// Buffer full, log synchronously with fallback
		l.baseLogger.Warn("audit buffer full, dropping event",
			slog.String("event_id", event.ID),
			slog.String("event_type", string(event.EventType)),
		)
	}
}

// LogSync logs an audit event synchronously.
func (l *Logger) LogSync(ctx context.Context, event *Event) error {
	if !l.enabled {
		return nil
	}

	if event.ID == "" {
		event.ID = generateEventID()
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	l.redactEvent(event)
	return l.writeToSinks(ctx, event)
}

func (l *Logger) worker() {
	for event := range l.buffer {
		ctx := context.Background()
		if err := l.writeToSinks(ctx, event); err != nil {
			l.baseLogger.Error("failed to write audit event",
				slog.String("event_id", event.ID),
				slog.String("error", err.Error()),
			)
		}
	}
}

func (l *Logger) writeToSinks(ctx context.Context, event *Event) error {
	l.sinksMu.RLock()
	sinks := l.sinks
	l.sinksMu.RUnlock()

	var lastErr error
	for _, sink := range sinks {
		if err := sink.Write(ctx, event); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func (l *Logger) redactEvent(event *Event) {
	if event.Details == nil {
		return
	}

	for _, field := range l.redactFields {
		if _, exists := event.Details[field]; exists {
			event.Details[field] = "[REDACTED]"
		}
	}
}

// Close closes the audit logger.
func (l *Logger) Close() error {
	close(l.buffer)

	l.sinksMu.Lock()
	defer l.sinksMu.Unlock()

	for _, sink := range l.sinks {
		sink.Close()
	}
	return nil
}

// ConsoleSink writes audit events to console.
type ConsoleSink struct {
	logger *slog.Logger
}

// NewConsoleSink creates a new console sink.
func NewConsoleSink(logger *slog.Logger) *ConsoleSink {
	return &ConsoleSink{logger: logger}
}

func (s *ConsoleSink) Write(ctx context.Context, event *Event) error {
	s.logger.Info("audit event",
		slog.String("event_id", event.ID),
		slog.String("event_type", string(event.EventType)),
		slog.String("action", string(event.Action)),
		slog.String("outcome", string(event.Outcome)),
		slog.String("actor_id", event.ActorID),
		slog.String("resource_type", event.ResourceType),
		slog.String("resource_id", event.ResourceID),
	)
	return nil
}

func (s *ConsoleSink) Close() error {
	return nil
}

// JSONFileSink writes audit events to a JSON file.
type JSONFileSink struct {
	file   chan []byte
	closed bool
	mu     sync.Mutex
}

func (s *JSONFileSink) Write(ctx context.Context, event *Event) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()

	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	select {
	case s.file <- data:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *JSONFileSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

// Helper functions

func generateEventID() string {
	return fmt.Sprintf("audit-%d", time.Now().UnixNano())
}

// EventBuilder helps build audit events.
type EventBuilder struct {
	event Event
}

// NewEventBuilder creates a new event builder.
func NewEventBuilder() *EventBuilder {
	return &EventBuilder{
		event: Event{
			Details: make(map[string]interface{}),
		},
	}
}

// WithType sets the event type.
func (b *EventBuilder) WithType(t EventType) *EventBuilder {
	b.event.EventType = t
	return b
}

// WithAction sets the action.
func (b *EventBuilder) WithAction(a Action) *EventBuilder {
	b.event.Action = a
	return b
}

// WithOutcome sets the outcome.
func (b *EventBuilder) WithOutcome(o Outcome) *EventBuilder {
	b.event.Outcome = o
	return b
}

// WithActor sets actor information.
func (b *EventBuilder) WithActor(id, actorType, ip string) *EventBuilder {
	b.event.ActorID = id
	b.event.ActorType = actorType
	b.event.ActorIP = ip
	return b
}

// WithResource sets resource information.
func (b *EventBuilder) WithResource(resourceType, id, name string) *EventBuilder {
	b.event.ResourceType = resourceType
	b.event.ResourceID = id
	b.event.ResourceName = name
	return b
}

// WithWorkspace sets workspace ID.
func (b *EventBuilder) WithWorkspace(id string) *EventBuilder {
	b.event.WorkspaceID = id
	return b
}

// WithDetail adds a detail.
func (b *EventBuilder) WithDetail(key string, value interface{}) *EventBuilder {
	b.event.Details[key] = value
	return b
}

// WithError sets error message.
func (b *EventBuilder) WithError(err error) *EventBuilder {
	if err != nil {
		b.event.ErrorMessage = err.Error()
	}
	return b
}

// Build returns the built event.
func (b *EventBuilder) Build() *Event {
	return &b.event
}
