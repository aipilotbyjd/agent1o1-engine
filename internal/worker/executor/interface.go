package executor

import (
	"context"
	"encoding/json"
	"time"
)

type Executor interface {
	Execute(ctx context.Context, req *ExecuteRequest) (*ExecuteResponse, error)
	NodeType() string
}

type ExecuteRequest struct {
	NodeType      string
	NodeID        string
	WorkflowID    string
	RunID         string
	Namespace     string
	Config        json.RawMessage
	Input         json.RawMessage
	Deterministic *DeterministicContext
	Attempt       int32
	Timeout       time.Duration
}

type ExecuteResponse struct {
	Output                json.RawMessage
	Error                 *ExecutionError
	ConnectorAttempts     []ConnectorAttempt
	DeterministicFixtures []DeterministicFixture
	Logs                  []LogEntry
	Metadata              map[string]string // Optional executor metadata (e.g., timer_requested)
	Duration              time.Duration
}

type DeterministicContext struct {
	Mode              string                 `json:"mode"`
	Seed              string                 `json:"seed"`
	SourceExecutionID int                    `json:"source_execution_id"`
	Fixtures          []DeterministicFixture `json:"fixtures"`
}

type DeterministicFixture struct {
	RequestFingerprint string          `json:"request_fingerprint"`
	NodeID             string          `json:"node_id,omitempty"`
	NodeType           string          `json:"node_type,omitempty"`
	Request            json.RawMessage `json:"request,omitempty"`
	Response           json.RawMessage `json:"response,omitempty"`
}

type ConnectorAttempt struct {
	NodeID             string                 `json:"node_id,omitempty"`
	ConnectorKey       string                 `json:"connector_key"`
	ConnectorOperation string                 `json:"connector_operation"`
	Provider           string                 `json:"provider,omitempty"`
	AttemptNo          int32                  `json:"attempt_no"`
	IsRetry            bool                   `json:"is_retry"`
	Status             string                 `json:"status"`
	StatusCode         int32                  `json:"status_code,omitempty"`
	DurationMS         int64                  `json:"duration_ms,omitempty"`
	RequestFingerprint string                 `json:"request_fingerprint"`
	IdempotencyKey     string                 `json:"idempotency_key,omitempty"`
	ErrorCode          string                 `json:"error_code,omitempty"`
	ErrorMessage       string                 `json:"error_message,omitempty"`
	HappenedAt         time.Time              `json:"happened_at"`
	Meta               map[string]interface{} `json:"meta,omitempty"`
}

type ExecutionError struct {
	Message    string
	Type       string // RETRYABLE, NON_RETRYABLE, TIMEOUT
	StackTrace string
}

type LogEntry struct {
	Timestamp time.Time
	Level     string
	Message   string
}

const (
	ErrorTypeRetryable    = "RETRYABLE"
	ErrorTypeNonRetryable = "NON_RETRYABLE"
	ErrorTypeTimeout      = "TIMEOUT"
)
