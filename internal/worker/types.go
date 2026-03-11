package worker

import (
	"errors"
)

var (
	ErrExecutorNotFound    = errors.New("executor not found for node type")
	ErrTaskPollFailed      = errors.New("failed to poll for tasks")
	ErrExecutionFailed     = errors.New("task execution failed")
	ErrExecutionTimeout    = errors.New("task execution timed out")
	ErrServiceNotRunning   = errors.New("worker service is not running")
	ErrServiceAlreadyStart = errors.New("worker service already started")
)

type Task struct {
	TaskID           string `json:"task_id"`
	WorkflowID       string `json:"workflow_id"`
	RunID            string `json:"run_id"`
	Namespace        string `json:"namespace"`
	NodeType         string `json:"node_type"`
	NodeID           string `json:"node_id"`
	Config           []byte `json:"config"`
	Input            []byte `json:"input"`
	Attempt          int32  `json:"attempt"`
	TimeoutSec       int32  `json:"timeout_sec"`
	ScheduledEventID int64  `json:"scheduled_event_id"`
}

type TaskResult struct {
	TaskID    string `json:"task_id"`
	Output    []byte `json:"output"`
	Error     string `json:"error"`
	ErrorType string `json:"error_type"`
	Logs      []byte `json:"logs"`
}

const (
	ErrorTypeRetryable    = "RETRYABLE"
	ErrorTypeNonRetryable = "NON_RETRYABLE"
	ErrorTypeTimeout      = "TIMEOUT"
)
