package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// ApprovalExecutor creates a deterministic pause point for human-in-the-loop review.
// It intentionally returns a non-retryable approval-required error so the API can
// generate an inbox item and resume using a new execution once approved.
type ApprovalExecutor struct{}

type ApprovalConfig struct {
	Title       string                 `json:"title"`
	Description string                 `json:"description"`
	Payload     map[string]interface{} `json:"payload"`
}

func NewApprovalExecutor() *ApprovalExecutor {
	return &ApprovalExecutor{}
}

func (e *ApprovalExecutor) NodeType() string {
	return "action_approval"
}

func (e *ApprovalExecutor) Execute(ctx context.Context, req *ExecuteRequest) (*ExecuteResponse, error) {
	_ = ctx

	config := ApprovalConfig{}
	_ = json.Unmarshal(req.Config, &config)

	title := config.Title
	if title == "" {
		title = "Approval required"
	}

	message := fmt.Sprintf("APPROVAL_REQUIRED: %s", title)
	if config.Description != "" {
		message = fmt.Sprintf("%s - %s", message, config.Description)
	}

	return &ExecuteResponse{
		Output: nil,
		Error: &ExecutionError{
			Message: message,
			Type:    ErrorTypeNonRetryable,
		},
		Logs: []LogEntry{
			{
				Timestamp: time.Now().UTC(),
				Level:     "INFO",
				Message:   "approval checkpoint reached",
			},
		},
		Duration: 0,
	}, nil
}
