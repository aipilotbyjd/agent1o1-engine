package executor

import (
	"context"
	"time"
)

type CodeExecutor struct {
	// For future WASM/container execution
}

func NewCodeExecutor() *CodeExecutor {
	return &CodeExecutor{}
}

func (e *CodeExecutor) NodeType() string {
	return "code"
}

func (e *CodeExecutor) Execute(ctx context.Context, req *ExecuteRequest) (*ExecuteResponse, error) {
	start := time.Now()

	return &ExecuteResponse{
		Error: &ExecutionError{
			Message: "code execution is not yet implemented",
			Type:    ErrorTypeNonRetryable,
		},
		Logs: []LogEntry{
			{
				Timestamp: time.Now(),
				Level:     "WARN",
				Message:   "Code executor is a placeholder - WASM/container execution not implemented",
			},
		},
		Duration: time.Since(start),
	}, nil
}
