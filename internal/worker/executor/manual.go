package executor

import (
	"context"
	"encoding/json"
	"time"
)

type ManualExecutor struct{}

func NewManualExecutor() *ManualExecutor {
	return &ManualExecutor{}
}

func (e *ManualExecutor) NodeType() string {
	return "trigger_manual"
}

func (e *ManualExecutor) Execute(ctx context.Context, req *ExecuteRequest) (*ExecuteResponse, error) {
	// Pass input to output
	// If input is empty, ensure valid JSON
	output := req.Input
	if len(output) == 0 {
		output = json.RawMessage("{}")
	}

	return &ExecuteResponse{
		Output: output,
		Logs: []LogEntry{
			{
				Timestamp: time.Now(),
				Level:     "INFO",
				Message:   "Manual trigger execution completed",
			},
		},
		Duration: time.Millisecond,
	}, nil
}
