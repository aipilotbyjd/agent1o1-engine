package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// ScriptExecutor handles action_script nodes.
// This is an alias/wrapper that can execute JavaScript-like expressions.
type ScriptExecutor struct{}

// NewScriptExecutor creates a new script executor.
func NewScriptExecutor() *ScriptExecutor {
	return &ScriptExecutor{}
}

func (e *ScriptExecutor) NodeType() string {
	return "action_script"
}

func (e *ScriptExecutor) Execute(ctx context.Context, req *ExecuteRequest) (*ExecuteResponse, error) {
	start := time.Now()
	logs := make([]LogEntry, 0)

	logs = append(logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "info",
		Message:   fmt.Sprintf("executing script node %s", req.NodeID),
	})

	// Parse script configuration
	var config struct {
		Code     string `json:"code"`
		Language string `json:"language"` // javascript, python (future)
		Timeout  int    `json:"timeout"`  // seconds
	}

	if err := json.Unmarshal(req.Config, &config); err != nil {
		// If no config, treat as passthrough
		logs = append(logs, LogEntry{
			Timestamp: time.Now(),
			Level:     "warn",
			Message:   "no script configuration provided, passing input through",
		})

		return &ExecuteResponse{
			Output:   req.Input,
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	logs = append(logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "warn",
		Message:   fmt.Sprintf("script executor not yet available (language: %s, code length: %d chars)", config.Language, len(config.Code)),
	})

	return &ExecuteResponse{
		Error: &ExecutionError{
			Message: "script execution is not yet available — sandboxed runtime required",
			Type:    ErrorTypeNonRetryable,
		},
		Logs:     logs,
		Duration: time.Since(start),
	}, nil
}
