package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// OutputExecutor handles output_log nodes.
// This node logs data and passes it through to the next node.
type OutputExecutor struct{}

// NewOutputExecutor creates a new output executor.
func NewOutputExecutor() *OutputExecutor {
	return &OutputExecutor{}
}

func (e *OutputExecutor) NodeType() string {
	return "output_log"
}

func (e *OutputExecutor) Execute(ctx context.Context, req *ExecuteRequest) (*ExecuteResponse, error) {
	start := time.Now()
	logs := make([]LogEntry, 0)

	logs = append(logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "info",
		Message:   fmt.Sprintf("output_log node %s started", req.NodeID),
	})

	// Parse output configuration
	var config struct {
		Label   string   `json:"label"`
		Level   string   `json:"level"` // info, warn, error, debug
		Message string   `json:"message"`
		Fields  []string `json:"fields"` // specific fields to log
	}

	if err := json.Unmarshal(req.Config, &config); err != nil {
		// Default config if not provided
		config.Label = "Output"
		config.Level = "info"
	}

	if config.Level == "" {
		config.Level = "info"
	}

	// Parse input data
	var inputData map[string]interface{}
	if len(req.Input) > 0 {
		if err := json.Unmarshal(req.Input, &inputData); err != nil {
			inputData = make(map[string]interface{})
		}
	} else {
		inputData = make(map[string]interface{})
	}

	// Log the data
	var logMessage string
	switch {
	case config.Message != "":
		logMessage = config.Message
	case config.Label != "":
		logMessage = fmt.Sprintf("[%s] Output data received", config.Label)
	default:
		logMessage = "Output data received"
	}

	logs = append(logs, LogEntry{
		Timestamp: time.Now(),
		Level:     config.Level,
		Message:   logMessage,
	})

	// Log specific fields or all data
	if len(config.Fields) > 0 {
		for _, field := range config.Fields {
			if val, exists := inputData[field]; exists {
				logs = append(logs, LogEntry{
					Timestamp: time.Now(),
					Level:     "debug",
					Message:   fmt.Sprintf("  %s: %v", field, val),
				})
			}
		}
	} else {
		// Log all input data summary
		dataJSON, _ := json.MarshalIndent(inputData, "", "  ")
		logs = append(logs, LogEntry{
			Timestamp: time.Now(),
			Level:     "debug",
			Message:   fmt.Sprintf("data: %s", string(dataJSON)),
		})
	}

	// Create output with metadata
	result := map[string]interface{}{
		"logged":      true,
		"logged_at":   time.Now().UTC().Format(time.RFC3339),
		"node_id":     req.NodeID,
		"label":       config.Label,
		"input":       inputData,
		"field_count": len(inputData),
	}

	output, err := json.Marshal(result)
	if err != nil {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("failed to marshal output: %v", err),
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	logs = append(logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "info",
		Message:   "output_log completed successfully",
	})

	return &ExecuteResponse{
		Output:   output,
		Logs:     logs,
		Duration: time.Since(start),
	}, nil
}
