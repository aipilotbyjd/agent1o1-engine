package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// DelayExecutor handles delay/wait nodes.
type DelayExecutor struct{}

// DelayConfig represents the configuration for a delay node.
type DelayConfig struct {
	// Duration in various units (only one should be set)
	Seconds      int `json:"seconds"`
	Minutes      int `json:"minutes"`
	Hours        int `json:"hours"`
	Days         int `json:"days"`
	Milliseconds int `json:"milliseconds"`

	// Alternative: duration string (e.g., "5m", "1h30m")
	Duration string `json:"duration"`

	// Alternative: wait until a specific time
	Until string `json:"until"` // RFC3339 format
}

// DelayResponse represents the result of a delay.
type DelayResponse struct {
	StartedAt  string `json:"started_at"`
	EndedAt    string `json:"ended_at"`
	Duration   string `json:"duration"`
	DurationMs int64  `json:"duration_ms"`
}

// NewDelayExecutor creates a new delay executor.
func NewDelayExecutor() *DelayExecutor {
	return &DelayExecutor{}
}

func (e *DelayExecutor) NodeType() string {
	return "delay"
}

func (e *DelayExecutor) Execute(ctx context.Context, req *ExecuteRequest) (*ExecuteResponse, error) {
	start := time.Now()
	logs := make([]LogEntry, 0)

	logs = append(logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "INFO",
		Message:   fmt.Sprintf("Starting delay execution for node %s", req.NodeID),
	})

	var config DelayConfig
	if err := json.Unmarshal(req.Config, &config); err != nil {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("failed to parse delay config: %v", err),
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	// Calculate the delay duration
	var delayDuration time.Duration

	switch {
	case config.Until != "":
		// Wait until a specific time
		untilTime, err := time.Parse(time.RFC3339, config.Until)
		if err != nil {
			return &ExecuteResponse{
				Error: &ExecutionError{
					Message: fmt.Sprintf("invalid 'until' time format: %v", err),
					Type:    ErrorTypeNonRetryable,
				},
				Logs:     logs,
				Duration: time.Since(start),
			}, nil
		}
		delayDuration = time.Until(untilTime)
		if delayDuration < 0 {
			delayDuration = 0 // Already past the time
		}
	case config.Duration != "":
		// Parse duration string
		var err error
		delayDuration, err = time.ParseDuration(config.Duration)
		if err != nil {
			return &ExecuteResponse{
				Error: &ExecutionError{
					Message: fmt.Sprintf("invalid duration format: %v", err),
					Type:    ErrorTypeNonRetryable,
				},
				Logs:     logs,
				Duration: time.Since(start),
			}, nil
		}
	default:
		// Calculate from individual components
		delayDuration = time.Duration(config.Milliseconds)*time.Millisecond +
			time.Duration(config.Seconds)*time.Second +
			time.Duration(config.Minutes)*time.Minute +
			time.Duration(config.Hours)*time.Hour +
			time.Duration(config.Days)*24*time.Hour
	}

	// Validate the delay
	if delayDuration < 0 {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: "delay duration cannot be negative",
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	// Cap maximum delay for safety (72 hours)
	maxDelay := 72 * time.Hour
	if delayDuration > maxDelay {
		logs = append(logs, LogEntry{
			Timestamp: time.Now(),
			Level:     "WARN",
			Message:   fmt.Sprintf("Delay capped to maximum of %v (was %v)", maxDelay, delayDuration),
		})
		delayDuration = maxDelay
	}

	// For short delays (≤30s), block in-process to avoid timer overhead
	const shortDelayThreshold = 30 * time.Second

	if delayDuration <= shortDelayThreshold {
		logs = append(logs, LogEntry{
			Timestamp: time.Now(),
			Level:     "INFO",
			Message:   fmt.Sprintf("Short delay: waiting %v in-process", delayDuration),
		})

		select {
		case <-ctx.Done():
			return &ExecuteResponse{
				Error: &ExecutionError{
					Message: "delay was canceled",
					Type:    ErrorTypeNonRetryable,
				},
				Logs:     logs,
				Duration: time.Since(start),
			}, nil
		case <-time.After(delayDuration):
			// Delay completed
		}

		return e.buildCompletedResponse(start, logs)
	}

	// For long delays (>30s), return immediately with timer metadata.
	// This avoids blocking a worker goroutine for extended periods.
	// The workflow executor should use the Timer service to schedule a wake-up.
	logs = append(logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "INFO",
		Message:   fmt.Sprintf("Long delay requested (%v): returning timer request instead of blocking", delayDuration),
	})

	resumeAt := time.Now().Add(delayDuration)
	timerResponse := map[string]interface{}{
		"timer_requested": true,
		"delay_duration":  delayDuration.String(),
		"delay_seconds":   int64(delayDuration.Seconds()),
		"resume_at":       resumeAt.Format(time.RFC3339),
		"started_at":      start.Format(time.RFC3339),
	}

	output, err := json.Marshal(timerResponse)
	if err != nil {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("failed to marshal timer response: %v", err),
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	return &ExecuteResponse{
		Output: output,
		Logs:   logs,
		Metadata: map[string]string{
			"timer_requested":  "true",
			"timer_duration_s": strconv.FormatInt(int64(delayDuration.Seconds()), 10),
			"resume_at":        resumeAt.Format(time.RFC3339),
		},
		Duration: time.Since(start),
	}, nil
}

func (e *DelayExecutor) buildCompletedResponse(start time.Time, logs []LogEntry) (*ExecuteResponse, error) {
	endTime := time.Now()

	logs = append(logs, LogEntry{
		Timestamp: endTime,
		Level:     "INFO",
		Message:   "Delay completed",
	})

	response := DelayResponse{
		StartedAt:  start.Format(time.RFC3339),
		EndedAt:    endTime.Format(time.RFC3339),
		Duration:   time.Since(start).String(),
		DurationMs: time.Since(start).Milliseconds(),
	}

	output, err := json.Marshal(response)
	if err != nil {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("failed to marshal response: %v", err),
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	return &ExecuteResponse{
		Output:   output,
		Logs:     logs,
		Duration: time.Since(start),
	}, nil
}
