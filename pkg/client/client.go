// Package client provides a Go SDK for connecting to the LinkFlow Engine
// This can be used by Laravel (via FFI) or any other Go-based service
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Client is the LinkFlow Engine client.
type Client struct {
	baseURL    string
	httpClient *http.Client
	apiKey     string
}

// Config holds client configuration.
type Config struct {
	BaseURL string        // e.g., "http://localhost:7233" or "http://engine:7233"
	APIKey  string        // Authentication key
	Timeout time.Duration // Request timeout
}

// DefaultConfig returns default client configuration.
func DefaultConfig() Config {
	return Config{
		BaseURL: "http://localhost:7233",
		Timeout: 30 * time.Second,
	}
}

// New creates a new LinkFlow client.
func New(cfg Config) *Client {
	if cfg.Timeout == 0 {
		cfg.Timeout = 30 * time.Second
	}
	return &Client{
		baseURL: cfg.BaseURL,
		apiKey:  cfg.APIKey,
		httpClient: &http.Client{
			Timeout: cfg.Timeout,
		},
	}
}

// --- Workflow Execution ---

// StartWorkflowRequest is the request to start a workflow.
type StartWorkflowRequest struct {
	WorkspaceID    string                 `json:"workspace_id"`
	WorkflowID     string                 `json:"workflow_id"`
	ExecutionID    string                 `json:"execution_id,omitempty"`    // Optional, auto-generated if empty
	IdempotencyKey string                 `json:"idempotency_key,omitempty"` // Prevent duplicate executions
	Input          map[string]interface{} `json:"input"`                     // Trigger data
	TaskQueue      string                 `json:"task_queue,omitempty"`      // Optional worker routing
	Priority       int                    `json:"priority,omitempty"`        // Execution priority
	CallbackURL    string                 `json:"callback_url,omitempty"`    // URL to call when complete
}

// StartWorkflowResponse is the response from starting a workflow.
type StartWorkflowResponse struct {
	ExecutionID string `json:"execution_id"`
	RunID       string `json:"run_id"`
	Started     bool   `json:"started"` // false if idempotent duplicate
}

// StartWorkflow starts a new workflow execution.
func (c *Client) StartWorkflow(ctx context.Context, req *StartWorkflowRequest) (*StartWorkflowResponse, error) {
	var resp StartWorkflowResponse
	err := c.post(ctx, "/api/v1/workflows/execute", req, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// --- Execution Status ---

// GetExecutionRequest is the request to get execution status.
type GetExecutionRequest struct {
	WorkspaceID string `json:"workspace_id"`
	ExecutionID string `json:"execution_id"`
}

// ExecutionStatus represents execution status.
type ExecutionStatus string

const (
	ExecutionStatusPending   ExecutionStatus = "pending"
	ExecutionStatusRunning   ExecutionStatus = "running"
	ExecutionStatusCompleted ExecutionStatus = "completed"
	ExecutionStatusFailed    ExecutionStatus = "failed"
	ExecutionStatusCanceled  ExecutionStatus = "canceled"
	ExecutionStatusTimedOut  ExecutionStatus = "timed_out"
)

// ExecutionInfo holds execution information.
type ExecutionInfo struct {
	ExecutionID string                 `json:"execution_id"`
	WorkflowID  string                 `json:"workflow_id"`
	RunID       string                 `json:"run_id"`
	Status      ExecutionStatus        `json:"status"`
	StartedAt   time.Time              `json:"started_at"`
	FinishedAt  *time.Time             `json:"finished_at,omitempty"`
	Input       map[string]interface{} `json:"input"`
	Output      map[string]interface{} `json:"output,omitempty"`
	Error       string                 `json:"error,omitempty"`
	NodeResults []NodeResult           `json:"node_results,omitempty"`
}

// NodeResult holds individual node execution result.
type NodeResult struct {
	NodeID     string                 `json:"node_id"`
	NodeType   string                 `json:"node_type"`
	Status     string                 `json:"status"`
	StartedAt  time.Time              `json:"started_at"`
	FinishedAt *time.Time             `json:"finished_at,omitempty"`
	Output     map[string]interface{} `json:"output,omitempty"`
	Error      string                 `json:"error,omitempty"`
	Attempt    int                    `json:"attempt"`
}

// GetExecution gets execution status.
func (c *Client) GetExecution(ctx context.Context, workspaceID, executionID string) (*ExecutionInfo, error) {
	var resp ExecutionInfo
	url := fmt.Sprintf("/api/v1/workspaces/%s/executions/%s", workspaceID, executionID)
	err := c.get(ctx, url, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// --- Execution Control ---

// CancelExecution cancels a running execution.
func (c *Client) CancelExecution(ctx context.Context, workspaceID, executionID, reason string) error {
	url := fmt.Sprintf("/api/v1/workspaces/%s/executions/%s/cancel", workspaceID, executionID)
	return c.post(ctx, url, map[string]string{"reason": reason}, nil)
}

// RetryExecution retries a failed execution.
func (c *Client) RetryExecution(ctx context.Context, workspaceID, executionID string) (*StartWorkflowResponse, error) {
	var resp StartWorkflowResponse
	url := fmt.Sprintf("/api/v1/workspaces/%s/executions/%s/retry", workspaceID, executionID)
	err := c.post(ctx, url, nil, &resp)
	return &resp, err
}

// --- Signal/Event ---

// SendSignal sends a signal to a running execution.
func (c *Client) SendSignal(ctx context.Context, workspaceID, executionID, signalName string, data interface{}) error {
	url := fmt.Sprintf("/api/v1/workspaces/%s/executions/%s/signal", workspaceID, executionID)
	return c.post(ctx, url, map[string]interface{}{
		"signal_name": signalName,
		"data":        data,
	}, nil)
}

// --- HTTP Helpers ---

func (c *Client) get(ctx context.Context, path string, result interface{}) error {
	req, err := http.NewRequestWithContext(ctx, "GET", c.baseURL+path, http.NoBody)
	if err != nil {
		return err
	}
	return c.do(req, result)
}

func (c *Client) post(ctx context.Context, path string, body, result interface{}) error {
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		bodyReader = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+path, bodyReader)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	return c.do(req, result)
}

func (c *Client) do(req *http.Request, result interface{}) error {
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		errBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("API error %d: %s", resp.StatusCode, string(errBody))
	}

	if result != nil {
		if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
			return fmt.Errorf("failed to decode response: %w", err)
		}
	}

	return nil
}
