package handler

import (
	"crypto/rand"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/linkflow/engine/internal/frontend"
)

const (
	// MaxRequestBodySize limits request body to 1MB to prevent memory exhaustion.
	MaxRequestBodySize = 1 << 20 // 1 MB
)

// Laravel will call these endpoints to interact with the engine.
type HTTPHandler struct {
	service *frontend.Service
	logger  *slog.Logger
}

// NewHTTPHandler creates a new HTTP handler.
func NewHTTPHandler(service *frontend.Service, logger *slog.Logger) *HTTPHandler {
	return &HTTPHandler{
		service: service,
		logger:  logger,
	}
}

// RegisterRoutes registers all HTTP routes.
func (h *HTTPHandler) RegisterRoutes(mux *http.ServeMux) {
	// Workflow execution endpoints - all wrapped with security middleware
	mux.HandleFunc("POST /api/v1/workflows/execute", h.securityMiddleware(h.StartWorkflow))
	mux.HandleFunc("GET /api/v1/workspaces/{workspace_id}/executions/{execution_id}", h.securityMiddleware(h.GetExecution))
	mux.HandleFunc("POST /api/v1/workspaces/{workspace_id}/executions/{execution_id}/cancel", h.securityMiddleware(h.CancelExecution))
	mux.HandleFunc("POST /api/v1/workspaces/{workspace_id}/executions/{execution_id}/retry", h.securityMiddleware(h.RetryExecution))
	mux.HandleFunc("POST /api/v1/workspaces/{workspace_id}/executions/{execution_id}/signal", h.securityMiddleware(h.SendSignal))

	// List executions
	mux.HandleFunc("GET /api/v1/workspaces/{workspace_id}/executions", h.securityMiddleware(h.ListExecutions))

	// Health check (no security middleware needed for health endpoints)
	mux.HandleFunc("GET /health", h.Health)
	mux.HandleFunc("GET /ready", h.Ready)
}

// securityMiddleware adds security headers and request limits to handlers.
func (h *HTTPHandler) securityMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Set security headers
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("X-XSS-Protection", "1; mode=block")
		w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
		w.Header().Set("Pragma", "no-cache")
		w.Header().Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
		w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")

		// Limit request body size to prevent memory exhaustion attacks
		if r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, MaxRequestBodySize)
		}

		next(w, r)
	}
}

// StartWorkflowRequest is the request to start a workflow.
type StartWorkflowRequest struct {
	WorkspaceID    string                 `json:"workspace_id"`
	WorkflowID     string                 `json:"workflow_id"`
	ExecutionID    string                 `json:"execution_id,omitempty"`
	IdempotencyKey string                 `json:"idempotency_key,omitempty"`
	Input          map[string]interface{} `json:"input"`
	TaskQueue      string                 `json:"task_queue,omitempty"`
	Priority       int                    `json:"priority,omitempty"`
	CallbackURL    string                 `json:"callback_url,omitempty"`
}

// StartWorkflowResponse is the response from starting a workflow.
type StartWorkflowResponse struct {
	ExecutionID string `json:"execution_id"`
	RunID       string `json:"run_id"`
	Started     bool   `json:"started"`
}

// POST /api/v1/workflows/execute.
func (h *HTTPHandler) StartWorkflow(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var req StartWorkflowRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		// Handle MaxBytesReader error specially
		if err.Error() == "http: request body too large" {
			h.writeError(w, http.StatusRequestEntityTooLarge, "Request body too large")
			return
		}
		h.writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	// Validate required fields
	if req.WorkspaceID == "" {
		h.writeError(w, http.StatusBadRequest, "workspace_id is required")
		return
	}
	if req.WorkflowID == "" {
		h.writeError(w, http.StatusBadRequest, "workflow_id is required")
		return
	}

	// Generate execution ID if not provided
	if req.ExecutionID == "" {
		req.ExecutionID = generateExecutionID()
	}

	// Start the workflow
	inputBytes, _ := json.Marshal(req.Input)
	frontendReq := &frontend.StartWorkflowExecutionRequest{
		Namespace:  req.WorkspaceID,
		WorkflowID: req.WorkflowID,
		TaskQueue:  req.TaskQueue,
		RequestID:  req.IdempotencyKey,
		Input:      inputBytes,
	}

	resp, err := h.service.StartWorkflowExecution(ctx, frontendReq)
	if err != nil {
		h.logger.Error("failed to start workflow",
			slog.String("workspace_id", req.WorkspaceID),
			slog.String("workflow_id", req.WorkflowID),
			slog.String("error", err.Error()),
		)
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.logger.Info("workflow started",
		slog.String("workspace_id", req.WorkspaceID),
		slog.String("workflow_id", req.WorkflowID),
		slog.String("execution_id", req.ExecutionID),
		slog.String("run_id", resp.RunID),
	)

	h.writeJSON(w, http.StatusOK, StartWorkflowResponse{
		ExecutionID: req.ExecutionID,
		RunID:       resp.RunID,
		Started:     true,
	})
}

// ExecutionInfo holds execution information.
type ExecutionInfo struct {
	ExecutionID string                 `json:"execution_id"`
	WorkflowID  string                 `json:"workflow_id"`
	RunID       string                 `json:"run_id"`
	Status      string                 `json:"status"`
	StartedAt   time.Time              `json:"started_at"`
	FinishedAt  *time.Time             `json:"finished_at,omitempty"`
	Input       map[string]interface{} `json:"input"`
	Output      map[string]interface{} `json:"output,omitempty"`
	Error       string                 `json:"error,omitempty"`
}

// GET /api/v1/workspaces/{workspace_id}/executions/{execution_id}.
func (h *HTTPHandler) GetExecution(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	workspaceID := r.PathValue("workspace_id")
	executionID := r.PathValue("execution_id")

	req := &frontend.GetExecutionRequest{
		Namespace:  workspaceID,
		WorkflowID: executionID,
		RunID:      "",
	}

	resp, err := h.service.GetExecution(ctx, req)
	if err != nil {
		h.writeError(w, http.StatusNotFound, "Execution not found")
		return
	}

	info := ExecutionInfo{
		ExecutionID: executionID,
		WorkflowID:  resp.Execution.WorkflowID,
		RunID:       resp.Execution.RunID,
		Status:      statusToString(resp.Execution.Status),
		StartedAt:   resp.Execution.StartTime,
	}

	h.writeJSON(w, http.StatusOK, info)
}

// GET /api/v1/workspaces/{workspace_id}/executions.
func (h *HTTPHandler) ListExecutions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	workspaceID := r.PathValue("workspace_id")

	req := &frontend.ListExecutionsRequest{
		Namespace: workspaceID,
		PageSize:  100,
	}

	resp, err := h.service.ListExecutions(ctx, req)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"executions": resp.Executions,
		"has_more":   len(resp.NextPageToken) > 0,
	})
}

// POST /api/v1/workspaces/{workspace_id}/executions/{execution_id}/cancel.
func (h *HTTPHandler) CancelExecution(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	workspaceID := r.PathValue("workspace_id")
	executionID := r.PathValue("execution_id")

	var body struct {
		Reason string `json:"reason"`
	}
	json.NewDecoder(r.Body).Decode(&body)

	req := &frontend.TerminateWorkflowExecutionRequest{
		Namespace:  workspaceID,
		WorkflowID: executionID,
		Reason:     body.Reason,
	}

	if err := h.service.TerminateWorkflowExecution(ctx, req); err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]string{"status": "canceled"})
}

// RetryExecutionRequest contains optional retry configuration.
type RetryExecutionRequest struct {
	MaxAttempts int    `json:"max_attempts,omitempty"`
	TaskQueue   string `json:"task_queue,omitempty"`
}

// RetryExecutionResponse is the response from retrying an execution.
type RetryExecutionResponse struct {
	ExecutionID         string `json:"execution_id"`
	RunID               string `json:"run_id"`
	OriginalExecutionID string `json:"original_execution_id"`
	Status              string `json:"status"`
}

// POST /api/v1/workspaces/{workspace_id}/executions/{execution_id}/retry.
func (h *HTTPHandler) RetryExecution(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	workspaceID := r.PathValue("workspace_id")
	executionID := r.PathValue("execution_id")

	if workspaceID == "" {
		h.writeError(w, http.StatusBadRequest, "workspace_id is required")
		return
	}
	if executionID == "" {
		h.writeError(w, http.StatusBadRequest, "execution_id is required")
		return
	}

	var retryReq RetryExecutionRequest
	if r.Body != nil && r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&retryReq); err != nil {
			if err.Error() == "http: request body too large" {
				h.writeError(w, http.StatusRequestEntityTooLarge, "Request body too large")
				return
			}
			h.writeError(w, http.StatusBadRequest, "Invalid request body")
			return
		}
	}

	getReq := &frontend.GetExecutionRequest{
		Namespace:  workspaceID,
		WorkflowID: executionID,
	}

	execResp, err := h.service.GetExecution(ctx, getReq)
	if err != nil {
		h.logger.Error("failed to get execution for retry",
			slog.String("workspace_id", workspaceID),
			slog.String("execution_id", executionID),
			slog.String("error", err.Error()),
		)
		h.writeError(w, http.StatusNotFound, "Execution not found")
		return
	}

	status := execResp.Execution.Status
	if status != frontend.ExecutionStatusFailed &&
		status != frontend.ExecutionStatusCanceled &&
		status != frontend.ExecutionStatusTerminated &&
		status != frontend.ExecutionStatusTimedOut {
		h.logger.Warn("retry attempted on non-retryable execution",
			slog.String("workspace_id", workspaceID),
			slog.String("execution_id", executionID),
			slog.String("status", statusToString(status)),
		)
		h.writeError(w, http.StatusConflict, "Only failed, canceled, terminated, or timed_out executions can be retried")
		return
	}

	descReq := &frontend.DescribeExecutionRequest{
		Namespace:  workspaceID,
		WorkflowID: executionID,
		RunID:      execResp.Execution.RunID,
	}

	descResp, err := h.service.DescribeExecution(ctx, descReq)
	if err != nil {
		h.logger.Error("failed to describe execution for retry",
			slog.String("workspace_id", workspaceID),
			slog.String("execution_id", executionID),
			slog.String("error", err.Error()),
		)
		h.writeError(w, http.StatusInternalServerError, "Failed to retrieve execution details")
		return
	}

	histReq := &frontend.GetHistoryRequest{
		NamespaceID:  workspaceID,
		WorkflowID:   executionID,
		RunID:        execResp.Execution.RunID,
		FirstEventID: 1,
		NextEventID:  2,
		PageSize:     1,
	}

	histResp, err := h.service.HistoryClient().GetHistory(ctx, histReq)
	if err != nil {
		h.logger.Error("failed to get execution history for retry",
			slog.String("workspace_id", workspaceID),
			slog.String("execution_id", executionID),
			slog.String("error", err.Error()),
		)
		h.writeError(w, http.StatusInternalServerError, "Failed to retrieve execution history")
		return
	}

	var originalInput []byte
	if len(histResp.Events) > 0 {
		originalInput = histResp.Events[0].Data
	}

	newExecutionID := generateExecutionID()
	taskQueue := descResp.Execution.TaskQueue
	if retryReq.TaskQueue != "" {
		taskQueue = retryReq.TaskQueue
	}

	startReq := &frontend.StartWorkflowExecutionRequest{
		Namespace:    workspaceID,
		WorkflowID:   newExecutionID,
		WorkflowType: descResp.Execution.WorkflowType,
		TaskQueue:    taskQueue,
		Input:        originalInput,
		Memo:         descResp.Execution.Memo,
	}

	if retryReq.MaxAttempts > 0 {
		startReq.RetryPolicy = &frontend.RetryPolicy{
			MaximumAttempts: int32(retryReq.MaxAttempts),
		}
	}

	resp, err := h.service.StartWorkflowExecution(ctx, startReq)
	if err != nil {
		h.logger.Error("failed to start retry execution",
			slog.String("workspace_id", workspaceID),
			slog.String("original_execution_id", executionID),
			slog.String("new_execution_id", newExecutionID),
			slog.String("error", err.Error()),
		)
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.logger.Info("execution retry started",
		slog.String("workspace_id", workspaceID),
		slog.String("original_execution_id", executionID),
		slog.String("new_execution_id", newExecutionID),
		slog.String("run_id", resp.RunID),
		slog.String("original_status", statusToString(status)),
	)

	h.writeJSON(w, http.StatusOK, RetryExecutionResponse{
		ExecutionID:         newExecutionID,
		RunID:               resp.RunID,
		OriginalExecutionID: executionID,
		Status:              "retry_initiated",
	})
}

// POST /api/v1/workspaces/{workspace_id}/executions/{execution_id}/signal.
func (h *HTTPHandler) SendSignal(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	workspaceID := r.PathValue("workspace_id")
	executionID := r.PathValue("execution_id")

	var body struct {
		SignalName string      `json:"signal_name"`
		Data       interface{} `json:"data"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	inputData, _ := json.Marshal(body.Data)

	req := &frontend.SignalWorkflowExecutionRequest{
		Namespace:  workspaceID,
		WorkflowID: executionID,
		SignalName: body.SignalName,
		Input:      inputData,
	}

	if err := h.service.SignalWorkflowExecution(ctx, req); err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]string{"status": "signal_sent"})
}

// Health check endpoint.
func (h *HTTPHandler) Health(w http.ResponseWriter, r *http.Request) {
	h.writeJSON(w, http.StatusOK, map[string]string{"status": "healthy"})
}

// Ready check endpoint.
func (h *HTTPHandler) Ready(w http.ResponseWriter, r *http.Request) {
	h.writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

// Helper functions

func (h *HTTPHandler) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func (h *HTTPHandler) writeError(w http.ResponseWriter, status int, message string) {
	h.writeJSON(w, status, map[string]string{"error": message})
}

func generateExecutionID() string {
	return "exec-" + randomString(16)
}

// of the specified length using crypto/rand.
func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	bytes := make([]byte, n)

	// Use crypto/rand for secure random generation
	if _, err := rand.Read(bytes); err != nil {
		// Fallback should never happen in practice, but handle gracefully
		panic("crypto/rand.Read failed: " + err.Error())
	}

	for i := range bytes {
		bytes[i] = letters[bytes[i]%byte(len(letters))]
	}
	return string(bytes)
}

func statusToString(status frontend.ExecutionStatus) string {
	switch status {
	case frontend.ExecutionStatusRunning:
		return "running"
	case frontend.ExecutionStatusCompleted:
		return "completed"
	case frontend.ExecutionStatusFailed:
		return "failed"
	case frontend.ExecutionStatusCanceled:
		return "canceled"
	case frontend.ExecutionStatusTerminated:
		return "terminated"
	case frontend.ExecutionStatusTimedOut:
		return "timed_out"
	default:
		return "pending"
	}
}
