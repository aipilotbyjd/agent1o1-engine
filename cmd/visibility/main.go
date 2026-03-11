package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/linkflow/engine/internal/version"
	"github.com/linkflow/engine/internal/visibility"
)

func main() {
	var (
		httpPort = flag.Int("http-port", 8085, "HTTP server port")
		dbURL    = flag.String("db-url", getEnv("DATABASE_URL", "postgres://linkflow:linkflow@localhost:5432/linkflow?sslmode=disable"), "Database URL")
	)
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	printBanner("Visibility", logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize PostgreSQL connection pool
	poolConfig, err := pgxpool.ParseConfig(*dbURL)
	if err != nil {
		logger.Error("failed to parse database URL", slog.String("error", err.Error()))
		os.Exit(1)
	}
	poolConfig.MaxConns = 25
	poolConfig.MinConns = 5

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		logger.Error("failed to connect to database", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer pool.Close()

	// Verify connection
	if err := pool.Ping(ctx); err != nil {
		logger.Error("failed to ping database", slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("connected to PostgreSQL")

	// Initialize visibility store and service
	store := visibility.NewPostgresStore(pool)
	svc := visibility.NewService(store, visibility.Config{Logger: logger})

	// Create HTTP handler
	handler := newVisibilityHandler(svc, logger)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		logger.Info("received signal, shutting down", slog.String("signal", sig.String()))
		cancel()
	}()

	// Start HTTP Server
	mux := http.NewServeMux()

	// Health endpoints
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) {
		if err := pool.Ping(context.Background()); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not ready", "error": "database unavailable"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})

	// Visibility API endpoints
	mux.HandleFunc("POST /api/v1/executions/started", handler.recordStarted)
	mux.HandleFunc("POST /api/v1/executions/closed", handler.recordClosed)
	mux.HandleFunc("GET /api/v1/executions/{namespaceId}/{workflowId}/{runId}", handler.getExecution)
	mux.HandleFunc("GET /api/v1/executions", handler.listExecutions)
	mux.HandleFunc("GET /api/v1/executions/count", handler.countExecutions)

	httpServer := &http.Server{
		Addr:              fmt.Sprintf(":%d", *httpPort),
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	go func() {
		logger.Info("starting HTTP server", slog.Int("port", *httpPort))
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http server failed", slog.String("error", err.Error()))
			cancel()
		}
	}()

	logger.Info("visibility service started", slog.Int("http_port", *httpPort))

	<-ctx.Done()

	// Graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	_ = httpServer.Shutdown(shutdownCtx)

	logger.Info("visibility service stopped")
}

// visibilityHandler handles HTTP requests for visibility operations
type visibilityHandler struct {
	svc    *visibility.Service
	logger *slog.Logger
}

func newVisibilityHandler(svc *visibility.Service, logger *slog.Logger) *visibilityHandler {
	return &visibilityHandler{svc: svc, logger: logger}
}

type recordStartedRequest struct {
	NamespaceID      string            `json:"namespace_id"`
	WorkflowID       string            `json:"workflow_id"`
	RunID            string            `json:"run_id"`
	WorkflowTypeName string            `json:"workflow_type_name"`
	TaskQueue        string            `json:"task_queue"`
	StartTime        *time.Time        `json:"start_time,omitempty"`
	Memo             json.RawMessage   `json:"memo,omitempty"`
	SearchAttributes map[string]string `json:"search_attributes,omitempty"`
	ParentWorkflowID string            `json:"parent_workflow_id,omitempty"`
	ParentRunID      string            `json:"parent_run_id,omitempty"`
}

func (h *visibilityHandler) recordStarted(w http.ResponseWriter, r *http.Request) {
	var req recordStartedRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}

	info := &visibility.ExecutionInfo{
		NamespaceID:      req.NamespaceID,
		WorkflowID:       req.WorkflowID,
		RunID:            req.RunID,
		WorkflowTypeName: req.WorkflowTypeName,
		TaskQueue:        req.TaskQueue,
		Memo:             req.Memo,
		ParentWorkflowID: req.ParentWorkflowID,
		ParentRunID:      req.ParentRunID,
	}

	if req.StartTime != nil {
		info.StartTime = *req.StartTime
	}

	// Convert search attributes
	if len(req.SearchAttributes) > 0 {
		info.SearchAttributes = make(map[string]interface{})
		for k, v := range req.SearchAttributes {
			info.SearchAttributes[k] = v
		}
	}

	if err := h.svc.RecordExecutionStarted(r.Context(), info); err != nil {
		h.logger.Error("failed to record execution started", slog.String("error", err.Error()))
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "recorded"})
}

type recordClosedRequest struct {
	NamespaceID string `json:"namespace_id"`
	WorkflowID  string `json:"workflow_id"`
	RunID       string `json:"run_id"`
	Status      string `json:"status"` // completed, failed, terminated, timed_out, canceled
	Reason      string `json:"reason,omitempty"`
}

func (h *visibilityHandler) recordClosed(w http.ResponseWriter, r *http.Request) {
	var req recordClosedRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}

	var recordErr error
	switch req.Status {
	case "completed":
		recordErr = h.svc.RecordExecutionCompleted(r.Context(), req.NamespaceID, req.WorkflowID, req.RunID, nil)
	case "failed":
		recordErr = h.svc.RecordExecutionFailed(r.Context(), req.NamespaceID, req.WorkflowID, req.RunID, req.Reason)
	case "terminated":
		recordErr = h.svc.RecordExecutionTerminated(r.Context(), req.NamespaceID, req.WorkflowID, req.RunID, req.Reason)
	case "timed_out":
		recordErr = h.svc.RecordExecutionTimedOut(r.Context(), req.NamespaceID, req.WorkflowID, req.RunID)
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid status"})
		return
	}

	if recordErr != nil {
		if recordErr == visibility.ErrExecutionNotFound {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "execution not found"})
			return
		}
		h.logger.Error("failed to record execution closed", slog.String("error", recordErr.Error()))
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": recordErr.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "recorded"})
}

func (h *visibilityHandler) getExecution(w http.ResponseWriter, r *http.Request) {
	namespaceID := r.PathValue("namespaceId")
	workflowID := r.PathValue("workflowId")
	runID := r.PathValue("runId")

	info, err := h.svc.GetExecution(r.Context(), namespaceID, workflowID, runID)
	if err != nil {
		if err == visibility.ErrExecutionNotFound {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "execution not found"})
			return
		}
		h.logger.Error("failed to get execution", slog.String("error", err.Error()))
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, toExecutionResponse(info))
}

func (h *visibilityHandler) listExecutions(w http.ResponseWriter, r *http.Request) {
	namespaceID := r.URL.Query().Get("namespace_id")
	if namespaceID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "namespace_id is required"})
		return
	}

	pageSize := int32(100)
	if ps := r.URL.Query().Get("page_size"); ps != "" {
		if parsed, err := strconv.ParseInt(ps, 10, 32); err == nil {
			pageSize = int32(parsed)
		}
	}

	req := &visibility.ListRequest{
		NamespaceID:   namespaceID,
		PageSize:      pageSize,
		NextPageToken: []byte(r.URL.Query().Get("next_page_token")),
		Query:         r.URL.Query().Get("query"),
	}

	resp, err := h.svc.ListExecutions(r.Context(), req)
	if err != nil {
		h.logger.Error("failed to list executions", slog.String("error", err.Error()))
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	executions := make([]executionResponse, len(resp.Executions))
	for i, exec := range resp.Executions {
		executions[i] = toExecutionResponse(exec)
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"executions":      executions,
		"next_page_token": string(resp.NextPageToken),
	})
}

func (h *visibilityHandler) countExecutions(w http.ResponseWriter, r *http.Request) {
	namespaceID := r.URL.Query().Get("namespace_id")
	if namespaceID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "namespace_id is required"})
		return
	}

	req := &visibility.CountRequest{
		NamespaceID: namespaceID,
		Query:       r.URL.Query().Get("query"),
	}

	resp, err := h.svc.CountExecutions(r.Context(), req)
	if err != nil {
		h.logger.Error("failed to count executions", slog.String("error", err.Error()))
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]int64{"count": resp.Count})
}

type executionResponse struct {
	NamespaceID      string                 `json:"namespace_id"`
	WorkflowID       string                 `json:"workflow_id"`
	RunID            string                 `json:"run_id"`
	WorkflowTypeName string                 `json:"workflow_type_name"`
	Status           string                 `json:"status"`
	StartTime        *time.Time             `json:"start_time,omitempty"`
	CloseTime        *time.Time             `json:"close_time,omitempty"`
	ExecutionTime    *time.Time             `json:"execution_time,omitempty"`
	TaskQueue        string                 `json:"task_queue"`
	SearchAttributes map[string]interface{} `json:"search_attributes,omitempty"`
	ParentWorkflowID string                 `json:"parent_workflow_id,omitempty"`
	ParentRunID      string                 `json:"parent_run_id,omitempty"`
}

func toExecutionResponse(info *visibility.ExecutionInfo) executionResponse {
	resp := executionResponse{
		NamespaceID:      info.NamespaceID,
		WorkflowID:       info.WorkflowID,
		RunID:            info.RunID,
		WorkflowTypeName: info.WorkflowTypeName,
		Status:           info.Status.String(),
		TaskQueue:        info.TaskQueue,
		SearchAttributes: info.SearchAttributes,
		ParentWorkflowID: info.ParentWorkflowID,
		ParentRunID:      info.ParentRunID,
	}

	if !info.StartTime.IsZero() {
		resp.StartTime = &info.StartTime
	}
	if !info.CloseTime.IsZero() {
		resp.CloseTime = &info.CloseTime
	}
	if !info.ExecutionTime.IsZero() {
		resp.ExecutionTime = &info.ExecutionTime
	}

	return resp
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func printBanner(service string, logger *slog.Logger) {
	logger.Info(fmt.Sprintf("LinkFlow %s Service", service),
		slog.String("version", version.Version),
		slog.String("commit", version.GitCommit),
		slog.String("build_time", version.BuildTime),
	)
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
