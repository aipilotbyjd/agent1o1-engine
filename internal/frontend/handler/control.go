package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/linkflow/engine/internal/frontend"
	"github.com/redis/go-redis/v9"
)

// HealthDetails returns comprehensive engine health with dependency checks.
func (h *HTTPHandler) HealthDetails(w http.ResponseWriter, r *http.Request) {
	if h.healthChecker == nil {
		h.writeJSON(w, http.StatusOK, map[string]string{"status": "healthy", "detail": "no health checker configured"})
		return
	}

	status := h.healthChecker.Check(r.Context())
	httpStatus := http.StatusOK
	if status.Status != "healthy" {
		httpStatus = http.StatusServiceUnavailable
	}

	h.writeJSON(w, httpStatus, status)
}

// PartitionStats returns per-partition queue depth information.
func (h *HTTPHandler) PartitionStats(w http.ResponseWriter, r *http.Request) {
	if h.healthChecker == nil {
		h.writeJSON(w, http.StatusOK, map[string]string{"error": "health checker not configured"})
		return
	}

	stats := h.healthChecker.PartitionStats(r.Context())
	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"partitions": stats,
	})
}

// DLQHealth returns Dead Letter Queue size and status.
func (h *HTTPHandler) DLQHealth(w http.ResponseWriter, r *http.Request) {
	if h.healthChecker == nil {
		h.writeJSON(w, http.StatusOK, map[string]string{"error": "health checker not configured"})
		return
	}

	stats := h.healthChecker.DLQStats(r.Context(), h.dlqStreamKey)
	h.writeJSON(w, http.StatusOK, stats)
}

// ListDLQ returns entries from the Dead Letter Queue with pagination.
func (h *HTTPHandler) ListDLQ(w http.ResponseWriter, r *http.Request) {
	if h.redisClient == nil {
		h.writeError(w, http.StatusInternalServerError, "Redis client not configured")
		return
	}

	ctx := r.Context()
	countStr := r.URL.Query().Get("count")
	count := 50
	if countStr != "" {
		if parsed, err := strconv.Atoi(countStr); err == nil && parsed > 0 && parsed <= 200 {
			count = parsed
		}
	}

	// Read the raw entries from the DLQ stream
	entries, err := h.readDLQEntries(ctx, count)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	total, _ := h.redisClient.XLen(ctx, h.dlqStreamKey).Result()

	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"entries": entries,
		"total":   total,
		"stream":  h.dlqStreamKey,
	})
}

func (h *HTTPHandler) readDLQEntries(ctx context.Context, count int) ([]map[string]interface{}, error) {
	messages, err := h.redisClient.XRangeN(ctx, h.dlqStreamKey, "-", "+", int64(count)).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to read DLQ: %w", err)
	}

	entries := make([]map[string]interface{}, 0, len(messages))
	for _, msg := range messages {
		entry := map[string]interface{}{
			"id": msg.ID,
		}

		if payloadStr, ok := msg.Values["payload"].(string); ok {
			var parsed map[string]interface{}
			if err := json.Unmarshal([]byte(payloadStr), &parsed); err == nil {
				entry["data"] = parsed
			} else {
				entry["raw_payload"] = payloadStr
			}
		}

		entries = append(entries, entry)
	}

	return entries, nil
}

// ReplayDLQ re-queues a DLQ entry back to its original partition.
func (h *HTTPHandler) ReplayDLQ(w http.ResponseWriter, r *http.Request) {
	if h.redisClient == nil {
		h.writeError(w, http.StatusInternalServerError, "Redis client not configured")
		return
	}

	ctx := r.Context()
	messageID := r.PathValue("message_id")
	if messageID == "" {
		h.writeError(w, http.StatusBadRequest, "message_id is required")
		return
	}

	// Read the specific message
	messages, err := h.redisClient.XRangeN(ctx, h.dlqStreamKey, messageID, messageID, 1).Result()
	if err != nil || len(messages) == 0 {
		h.writeError(w, http.StatusNotFound, "DLQ message not found")
		return
	}

	msg := messages[0]
	payloadStr, ok := msg.Values["payload"].(string)
	if !ok {
		h.writeError(w, http.StatusInternalServerError, "Invalid DLQ entry format")
		return
	}

	// Parse the DLQ entry to find original stream
	var dlqEntry frontend.DLQEntry
	if err := json.Unmarshal([]byte(payloadStr), &dlqEntry); err != nil {
		h.writeError(w, http.StatusInternalServerError, "Failed to parse DLQ entry")
		return
	}

	// Re-queue to original stream
	_, err = h.redisClient.XAdd(ctx, &redis.XAddArgs{
		Stream: dlqEntry.OriginalStream,
		Values: map[string]interface{}{"payload": dlqEntry.OriginalPayload},
	}).Result()
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to re-queue: %s", err.Error()))
		return
	}

	// Remove from DLQ
	h.redisClient.XDel(ctx, h.dlqStreamKey, messageID)

	h.logger.Info("replayed DLQ message",
		"message_id", messageID,
		"original_stream", dlqEntry.OriginalStream,
		"job_id", dlqEntry.JobID,
	)

	h.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":          "replayed",
		"message_id":      messageID,
		"original_stream": dlqEntry.OriginalStream,
		"job_id":          dlqEntry.JobID,
	})
}

// PauseExecution sends a pause signal to a running execution.
func (h *HTTPHandler) PauseExecution(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	workspaceID := r.PathValue("workspace_id")
	executionID := r.PathValue("execution_id")

	req := &frontend.SignalWorkflowExecutionRequest{
		Namespace:  workspaceID,
		WorkflowID: executionID,
		SignalName: "system:pause",
		Input:      []byte(`{"action":"pause"}`),
	}

	if err := h.service.SignalWorkflowExecution(ctx, req); err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]string{"status": "pause_signal_sent"})
}

// ResumeExecution sends a resume signal to a paused execution.
func (h *HTTPHandler) ResumeExecution(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	workspaceID := r.PathValue("workspace_id")
	executionID := r.PathValue("execution_id")

	req := &frontend.SignalWorkflowExecutionRequest{
		Namespace:  workspaceID,
		WorkflowID: executionID,
		SignalName: "system:resume",
		Input:      []byte(`{"action":"resume"}`),
	}

	if err := h.service.SignalWorkflowExecution(ctx, req); err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.writeJSON(w, http.StatusOK, map[string]string{"status": "resume_signal_sent"})
}

// CacheStats returns workflow cache statistics.
func (h *HTTPHandler) CacheStats(w http.ResponseWriter, r *http.Request) {
	stats := map[string]interface{}{
		"workflow_cache": map[string]interface{}{},
	}

	if h.workflowCache != nil {
		stats["workflow_cache"] = h.workflowCache.Stats()
	}

	h.writeJSON(w, http.StatusOK, stats)
}

// CacheInvalidate invalidates a cached workflow by version hash.
func (h *HTTPHandler) CacheInvalidate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		VersionHash string `json:"version_hash"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		h.writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if body.VersionHash == "" {
		h.writeError(w, http.StatusBadRequest, "version_hash is required")
		return
	}

	if h.workflowCache != nil {
		h.workflowCache.Invalidate(body.VersionHash)
	}

	h.writeJSON(w, http.StatusOK, map[string]string{
		"status":       "invalidated",
		"version_hash": body.VersionHash,
	})
}
