package frontend

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/redis/go-redis/v9"
)

// EventPublisher publishes real-time execution events via Redis Pub/Sub
// so the Laravel API can forward them to the browser via SSE.
type EventPublisher struct {
	client *redis.Client
	logger *slog.Logger
}

// ExecutionEvent is published to Redis Pub/Sub for real-time streaming.
type ExecutionEvent struct {
	Event       string                 `json:"event"`
	ExecutionID int                    `json:"execution_id"`
	WorkflowID  int                    `json:"workflow_id"`
	WorkspaceID int                    `json:"workspace_id"`
	NodeID      string                 `json:"node_id,omitempty"`
	NodeType    string                 `json:"node_type,omitempty"`
	NodeName    string                 `json:"node_name,omitempty"`
	Status      string                 `json:"status,omitempty"`
	Progress    int                    `json:"progress,omitempty"`
	DurationMs  int64                  `json:"duration_ms,omitempty"`
	Error       string                 `json:"error,omitempty"`
	Output      map[string]interface{} `json:"output,omitempty"`
	Data        map[string]interface{} `json:"data,omitempty"`
	Timestamp   string                 `json:"timestamp"`
}

// NewEventPublisher creates a new EventPublisher.
func NewEventPublisher(client *redis.Client, logger *slog.Logger) *EventPublisher {
	return &EventPublisher{
		client: client,
		logger: logger,
	}
}

// Publish sends an event to a Redis Pub/Sub channel AND the Redis Stream.
// Both channels allow Laravel to pick up the event:
//   - Pub/Sub: for instant SSE push (if Laravel is subscribed)
//   - Stream:  for reliable SSE catch-up (if the client reconnects)
func (p *EventPublisher) Publish(ctx context.Context, event *ExecutionEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Publish to Pub/Sub channel for instant push
	pubsubChannel := fmt.Sprintf("linkflow:execution:%d:live", event.ExecutionID)
	if err := p.client.Publish(ctx, pubsubChannel, string(payload)).Err(); err != nil {
		p.logger.Warn("failed to publish to pubsub",
			slog.String("channel", pubsubChannel),
			slog.String("error", err.Error()),
		)
	}

	// Also write to stream for reliable catch-up
	streamKey := fmt.Sprintf("execution:%d:events", event.ExecutionID)
	if err := p.client.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		Values: map[string]interface{}{"payload": string(payload)},
	}).Err(); err != nil {
		p.logger.Warn("failed to write to event stream",
			slog.String("stream", streamKey),
			slog.String("error", err.Error()),
		)
	}
	// Expire stream after 5 minutes
	p.client.Expire(ctx, streamKey, 300_000_000_000) // 300s in nanoseconds

	p.logger.Debug("published execution event",
		slog.String("event", event.Event),
		slog.Int("execution_id", event.ExecutionID),
	)

	return nil
}

// PublishNodeStarted emits a node.started event.
func (p *EventPublisher) PublishNodeStarted(ctx context.Context, executionID, workflowID, workspaceID int, nodeID, nodeType, nodeName string) {
	p.Publish(ctx, &ExecutionEvent{
		Event:       "node.started",
		ExecutionID: executionID,
		WorkflowID:  workflowID,
		WorkspaceID: workspaceID,
		NodeID:      nodeID,
		NodeType:    nodeType,
		NodeName:    nodeName,
		Status:      "running",
	})
}

// PublishNodeCompleted emits a node.completed event.
func (p *EventPublisher) PublishNodeCompleted(ctx context.Context, executionID, workflowID, workspaceID int, nodeID, nodeType, nodeName string, output map[string]interface{}, durationMs int64) {
	p.Publish(ctx, &ExecutionEvent{
		Event:       "node.completed",
		ExecutionID: executionID,
		WorkflowID:  workflowID,
		WorkspaceID: workspaceID,
		NodeID:      nodeID,
		NodeType:    nodeType,
		NodeName:    nodeName,
		Status:      "completed",
		DurationMs:  durationMs,
		Output:      output,
	})
}

// PublishNodeFailed emits a node.failed event.
func (p *EventPublisher) PublishNodeFailed(ctx context.Context, executionID, workflowID, workspaceID int, nodeID, nodeType, nodeName, errMsg string, durationMs int64) {
	p.Publish(ctx, &ExecutionEvent{
		Event:       "node.failed",
		ExecutionID: executionID,
		WorkflowID:  workflowID,
		WorkspaceID: workspaceID,
		NodeID:      nodeID,
		NodeType:    nodeType,
		NodeName:    nodeName,
		Status:      "failed",
		DurationMs:  durationMs,
		Error:       errMsg,
	})
}

// PublishExecutionProgress emits a progress update during execution.
func (p *EventPublisher) PublishExecutionProgress(ctx context.Context, executionID, workflowID, workspaceID, progress int, currentNode string) {
	p.Publish(ctx, &ExecutionEvent{
		Event:       "execution.progress",
		ExecutionID: executionID,
		WorkflowID:  workflowID,
		WorkspaceID: workspaceID,
		Progress:    progress,
		NodeID:      currentNode,
	})
}
