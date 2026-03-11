package matching

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	commonv1 "github.com/linkflow/engine/api/gen/linkflow/common/v1"
	matchingv1 "github.com/linkflow/engine/api/gen/linkflow/matching/v1"
	"github.com/linkflow/engine/internal/matching/engine"
)

type GRPCServer struct {
	matchingv1.UnimplementedMatchingServiceServer
	service *Service
}

func NewGRPCServer(service *Service) *GRPCServer {
	return &GRPCServer{service: service}
}

// generateTaskID creates a deterministic task ID from workflow identity and event.
// This ensures uniqueness and idempotency for task scheduling.
func generateTaskID(namespace, workflowID, runID string, taskType int32, scheduledEventID int64) string {
	return fmt.Sprintf("%s:%s:%s:%d:%d", namespace, workflowID, runID, taskType, scheduledEventID)
}

// generateSecureToken creates a cryptographically secure random token.
func generateSecureToken() ([]byte, error) {
	token := make([]byte, 32)
	if _, err := rand.Read(token); err != nil {
		return nil, fmt.Errorf("failed to generate secure token: %w", err)
	}
	return []byte(hex.EncodeToString(token)), nil
}

func (s *GRPCServer) AddTask(ctx context.Context, req *matchingv1.AddTaskRequest) (*matchingv1.AddTaskResponse, error) {
	// Validate required fields
	if req.WorkflowExecution == nil {
		return nil, fmt.Errorf("workflow_execution is required")
	}
	if req.WorkflowExecution.GetWorkflowId() == "" {
		return nil, fmt.Errorf("workflow_id is required")
	}

	// Generate deterministic task ID from workflow identity for idempotency
	taskID := generateTaskID(
		req.Namespace,
		req.WorkflowExecution.GetWorkflowId(),
		req.WorkflowExecution.GetRunId(),
		int32(req.TaskType),
		req.ScheduledEventId,
	)

	// Generate secure random token for task authentication.
	rawToken, err := generateSecureToken()
	if err != nil {
		return nil, err
	}

	queueName := req.TaskQueue.GetName()
	if queueName == "" {
		queueName = "default"
	}

	scheduledAt := time.Now().UTC()
	if req.ScheduleTime != nil {
		scheduledAt = req.ScheduleTime.AsTime()
	}

	// Token format: namespace|queue|taskID|random.
	// This lets workers complete tasks safely without additional lookups.
	token := []byte(fmt.Sprintf("%s|%s|%s|%s", req.Namespace, queueName, taskID, string(rawToken)))

	task := &engine.Task{
		ID:               taskID,
		Token:            token,
		WorkflowID:       req.WorkflowExecution.GetWorkflowId(),
		RunID:            req.WorkflowExecution.GetRunId(),
		Namespace:        req.Namespace,
		ScheduledTime:    scheduledAt,
		TaskType:         int32(req.TaskType),
		ScheduledEventID: req.ScheduledEventId,
		ActivityID:       fmt.Sprintf("%d", req.ScheduledEventId),
	}

	if err = s.service.AddTask(ctx, queueName, task); err != nil {
		return nil, err
	}

	return &matchingv1.AddTaskResponse{}, nil
}

func (s *GRPCServer) PollTask(ctx context.Context, req *matchingv1.PollTaskRequest) (*matchingv1.PollTaskResponse, error) {
	queueName := req.TaskQueue.GetName()
	if queueName == "" {
		queueName = "default"
	}

	// Auto-create task queue if it doesn't exist (workers poll before tasks arrive)
	s.service.GetOrCreateTaskQueue(queueName, engine.TaskQueueKindNormal)

	task, err := s.service.PollTask(ctx, queueName, req.Identity)
	if err != nil {
		return nil, err
	}
	if task == nil {
		return &matchingv1.PollTaskResponse{}, nil
	}

	// Map internal engine.Task to proto PollTaskResponse
	resp := &matchingv1.PollTaskResponse{
		TaskToken: task.Token,
		WorkflowExecution: &commonv1.WorkflowExecution{
			WorkflowId: task.WorkflowID,
			RunId:      task.RunID,
		},
		Attempt:        task.Attempt,
		StartedEventId: 1, // Placeholder
	}

	if commonv1.TaskType(task.TaskType) == commonv1.TaskType_TASK_TYPE_WORKFLOW_TASK {
		resp.WorkflowTaskInfo = &matchingv1.WorkflowTaskInfo{
			ScheduledEventId: task.ScheduledEventID,
		}
	} else {
		resp.ActivityTaskInfo = &matchingv1.ActivityTaskInfo{
			ActivityId:       task.ActivityID,
			ActivityType:     task.ActivityType,
			ScheduledEventId: task.ScheduledEventID,
		}
		if len(task.Input) > 0 {
			resp.ActivityTaskInfo.Input = &commonv1.Payloads{
				Payloads: []*commonv1.Payload{{Data: task.Input}},
			}
		}
	}

	return resp, nil
}

func (s *GRPCServer) CompleteTask(ctx context.Context, req *matchingv1.CompleteTaskRequest) (*matchingv1.CompleteTaskResponse, error) {
	_, queueName, taskID, err := parseTaskToken(req.GetTaskToken())
	if err != nil {
		return nil, err
	}
	if queueName == "" || taskID == "" {
		return nil, fmt.Errorf("invalid task token")
	}

	if err := s.service.CompleteTask(ctx, queueName, taskID); err != nil && err != ErrTaskNotFound {
		return nil, err
	}

	// Completion is idempotent; already-completed/not-found tasks are treated as success.
	return &matchingv1.CompleteTaskResponse{}, nil
}

func (s *GRPCServer) QueryWorkflow(ctx context.Context, req *matchingv1.MatchingServiceQueryWorkflowRequest) (*matchingv1.MatchingServiceQueryWorkflowResponse, error) {
	return &matchingv1.MatchingServiceQueryWorkflowResponse{}, nil
}

func (s *GRPCServer) HeartbeatTask(ctx context.Context, req *matchingv1.HeartbeatTaskRequest) (*matchingv1.HeartbeatTaskResponse, error) {
	return &matchingv1.HeartbeatTaskResponse{CancelRequested: false}, nil
}

func parseTaskToken(token []byte) (namespace string, queueName string, taskID string, err error) {
	parts := strings.SplitN(string(token), "|", 4)
	if len(parts) < 4 {
		return "", "", "", fmt.Errorf("malformed task token")
	}
	return parts[0], parts[1], parts[2], nil
}
