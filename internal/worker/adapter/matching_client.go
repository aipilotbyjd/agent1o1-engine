package adapter

import (
	"context"
	"fmt"
	"strings"

	commonv1 "github.com/linkflow/engine/api/gen/linkflow/common/v1"
	matchingv1 "github.com/linkflow/engine/api/gen/linkflow/matching/v1"
	"github.com/linkflow/engine/internal/worker/poller"
	"google.golang.org/grpc"
)

type MatchingClient struct {
	client matchingv1.MatchingServiceClient
}

func NewMatchingClient(conn *grpc.ClientConn) *MatchingClient {
	return &MatchingClient{
		client: matchingv1.NewMatchingServiceClient(conn),
	}
}

func (c *MatchingClient) PollTask(ctx context.Context, taskQueue string, identity string) (*poller.Task, error) {
	req := &matchingv1.PollTaskRequest{
		Namespace: "default",
		TaskQueue: &matchingv1.TaskQueue{
			Name: taskQueue,
			Kind: commonv1.TaskQueueKind_TASK_QUEUE_KIND_NORMAL,
		},
		Identity: identity,
	}

	resp, err := c.client.PollTask(ctx, req)
	if err != nil {
		return nil, err
	}

	if resp.TaskToken == nil {
		return nil, nil
	}

	var task *poller.Task

	// Extract namespace from TaskToken (format: namespace|taskID)
	token := string(resp.TaskToken)
	parts := strings.Split(token, "|")
	namespace := "default"
	if len(parts) >= 2 {
		namespace = parts[0]
	}

	if resp.ActivityTaskInfo != nil {
		task = &poller.Task{
			TaskToken:        resp.TaskToken,
			TaskID:           resp.ActivityTaskInfo.ActivityId,
			WorkflowID:       resp.WorkflowExecution.GetWorkflowId(),
			RunID:            resp.WorkflowExecution.GetRunId(),
			Namespace:        namespace,
			NodeType:         resp.ActivityTaskInfo.ActivityType,
			Attempt:          resp.Attempt,
			TimeoutSec:       60, // Default timeout
			ScheduledEventID: resp.ActivityTaskInfo.ScheduledEventId,
		}

		if resp.ActivityTaskInfo.Input != nil && len(resp.ActivityTaskInfo.Input.Payloads) > 0 {
			task.Input = resp.ActivityTaskInfo.Input.Payloads[0].Data
		}
	} else if resp.WorkflowTaskInfo != nil {
		task = &poller.Task{
			TaskToken:        resp.TaskToken,
			TaskID:           fmt.Sprintf("%d", resp.WorkflowTaskInfo.ScheduledEventId),
			WorkflowID:       resp.WorkflowExecution.GetWorkflowId(),
			RunID:            resp.WorkflowExecution.GetRunId(),
			Namespace:        namespace,
			NodeType:         "workflow",
			Attempt:          resp.Attempt,
			TimeoutSec:       60,
			ScheduledEventID: resp.WorkflowTaskInfo.ScheduledEventId,
		}
	} else {
		return nil, nil
	}

	return task, nil
}

func (c *MatchingClient) CompleteTask(ctx context.Context, task *poller.Task, identity string) error {
	if task == nil || len(task.TaskToken) == 0 {
		return fmt.Errorf("task token is required")
	}

	req := &matchingv1.CompleteTaskRequest{
		TaskToken: task.TaskToken,
		Namespace: task.Namespace,
		Identity:  identity,
	}

	_, err := c.client.CompleteTask(ctx, req)
	return err
}
