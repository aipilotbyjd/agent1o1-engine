package adapter

import (
	"context"

	commonv1 "github.com/linkflow/engine/api/gen/linkflow/common/v1"
	matchingv1 "github.com/linkflow/engine/api/gen/linkflow/matching/v1"
	"github.com/linkflow/engine/internal/frontend"
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

func (c *MatchingClient) AddTask(ctx context.Context, req *frontend.AddTaskRequest) error {
	protoReq := &matchingv1.AddTaskRequest{
		Namespace: req.NamespaceID,
		TaskQueue: &matchingv1.TaskQueue{
			Name: req.TaskQueue,
			Kind: commonv1.TaskQueueKind_TASK_QUEUE_KIND_NORMAL,
		},
		TaskType:         commonv1.TaskType(req.TaskType),
		ScheduledEventId: req.ScheduledEventID,
		WorkflowExecution: &commonv1.WorkflowExecution{
			WorkflowId: req.WorkflowID,
			RunId:      req.RunID,
		},
		// We are losing TaskInfo here because proto doesn't support it directly in AddTaskRequest?
		// AddTaskRequest expects WorkflowExecution etc.
		// For now, simpler mapping to verify connectivity.
	}

	_, err := c.client.AddTask(ctx, protoReq)
	return err
}

func (c *MatchingClient) PollTask(ctx context.Context, req *frontend.PollTaskRequest) (*frontend.Task, error) {
	protoReq := &matchingv1.PollTaskRequest{
		Namespace: req.NamespaceID,
		TaskQueue: &matchingv1.TaskQueue{
			Name: req.TaskQueue,
			Kind: commonv1.TaskQueueKind_TASK_QUEUE_KIND_NORMAL,
		},
		Identity: req.Identity,
	}

	resp, err := c.client.PollTask(ctx, protoReq)
	if err != nil {
		return nil, err
	}

	return &frontend.Task{
		TaskToken: resp.TaskToken,
		// Map other fields
	}, nil
}
