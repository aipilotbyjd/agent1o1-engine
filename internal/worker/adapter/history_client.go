package adapter

import (
	"context"

	commonv1 "github.com/linkflow/engine/api/gen/linkflow/common/v1"
	historyv1 "github.com/linkflow/engine/api/gen/linkflow/history/v1"
	"google.golang.org/grpc"
)

type HistoryClient struct {
	client historyv1.HistoryServiceClient
}

func NewHistoryClient(conn *grpc.ClientConn) *HistoryClient {
	return &HistoryClient{
		client: historyv1.NewHistoryServiceClient(conn),
	}
}

func (c *HistoryClient) RecordEvent(ctx context.Context, namespaceID, workflowID, runID string, event *historyv1.HistoryEvent) error {
	req := &historyv1.RecordEventRequest{
		Namespace: namespaceID,
		WorkflowExecution: &commonv1.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		Event: event,
	}

	_, err := c.client.RecordEvent(ctx, req)
	return err
}

func (c *HistoryClient) GetMutableState(ctx context.Context, namespaceID, workflowID, runID string) (*historyv1.GetMutableStateResponse, error) {
	req := &historyv1.GetMutableStateRequest{
		Namespace: namespaceID,
		WorkflowExecution: &commonv1.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
	}
	return c.client.GetMutableState(ctx, req)
}

func (c *HistoryClient) GetHistory(ctx context.Context, namespaceID, workflowID, runID string) (*historyv1.GetHistoryResponse, error) {
	req := &historyv1.GetHistoryRequest{
		Namespace: namespaceID,
		WorkflowExecution: &commonv1.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		PageSize: 1000, // Fetch ample history
	}
	return c.client.GetHistory(ctx, req)
}

func (c *HistoryClient) RespondWorkflowTaskCompleted(ctx context.Context, req *historyv1.RespondWorkflowTaskCompletedRequest) (*historyv1.RespondWorkflowTaskCompletedResponse, error) {
	return c.client.RespondWorkflowTaskCompleted(ctx, req)
}

func (c *HistoryClient) RespondWorkflowTaskFailed(ctx context.Context, req *historyv1.RespondWorkflowTaskFailedRequest) (*historyv1.RespondWorkflowTaskFailedResponse, error) {
	return c.client.RespondWorkflowTaskFailed(ctx, req)
}

func (c *HistoryClient) RespondActivityTaskCompleted(ctx context.Context, req *historyv1.RespondActivityTaskCompletedRequest) (*historyv1.RespondActivityTaskCompletedResponse, error) {
	return c.client.RespondActivityTaskCompleted(ctx, req)
}

func (c *HistoryClient) RespondActivityTaskFailed(ctx context.Context, req *historyv1.RespondActivityTaskFailedRequest) (*historyv1.RespondActivityTaskFailedResponse, error) {
	return c.client.RespondActivityTaskFailed(ctx, req)
}
