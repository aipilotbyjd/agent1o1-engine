package frontend

import (
	"context"
	"log/slog"
)

type StubHistoryClient struct {
	Logger *slog.Logger
}

func (c *StubHistoryClient) RecordEvent(ctx context.Context, req *RecordEventRequest) error {
	c.Logger.Info("STUB: RecordEvent", "namespace", req.NamespaceID, "workflow_id", req.WorkflowID, "event_type", req.EventType)
	return nil
}

func (c *StubHistoryClient) GetHistory(ctx context.Context, req *GetHistoryRequest) (*GetHistoryResponse, error) {
	c.Logger.Info("STUB: GetHistory")
	return &GetHistoryResponse{}, nil
}

func (c *StubHistoryClient) GetMutableState(ctx context.Context, key ExecutionKey) (*MutableState, error) {
	c.Logger.Info("STUB: GetMutableState")
	return &MutableState{
		ExecutionInfo: &WorkflowExecution{
			Status: ExecutionStatusRunning,
		},
	}, nil
}

type StubMatchingClient struct {
	Logger *slog.Logger
}

func (c *StubMatchingClient) AddTask(ctx context.Context, req *AddTaskRequest) error {
	c.Logger.Info("STUB: AddTask", "task_queue", req.TaskQueue)
	return nil
}

func (c *StubMatchingClient) PollTask(ctx context.Context, req *PollTaskRequest) (*Task, error) {
	c.Logger.Info("STUB: PollTask")
	return nil, nil
}
