package adapter

import (
	"context"
	"encoding/json"

	apiv1 "github.com/linkflow/engine/api/gen/linkflow/api/v1"
	commonv1 "github.com/linkflow/engine/api/gen/linkflow/common/v1"
	historyv1 "github.com/linkflow/engine/api/gen/linkflow/history/v1"
	"github.com/linkflow/engine/internal/frontend"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type HistoryClient struct {
	client historyv1.HistoryServiceClient
}

func NewHistoryClient(conn *grpc.ClientConn) *HistoryClient {
	return &HistoryClient{
		client: historyv1.NewHistoryServiceClient(conn),
	}
}

func (c *HistoryClient) RecordEvent(ctx context.Context, req *frontend.RecordEventRequest) error {
	event := &historyv1.HistoryEvent{
		EventId:   1,
		EventTime: timestamppb.Now(),
		EventType: mapEventType(req.EventType),
	}

	switch req.EventType {
	case "WorkflowExecutionStarted":
		if attrs, ok := req.Attributes.(*frontend.ExecutionStartedAttributes); ok {
			event.Attributes = &historyv1.HistoryEvent_ExecutionStartedAttributes{
				ExecutionStartedAttributes: &historyv1.ExecutionStartedEventAttributes{
					WorkflowType: &apiv1.WorkflowType{Name: attrs.WorkflowType},
					TaskQueue:    &apiv1.TaskQueue{Name: attrs.TaskQueue},
					Input:        &commonv1.Payloads{Payloads: []*commonv1.Payload{{Data: attrs.Input}}},
				},
			}
		}
	}

	protoReq := &historyv1.RecordEventRequest{
		Namespace: req.NamespaceID,
		WorkflowExecution: &commonv1.WorkflowExecution{
			WorkflowId: req.WorkflowID,
			RunId:      req.RunID,
		},
		Event: event,
	}

	_, err := c.client.RecordEvent(ctx, protoReq)
	return err
}

func (c *HistoryClient) GetHistory(ctx context.Context, req *frontend.GetHistoryRequest) (*frontend.GetHistoryResponse, error) {
	protoReq := &historyv1.GetHistoryRequest{
		Namespace: req.NamespaceID,
		WorkflowExecution: &commonv1.WorkflowExecution{
			WorkflowId: req.WorkflowID,
			RunId:      req.RunID,
		},
		FirstEventId: req.FirstEventID,
		NextEventId:  req.NextEventID,
		PageSize:     req.PageSize,
	}

	resp, err := c.client.GetHistory(ctx, protoReq)
	if err != nil {
		return nil, err
	}

	var events []*frontend.HistoryEvent
	if resp.History != nil {
		events = make([]*frontend.HistoryEvent, 0, len(resp.History.Events))
		for _, e := range resp.History.Events {
			data, _ := extractAttributes(e)
			events = append(events, &frontend.HistoryEvent{
				EventID:   e.EventId,
				EventType: e.EventType.String(),
				Timestamp: e.EventTime.AsTime(),
				Data:      data,
			})
		}
	}

	return &frontend.GetHistoryResponse{
		Events:        events,
		NextPageToken: resp.NextPageToken,
	}, nil
}

func extractAttributes(e *historyv1.HistoryEvent) ([]byte, error) {
	var attrs interface{}
	switch e.EventType {
	case commonv1.EventType_EVENT_TYPE_EXECUTION_STARTED:
		if a := e.GetExecutionStartedAttributes(); a != nil {
			attrs = a
		}
	case commonv1.EventType_EVENT_TYPE_EXECUTION_COMPLETED:
		if a := e.GetExecutionCompletedAttributes(); a != nil {
			attrs = a
		}
	case commonv1.EventType_EVENT_TYPE_EXECUTION_FAILED:
		if a := e.GetExecutionFailedAttributes(); a != nil {
			attrs = a
		}
	case commonv1.EventType_EVENT_TYPE_NODE_SCHEDULED:
		if a := e.GetNodeScheduledAttributes(); a != nil {
			attrs = a
		}
	case commonv1.EventType_EVENT_TYPE_NODE_STARTED:
		if a := e.GetNodeStartedAttributes(); a != nil {
			attrs = a
		}
	case commonv1.EventType_EVENT_TYPE_NODE_COMPLETED:
		if a := e.GetNodeCompletedAttributes(); a != nil {
			attrs = a
		}
	case commonv1.EventType_EVENT_TYPE_NODE_FAILED:
		if a := e.GetNodeFailedAttributes(); a != nil {
			attrs = a
		}
	case commonv1.EventType_EVENT_TYPE_TIMER_STARTED:
		if a := e.GetTimerStartedAttributes(); a != nil {
			attrs = a
		}
	case commonv1.EventType_EVENT_TYPE_TIMER_FIRED:
		if a := e.GetTimerFiredAttributes(); a != nil {
			attrs = a
		}
	}

	if attrs == nil {
		return nil, nil
	}

	// Use protojson if available, or just standard json since generated structs have json tags
	return json.Marshal(attrs)
}

func (c *HistoryClient) GetMutableState(ctx context.Context, key frontend.ExecutionKey) (*frontend.MutableState, error) {
	protoReq := &historyv1.GetMutableStateRequest{
		Namespace: key.NamespaceID,
		WorkflowExecution: &commonv1.WorkflowExecution{
			WorkflowId: key.WorkflowID,
			RunId:      key.RunID,
		},
	}

	resp, err := c.client.GetMutableState(ctx, protoReq)
	if err != nil {
		return nil, err
	}

	return &frontend.MutableState{
		ExecutionInfo: &frontend.WorkflowExecution{
			WorkflowID:   resp.WorkflowExecution.GetWorkflowId(),
			RunID:        resp.WorkflowExecution.GetRunId(),
			Status:       mapExecutionStatus(resp.WorkflowStatus),
			WorkflowType: resp.WorkflowType,
			TaskQueue:    resp.TaskQueue,
		},
		ActivityInfos:   make(map[int64]*frontend.ActivityInfo),
		ChildExecutions: make(map[int64]*frontend.ChildExecutionInfo),
	}, nil
}

func (c *HistoryClient) ListWorkflowExecutions(ctx context.Context, req *historyv1.ListWorkflowExecutionsRequest) (*historyv1.ListWorkflowExecutionsResponse, error) {
	return c.client.ListWorkflowExecutions(ctx, req)
}

func mapEventType(eventType string) commonv1.EventType {
	switch eventType {
	case "WorkflowExecutionStarted":
		return commonv1.EventType_EVENT_TYPE_EXECUTION_STARTED
	case "WorkflowExecutionCompleted":
		return commonv1.EventType_EVENT_TYPE_EXECUTION_COMPLETED
	case "WorkflowExecutionFailed":
		return commonv1.EventType_EVENT_TYPE_EXECUTION_FAILED
	case "WorkflowExecutionSignaled":
		return commonv1.EventType_EVENT_TYPE_SIGNAL_RECEIVED
	case "WorkflowExecutionTerminated":
		return commonv1.EventType_EVENT_TYPE_EXECUTION_TERMINATED
	case "ActivityTaskScheduled", "NodeScheduled":
		return commonv1.EventType_EVENT_TYPE_NODE_SCHEDULED
	case "ActivityTaskStarted", "NodeStarted":
		return commonv1.EventType_EVENT_TYPE_NODE_STARTED
	case "ActivityTaskCompleted", "NodeCompleted":
		return commonv1.EventType_EVENT_TYPE_NODE_COMPLETED
	case "ActivityTaskFailed", "NodeFailed":
		return commonv1.EventType_EVENT_TYPE_NODE_FAILED
	default:
		return commonv1.EventType_EVENT_TYPE_UNSPECIFIED
	}
}

func mapExecutionStatus(status commonv1.ExecutionStatus) frontend.ExecutionStatus {
	switch status {
	case commonv1.ExecutionStatus_EXECUTION_STATUS_RUNNING:
		return frontend.ExecutionStatusRunning
	case commonv1.ExecutionStatus_EXECUTION_STATUS_COMPLETED:
		return frontend.ExecutionStatusCompleted
	case commonv1.ExecutionStatus_EXECUTION_STATUS_FAILED:
		return frontend.ExecutionStatusFailed
	case commonv1.ExecutionStatus_EXECUTION_STATUS_CANCELLED:
		return frontend.ExecutionStatusCanceled
	case commonv1.ExecutionStatus_EXECUTION_STATUS_TERMINATED:
		return frontend.ExecutionStatusTerminated
	case commonv1.ExecutionStatus_EXECUTION_STATUS_TIMED_OUT:
		return frontend.ExecutionStatusTimedOut
	default:
		return frontend.ExecutionStatusRunning
	}
}
