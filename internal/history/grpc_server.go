package history

import (
	"context"
	"errors"
	"time"

	apiv1 "github.com/linkflow/engine/api/gen/linkflow/api/v1"
	commonv1 "github.com/linkflow/engine/api/gen/linkflow/common/v1"
	historyv1 "github.com/linkflow/engine/api/gen/linkflow/history/v1"
	"github.com/linkflow/engine/internal/history/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type GRPCServer struct {
	historyv1.UnimplementedHistoryServiceServer
	service *Service
}

func NewGRPCServer(service *Service) *GRPCServer {
	return &GRPCServer{service: service}
}

func (s *GRPCServer) RecordEvent(ctx context.Context, req *historyv1.RecordEventRequest) (*historyv1.RecordEventResponse, error) {
	key := types.ExecutionKey{
		NamespaceID: req.GetNamespace(),
		WorkflowID:  req.GetWorkflowExecution().GetWorkflowId(),
		RunID:       req.GetWorkflowExecution().GetRunId(),
	}

	event := protoEventToInternal(req.GetEvent())

	if err := s.service.RecordEvent(ctx, key, event); err != nil {
		return nil, s.toGRPCError(err)
	}

	return &historyv1.RecordEventResponse{
		EventId: event.EventID,
	}, nil
}

func (s *GRPCServer) GetHistory(ctx context.Context, req *historyv1.GetHistoryRequest) (*historyv1.GetHistoryResponse, error) {
	key := types.ExecutionKey{
		NamespaceID: req.GetNamespace(),
		WorkflowID:  req.GetWorkflowExecution().GetWorkflowId(),
		RunID:       req.GetWorkflowExecution().GetRunId(),
	}

	events, err := s.service.GetHistory(ctx, key, req.GetFirstEventId(), req.GetNextEventId())
	if err != nil {
		return nil, s.toGRPCError(err)
	}

	protoEvents := make([]*historyv1.HistoryEvent, len(events))
	for i, e := range events {
		protoEvents[i] = internalEventToProto(e)
	}

	return &historyv1.GetHistoryResponse{
		History: &historyv1.History{
			Events: protoEvents,
		},
	}, nil
}

func (s *GRPCServer) GetMutableState(ctx context.Context, req *historyv1.GetMutableStateRequest) (*historyv1.GetMutableStateResponse, error) {
	key := types.ExecutionKey{
		NamespaceID: req.GetNamespace(),
		WorkflowID:  req.GetWorkflowExecution().GetWorkflowId(),
		RunID:       req.GetWorkflowExecution().GetRunId(),
	}

	state, err := s.service.GetMutableState(ctx, key)
	if err != nil {
		return nil, s.toGRPCError(err)
	}

	return &historyv1.GetMutableStateResponse{
		WorkflowExecution: &commonv1.WorkflowExecution{
			WorkflowId: key.WorkflowID,
			RunId:      key.RunID,
		},
		NextEventId:    state.NextEventID,
		WorkflowStatus: commonv1.ExecutionStatus(state.ExecutionInfo.Status),
	}, nil
}

func (s *GRPCServer) ResetExecution(ctx context.Context, req *historyv1.ResetExecutionRequest) (*historyv1.ResetExecutionResponse, error) {
	key := types.ExecutionKey{
		NamespaceID: req.GetNamespace(),
		WorkflowID:  req.GetWorkflowExecution().GetWorkflowId(),
		RunID:       req.GetWorkflowExecution().GetRunId(),
	}

	runID, err := s.service.ResetExecution(ctx, key, req.GetReason(), req.GetResetEventId())
	if err != nil {
		return nil, s.toGRPCError(err)
	}

	return &historyv1.ResetExecutionResponse{
		RunId: runID,
	}, nil
}

func (s *GRPCServer) toGRPCError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, types.ErrExecutionNotFound) || errors.Is(err, ErrEventNotFound) {
		return status.Error(codes.NotFound, err.Error())
	}
	if errors.Is(err, ErrServiceNotRunning) {
		return status.Error(codes.Unavailable, err.Error())
	}
	if errors.Is(err, types.ErrOptimisticLock) {
		return status.Error(codes.Aborted, err.Error())
	}
	// Add other mappings as needed
	return err
}

func protoEventToInternal(pe *historyv1.HistoryEvent) *types.HistoryEvent {
	if pe == nil {
		return nil
	}

	event := &types.HistoryEvent{
		EventID:   pe.GetEventId(),
		EventType: protoEventTypeToInternal(pe.GetEventType()),
		Version:   pe.GetVersion(),
		TaskID:    pe.GetTaskId(),
	}

	if pe.GetEventTime() != nil {
		event.Timestamp = pe.GetEventTime().AsTime()
	} else {
		event.Timestamp = time.Now()
	}

	switch event.EventType {
	case types.EventTypeExecutionStarted:
		if attr := pe.GetExecutionStartedAttributes(); attr != nil {
			internalAttr := &types.ExecutionStartedAttributes{
				WorkflowType: attr.GetWorkflowType().GetName(),
				TaskQueue:    attr.GetTaskQueue().GetName(),
			}
			if input := attr.GetInput(); input != nil && len(input.GetPayloads()) > 0 {
				internalAttr.Input = input.GetPayloads()[0].GetData()
			}
			event.Attributes = internalAttr
		}
	case types.EventTypeNodeScheduled:
		if attr := pe.GetNodeScheduledAttributes(); attr != nil {
			internalAttr := &types.NodeScheduledAttributes{
				NodeID:    attr.GetNodeId(),
				NodeType:  attr.GetNodeType(),
				TaskQueue: attr.GetTaskQueue().GetName(),
			}
			if input := attr.GetInput(); input != nil && len(input.GetPayloads()) > 0 {
				internalAttr.Input = input.GetPayloads()[0].GetData()
			}
			event.Attributes = internalAttr
		}
	case types.EventTypeNodeStarted:
		if attr := pe.GetNodeStartedAttributes(); attr != nil {
			event.Attributes = &types.NodeStartedAttributes{
				ScheduledEventID: attr.GetScheduledEventId(),
				Identity:         attr.GetIdentity(),
			}
		}
	case types.EventTypeNodeCompleted:
		if attr := pe.GetNodeCompletedAttributes(); attr != nil {
			internalAttr := &types.NodeCompletedAttributes{
				ScheduledEventID: attr.GetScheduledEventId(),
				StartedEventID:   attr.GetStartedEventId(),
			}
			if result := attr.GetResult(); result != nil && len(result.GetPayloads()) > 0 {
				internalAttr.Result = result.GetPayloads()[0].GetData()
			}
			if logs := attr.GetLogs(); logs != nil && len(logs.GetPayloads()) > 0 {
				internalAttr.Logs = logs.GetPayloads()[0].GetData()
			}
			event.Attributes = internalAttr
		}
	case types.EventTypeNodeFailed:
		if attr := pe.GetNodeFailedAttributes(); attr != nil {
			internalAttr := &types.NodeFailedAttributes{
				ScheduledEventID: attr.GetScheduledEventId(),
				StartedEventID:   attr.GetStartedEventId(),
				Reason:           attr.GetFailure().GetMessage(),
				Details:          []byte(attr.GetFailure().GetStackTrace()),
			}
			if logs := attr.GetLogs(); logs != nil && len(logs.GetPayloads()) > 0 {
				internalAttr.Logs = logs.GetPayloads()[0].GetData()
			}
			event.Attributes = internalAttr
		}
		// TODO: Add Timer and Activity mappings if needed for future tasks
		// For now, Node events are critical for workflow progress.
	}

	return event
}

func protoEventTypeToInternal(et commonv1.EventType) types.EventType {
	switch et {
	case commonv1.EventType_EVENT_TYPE_EXECUTION_STARTED:
		return types.EventTypeExecutionStarted
	case commonv1.EventType_EVENT_TYPE_EXECUTION_COMPLETED:
		return types.EventTypeExecutionCompleted
	case commonv1.EventType_EVENT_TYPE_EXECUTION_FAILED:
		return types.EventTypeExecutionFailed
	case commonv1.EventType_EVENT_TYPE_EXECUTION_TERMINATED:
		return types.EventTypeExecutionTerminated
	case commonv1.EventType_EVENT_TYPE_NODE_SCHEDULED:
		return types.EventTypeNodeScheduled
	case commonv1.EventType_EVENT_TYPE_NODE_STARTED:
		return types.EventTypeNodeStarted
	case commonv1.EventType_EVENT_TYPE_NODE_COMPLETED:
		return types.EventTypeNodeCompleted
	case commonv1.EventType_EVENT_TYPE_NODE_FAILED:
		return types.EventTypeNodeFailed
	case commonv1.EventType_EVENT_TYPE_NODE_TIMED_OUT:
		return types.EventTypeNodeTimedOut
	case commonv1.EventType_EVENT_TYPE_TIMER_STARTED:
		return types.EventTypeTimerStarted
	case commonv1.EventType_EVENT_TYPE_TIMER_FIRED:
		return types.EventTypeTimerFired
	case commonv1.EventType_EVENT_TYPE_TIMER_CANCELLED:
		return types.EventTypeTimerCanceled
	default:
		return types.EventTypeUnspecified
	}
}

func internalEventTypeToProto(et types.EventType) commonv1.EventType {
	switch et {
	case types.EventTypeExecutionStarted:
		return commonv1.EventType_EVENT_TYPE_EXECUTION_STARTED
	case types.EventTypeExecutionCompleted:
		return commonv1.EventType_EVENT_TYPE_EXECUTION_COMPLETED
	case types.EventTypeExecutionFailed:
		return commonv1.EventType_EVENT_TYPE_EXECUTION_FAILED
	case types.EventTypeExecutionTerminated:
		return commonv1.EventType_EVENT_TYPE_EXECUTION_TERMINATED
	case types.EventTypeNodeScheduled:
		return commonv1.EventType_EVENT_TYPE_NODE_SCHEDULED
	case types.EventTypeNodeStarted:
		return commonv1.EventType_EVENT_TYPE_NODE_STARTED
	case types.EventTypeNodeCompleted:
		return commonv1.EventType_EVENT_TYPE_NODE_COMPLETED
	case types.EventTypeNodeFailed:
		return commonv1.EventType_EVENT_TYPE_NODE_FAILED
	case types.EventTypeNodeTimedOut:
		return commonv1.EventType_EVENT_TYPE_NODE_TIMED_OUT
	case types.EventTypeTimerStarted:
		return commonv1.EventType_EVENT_TYPE_TIMER_STARTED
	case types.EventTypeTimerFired:
		return commonv1.EventType_EVENT_TYPE_TIMER_FIRED
	case types.EventTypeTimerCanceled:
		return commonv1.EventType_EVENT_TYPE_TIMER_CANCELLED
	default:
		return commonv1.EventType_EVENT_TYPE_UNSPECIFIED
	}
}

func internalEventToProto(e *types.HistoryEvent) *historyv1.HistoryEvent {
	if e == nil {
		return nil
	}

	event := &historyv1.HistoryEvent{
		EventId:   e.EventID,
		EventType: internalEventTypeToProto(e.EventType),
		EventTime: timestamppb.New(e.Timestamp),
		Version:   e.Version,
		TaskId:    e.TaskID,
	}

	switch e.EventType {
	case types.EventTypeExecutionStarted:
		if attr, ok := e.Attributes.(*types.ExecutionStartedAttributes); ok {
			event.Attributes = &historyv1.HistoryEvent_ExecutionStartedAttributes{ // This one was correct
				ExecutionStartedAttributes: &historyv1.ExecutionStartedEventAttributes{
					WorkflowType: &apiv1.WorkflowType{Name: attr.WorkflowType},
					TaskQueue:    &apiv1.TaskQueue{Name: attr.TaskQueue},
					Input:        &commonv1.Payloads{Payloads: []*commonv1.Payload{{Data: attr.Input}}},
				},
			}
		}
	case types.EventTypeNodeScheduled:
		if attr, ok := e.Attributes.(*types.NodeScheduledAttributes); ok {
			event.Attributes = &historyv1.HistoryEvent_NodeScheduledAttributes{ // Wrapper name fixed
				NodeScheduledAttributes: &historyv1.NodeScheduledEventAttributes{
					NodeId:    attr.NodeID,
					NodeType:  attr.NodeType,
					Input:     &commonv1.Payloads{Payloads: []*commonv1.Payload{{Data: attr.Input}}},
					TaskQueue: &apiv1.TaskQueue{Name: attr.TaskQueue},
				},
			}
		}
	case types.EventTypeNodeStarted:
		if attr, ok := e.Attributes.(*types.NodeStartedAttributes); ok {
			event.Attributes = &historyv1.HistoryEvent_NodeStartedAttributes{ // Wrapper name fixed
				NodeStartedAttributes: &historyv1.NodeStartedEventAttributes{
					ScheduledEventId: attr.ScheduledEventID,
					Identity:         attr.Identity,
				},
			}
		}
	case types.EventTypeNodeCompleted:
		if attr, ok := e.Attributes.(*types.NodeCompletedAttributes); ok {
			event.Attributes = &historyv1.HistoryEvent_NodeCompletedAttributes{ // Wrapper name fixed
				NodeCompletedAttributes: &historyv1.NodeCompletedEventAttributes{
					ScheduledEventId: attr.ScheduledEventID,
					StartedEventId:   attr.StartedEventID,
					Result:           &commonv1.Payloads{Payloads: []*commonv1.Payload{{Data: attr.Result}}},
				},
			}
			if len(attr.Logs) > 0 {
				event.GetNodeCompletedAttributes().Logs = &commonv1.Payloads{Payloads: []*commonv1.Payload{{Data: attr.Logs}}}
			}
		}
	case types.EventTypeNodeFailed:
		if attr, ok := e.Attributes.(*types.NodeFailedAttributes); ok {
			event.Attributes = &historyv1.HistoryEvent_NodeFailedAttributes{ // Wrapper name fixed
				NodeFailedAttributes: &historyv1.NodeFailedEventAttributes{
					ScheduledEventId: attr.ScheduledEventID,
					StartedEventId:   attr.StartedEventID,
					Failure:          &commonv1.Failure{Message: attr.Reason, StackTrace: string(attr.Details)},
				},
			}
			if len(attr.Logs) > 0 {
				event.GetNodeFailedAttributes().Logs = &commonv1.Payloads{Payloads: []*commonv1.Payload{{Data: attr.Logs}}}
			}
		}
	}

	return event
}
