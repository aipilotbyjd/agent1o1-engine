package events

import (
	"time"

	"github.com/linkflow/engine/internal/history/types"
)

type EventBuilder struct {
	namespaceID string
	workflowID  string
	runID       string
	version     int64
	taskID      int64
}

func NewEventBuilder(namespaceID, workflowID, runID string) *EventBuilder {
	return &EventBuilder{
		namespaceID: namespaceID,
		workflowID:  workflowID,
		runID:       runID,
		version:     1,
		taskID:      0,
	}
}

func (b *EventBuilder) WithVersion(version int64) *EventBuilder {
	b.version = version
	return b
}

func (b *EventBuilder) WithTaskID(taskID int64) *EventBuilder {
	b.taskID = taskID
	return b
}

func (b *EventBuilder) newEvent(eventID int64, eventType types.EventType, attrs any) *types.HistoryEvent {
	return &types.HistoryEvent{
		EventID:    eventID,
		EventType:  eventType,
		Timestamp:  time.Now(),
		Version:    b.version,
		TaskID:     b.taskID,
		Attributes: attrs,
	}
}

func (b *EventBuilder) BuildExecutionStarted(
	eventID int64,
	workflowType, taskQueue string,
	input []byte,
	executionTimeout, runTimeout, taskTimeout time.Duration,
	parentExecution *types.ExecutionKey,
	initiator string,
) *types.HistoryEvent {
	return b.newEvent(eventID, types.EventTypeExecutionStarted, &types.ExecutionStartedAttributes{
		WorkflowType:     workflowType,
		TaskQueue:        taskQueue,
		Input:            input,
		ExecutionTimeout: executionTimeout,
		RunTimeout:       runTimeout,
		TaskTimeout:      taskTimeout,
		ParentExecution:  parentExecution,
		Initiator:        initiator,
	})
}

func (b *EventBuilder) BuildExecutionCompleted(eventID int64, result []byte) *types.HistoryEvent {
	return b.newEvent(eventID, types.EventTypeExecutionCompleted, &types.ExecutionCompletedAttributes{
		Result: result,
	})
}

func (b *EventBuilder) BuildExecutionFailed(eventID int64, reason string, details []byte) *types.HistoryEvent {
	return b.newEvent(eventID, types.EventTypeExecutionFailed, &types.ExecutionFailedAttributes{
		Reason:  reason,
		Details: details,
	})
}

func (b *EventBuilder) BuildExecutionTerminated(eventID int64, reason, identity string) *types.HistoryEvent {
	return b.newEvent(eventID, types.EventTypeExecutionTerminated, &types.ExecutionTerminatedAttributes{
		Reason:   reason,
		Identity: identity,
	})
}

func (b *EventBuilder) BuildNodeScheduled(eventID int64, nodeID, nodeType string, input []byte, taskQueue string) *types.HistoryEvent {
	return b.newEvent(eventID, types.EventTypeNodeScheduled, &types.NodeScheduledAttributes{
		NodeID:    nodeID,
		NodeType:  nodeType,
		Input:     input,
		TaskQueue: taskQueue,
	})
}

func (b *EventBuilder) BuildNodeStarted(eventID int64, nodeID string, scheduledEventID int64, identity string) *types.HistoryEvent {
	return b.newEvent(eventID, types.EventTypeNodeStarted, &types.NodeStartedAttributes{
		NodeID:           nodeID,
		ScheduledEventID: scheduledEventID,
		Identity:         identity,
	})
}

func (b *EventBuilder) BuildNodeCompleted(eventID int64, nodeID string, scheduledEventID, startedEventID int64, result []byte) *types.HistoryEvent {
	return b.newEvent(eventID, types.EventTypeNodeCompleted, &types.NodeCompletedAttributes{
		NodeID:           nodeID,
		ScheduledEventID: scheduledEventID,
		StartedEventID:   startedEventID,
		Result:           result,
	})
}

func (b *EventBuilder) BuildNodeFailed(eventID int64, nodeID string, scheduledEventID, startedEventID int64, reason string, details []byte, retryState int32) *types.HistoryEvent {
	return b.newEvent(eventID, types.EventTypeNodeFailed, &types.NodeFailedAttributes{
		NodeID:           nodeID,
		ScheduledEventID: scheduledEventID,
		StartedEventID:   startedEventID,
		Reason:           reason,
		Details:          details,
		RetryState:       retryState,
	})
}

func (b *EventBuilder) BuildTimerStarted(eventID int64, timerID string, startToFire time.Duration) *types.HistoryEvent {
	return b.newEvent(eventID, types.EventTypeTimerStarted, &types.TimerStartedAttributes{
		TimerID:     timerID,
		StartToFire: startToFire,
	})
}

func (b *EventBuilder) BuildTimerFired(eventID int64, timerID string, startedEventID int64) *types.HistoryEvent {
	return b.newEvent(eventID, types.EventTypeTimerFired, &types.TimerFiredAttributes{
		TimerID:        timerID,
		StartedEventID: startedEventID,
	})
}

func (b *EventBuilder) BuildTimerCanceled(eventID int64, timerID string, startedEventID int64, identity string) *types.HistoryEvent {
	return b.newEvent(eventID, types.EventTypeTimerCanceled, &types.TimerCanceledAttributes{
		TimerID:        timerID,
		StartedEventID: startedEventID,
		Identity:       identity,
	})
}

func (b *EventBuilder) BuildActivityScheduled(
	eventID int64,
	activityID, activityType, taskQueue string,
	input []byte,
	scheduleToClose, scheduleToStart, startToClose, heartbeatTimeout time.Duration,
	retryPolicy *types.RetryPolicy,
) *types.HistoryEvent {
	return b.newEvent(eventID, types.EventTypeActivityScheduled, &types.ActivityScheduledAttributes{
		ActivityID:       activityID,
		ActivityType:     activityType,
		TaskQueue:        taskQueue,
		Input:            input,
		ScheduleToClose:  scheduleToClose,
		ScheduleToStart:  scheduleToStart,
		StartToClose:     startToClose,
		HeartbeatTimeout: heartbeatTimeout,
		RetryPolicy:      retryPolicy,
	})
}

func (b *EventBuilder) BuildActivityStarted(eventID int64, scheduledEventID int64, identity string, attempt int32) *types.HistoryEvent {
	return b.newEvent(eventID, types.EventTypeActivityStarted, &types.ActivityStartedAttributes{
		ScheduledEventID: scheduledEventID,
		Identity:         identity,
		Attempt:          attempt,
	})
}

func (b *EventBuilder) BuildActivityCompleted(eventID int64, scheduledEventID, startedEventID int64, result []byte) *types.HistoryEvent {
	return b.newEvent(eventID, types.EventTypeActivityCompleted, &types.ActivityCompletedAttributes{
		ScheduledEventID: scheduledEventID,
		StartedEventID:   startedEventID,
		Result:           result,
	})
}

func (b *EventBuilder) BuildActivityFailed(eventID int64, scheduledEventID, startedEventID int64, reason string, details []byte, retryState int32) *types.HistoryEvent {
	return b.newEvent(eventID, types.EventTypeActivityFailed, &types.ActivityFailedAttributes{
		ScheduledEventID: scheduledEventID,
		StartedEventID:   startedEventID,
		Reason:           reason,
		Details:          details,
		RetryState:       retryState,
	})
}

func (b *EventBuilder) BuildSignalReceived(eventID int64, signalName string, input []byte, identity string) *types.HistoryEvent {
	return b.newEvent(eventID, types.EventTypeSignalReceived, &types.SignalReceivedAttributes{
		SignalName: signalName,
		Input:      input,
		Identity:   identity,
	})
}

func (b *EventBuilder) BuildMarkerRecorded(eventID int64, markerName string, details map[string][]byte) *types.HistoryEvent {
	return b.newEvent(eventID, types.EventTypeMarkerRecorded, &types.MarkerRecordedAttributes{
		MarkerName: markerName,
		Details:    details,
	})
}
