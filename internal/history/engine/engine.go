package engine

import (
	"errors"
	"log/slog"
	"time"

	"github.com/linkflow/engine/internal/history/types"
)

var (
	ErrInvalidEvent       = errors.New("invalid event")
	ErrEventOutOfOrder    = errors.New("event out of order")
	ErrDuplicateTimer     = errors.New("duplicate timer")
	ErrTimerNotFound      = errors.New("timer not found")
	ErrActivityNotFound   = errors.New("activity not found")
	ErrWorkflowNotRunning = errors.New("workflow not running")
	ErrInvalidEventType   = errors.New("invalid event type")
)

type Engine struct {
	logger *slog.Logger
}

func NewEngine(logger *slog.Logger) *Engine {
	if logger == nil {
		logger = slog.Default()
	}
	return &Engine{
		logger: logger,
	}
}

func (e *Engine) ProcessEvent(state *MutableState, event *types.HistoryEvent) error {
	if err := e.ValidateEvent(state, event); err != nil {
		return err
	}
	return state.ApplyEvent(event)
}

func (e *Engine) ValidateEvent(state *MutableState, event *types.HistoryEvent) error {
	if event == nil {
		return ErrInvalidEvent
	}

	if event.EventID != state.NextEventID {
		return ErrEventOutOfOrder
	}

	switch event.EventType {
	case types.EventTypeExecutionStarted:
		return e.validateExecutionStarted(state, event)
	case types.EventTypeExecutionCompleted, types.EventTypeExecutionFailed, types.EventTypeExecutionTerminated:
		return e.validateExecutionClose(state)
	case types.EventTypeTimerStarted:
		return e.validateTimerStarted(state, event)
	case types.EventTypeTimerFired, types.EventTypeTimerCanceled:
		return e.validateTimerOperation(state, event)
	case types.EventTypeActivityScheduled:
		return e.validateActivityScheduled(state)
	case types.EventTypeActivityStarted:
		return e.validateActivityStarted(state, event)
	case types.EventTypeActivityCompleted, types.EventTypeActivityFailed, types.EventTypeActivityTimedOut:
		return e.validateActivityClose(state, event)
	}

	return nil
}

func (e *Engine) validateExecutionStarted(state *MutableState, event *types.HistoryEvent) error {
	if event.EventID != 1 {
		return ErrEventOutOfOrder
	}
	return nil
}

func (e *Engine) validateExecutionClose(state *MutableState) error {
	if !state.IsWorkflowExecutionRunning() {
		return ErrWorkflowNotRunning
	}
	return nil
}

func (e *Engine) validateTimerStarted(state *MutableState, event *types.HistoryEvent) error {
	if !state.IsWorkflowExecutionRunning() {
		return ErrWorkflowNotRunning
	}
	attrs, ok := event.Attributes.(*types.TimerStartedAttributes)
	if !ok {
		return ErrInvalidEventType
	}
	if _, exists := state.PendingTimers[attrs.TimerID]; exists {
		return ErrDuplicateTimer
	}
	return nil
}

func (e *Engine) validateTimerOperation(state *MutableState, event *types.HistoryEvent) error {
	if !state.IsWorkflowExecutionRunning() {
		return ErrWorkflowNotRunning
	}
	var timerID string
	switch attrs := event.Attributes.(type) {
	case *types.TimerFiredAttributes:
		timerID = attrs.TimerID
	case *types.TimerCanceledAttributes:
		timerID = attrs.TimerID
	default:
		return ErrInvalidEventType
	}
	if _, exists := state.PendingTimers[timerID]; !exists {
		return ErrTimerNotFound
	}
	return nil
}

func (e *Engine) validateActivityScheduled(state *MutableState) error {
	if !state.IsWorkflowExecutionRunning() {
		return ErrWorkflowNotRunning
	}
	return nil
}

func (e *Engine) validateActivityStarted(state *MutableState, event *types.HistoryEvent) error {
	if !state.IsWorkflowExecutionRunning() {
		return ErrWorkflowNotRunning
	}
	attrs, ok := event.Attributes.(*types.ActivityStartedAttributes)
	if !ok {
		return ErrInvalidEventType
	}
	if _, exists := state.PendingActivities[attrs.ScheduledEventID]; !exists {
		return ErrActivityNotFound
	}
	return nil
}

func (e *Engine) validateActivityClose(state *MutableState, event *types.HistoryEvent) error {
	if !state.IsWorkflowExecutionRunning() {
		return ErrWorkflowNotRunning
	}
	var scheduledEventID int64
	switch attrs := event.Attributes.(type) {
	case *types.ActivityCompletedAttributes:
		scheduledEventID = attrs.ScheduledEventID
	case *types.ActivityFailedAttributes:
		scheduledEventID = attrs.ScheduledEventID
	default:
		return ErrInvalidEventType
	}
	if _, exists := state.PendingActivities[scheduledEventID]; !exists {
		return ErrActivityNotFound
	}
	return nil
}

func (e *Engine) ScheduleNode(state *MutableState, nodeID, nodeType string, input []byte, taskQueue string) (*types.HistoryEvent, error) {
	if !state.IsWorkflowExecutionRunning() {
		return nil, ErrWorkflowNotRunning
	}

	eventID := state.IncrementNextEventID()
	event := &types.HistoryEvent{
		EventID:   eventID,
		EventType: types.EventTypeNodeScheduled,
		Timestamp: time.Now(),
		Attributes: &types.NodeScheduledAttributes{
			NodeID:    nodeID,
			NodeType:  nodeType,
			Input:     input,
			TaskQueue: taskQueue,
		},
	}

	return event, nil
}

func (e *Engine) CompleteNode(state *MutableState, nodeID string, scheduledEventID, startedEventID int64, result []byte) (*types.HistoryEvent, error) {
	if !state.IsWorkflowExecutionRunning() {
		return nil, ErrWorkflowNotRunning
	}

	eventID := state.IncrementNextEventID()
	event := &types.HistoryEvent{
		EventID:   eventID,
		EventType: types.EventTypeNodeCompleted,
		Timestamp: time.Now(),
		Attributes: &types.NodeCompletedAttributes{
			NodeID:           nodeID,
			ScheduledEventID: scheduledEventID,
			StartedEventID:   startedEventID,
			Result:           result,
		},
	}

	state.AddCompletedNode(nodeID, &types.NodeResult{
		NodeID:        nodeID,
		CompletedTime: event.Timestamp,
		Output:        result,
	})

	return event, nil
}

func (e *Engine) FailNode(state *MutableState, nodeID string, scheduledEventID, startedEventID int64, reason string, details []byte) (*types.HistoryEvent, error) {
	if !state.IsWorkflowExecutionRunning() {
		return nil, ErrWorkflowNotRunning
	}

	eventID := state.IncrementNextEventID()
	event := &types.HistoryEvent{
		EventID:   eventID,
		EventType: types.EventTypeNodeFailed,
		Timestamp: time.Now(),
		Attributes: &types.NodeFailedAttributes{
			NodeID:           nodeID,
			ScheduledEventID: scheduledEventID,
			StartedEventID:   startedEventID,
			Reason:           reason,
			Details:          details,
		},
	}

	state.AddCompletedNode(nodeID, &types.NodeResult{
		NodeID:         nodeID,
		CompletedTime:  event.Timestamp,
		FailureReason:  reason,
		FailureDetails: details,
	})

	return event, nil
}

func (e *Engine) StartTimer(state *MutableState, timerID string, duration time.Duration) (*types.HistoryEvent, error) {
	if !state.IsWorkflowExecutionRunning() {
		return nil, ErrWorkflowNotRunning
	}

	if _, exists := state.PendingTimers[timerID]; exists {
		return nil, ErrDuplicateTimer
	}

	eventID := state.IncrementNextEventID()
	now := time.Now()
	event := &types.HistoryEvent{
		EventID:   eventID,
		EventType: types.EventTypeTimerStarted,
		Timestamp: now,
		Attributes: &types.TimerStartedAttributes{
			TimerID:     timerID,
			StartToFire: duration,
		},
	}

	state.AddPendingTimer(timerID, &types.TimerInfo{
		TimerID:        timerID,
		StartedEventID: eventID,
		FireTime:       now.Add(duration),
		ExpiryTime:     now.Add(duration),
	})

	return event, nil
}

func (e *Engine) FireTimer(state *MutableState, timerID string) (*types.HistoryEvent, error) {
	if !state.IsWorkflowExecutionRunning() {
		return nil, ErrWorkflowNotRunning
	}

	timerInfo, exists := state.PendingTimers[timerID]
	if !exists {
		return nil, ErrTimerNotFound
	}

	eventID := state.IncrementNextEventID()
	event := &types.HistoryEvent{
		EventID:   eventID,
		EventType: types.EventTypeTimerFired,
		Timestamp: time.Now(),
		Attributes: &types.TimerFiredAttributes{
			TimerID:        timerID,
			StartedEventID: timerInfo.StartedEventID,
		},
	}

	state.DeletePendingTimer(timerID)

	return event, nil
}

func (e *Engine) CancelTimer(state *MutableState, timerID, identity string) (*types.HistoryEvent, error) {
	if !state.IsWorkflowExecutionRunning() {
		return nil, ErrWorkflowNotRunning
	}

	timerInfo, exists := state.PendingTimers[timerID]
	if !exists {
		return nil, ErrTimerNotFound
	}

	eventID := state.IncrementNextEventID()
	event := &types.HistoryEvent{
		EventID:   eventID,
		EventType: types.EventTypeTimerCanceled,
		Timestamp: time.Now(),
		Attributes: &types.TimerCanceledAttributes{
			TimerID:        timerID,
			StartedEventID: timerInfo.StartedEventID,
			Identity:       identity,
		},
	}

	state.DeletePendingTimer(timerID)

	return event, nil
}

func (e *Engine) ScheduleActivity(state *MutableState, attrs *types.ActivityScheduledAttributes) (*types.HistoryEvent, error) {
	if !state.IsWorkflowExecutionRunning() {
		return nil, ErrWorkflowNotRunning
	}

	eventID := state.IncrementNextEventID()
	now := time.Now()
	event := &types.HistoryEvent{
		EventID:    eventID,
		EventType:  types.EventTypeActivityScheduled,
		Timestamp:  now,
		Attributes: attrs,
	}

	state.AddPendingActivity(eventID, &types.ActivityInfo{
		ScheduledEventID: eventID,
		ActivityID:       attrs.ActivityID,
		ActivityType:     attrs.ActivityType,
		TaskQueue:        attrs.TaskQueue,
		Input:            attrs.Input,
		ScheduledTime:    now,
		HeartbeatTimeout: attrs.HeartbeatTimeout,
		ScheduleTimeout:  attrs.ScheduleToClose,
		StartToClose:     attrs.StartToClose,
	})

	return event, nil
}

func (e *Engine) CompleteActivity(state *MutableState, scheduledEventID, startedEventID int64, result []byte) (*types.HistoryEvent, error) {
	if !state.IsWorkflowExecutionRunning() {
		return nil, ErrWorkflowNotRunning
	}

	if _, exists := state.PendingActivities[scheduledEventID]; !exists {
		return nil, ErrActivityNotFound
	}

	eventID := state.IncrementNextEventID()
	event := &types.HistoryEvent{
		EventID:   eventID,
		EventType: types.EventTypeActivityCompleted,
		Timestamp: time.Now(),
		Attributes: &types.ActivityCompletedAttributes{
			ScheduledEventID: scheduledEventID,
			StartedEventID:   startedEventID,
			Result:           result,
		},
	}

	state.DeletePendingActivity(scheduledEventID)

	return event, nil
}

func (e *Engine) FailActivity(state *MutableState, scheduledEventID, startedEventID int64, reason string, details []byte) (*types.HistoryEvent, error) {
	if !state.IsWorkflowExecutionRunning() {
		return nil, ErrWorkflowNotRunning
	}

	if _, exists := state.PendingActivities[scheduledEventID]; !exists {
		return nil, ErrActivityNotFound
	}

	eventID := state.IncrementNextEventID()
	event := &types.HistoryEvent{
		EventID:   eventID,
		EventType: types.EventTypeActivityFailed,
		Timestamp: time.Now(),
		Attributes: &types.ActivityFailedAttributes{
			ScheduledEventID: scheduledEventID,
			StartedEventID:   startedEventID,
			Reason:           reason,
			Details:          details,
		},
	}

	state.DeletePendingActivity(scheduledEventID)

	return event, nil
}
