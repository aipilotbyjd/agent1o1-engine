package engine

import (
	"time"

	"github.com/linkflow/engine/internal/history/types"
)

type MutableState struct {
	ExecutionInfo     *types.ExecutionInfo
	NextEventID       int64
	PendingActivities map[int64]*types.ActivityInfo
	PendingTimers     map[string]*types.TimerInfo
	CompletedNodes    map[string]*types.NodeResult
	BufferedEvents    []*types.HistoryEvent
	DBVersion         int64
}

func NewMutableState(info *types.ExecutionInfo) *MutableState {
	return &MutableState{
		ExecutionInfo:     info,
		NextEventID:       1,
		PendingActivities: make(map[int64]*types.ActivityInfo),
		PendingTimers:     make(map[string]*types.TimerInfo),
		CompletedNodes:    make(map[string]*types.NodeResult),
		BufferedEvents:    make([]*types.HistoryEvent, 0),
		DBVersion:         0,
	}
}

func (ms *MutableState) Clone() *MutableState {
	clone := &MutableState{
		ExecutionInfo:     ms.cloneExecutionInfo(),
		NextEventID:       ms.NextEventID,
		PendingActivities: make(map[int64]*types.ActivityInfo, len(ms.PendingActivities)),
		PendingTimers:     make(map[string]*types.TimerInfo, len(ms.PendingTimers)),
		CompletedNodes:    make(map[string]*types.NodeResult, len(ms.CompletedNodes)),
		BufferedEvents:    make([]*types.HistoryEvent, len(ms.BufferedEvents)),
		DBVersion:         ms.DBVersion,
	}

	for k, v := range ms.PendingActivities {
		clone.PendingActivities[k] = ms.cloneActivityInfo(v)
	}
	for k, v := range ms.PendingTimers {
		clone.PendingTimers[k] = ms.cloneTimerInfo(v)
	}
	for k, v := range ms.CompletedNodes {
		clone.CompletedNodes[k] = ms.cloneNodeResult(v)
	}
	copy(clone.BufferedEvents, ms.BufferedEvents)

	return clone
}

func (ms *MutableState) cloneExecutionInfo() *types.ExecutionInfo {
	if ms.ExecutionInfo == nil {
		return nil
	}
	info := *ms.ExecutionInfo
	if ms.ExecutionInfo.Input != nil {
		info.Input = make([]byte, len(ms.ExecutionInfo.Input))
		copy(info.Input, ms.ExecutionInfo.Input)
	}
	return &info
}

func (ms *MutableState) cloneActivityInfo(ai *types.ActivityInfo) *types.ActivityInfo {
	if ai == nil {
		return nil
	}
	clone := *ai
	if ai.Input != nil {
		clone.Input = make([]byte, len(ai.Input))
		copy(clone.Input, ai.Input)
	}
	if ai.HeartbeatDetails != nil {
		clone.HeartbeatDetails = make([]byte, len(ai.HeartbeatDetails))
		copy(clone.HeartbeatDetails, ai.HeartbeatDetails)
	}
	return &clone
}

func (ms *MutableState) cloneTimerInfo(ti *types.TimerInfo) *types.TimerInfo {
	if ti == nil {
		return nil
	}
	clone := *ti
	return &clone
}

func (ms *MutableState) cloneNodeResult(nr *types.NodeResult) *types.NodeResult {
	if nr == nil {
		return nil
	}
	clone := *nr
	if nr.Output != nil {
		clone.Output = make([]byte, len(nr.Output))
		copy(clone.Output, nr.Output)
	}
	if nr.FailureDetails != nil {
		clone.FailureDetails = make([]byte, len(nr.FailureDetails))
		copy(clone.FailureDetails, nr.FailureDetails)
	}
	return &clone
}

func (ms *MutableState) ApplyEvent(event *types.HistoryEvent) error {
	switch event.EventType {
	case types.EventTypeExecutionStarted:
		return ms.applyExecutionStarted(event)
	case types.EventTypeExecutionCompleted:
		return ms.applyExecutionCompleted(event)
	case types.EventTypeExecutionFailed:
		return ms.applyExecutionFailed(event)
	case types.EventTypeExecutionTerminated:
		return ms.applyExecutionTerminated(event)
	case types.EventTypeNodeScheduled:
		return ms.applyNodeScheduled(event)
	case types.EventTypeNodeCompleted:
		return ms.applyNodeCompleted(event)
	case types.EventTypeNodeFailed:
		return ms.applyNodeFailed(event)
	case types.EventTypeTimerStarted:
		return ms.applyTimerStarted(event)
	case types.EventTypeTimerFired:
		return ms.applyTimerFired(event)
	case types.EventTypeTimerCanceled:
		return ms.applyTimerCanceled(event)
	case types.EventTypeActivityScheduled:
		return ms.applyActivityScheduled(event)
	case types.EventTypeActivityStarted:
		return ms.applyActivityStarted(event)
	case types.EventTypeActivityCompleted:
		return ms.applyActivityCompleted(event)
	case types.EventTypeActivityFailed:
		return ms.applyActivityFailed(event)
	}

	ms.NextEventID = event.EventID + 1
	return nil
}

func (ms *MutableState) applyExecutionStarted(event *types.HistoryEvent) error {
	attrs, ok := event.Attributes.(*types.ExecutionStartedAttributes)
	if !ok {
		return nil
	}
	ms.ExecutionInfo.WorkflowTypeName = attrs.WorkflowType
	ms.ExecutionInfo.TaskQueue = attrs.TaskQueue
	ms.ExecutionInfo.Input = attrs.Input
	ms.ExecutionInfo.ExecutionTimeout = attrs.ExecutionTimeout
	ms.ExecutionInfo.RunTimeout = attrs.RunTimeout
	ms.ExecutionInfo.TaskTimeout = attrs.TaskTimeout
	ms.ExecutionInfo.Status = types.ExecutionStatusRunning
	ms.ExecutionInfo.StartTime = event.Timestamp
	ms.NextEventID = event.EventID + 1
	return nil
}

func (ms *MutableState) applyExecutionCompleted(event *types.HistoryEvent) error {
	ms.ExecutionInfo.Status = types.ExecutionStatusCompleted
	ms.ExecutionInfo.CloseTime = event.Timestamp
	ms.NextEventID = event.EventID + 1
	return nil
}

func (ms *MutableState) applyExecutionFailed(event *types.HistoryEvent) error {
	ms.ExecutionInfo.Status = types.ExecutionStatusFailed
	ms.ExecutionInfo.CloseTime = event.Timestamp
	ms.NextEventID = event.EventID + 1
	return nil
}

func (ms *MutableState) applyExecutionTerminated(event *types.HistoryEvent) error {
	ms.ExecutionInfo.Status = types.ExecutionStatusTerminated
	ms.ExecutionInfo.CloseTime = event.Timestamp
	ms.NextEventID = event.EventID + 1
	return nil
}

func (ms *MutableState) applyNodeScheduled(event *types.HistoryEvent) error {
	ms.NextEventID = event.EventID + 1
	return nil
}

func (ms *MutableState) applyNodeCompleted(event *types.HistoryEvent) error {
	attrs, ok := event.Attributes.(*types.NodeCompletedAttributes)
	if !ok {
		return nil
	}
	ms.CompletedNodes[attrs.NodeID] = &types.NodeResult{
		NodeID:        attrs.NodeID,
		CompletedTime: event.Timestamp,
		Output:        attrs.Result,
	}
	ms.NextEventID = event.EventID + 1
	return nil
}

func (ms *MutableState) applyNodeFailed(event *types.HistoryEvent) error {
	attrs, ok := event.Attributes.(*types.NodeFailedAttributes)
	if !ok {
		return nil
	}
	ms.CompletedNodes[attrs.NodeID] = &types.NodeResult{
		NodeID:         attrs.NodeID,
		CompletedTime:  event.Timestamp,
		FailureReason:  attrs.Reason,
		FailureDetails: attrs.Details,
	}
	ms.NextEventID = event.EventID + 1
	return nil
}

func (ms *MutableState) applyTimerStarted(event *types.HistoryEvent) error {
	attrs, ok := event.Attributes.(*types.TimerStartedAttributes)
	if !ok {
		return nil
	}
	ms.PendingTimers[attrs.TimerID] = &types.TimerInfo{
		TimerID:        attrs.TimerID,
		StartedEventID: event.EventID,
		FireTime:       event.Timestamp.Add(attrs.StartToFire),
		ExpiryTime:     event.Timestamp.Add(attrs.StartToFire),
	}
	ms.NextEventID = event.EventID + 1
	return nil
}

func (ms *MutableState) applyTimerFired(event *types.HistoryEvent) error {
	attrs, ok := event.Attributes.(*types.TimerFiredAttributes)
	if !ok {
		return nil
	}
	delete(ms.PendingTimers, attrs.TimerID)
	ms.NextEventID = event.EventID + 1
	return nil
}

func (ms *MutableState) applyTimerCanceled(event *types.HistoryEvent) error {
	attrs, ok := event.Attributes.(*types.TimerCanceledAttributes)
	if !ok {
		return nil
	}
	delete(ms.PendingTimers, attrs.TimerID)
	ms.NextEventID = event.EventID + 1
	return nil
}

func (ms *MutableState) applyActivityScheduled(event *types.HistoryEvent) error {
	attrs, ok := event.Attributes.(*types.ActivityScheduledAttributes)
	if !ok {
		return nil
	}
	ms.PendingActivities[event.EventID] = &types.ActivityInfo{
		ScheduledEventID: event.EventID,
		ActivityID:       attrs.ActivityID,
		ActivityType:     attrs.ActivityType,
		TaskQueue:        attrs.TaskQueue,
		Input:            attrs.Input,
		ScheduledTime:    event.Timestamp,
		HeartbeatTimeout: attrs.HeartbeatTimeout,
		ScheduleTimeout:  attrs.ScheduleToClose,
		StartToClose:     attrs.StartToClose,
	}
	ms.NextEventID = event.EventID + 1
	return nil
}

func (ms *MutableState) applyActivityStarted(event *types.HistoryEvent) error {
	attrs, ok := event.Attributes.(*types.ActivityStartedAttributes)
	if !ok {
		return nil
	}
	if ai, exists := ms.PendingActivities[attrs.ScheduledEventID]; exists {
		ai.StartedEventID = event.EventID
		ai.StartedTime = event.Timestamp
		ai.Attempt = attrs.Attempt
	}
	ms.NextEventID = event.EventID + 1
	return nil
}

func (ms *MutableState) applyActivityCompleted(event *types.HistoryEvent) error {
	attrs, ok := event.Attributes.(*types.ActivityCompletedAttributes)
	if !ok {
		return nil
	}
	delete(ms.PendingActivities, attrs.ScheduledEventID)
	ms.NextEventID = event.EventID + 1
	return nil
}

func (ms *MutableState) applyActivityFailed(event *types.HistoryEvent) error {
	attrs, ok := event.Attributes.(*types.ActivityFailedAttributes)
	if !ok {
		return nil
	}
	delete(ms.PendingActivities, attrs.ScheduledEventID)
	ms.NextEventID = event.EventID + 1
	return nil
}

func (ms *MutableState) AddPendingActivity(scheduledEventID int64, info *types.ActivityInfo) {
	ms.PendingActivities[scheduledEventID] = info
}

func (ms *MutableState) GetPendingActivity(scheduledEventID int64) (*types.ActivityInfo, bool) {
	info, ok := ms.PendingActivities[scheduledEventID]
	return info, ok
}

func (ms *MutableState) DeletePendingActivity(scheduledEventID int64) {
	delete(ms.PendingActivities, scheduledEventID)
}

func (ms *MutableState) AddPendingTimer(timerID string, info *types.TimerInfo) {
	ms.PendingTimers[timerID] = info
}

func (ms *MutableState) GetPendingTimer(timerID string) (*types.TimerInfo, bool) {
	info, ok := ms.PendingTimers[timerID]
	return info, ok
}

func (ms *MutableState) DeletePendingTimer(timerID string) {
	delete(ms.PendingTimers, timerID)
}

func (ms *MutableState) AddCompletedNode(nodeID string, result *types.NodeResult) {
	ms.CompletedNodes[nodeID] = result
}

func (ms *MutableState) GetCompletedNode(nodeID string) (*types.NodeResult, bool) {
	result, ok := ms.CompletedNodes[nodeID]
	return result, ok
}

func (ms *MutableState) AddBufferedEvent(event *types.HistoryEvent) {
	ms.BufferedEvents = append(ms.BufferedEvents, event)
}

func (ms *MutableState) ClearBufferedEvents() {
	ms.BufferedEvents = ms.BufferedEvents[:0]
}

func (ms *MutableState) GetNextEventID() int64 {
	return ms.NextEventID
}

func (ms *MutableState) IncrementNextEventID() int64 {
	id := ms.NextEventID
	ms.NextEventID++
	return id
}

func (ms *MutableState) IsWorkflowExecutionRunning() bool {
	return ms.ExecutionInfo != nil && ms.ExecutionInfo.Status == types.ExecutionStatusRunning
}

func (ms *MutableState) GetStartTime() time.Time {
	if ms.ExecutionInfo == nil {
		return time.Time{}
	}
	return ms.ExecutionInfo.StartTime
}

func (ms *MutableState) GetCloseTime() time.Time {
	if ms.ExecutionInfo == nil {
		return time.Time{}
	}
	return ms.ExecutionInfo.CloseTime
}
