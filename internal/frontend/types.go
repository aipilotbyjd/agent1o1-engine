package frontend

import (
	"time"
)

type ExecutionKey struct {
	NamespaceID string
	WorkflowID  string
	RunID       string
}

type StartWorkflowExecutionRequest struct {
	Namespace                string
	WorkflowID               string
	WorkflowType             string
	TaskQueue                string
	Input                    []byte
	WorkflowExecutionTimeout time.Duration
	WorkflowRunTimeout       time.Duration
	WorkflowTaskTimeout      time.Duration
	RequestID                string
	RetryPolicy              *RetryPolicy
	Memo                     map[string][]byte
	SearchAttributes         map[string][]byte
}

type StartWorkflowExecutionResponse struct {
	RunID string
}

type SignalWorkflowExecutionRequest struct {
	Namespace  string
	WorkflowID string
	RunID      string
	SignalName string
	Input      []byte
	RequestID  string
}

type TerminateWorkflowExecutionRequest struct {
	Namespace  string
	WorkflowID string
	RunID      string
	Reason     string
	Details    []byte
}

type QueryWorkflowRequest struct {
	Namespace  string
	WorkflowID string
	RunID      string
	QueryType  string
	QueryArgs  []byte
}

type QueryWorkflowResponse struct {
	QueryResult []byte
}

type GetExecutionRequest struct {
	Namespace  string
	WorkflowID string
	RunID      string
}

type GetExecutionResponse struct {
	Execution *WorkflowExecution
}

type ListExecutionsRequest struct {
	Namespace     string
	PageSize      int32
	NextPageToken []byte
	Query         string
}

type ListExecutionsResponse struct {
	Executions    []*WorkflowExecution
	NextPageToken []byte
}

type DescribeExecutionRequest struct {
	Namespace  string
	WorkflowID string
	RunID      string
}

type DescribeExecutionResponse struct {
	Execution         *WorkflowExecution
	PendingActivities []*PendingActivity
	PendingChildExecs []*PendingChildExecution
}

type WorkflowExecution struct {
	WorkflowID    string
	RunID         string
	WorkflowType  string
	TaskQueue     string
	Status        ExecutionStatus
	StartTime     time.Time
	CloseTime     *time.Time
	HistoryLength int64
	Memo          map[string][]byte
	SearchAttrs   map[string][]byte
}

type ExecutionStatus int32

const (
	ExecutionStatusUnspecified ExecutionStatus = iota
	ExecutionStatusRunning
	ExecutionStatusCompleted
	ExecutionStatusFailed
	ExecutionStatusCanceled
	ExecutionStatusTerminated
	ExecutionStatusContinuedAsNew
	ExecutionStatusTimedOut
)

type PendingActivity struct {
	ActivityID        string
	ActivityType      string
	State             PendingActivityState
	ScheduledTime     time.Time
	LastStartedTime   *time.Time
	Attempt           int32
	MaximumAttempts   int32
	LastFailure       *Failure
	LastHeartbeatTime *time.Time
}

type PendingActivityState int32

const (
	PendingActivityStateScheduled PendingActivityState = iota
	PendingActivityStateStarted
	PendingActivityStateCancelRequested
)

type PendingChildExecution struct {
	WorkflowID   string
	RunID        string
	WorkflowType string
	InitiatedID  int64
}

type RetryPolicy struct {
	InitialInterval    time.Duration
	BackoffCoefficient float64
	MaximumInterval    time.Duration
	MaximumAttempts    int32
}

type Failure struct {
	Message    string
	Source     string
	StackTrace string
	Cause      *Failure
}

type RecordEventRequest struct {
	NamespaceID string
	WorkflowID  string
	RunID       string
	EventType   string
	Attributes  any
}

type ExecutionStartedAttributes struct {
	WorkflowType string
	TaskQueue    string
	Input        []byte
}

type GetHistoryRequest struct {
	NamespaceID   string
	WorkflowID    string
	RunID         string
	FirstEventID  int64
	NextEventID   int64
	PageSize      int32
	NextPageToken []byte
}

type GetHistoryResponse struct {
	Events        []*HistoryEvent
	NextPageToken []byte
}

type HistoryEvent struct {
	EventID   int64
	EventType string
	Timestamp time.Time
	Data      []byte
}

type MutableState struct {
	ExecutionInfo   *WorkflowExecution
	NextEventID     int64
	LastEventTaskID int64
	ActivityInfos   map[int64]*ActivityInfo
	TimerInfos      map[string]*TimerInfo
	ChildExecutions map[int64]*ChildExecutionInfo
	SignalInfos     map[int64]*SignalInfo
	BufferedEvents  []*HistoryEvent
}

type ActivityInfo struct {
	ScheduleID    int64
	ActivityID    string
	ActivityType  string
	TaskQueue     string
	StartedID     int64
	Attempt       int32
	ScheduledTime time.Time
	StartedTime   *time.Time
}

type TimerInfo struct {
	TimerID    string
	StartedID  int64
	ExpiryTime time.Time
	TaskStatus int64
}

type ChildExecutionInfo struct {
	InitiatedID  int64
	StartedID    int64
	Namespace    string
	WorkflowID   string
	RunID        string
	WorkflowType string
}

type SignalInfo struct {
	InitiatedID int64
	SignalName  string
}

type AddTaskRequest struct {
	NamespaceID      string
	WorkflowID       string
	RunID            string
	TaskQueue        string
	TaskType         TaskType
	TaskInfo         []byte
	ScheduledEventID int64
}

type TaskType int32

const (
	TaskTypeUnspecified TaskType = iota
	TaskTypeWorkflow
	TaskTypeActivity
)

type PollTaskRequest struct {
	NamespaceID string
	TaskQueue   string
	TaskType    TaskType
	Identity    string
}

type Task struct {
	TaskToken []byte
	TaskType  TaskType
	TaskInfo  []byte
}
