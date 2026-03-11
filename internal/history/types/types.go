package types

import (
	"errors"
	"time"
)

var (
	ErrExecutionNotFound = errors.New("execution not found")
	ErrOptimisticLock    = errors.New("optimistic lock failure")
)

type EventType int32

const (
	EventTypeUnspecified EventType = iota
	EventTypeExecutionStarted
	EventTypeExecutionCompleted
	EventTypeExecutionFailed
	EventTypeExecutionTerminated
	EventTypeNodeScheduled
	EventTypeNodeStarted
	EventTypeNodeCompleted
	EventTypeNodeFailed
	EventTypeNodeTimedOut
	EventTypeTimerStarted
	EventTypeTimerFired
	EventTypeTimerCanceled
	EventTypeActivityScheduled
	EventTypeActivityStarted
	EventTypeActivityCompleted
	EventTypeActivityFailed
	EventTypeActivityTimedOut
	EventTypeSignalReceived
	EventTypeMarkerRecorded
	EventTypeWorkflowTaskScheduled
	EventTypeWorkflowTaskStarted
	EventTypeWorkflowTaskCompleted
	EventTypeWorkflowTaskFailed
	EventTypeWorkflowTaskTimedOut
)

func (e EventType) String() string {
	names := map[EventType]string{
		EventTypeUnspecified:           "Unspecified",
		EventTypeExecutionStarted:      "ExecutionStarted",
		EventTypeExecutionCompleted:    "ExecutionCompleted",
		EventTypeExecutionFailed:       "ExecutionFailed",
		EventTypeExecutionTerminated:   "ExecutionTerminated",
		EventTypeNodeScheduled:         "NodeScheduled",
		EventTypeNodeStarted:           "NodeStarted",
		EventTypeNodeCompleted:         "NodeCompleted",
		EventTypeNodeFailed:            "NodeFailed",
		EventTypeNodeTimedOut:          "NodeTimedOut",
		EventTypeTimerStarted:          "TimerStarted",
		EventTypeTimerFired:            "TimerFired",
		EventTypeTimerCanceled:         "TimerCanceled",
		EventTypeActivityScheduled:     "ActivityScheduled",
		EventTypeActivityStarted:       "ActivityStarted",
		EventTypeActivityCompleted:     "ActivityCompleted",
		EventTypeActivityFailed:        "ActivityFailed",
		EventTypeActivityTimedOut:      "ActivityTimedOut",
		EventTypeSignalReceived:        "SignalReceived",
		EventTypeMarkerRecorded:        "MarkerRecorded",
		EventTypeWorkflowTaskScheduled: "WorkflowTaskScheduled",
		EventTypeWorkflowTaskStarted:   "WorkflowTaskStarted",
		EventTypeWorkflowTaskCompleted: "WorkflowTaskCompleted",
		EventTypeWorkflowTaskFailed:    "WorkflowTaskFailed",
		EventTypeWorkflowTaskTimedOut:  "WorkflowTaskTimedOut",
	}
	if name, ok := names[e]; ok {
		return name
	}
	return "Unknown"
}

type ExecutionStatus int32

const (
	ExecutionStatusUnspecified ExecutionStatus = iota
	ExecutionStatusRunning
	ExecutionStatusCompleted
	ExecutionStatusFailed
	ExecutionStatusTerminated
	ExecutionStatusTimedOut
)

type ExecutionKey struct {
	NamespaceID string
	WorkflowID  string
	RunID       string
}

type ExecutionInfo struct {
	NamespaceID       string
	WorkflowID        string
	RunID             string
	WorkflowTypeName  string
	TaskQueue         string
	Input             []byte
	Status            ExecutionStatus
	StartTime         time.Time
	CloseTime         time.Time
	ExecutionTimeout  time.Duration
	RunTimeout        time.Duration
	TaskTimeout       time.Duration
	LastEventTaskID   int64
	LastProcessedNode string
}

type ActivityInfo struct {
	ScheduledEventID int64
	StartedEventID   int64
	ActivityID       string
	ActivityType     string
	TaskQueue        string
	Input            []byte
	ScheduledTime    time.Time
	StartedTime      time.Time
	Attempt          int32
	MaxRetries       int32
	HeartbeatTimeout time.Duration
	ScheduleTimeout  time.Duration
	StartToClose     time.Duration
	HeartbeatDetails []byte
	LastHeartbeat    time.Time
}

type TimerInfo struct {
	TimerID        string
	StartedEventID int64
	FireTime       time.Time
	ExpiryTime     time.Time
	TaskStatus     int32
}

type NodeResult struct {
	NodeID         string
	CompletedTime  time.Time
	Output         []byte
	FailureReason  string
	FailureDetails []byte
}

type HistoryEvent struct {
	EventID    int64
	EventType  EventType
	Timestamp  time.Time
	Version    int64
	TaskID     int64
	Attributes any
}

type ExecutionStartedAttributes struct {
	WorkflowType     string
	TaskQueue        string
	Input            []byte
	ExecutionTimeout time.Duration
	RunTimeout       time.Duration
	TaskTimeout      time.Duration
	ParentExecution  *ExecutionKey
	Initiator        string
}

type ExecutionCompletedAttributes struct {
	Result []byte
}

type ExecutionFailedAttributes struct {
	Reason  string
	Details []byte
}

type ExecutionTerminatedAttributes struct {
	Reason   string
	Identity string
}

type NodeScheduledAttributes struct {
	NodeID    string
	NodeType  string
	Input     []byte
	TaskQueue string
}

type NodeStartedAttributes struct {
	NodeID           string
	ScheduledEventID int64
	Identity         string
}

type NodeCompletedAttributes struct {
	NodeID           string
	ScheduledEventID int64
	StartedEventID   int64
	Result           []byte
	Logs             []byte
}

type NodeFailedAttributes struct {
	NodeID           string
	ScheduledEventID int64
	StartedEventID   int64
	Reason           string
	Details          []byte
	RetryState       int32
	Logs             []byte
}

type TimerStartedAttributes struct {
	TimerID     string
	StartToFire time.Duration
}

type TimerFiredAttributes struct {
	TimerID        string
	StartedEventID int64
}

type TimerCanceledAttributes struct {
	TimerID        string
	StartedEventID int64
	Identity       string
}

type ActivityScheduledAttributes struct {
	ActivityID       string
	ActivityType     string
	TaskQueue        string
	Input            []byte
	ScheduleToClose  time.Duration
	ScheduleToStart  time.Duration
	StartToClose     time.Duration
	HeartbeatTimeout time.Duration
	RetryPolicy      *RetryPolicy
}

type ActivityStartedAttributes struct {
	ScheduledEventID int64
	Identity         string
	Attempt          int32
}

type ActivityCompletedAttributes struct {
	ScheduledEventID int64
	StartedEventID   int64
	Result           []byte
}

type ActivityFailedAttributes struct {
	ScheduledEventID int64
	StartedEventID   int64
	Reason           string
	Details          []byte
	RetryState       int32
}

type RetryPolicy struct {
	InitialInterval    time.Duration
	BackoffCoefficient float64
	MaxInterval        time.Duration
	MaxAttempts        int32
}

type SignalReceivedAttributes struct {
	SignalName string
	Input      []byte
	Identity   string
}

type MarkerRecordedAttributes struct {
	MarkerName string
	Details    map[string][]byte
}

type WorkflowTaskScheduledAttributes struct {
	TaskQueue    string
	StartToClose time.Duration
	Attempt      int32
}

type WorkflowTaskStartedAttributes struct {
	ScheduledEventID int64
	Identity         string
	RequestID        string
}

type WorkflowTaskCompletedAttributes struct {
	ScheduledEventID int64
	StartedEventID   int64
	Identity         string
	BinaryChecksum   string
}

type WorkflowTaskFailedAttributes struct {
	ScheduledEventID int64
	StartedEventID   int64
	Cause            string
	FailureReason    string
	FailureDetails   []byte
	Identity         string
	BinaryChecksum   string
}

type WorkflowTaskTimedOutAttributes struct {
	ScheduledEventID int64
	StartedEventID   int64
	TimeoutType      string
}
