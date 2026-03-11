package engine

import "time"

type TaskQueueKind int

const (
	TaskQueueKindNormal TaskQueueKind = iota
	TaskQueueKindSticky
)

type Task struct {
	ID               string
	Token            []byte
	WorkflowID       string
	RunID            string
	Namespace        string
	ActivityID       string
	ActivityType     string
	Input            []byte
	ScheduledTime    time.Time
	StartedTime      time.Time
	Attempt          int32
	Priority         int32
	TaskType         int32
	ScheduledEventID int64
}

type Poller struct {
	Identity  string
	ResultCh  chan *Task
	CreatedAt time.Time
}
