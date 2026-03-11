package visibility

import (
	"context"
	"time"

	apiv1 "github.com/linkflow/engine/api/gen/linkflow/api/v1"
	commonv1 "github.com/linkflow/engine/api/gen/linkflow/common/v1"
)

// ListRequest specifies the criteria for listing executions.
type ListRequest struct {
	NamespaceID   string
	PageSize      int
	NextPageToken []byte
	Query         string // Simple query support (e.g. "WorkflowType = 'foo'")
}

// ListResponse contains the list of executions.
type ListResponse struct {
	Executions    []*WorkflowExecutionInfo
	NextPageToken []byte
}

// WorkflowExecutionInfo contains summary information about a workflow execution.
type WorkflowExecutionInfo struct {
	Execution     *commonv1.WorkflowExecution
	Type          *apiv1.WorkflowType
	StartTime     time.Time
	CloseTime     time.Time
	Status        commonv1.ExecutionStatus
	HistoryLength int64
	Memo          *commonv1.Memo
}

// Store defines the interface for visibility storage.
type Store interface {
	RecordWorkflowExecutionStarted(ctx context.Context, req *RecordWorkflowExecutionStartedRequest) error
	RecordWorkflowExecutionClosed(ctx context.Context, req *RecordWorkflowExecutionClosedRequest) error
	ListOpenWorkflowExecutions(ctx context.Context, req *ListRequest) (*ListResponse, error)
	ListClosedWorkflowExecutions(ctx context.Context, req *ListRequest) (*ListResponse, error)
	// TODO: Add generic ListWorkflowExecutions with query support
}

type RecordWorkflowExecutionStartedRequest struct {
	NamespaceID  string
	Execution    *commonv1.WorkflowExecution
	WorkflowType *apiv1.WorkflowType
	StartTime    time.Time
	Status       commonv1.ExecutionStatus
	Memo         *commonv1.Memo
}

type RecordWorkflowExecutionClosedRequest struct {
	NamespaceID   string
	Execution     *commonv1.WorkflowExecution
	WorkflowType  *apiv1.WorkflowType
	StartTime     time.Time
	CloseTime     time.Time
	Status        commonv1.ExecutionStatus
	HistoryLength int64
	Memo          *commonv1.Memo
}
