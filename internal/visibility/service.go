package visibility

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"
)

var (
	ErrExecutionNotFound = errors.New("execution not found")
	ErrInvalidQuery      = errors.New("invalid query")
)

// ExecutionStatus represents the status of a workflow execution.
type ExecutionStatus int32

const (
	ExecutionStatusUnspecified ExecutionStatus = iota
	ExecutionStatusRunning
	ExecutionStatusCompleted
	ExecutionStatusFailed
	ExecutionStatusTerminated
	ExecutionStatusTimedOut
	ExecutionStatusCanceled
)

func (s ExecutionStatus) String() string {
	names := map[ExecutionStatus]string{
		ExecutionStatusUnspecified: "Unspecified",
		ExecutionStatusRunning:     "Running",
		ExecutionStatusCompleted:   "Completed",
		ExecutionStatusFailed:      "Failed",
		ExecutionStatusTerminated:  "Terminated",
		ExecutionStatusTimedOut:    "TimedOut",
		ExecutionStatusCanceled:    "Canceled",
	}
	if name, ok := names[s]; ok {
		return name
	}
	return "Unknown"
}

// ExecutionInfo contains visibility information about an execution.
type ExecutionInfo struct {
	NamespaceID      string
	WorkflowID       string
	RunID            string
	WorkflowTypeName string
	Status           ExecutionStatus
	StartTime        time.Time
	CloseTime        time.Time
	ExecutionTime    time.Time
	Memo             json.RawMessage
	SearchAttributes map[string]interface{}
	TaskQueue        string
	ParentWorkflowID string
	ParentRunID      string
}

// ListRequest contains parameters for listing executions.
type ListRequest struct {
	NamespaceID   string
	PageSize      int32
	NextPageToken []byte
	Query         string // SQL-like query for filtering
}

// ListResponse contains the results of a list operation.
type ListResponse struct {
	Executions    []*ExecutionInfo
	NextPageToken []byte
}

// CountRequest contains parameters for counting executions.
type CountRequest struct {
	NamespaceID string
	Query       string
}

// CountResponse contains the result of a count operation.
type CountResponse struct {
	Count int64
}

// Store defines the interface for visibility persistence.
type Store interface {
	// RecordExecutionStarted records a started execution
	RecordExecutionStarted(ctx context.Context, info *ExecutionInfo) error
	// RecordExecutionClosed records a closed execution
	RecordExecutionClosed(ctx context.Context, info *ExecutionInfo) error
	// UpsertExecution upserts an execution record
	UpsertExecution(ctx context.Context, info *ExecutionInfo) error
	// GetExecution retrieves an execution
	GetExecution(ctx context.Context, namespaceID, workflowID, runID string) (*ExecutionInfo, error)
	// ListExecutions lists executions matching the criteria
	ListExecutions(ctx context.Context, req *ListRequest) (*ListResponse, error)
	// CountExecutions counts executions matching the criteria
	CountExecutions(ctx context.Context, req *CountRequest) (*CountResponse, error)
	// DeleteExecution deletes an execution record
	DeleteExecution(ctx context.Context, namespaceID, workflowID, runID string) error
}

// Config holds the configuration for the visibility service.
type Config struct {
	Logger *slog.Logger
}

// Service provides workflow execution visibility.
type Service struct {
	store  Store
	logger *slog.Logger
	mu     sync.RWMutex
}

// NewService creates a new visibility service.
func NewService(store Store, config Config) *Service {
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	return &Service{
		store:  store,
		logger: config.Logger,
	}
}

// RecordExecutionStarted records that an execution has started.
func (s *Service) RecordExecutionStarted(ctx context.Context, info *ExecutionInfo) error {
	info.Status = ExecutionStatusRunning
	if info.StartTime.IsZero() {
		info.StartTime = time.Now()
	}
	info.ExecutionTime = info.StartTime

	s.logger.Debug("recording execution started",
		slog.String("workflow_id", info.WorkflowID),
		slog.String("run_id", info.RunID),
	)

	return s.store.RecordExecutionStarted(ctx, info)
}

// RecordExecutionCompleted records that an execution has completed.
func (s *Service) RecordExecutionCompleted(ctx context.Context, namespaceID, workflowID, runID string, result json.RawMessage) error {
	info, err := s.store.GetExecution(ctx, namespaceID, workflowID, runID)
	if err != nil {
		return err
	}

	info.Status = ExecutionStatusCompleted
	info.CloseTime = time.Now()

	s.logger.Debug("recording execution completed",
		slog.String("workflow_id", workflowID),
		slog.String("run_id", runID),
	)

	return s.store.RecordExecutionClosed(ctx, info)
}

// RecordExecutionFailed records that an execution has failed.
func (s *Service) RecordExecutionFailed(ctx context.Context, namespaceID, workflowID, runID, reason string) error {
	info, err := s.store.GetExecution(ctx, namespaceID, workflowID, runID)
	if err != nil {
		return err
	}

	info.Status = ExecutionStatusFailed
	info.CloseTime = time.Now()

	s.logger.Debug("recording execution failed",
		slog.String("workflow_id", workflowID),
		slog.String("run_id", runID),
		slog.String("reason", reason),
	)

	return s.store.RecordExecutionClosed(ctx, info)
}

// RecordExecutionTerminated records that an execution has been terminated.
func (s *Service) RecordExecutionTerminated(ctx context.Context, namespaceID, workflowID, runID, reason string) error {
	info, err := s.store.GetExecution(ctx, namespaceID, workflowID, runID)
	if err != nil {
		return err
	}

	info.Status = ExecutionStatusTerminated
	info.CloseTime = time.Now()

	s.logger.Debug("recording execution terminated",
		slog.String("workflow_id", workflowID),
		slog.String("run_id", runID),
	)

	return s.store.RecordExecutionClosed(ctx, info)
}

// RecordExecutionTimedOut records that an execution has timed out.
func (s *Service) RecordExecutionTimedOut(ctx context.Context, namespaceID, workflowID, runID string) error {
	info, err := s.store.GetExecution(ctx, namespaceID, workflowID, runID)
	if err != nil {
		return err
	}

	info.Status = ExecutionStatusTimedOut
	info.CloseTime = time.Now()

	s.logger.Debug("recording execution timed out",
		slog.String("workflow_id", workflowID),
		slog.String("run_id", runID),
	)

	return s.store.RecordExecutionClosed(ctx, info)
}

// GetExecution retrieves visibility info for an execution.
func (s *Service) GetExecution(ctx context.Context, namespaceID, workflowID, runID string) (*ExecutionInfo, error) {
	return s.store.GetExecution(ctx, namespaceID, workflowID, runID)
}

// ListExecutions lists executions matching the request criteria.
func (s *Service) ListExecutions(ctx context.Context, req *ListRequest) (*ListResponse, error) {
	if req.PageSize <= 0 {
		req.PageSize = 100
	}
	if req.PageSize > 1000 {
		req.PageSize = 1000
	}

	return s.store.ListExecutions(ctx, req)
}

// ListOpenExecutions lists running executions.
func (s *Service) ListOpenExecutions(ctx context.Context, namespaceID string, pageSize int32, nextPageToken []byte) (*ListResponse, error) {
	return s.ListExecutions(ctx, &ListRequest{
		NamespaceID:   namespaceID,
		PageSize:      pageSize,
		NextPageToken: nextPageToken,
		Query:         "ExecutionStatus = 'Running'",
	})
}

// ListClosedExecutions lists closed executions.
func (s *Service) ListClosedExecutions(ctx context.Context, namespaceID string, pageSize int32, nextPageToken []byte) (*ListResponse, error) {
	return s.ListExecutions(ctx, &ListRequest{
		NamespaceID:   namespaceID,
		PageSize:      pageSize,
		NextPageToken: nextPageToken,
		Query:         "ExecutionStatus != 'Running'",
	})
}

// CountExecutions counts executions matching the query.
func (s *Service) CountExecutions(ctx context.Context, req *CountRequest) (*CountResponse, error) {
	return s.store.CountExecutions(ctx, req)
}

// UpdateSearchAttributes updates the search attributes for an execution.
func (s *Service) UpdateSearchAttributes(ctx context.Context, namespaceID, workflowID, runID string, attrs map[string]interface{}) error {
	info, err := s.store.GetExecution(ctx, namespaceID, workflowID, runID)
	if err != nil {
		return err
	}

	if info.SearchAttributes == nil {
		info.SearchAttributes = make(map[string]interface{})
	}

	for k, v := range attrs {
		info.SearchAttributes[k] = v
	}

	return s.store.UpsertExecution(ctx, info)
}

// Query represents a parsed visibility query.
type Query struct {
	Filters   []Filter
	OrderBy   string
	OrderDesc bool
	Limit     int32
	Offset    int64
}

// Filter represents a single filter condition.
type Filter struct {
	Field    string
	Operator string
	Value    interface{}
}

// ParseQuery parses a SQL-like query string into a Query struct.
func ParseQuery(query string) (*Query, error) {
	if query == "" {
		return &Query{}, nil
	}

	q := &Query{
		Filters: make([]Filter, 0),
	}

	// Simple query parser - handles basic conditions
	// Example: "ExecutionStatus = 'Running' AND WorkflowType = 'MyWorkflow'"

	query = strings.TrimSpace(query)

	// Handle ORDER BY
	orderByIdx := strings.Index(strings.ToUpper(query), " ORDER BY ")
	if orderByIdx > 0 {
		orderPart := query[orderByIdx+10:]
		query = query[:orderByIdx]

		orderParts := strings.Fields(orderPart)
		if len(orderParts) > 0 {
			q.OrderBy = orderParts[0]
			if len(orderParts) > 1 && strings.EqualFold(orderParts[1], "DESC") {
				q.OrderDesc = true
			}
		}
	}

	// Handle LIMIT
	limitIdx := strings.Index(strings.ToUpper(query), " LIMIT ")
	if limitIdx > 0 {
		// Parse limit
		query = query[:limitIdx]
	}

	// Parse conditions
	if query != "" {
		conditions := splitConditions(query)
		for _, cond := range conditions {
			filter, err := parseCondition(cond)
			if err != nil {
				return nil, err
			}
			if filter != nil {
				q.Filters = append(q.Filters, *filter)
			}
		}
	}

	return q, nil
}

func splitConditions(query string) []string {
	// Split by AND (case insensitive)
	upper := strings.ToUpper(query)
	var conditions []string
	var lastIdx int

	for {
		idx := strings.Index(upper[lastIdx:], " AND ")
		if idx < 0 {
			conditions = append(conditions, strings.TrimSpace(query[lastIdx:]))
			break
		}
		conditions = append(conditions, strings.TrimSpace(query[lastIdx:lastIdx+idx]))
		lastIdx += idx + 5
	}

	return conditions
}

func parseCondition(cond string) (*Filter, error) {
	cond = strings.TrimSpace(cond)
	if cond == "" {
		return nil, nil
	}

	// Try different operators
	operators := []string{"!=", ">=", "<=", "=", ">", "<", " LIKE ", " IN "}

	for _, op := range operators {
		opUpper := strings.ToUpper(op)
		condUpper := strings.ToUpper(cond)

		idx := strings.Index(condUpper, opUpper)
		if idx > 0 {
			field := strings.TrimSpace(cond[:idx])
			value := strings.TrimSpace(cond[idx+len(op):])

			// Clean up the value (remove quotes)
			value = strings.Trim(value, "'\"")

			return &Filter{
				Field:    field,
				Operator: strings.TrimSpace(op),
				Value:    value,
			}, nil
		}
	}

	return nil, ErrInvalidQuery
}
