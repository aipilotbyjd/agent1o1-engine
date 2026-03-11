package visibility

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"time"
)

// MemoryStore is an in-memory implementation of the visibility store.
type MemoryStore struct {
	executions map[string]*ExecutionInfo
	mu         sync.RWMutex
}

// NewMemoryStore creates a new in-memory visibility store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		executions: make(map[string]*ExecutionInfo),
	}
}

func (s *MemoryStore) makeKey(namespaceID, workflowID, runID string) string {
	return namespaceID + "/" + workflowID + "/" + runID
}

// RecordExecutionStarted records a started execution.
func (s *MemoryStore) RecordExecutionStarted(ctx context.Context, info *ExecutionInfo) error {
	return s.UpsertExecution(ctx, info)
}

// RecordExecutionClosed records a closed execution.
func (s *MemoryStore) RecordExecutionClosed(ctx context.Context, info *ExecutionInfo) error {
	return s.UpsertExecution(ctx, info)
}

// UpsertExecution upserts an execution record.
func (s *MemoryStore) UpsertExecution(ctx context.Context, info *ExecutionInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := s.makeKey(info.NamespaceID, info.WorkflowID, info.RunID)

	// Clone to avoid external modification
	clone := *info
	if info.SearchAttributes != nil {
		clone.SearchAttributes = make(map[string]interface{})
		for k, v := range info.SearchAttributes {
			clone.SearchAttributes[k] = v
		}
	}

	s.executions[key] = &clone
	return nil
}

// GetExecution retrieves an execution.
func (s *MemoryStore) GetExecution(ctx context.Context, namespaceID, workflowID, runID string) (*ExecutionInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := s.makeKey(namespaceID, workflowID, runID)
	info, exists := s.executions[key]
	if !exists {
		return nil, ErrExecutionNotFound
	}

	// Return a clone
	clone := *info
	return &clone, nil
}

// ListExecutions lists executions matching the criteria.
func (s *MemoryStore) ListExecutions(ctx context.Context, req *ListRequest) (*ListResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	query, err := ParseQuery(req.Query)
	if err != nil {
		return nil, err
	}

	// Collect matching executions
	var matches []*ExecutionInfo
	for _, info := range s.executions {
		if info.NamespaceID != req.NamespaceID {
			continue
		}

		if s.matchesQuery(info, query) {
			clone := *info
			matches = append(matches, &clone)
		}
	}

	// Sort results
	s.sortExecutions(matches, query)

	// Apply pagination
	var startIdx int
	if len(req.NextPageToken) > 0 {
		decoded, err := base64.StdEncoding.DecodeString(string(req.NextPageToken))
		if err == nil {
			var token struct {
				Offset int `json:"offset"`
			}
			if json.Unmarshal(decoded, &token) == nil {
				startIdx = token.Offset
			}
		}
	}

	endIdx := startIdx + int(req.PageSize)
	if endIdx > len(matches) {
		endIdx = len(matches)
	}

	var resultExecutions []*ExecutionInfo
	if startIdx < len(matches) {
		resultExecutions = matches[startIdx:endIdx]
	}

	// Generate next page token
	var nextToken []byte
	if endIdx < len(matches) {
		tokenData, _ := json.Marshal(struct {
			Offset int `json:"offset"`
		}{Offset: endIdx})
		nextToken = []byte(base64.StdEncoding.EncodeToString(tokenData))
	}

	return &ListResponse{
		Executions:    resultExecutions,
		NextPageToken: nextToken,
	}, nil
}

// CountExecutions counts executions matching the criteria.
func (s *MemoryStore) CountExecutions(ctx context.Context, req *CountRequest) (*CountResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	query, err := ParseQuery(req.Query)
	if err != nil {
		return nil, err
	}

	var count int64
	for _, info := range s.executions {
		if info.NamespaceID != req.NamespaceID {
			continue
		}
		if s.matchesQuery(info, query) {
			count++
		}
	}

	return &CountResponse{Count: count}, nil
}

// DeleteExecution deletes an execution record.
func (s *MemoryStore) DeleteExecution(ctx context.Context, namespaceID, workflowID, runID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := s.makeKey(namespaceID, workflowID, runID)
	delete(s.executions, key)
	return nil
}

func (s *MemoryStore) matchesQuery(info *ExecutionInfo, query *Query) bool {
	if query == nil || len(query.Filters) == 0 {
		return true
	}

	for _, filter := range query.Filters {
		if !s.matchesFilter(info, filter) {
			return false
		}
	}

	return true
}

func (s *MemoryStore) matchesFilter(info *ExecutionInfo, filter Filter) bool {
	var fieldValue interface{}

	switch strings.ToLower(filter.Field) {
	case "executionstatus", "status":
		fieldValue = info.Status.String()
	case "workflowtype", "workflowtypename":
		fieldValue = info.WorkflowTypeName
	case "workflowid":
		fieldValue = info.WorkflowID
	case "runid":
		fieldValue = info.RunID
	case "taskqueue":
		fieldValue = info.TaskQueue
	case "starttime":
		fieldValue = info.StartTime
	case "closetime":
		fieldValue = info.CloseTime
	default:
		// Check search attributes
		if info.SearchAttributes != nil {
			fieldValue = info.SearchAttributes[filter.Field]
		}
	}

	return s.compareValues(fieldValue, filter.Operator, filter.Value)
}

func (s *MemoryStore) compareValues(fieldValue interface{}, operator string, filterValue interface{}) bool {
	op := strings.TrimSpace(strings.ToUpper(operator))
	filterStr, _ := filterValue.(string)

	switch v := fieldValue.(type) {
	case string:
		switch op {
		case "=":
			return strings.EqualFold(v, filterStr)
		case "!=":
			return !strings.EqualFold(v, filterStr)
		case "LIKE":
			pattern := strings.ReplaceAll(filterStr, "%", "")
			return strings.Contains(strings.ToLower(v), strings.ToLower(pattern))
		}
	case time.Time:
		filterTime, err := time.Parse(time.RFC3339, filterStr)
		if err != nil {
			return false
		}
		switch op {
		case "=":
			return v.Equal(filterTime)
		case "!=":
			return !v.Equal(filterTime)
		case ">":
			return v.After(filterTime)
		case ">=":
			return v.After(filterTime) || v.Equal(filterTime)
		case "<":
			return v.Before(filterTime)
		case "<=":
			return v.Before(filterTime) || v.Equal(filterTime)
		}
	case int, int32, int64, float32, float64:
		// Handle numeric comparisons if needed
	}

	// Fallback to string comparison
	return strings.EqualFold(toString(fieldValue), filterStr)
}

func toString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	default:
		data, _ := json.Marshal(v)
		return string(data)
	}
}

func (s *MemoryStore) sortExecutions(executions []*ExecutionInfo, query *Query) {
	orderBy := query.OrderBy
	if orderBy == "" {
		orderBy = "StartTime"
	}
	desc := query.OrderDesc

	sort.Slice(executions, func(i, j int) bool {
		var less bool
		switch strings.ToLower(orderBy) {
		case "starttime":
			less = executions[i].StartTime.Before(executions[j].StartTime)
		case "closetime":
			less = executions[i].CloseTime.Before(executions[j].CloseTime)
		case "workflowid":
			less = executions[i].WorkflowID < executions[j].WorkflowID
		case "workflowtype", "workflowtypename":
			less = executions[i].WorkflowTypeName < executions[j].WorkflowTypeName
		case "status", "executionstatus":
			less = executions[i].Status < executions[j].Status
		default:
			less = executions[i].StartTime.Before(executions[j].StartTime)
		}
		if desc {
			return !less
		}
		return less
	})
}

// Clear removes all executions (for testing).
func (s *MemoryStore) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.executions = make(map[string]*ExecutionInfo)
}

// Count returns the number of executions.
func (s *MemoryStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.executions)
}
