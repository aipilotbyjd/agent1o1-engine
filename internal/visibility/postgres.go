package visibility

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresStore is a PostgreSQL implementation of the visibility store.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore creates a new PostgreSQL visibility store.
func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

// RecordExecutionStarted records a started execution.
func (s *PostgresStore) RecordExecutionStarted(ctx context.Context, info *ExecutionInfo) error {
	return s.UpsertExecution(ctx, info)
}

// RecordExecutionClosed records a closed execution.
func (s *PostgresStore) RecordExecutionClosed(ctx context.Context, info *ExecutionInfo) error {
	return s.UpsertExecution(ctx, info)
}

// UpsertExecution upserts an execution record.
func (s *PostgresStore) UpsertExecution(ctx context.Context, info *ExecutionInfo) error {
	searchAttrsJSON, _ := json.Marshal(info.SearchAttributes)

	_, err := s.pool.Exec(ctx, `
		INSERT INTO visibility (
			namespace_id, workflow_id, run_id, workflow_type_name,
			status, start_time, close_time, execution_time,
			memo, search_attributes, task_queue,
			parent_workflow_id, parent_run_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (namespace_id, workflow_id, run_id)
		DO UPDATE SET
			status = EXCLUDED.status,
			close_time = EXCLUDED.close_time,
			memo = EXCLUDED.memo,
			search_attributes = EXCLUDED.search_attributes
	`,
		info.NamespaceID,
		info.WorkflowID,
		info.RunID,
		info.WorkflowTypeName,
		int16(info.Status),
		info.StartTime,
		nullableTime(info.CloseTime),
		info.ExecutionTime,
		info.Memo,
		searchAttrsJSON,
		info.TaskQueue,
		nullableString(info.ParentWorkflowID),
		nullableString(info.ParentRunID),
	)
	if err != nil {
		return fmt.Errorf("failed to upsert execution: %w", err)
	}
	return nil
}

// GetExecution retrieves an execution.
func (s *PostgresStore) GetExecution(ctx context.Context, namespaceID, workflowID, runID string) (*ExecutionInfo, error) {
	var info ExecutionInfo
	var status int16
	var closeTime *time.Time
	var searchAttrsJSON []byte
	var parentWorkflowID, parentRunID *string

	err := s.pool.QueryRow(ctx, `
		SELECT namespace_id, workflow_id, run_id, workflow_type_name,
			   status, start_time, close_time, execution_time,
			   memo, search_attributes, task_queue,
			   parent_workflow_id, parent_run_id
		FROM visibility
		WHERE namespace_id = $1 AND workflow_id = $2 AND run_id = $3
	`, namespaceID, workflowID, runID).Scan(
		&info.NamespaceID,
		&info.WorkflowID,
		&info.RunID,
		&info.WorkflowTypeName,
		&status,
		&info.StartTime,
		&closeTime,
		&info.ExecutionTime,
		&info.Memo,
		&searchAttrsJSON,
		&info.TaskQueue,
		&parentWorkflowID,
		&parentRunID,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrExecutionNotFound
		}
		return nil, fmt.Errorf("failed to get execution: %w", err)
	}

	info.Status = ExecutionStatus(status)
	if closeTime != nil {
		info.CloseTime = *closeTime
	}
	if len(searchAttrsJSON) > 0 {
		json.Unmarshal(searchAttrsJSON, &info.SearchAttributes)
	}
	if parentWorkflowID != nil {
		info.ParentWorkflowID = *parentWorkflowID
	}
	if parentRunID != nil {
		info.ParentRunID = *parentRunID
	}

	return &info, nil
}

// ListExecutions lists executions matching the criteria.
func (s *PostgresStore) ListExecutions(ctx context.Context, req *ListRequest) (*ListResponse, error) {
	query, err := ParseQuery(req.Query)
	if err != nil {
		return nil, err
	}

	// Build SQL query
	sql := `
		SELECT namespace_id, workflow_id, run_id, workflow_type_name,
			   status, start_time, close_time, execution_time,
			   memo, search_attributes, task_queue,
			   parent_workflow_id, parent_run_id
		FROM visibility
		WHERE namespace_id = $1
	`
	args := []interface{}{req.NamespaceID}
	argIdx := 2

	// Apply filters
	for _, filter := range query.Filters {
		col := mapFieldToColumn(filter.Field)
		if col == "" {
			continue
		}
		sql += fmt.Sprintf(" AND %s %s $%d", col, mapOperator(filter.Operator), argIdx)
		args = append(args, filter.Value)
		argIdx++
	}

	// Apply ordering
	orderBy := "start_time"
	if query.OrderBy != "" {
		orderBy = mapFieldToColumn(query.OrderBy)
		if orderBy == "" {
			orderBy = "start_time"
		}
	}
	orderDir := "DESC"
	if !query.OrderDesc {
		orderDir = "ASC"
	}
	sql += fmt.Sprintf(" ORDER BY %s %s", orderBy, orderDir)

	// Apply pagination
	offset := int64(0)
	if len(req.NextPageToken) > 0 {
		decoded, _ := base64.StdEncoding.DecodeString(string(req.NextPageToken))
		var token struct {
			Offset int64 `json:"offset"`
		}
		if json.Unmarshal(decoded, &token) == nil {
			offset = token.Offset
		}
	}

	sql += fmt.Sprintf(" LIMIT $%d OFFSET $%d", argIdx, argIdx+1)
	args = append(args, req.PageSize+1, offset) // Fetch one extra to check if there's more

	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list executions: %w", err)
	}
	defer rows.Close()

	var executions []*ExecutionInfo
	for rows.Next() {
		var info ExecutionInfo
		var status int16
		var closeTime *time.Time
		var searchAttrsJSON []byte
		var parentWorkflowID, parentRunID *string

		if err := rows.Scan(
			&info.NamespaceID,
			&info.WorkflowID,
			&info.RunID,
			&info.WorkflowTypeName,
			&status,
			&info.StartTime,
			&closeTime,
			&info.ExecutionTime,
			&info.Memo,
			&searchAttrsJSON,
			&info.TaskQueue,
			&parentWorkflowID,
			&parentRunID,
		); err != nil {
			return nil, fmt.Errorf("failed to scan execution: %w", err)
		}

		info.Status = ExecutionStatus(status)
		if closeTime != nil {
			info.CloseTime = *closeTime
		}
		if len(searchAttrsJSON) > 0 {
			json.Unmarshal(searchAttrsJSON, &info.SearchAttributes)
		}
		if parentWorkflowID != nil {
			info.ParentWorkflowID = *parentWorkflowID
		}
		if parentRunID != nil {
			info.ParentRunID = *parentRunID
		}

		executions = append(executions, &info)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating executions: %w", err)
	}

	// Check if there are more results
	var nextToken []byte
	if len(executions) > int(req.PageSize) {
		executions = executions[:req.PageSize]
		tokenData, _ := json.Marshal(struct {
			Offset int64 `json:"offset"`
		}{Offset: offset + int64(req.PageSize)})
		nextToken = []byte(base64.StdEncoding.EncodeToString(tokenData))
	}

	return &ListResponse{
		Executions:    executions,
		NextPageToken: nextToken,
	}, nil
}

// CountExecutions counts executions matching the criteria.
func (s *PostgresStore) CountExecutions(ctx context.Context, req *CountRequest) (*CountResponse, error) {
	query, err := ParseQuery(req.Query)
	if err != nil {
		return nil, err
	}

	sql := `SELECT COUNT(*) FROM visibility WHERE namespace_id = $1`
	args := []interface{}{req.NamespaceID}
	argIdx := 2

	for _, filter := range query.Filters {
		col := mapFieldToColumn(filter.Field)
		if col == "" {
			continue
		}
		sql += fmt.Sprintf(" AND %s %s $%d", col, mapOperator(filter.Operator), argIdx)
		args = append(args, filter.Value)
		argIdx++
	}

	var count int64
	if err := s.pool.QueryRow(ctx, sql, args...).Scan(&count); err != nil {
		return nil, fmt.Errorf("failed to count executions: %w", err)
	}

	return &CountResponse{Count: count}, nil
}

// DeleteExecution deletes an execution record.
func (s *PostgresStore) DeleteExecution(ctx context.Context, namespaceID, workflowID, runID string) error {
	_, err := s.pool.Exec(ctx, `
		DELETE FROM visibility
		WHERE namespace_id = $1 AND workflow_id = $2 AND run_id = $3
	`, namespaceID, workflowID, runID)
	if err != nil {
		return fmt.Errorf("failed to delete execution: %w", err)
	}
	return nil
}

func mapFieldToColumn(field string) string {
	mapping := map[string]string{
		"ExecutionStatus":  "status",
		"Status":           "status",
		"WorkflowType":     "workflow_type_name",
		"WorkflowTypeName": "workflow_type_name",
		"WorkflowID":       "workflow_id",
		"RunID":            "run_id",
		"TaskQueue":        "task_queue",
		"StartTime":        "start_time",
		"CloseTime":        "close_time",
		"ExecutionTime":    "execution_time",
	}
	if col, ok := mapping[field]; ok {
		return col
	}
	return ""
}

func mapOperator(op string) string {
	switch op {
	case "=":
		return "="
	case "!=":
		return "!="
	case ">":
		return ">"
	case ">=":
		return ">="
	case "<":
		return "<"
	case "<=":
		return "<="
	case "LIKE":
		return "LIKE"
	default:
		return "="
	}
}

func nullableTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

func nullableString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
