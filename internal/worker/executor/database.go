package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DatabaseExecutor handles database operations.
type DatabaseExecutor struct {
	pools map[string]*pgxpool.Pool
	mu    sync.Mutex
}

// DatabaseConfig represents the configuration for a database node.
type DatabaseConfig struct {
	// Connection
	ConnectionString string `json:"connection_string"` // Full connection string
	ConnectionName   string `json:"connection_name"`   // Named connection from pool

	// Query
	Operation string            `json:"operation"` // query, execute, transaction
	Query     string            `json:"query"`     // SQL query
	Params    []interface{}     `json:"params"`    // Query parameters
	Queries   []QueryWithParams `json:"queries"`   // For transaction mode

	// Options
	Timeout     int  `json:"timeout"`       // Query timeout in seconds
	SingleRow   bool `json:"single_row"`    // Return only first row
	RowsAsArray bool `json:"rows_as_array"` // Return rows as array instead of objects
}

// QueryWithParams represents a query with its parameters.
type QueryWithParams struct {
	Query  string        `json:"query"`
	Params []interface{} `json:"params"`
}

// DatabaseResponse represents the result of a database operation.
type DatabaseResponse struct {
	Rows         []map[string]interface{} `json:"rows,omitempty"`
	Row          map[string]interface{}   `json:"row,omitempty"`
	RowsAffected int64                    `json:"rows_affected"`
	LastInsertID interface{}              `json:"last_insert_id,omitempty"`
	Duration     string                   `json:"duration"`
}

// NewDatabaseExecutor creates a new database executor.
func NewDatabaseExecutor() *DatabaseExecutor {
	return &DatabaseExecutor{
		pools: make(map[string]*pgxpool.Pool),
	}
}

// RegisterConnection registers a named connection pool.
func (e *DatabaseExecutor) RegisterConnection(name string, pool *pgxpool.Pool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.pools[name] = pool
}

// Close closes all cached connection pools.
func (e *DatabaseExecutor) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	for key, pool := range e.pools {
		pool.Close()
		delete(e.pools, key)
	}
}

func (e *DatabaseExecutor) NodeType() string {
	return "database"
}

func (e *DatabaseExecutor) Execute(ctx context.Context, req *ExecuteRequest) (*ExecuteResponse, error) {
	start := time.Now()
	logs := make([]LogEntry, 0)

	logs = append(logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "INFO",
		Message:   fmt.Sprintf("Starting database execution for node %s", req.NodeID),
	})

	var config DatabaseConfig
	if err := json.Unmarshal(req.Config, &config); err != nil {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("failed to parse database config: %v", err),
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	// Get or create connection pool
	pool, err := e.getPool(ctx, config)
	if err != nil {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("failed to get connection: %v", err),
				Type:    ErrorTypeRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	// Set timeout
	timeout := time.Duration(config.Timeout) * time.Second
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var response DatabaseResponse
	response.Duration = "" // Will be set at the end

	switch config.Operation {
	case "query", "":
		response, err = e.executeQuery(ctx, pool, config, &logs)
	case "execute":
		response, err = e.executeCommand(ctx, pool, config, &logs)
	case "transaction":
		response, err = e.executeTransaction(ctx, pool, config, &logs)
	default:
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("unknown operation: %s", config.Operation),
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	if err != nil {
		errorType := ErrorTypeRetryable
		// Classify error
		errStr := err.Error()
		if contains(errStr, "syntax error") ||
			contains(errStr, "does not exist") ||
			contains(errStr, "permission denied") ||
			contains(errStr, "violates") {
			errorType = ErrorTypeNonRetryable
		}

		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: err.Error(),
				Type:    errorType,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	response.Duration = time.Since(start).String()

	logs = append(logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "INFO",
		Message:   fmt.Sprintf("Query completed, %d rows affected", response.RowsAffected),
	})

	output, err := json.Marshal(response)
	if err != nil {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("failed to marshal response: %v", err),
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	return &ExecuteResponse{
		Output:   output,
		Logs:     logs,
		Duration: time.Since(start),
	}, nil
}

func (e *DatabaseExecutor) getPool(ctx context.Context, config DatabaseConfig) (*pgxpool.Pool, error) {
	// Try named connection first
	if config.ConnectionName != "" {
		e.mu.Lock()
		pool, ok := e.pools[config.ConnectionName]
		e.mu.Unlock()
		if ok {
			return pool, nil
		}
		return nil, fmt.Errorf("connection '%s' not found", config.ConnectionName)
	}

	// Create or reuse a cached pool from connection string
	if config.ConnectionString != "" {
		e.mu.Lock()
		defer e.mu.Unlock()

		if pool, ok := e.pools[config.ConnectionString]; ok {
			return pool, nil
		}

		pool, err := pgxpool.New(ctx, config.ConnectionString)
		if err != nil {
			return nil, err
		}
		e.pools[config.ConnectionString] = pool
		return pool, nil
	}

	// Check for default pool
	e.mu.Lock()
	pool, ok := e.pools["default"]
	e.mu.Unlock()
	if ok {
		return pool, nil
	}

	return nil, fmt.Errorf("no connection configured")
}

func (e *DatabaseExecutor) executeQuery(ctx context.Context, pool *pgxpool.Pool, config DatabaseConfig, logs *[]LogEntry) (DatabaseResponse, error) {
	var response DatabaseResponse

	*logs = append(*logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "DEBUG",
		Message:   fmt.Sprintf("Executing query: %s", truncateString(config.Query, 200)),
	})

	rows, err := pool.Query(ctx, config.Query, config.Params...)
	if err != nil {
		return response, err
	}
	defer rows.Close()

	// Get column names
	fieldDescs := rows.FieldDescriptions()
	columns := make([]string, len(fieldDescs))
	for i, fd := range fieldDescs {
		columns[i] = fd.Name
	}

	// Fetch all rows
	response.Rows = make([]map[string]interface{}, 0)
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return response, err
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			row[col] = convertValue(values[i])
		}
		response.Rows = append(response.Rows, row)

		if config.SingleRow {
			break
		}
	}

	if err := rows.Err(); err != nil {
		return response, err
	}

	response.RowsAffected = int64(len(response.Rows))

	if config.SingleRow && len(response.Rows) > 0 {
		response.Row = response.Rows[0]
		response.Rows = nil
	}

	return response, nil
}

func (e *DatabaseExecutor) executeCommand(ctx context.Context, pool *pgxpool.Pool, config DatabaseConfig, logs *[]LogEntry) (DatabaseResponse, error) {
	var response DatabaseResponse

	*logs = append(*logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "DEBUG",
		Message:   fmt.Sprintf("Executing command: %s", truncateString(config.Query, 200)),
	})

	tag, err := pool.Exec(ctx, config.Query, config.Params...)
	if err != nil {
		return response, err
	}

	response.RowsAffected = tag.RowsAffected()

	return response, nil
}

func (e *DatabaseExecutor) executeTransaction(ctx context.Context, pool *pgxpool.Pool, config DatabaseConfig, logs *[]LogEntry) (DatabaseResponse, error) {
	var response DatabaseResponse

	*logs = append(*logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "INFO",
		Message:   fmt.Sprintf("Starting transaction with %d queries", len(config.Queries)),
	})

	tx, err := pool.Begin(ctx)
	if err != nil {
		return response, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var totalRowsAffected int64

	for i, q := range config.Queries {
		*logs = append(*logs, LogEntry{
			Timestamp: time.Now(),
			Level:     "DEBUG",
			Message:   fmt.Sprintf("Executing query %d: %s", i+1, truncateString(q.Query, 100)),
		})

		tag, err := tx.Exec(ctx, q.Query, q.Params...)
		if err != nil {
			return response, fmt.Errorf("query %d failed: %w", i+1, err)
		}
		totalRowsAffected += tag.RowsAffected()
	}

	if err := tx.Commit(ctx); err != nil {
		return response, fmt.Errorf("failed to commit transaction: %w", err)
	}

	response.RowsAffected = totalRowsAffected

	*logs = append(*logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "INFO",
		Message:   "Transaction committed successfully",
	})

	return response, nil
}

// convertValue converts pgx values to JSON-serializable types.
func convertValue(v interface{}) interface{} {
	if v == nil {
		return nil
	}

	switch val := v.(type) {
	case time.Time:
		return val.Format(time.RFC3339)
	case []byte:
		return string(val)
	default:
		return val
	}
}

func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || substr == "" ||
		(s != "" && substr != "" && findIndex(s, substr) >= 0))
}

func findIndex(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
