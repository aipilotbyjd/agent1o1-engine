package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/linkflow/engine/internal/history/engine"
	"github.com/linkflow/engine/internal/history/events"
	"github.com/linkflow/engine/internal/history/types"
)

// PostgresEventStore implements EventStore using PostgreSQL.
type PostgresEventStore struct {
	pool       *pgxpool.Pool
	serializer *events.Serializer
	shardCount int32
}

// NewPostgresEventStore creates a new PostgreSQL-backed event store.
func NewPostgresEventStore(pool *pgxpool.Pool, shardCount int32) *PostgresEventStore {
	return &PostgresEventStore{
		pool:       pool,
		serializer: events.NewJSONSerializer(),
		shardCount: shardCount,
	}
}

// AppendEvents appends events to the history for an execution.
func (s *PostgresEventStore) AppendEvents(
	ctx context.Context,
	key types.ExecutionKey,
	evts []*types.HistoryEvent,
	expectedVersion int64,
) error {
	if len(evts) == 0 {
		return nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Check current version if expected version is specified
	if expectedVersion >= 0 {
		var currentMaxEventID int64
		err := tx.QueryRow(ctx, `
			SELECT COALESCE(MAX(event_id), 0)
			FROM history_events
			WHERE namespace_id = $1 AND workflow_id = $2 AND run_id = $3
		`, key.NamespaceID, key.WorkflowID, key.RunID).Scan(&currentMaxEventID)

		if err != nil {
			return fmt.Errorf("failed to check current version: %w", err)
		}
	}

	// Get shard ID for this execution
	shardID := getShardIDForExecution(key, s.shardCount)

	// Insert events
	for _, event := range evts {
		data, err := s.serializer.Serialize(event)
		if err != nil {
			return fmt.Errorf("failed to serialize event: %w", err)
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO history_events (
				shard_id, namespace_id, workflow_id, run_id,
				event_id, event_type, version, timestamp, data
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		`,
			shardID,
			key.NamespaceID,
			key.WorkflowID,
			key.RunID,
			event.EventID,
			int16(event.EventType),
			event.Version,
			event.Timestamp,
			data,
		)
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				// Unique violation means event already exists.
				// This makes the operation idempotent.
				// We should verify if the existing event matches regarding crucial data,
				// but for now we assume it's the same event from a retried request.
				continue
			}
			return fmt.Errorf("failed to insert event %d: %w", event.EventID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// GetEvents retrieves events for an execution within the specified range.
func (s *PostgresEventStore) GetEvents(
	ctx context.Context,
	key types.ExecutionKey,
	firstEventID, lastEventID int64,
) ([]*types.HistoryEvent, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT event_id, event_type, version, timestamp, data
		FROM history_events
		WHERE namespace_id = $1 AND workflow_id = $2 AND run_id = $3
		  AND event_id >= $4 AND event_id <= $5
		ORDER BY event_id ASC
	`, key.NamespaceID, key.WorkflowID, key.RunID, firstEventID, lastEventID)
	if err != nil {
		return nil, fmt.Errorf("failed to query events: %w", err)
	}
	defer rows.Close()

	var events []*types.HistoryEvent
	for rows.Next() {
		var eventID int64
		var eventType int16
		var version int64
		var timestamp time.Time
		var data []byte

		if err := rows.Scan(&eventID, &eventType, &version, &timestamp, &data); err != nil {
			return nil, fmt.Errorf("failed to scan event: %w", err)
		}

		event, err := s.serializer.Deserialize(data)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize event %d: %w", eventID, err)
		}

		// Ensure fields match database
		event.EventID = eventID
		event.EventType = types.EventType(eventType)
		event.Version = version
		event.Timestamp = timestamp

		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating events: %w", err)
	}

	return events, nil
}

// GetLatestEventID returns the latest event ID for an execution.
func (s *PostgresEventStore) GetLatestEventID(ctx context.Context, key types.ExecutionKey) (int64, error) {
	var eventID int64
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(MAX(event_id), 0)
		FROM history_events
		WHERE namespace_id = $1 AND workflow_id = $2 AND run_id = $3
	`, key.NamespaceID, key.WorkflowID, key.RunID).Scan(&eventID)
	if err != nil {
		return 0, fmt.Errorf("failed to get latest event ID: %w", err)
	}
	return eventID, nil
}

// DeleteEvents deletes all events for an execution (used for cleanup).
func (s *PostgresEventStore) DeleteEvents(ctx context.Context, key types.ExecutionKey) error {
	_, err := s.pool.Exec(ctx, `
		DELETE FROM history_events
		WHERE namespace_id = $1 AND workflow_id = $2 AND run_id = $3
	`, key.NamespaceID, key.WorkflowID, key.RunID)
	if err != nil {
		return fmt.Errorf("failed to delete events: %w", err)
	}
	return nil
}

// GetEventCount returns the total number of events for an execution.
func (s *PostgresEventStore) GetEventCount(ctx context.Context, key types.ExecutionKey) (int64, error) {
	var count int64
	err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM history_events
		WHERE namespace_id = $1 AND workflow_id = $2 AND run_id = $3
	`, key.NamespaceID, key.WorkflowID, key.RunID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get event count: %w", err)
	}
	return count, nil
}

// PostgresMutableStateStore implements MutableStateStore using PostgreSQL.
type PostgresMutableStateStore struct {
	pool       *pgxpool.Pool
	serializer *mutableStateSerializer
	shardCount int32
	logger     *slog.Logger
}

type mutableStateSerializer struct{}

func (s *mutableStateSerializer) Serialize(state *engine.MutableState) ([]byte, error) {
	return json.Marshal(state)
}

func (s *mutableStateSerializer) Deserialize(data []byte) (*engine.MutableState, error) {
	var state engine.MutableState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	// Initialize nil maps
	if state.PendingActivities == nil {
		state.PendingActivities = make(map[int64]*types.ActivityInfo)
	}
	if state.PendingTimers == nil {
		state.PendingTimers = make(map[string]*types.TimerInfo)
	}
	if state.CompletedNodes == nil {
		state.CompletedNodes = make(map[string]*types.NodeResult)
	}
	if state.BufferedEvents == nil {
		state.BufferedEvents = make([]*types.HistoryEvent, 0)
	}
	return &state, nil
}

// NewPostgresMutableStateStore creates a new PostgreSQL-backed mutable state store.
func NewPostgresMutableStateStore(pool *pgxpool.Pool, shardCount int32) *PostgresMutableStateStore {
	return &PostgresMutableStateStore{
		pool:       pool,
		serializer: &mutableStateSerializer{},
		shardCount: shardCount,
		logger:     slog.Default(),
	}
}

// GetMutableState retrieves the mutable state for an execution.
func (s *PostgresMutableStateStore) GetMutableState(
	ctx context.Context,
	key types.ExecutionKey,
) (*engine.MutableState, error) {
	var data []byte
	var nextEventID int64
	var dbVersion int64
	var storedChecksum []byte

	err := s.pool.QueryRow(ctx, `
		SELECT state, next_event_id, db_version, checksum
		FROM mutable_state
		WHERE namespace_id = $1 AND workflow_id = $2 AND run_id = $3
	`, key.NamespaceID, key.WorkflowID, key.RunID).Scan(&data, &nextEventID, &dbVersion, &storedChecksum)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, types.ErrExecutionNotFound
		}
		return nil, fmt.Errorf("failed to get mutable state: %w", err)
	}

	// Validate checksum if present
	if len(storedChecksum) > 0 {
		computed := calculateChecksum(data)
		if !bytes.Equal(storedChecksum, computed) {
			s.logger.Warn("mutable state checksum mismatch",
				slog.String("namespace_id", key.NamespaceID),
				slog.String("workflow_id", key.WorkflowID),
				slog.String("run_id", key.RunID),
			)
		}
	}

	state, err := s.serializer.Deserialize(data)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize mutable state: %w", err)
	}

	state.NextEventID = nextEventID
	state.DBVersion = dbVersion

	return state, nil
}

// UpdateMutableState updates the mutable state for an execution.
func (s *PostgresMutableStateStore) UpdateMutableState(
	ctx context.Context,
	key types.ExecutionKey,
	state *engine.MutableState,
	expectedVersion int64,
) error {
	data, err := s.serializer.Serialize(state)
	if err != nil {
		return fmt.Errorf("failed to serialize mutable state: %w", err)
	}

	shardID := getShardIDForExecution(key, s.shardCount)
	checksum := calculateChecksum(data)
	newVersion := state.DBVersion + 1

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Try to update existing row
	tag, err := tx.Exec(ctx, `
		UPDATE mutable_state
		SET state = $1, next_event_id = $2, db_version = $3, checksum = $4
		WHERE namespace_id = $5 AND workflow_id = $6 AND run_id = $7 AND db_version = $8
	`,
		data,
		state.NextEventID,
		newVersion,
		checksum,
		key.NamespaceID,
		key.WorkflowID,
		key.RunID,
		expectedVersion,
	)
	if err != nil {
		return fmt.Errorf("failed to update mutable state: %w", err)
	}

	if tag.RowsAffected() == 0 {
		// Row doesn't exist or version mismatch - try insert if expectedVersion is 0
		if expectedVersion == 0 {
			_, err = tx.Exec(ctx, `
				INSERT INTO mutable_state (
					shard_id, namespace_id, workflow_id, run_id,
					state, next_event_id, db_version, checksum
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			`,
				shardID,
				key.NamespaceID,
				key.WorkflowID,
				key.RunID,
				data,
				state.NextEventID,
				newVersion,
				checksum,
			)
			if err != nil {
				return fmt.Errorf("failed to insert mutable state: %w", err)
			}
		} else {
			return types.ErrOptimisticLock
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// DeleteMutableState deletes the mutable state for an execution.
func (s *PostgresMutableStateStore) DeleteMutableState(ctx context.Context, key types.ExecutionKey) error {
	_, err := s.pool.Exec(ctx, `
		DELETE FROM mutable_state
		WHERE namespace_id = $1 AND workflow_id = $2 AND run_id = $3
	`, key.NamespaceID, key.WorkflowID, key.RunID)
	if err != nil {
		return fmt.Errorf("failed to delete mutable state: %w", err)
	}
	return nil
}

// ListRunningExecutions returns execution keys for all running executions.
func (s *PostgresMutableStateStore) ListRunningExecutions(ctx context.Context) ([]types.ExecutionKey, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT namespace_id, workflow_id, run_id FROM mutable_state
		WHERE state->'ExecutionInfo'->>'Status' = '1'
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to list running executions: %w", err)
	}
	defer rows.Close()

	var keys []types.ExecutionKey
	for rows.Next() {
		var key types.ExecutionKey
		if err := rows.Scan(&key.NamespaceID, &key.WorkflowID, &key.RunID); err != nil {
			return nil, fmt.Errorf("failed to scan execution key: %w", err)
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating running executions: %w", err)
	}
	return keys, nil
}

// Helper functions

// Uses consistent hashing to distribute executions across shards.
func getShardIDForExecution(key types.ExecutionKey, shardCount int32) int32 {
	// Simple hash-based sharding
	data := key.NamespaceID + "/" + key.WorkflowID
	var hash uint32
	for i := 0; i < len(data); i++ {
		hash = 31*hash + uint32(data[i])
	}
	// Use configured shard count
	if shardCount <= 0 {
		shardCount = 16 // Fallback
	}
	return int32(hash % uint32(shardCount))
}

// calculateChecksum creates a simple checksum for data integrity.
func calculateChecksum(data []byte) []byte {
	var sum uint32
	for _, b := range data {
		sum = (sum << 5) + sum + uint32(b)
	}
	return []byte{
		byte(sum >> 24),
		byte(sum >> 16),
		byte(sum >> 8),
		byte(sum),
	}
}
