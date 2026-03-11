package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/linkflow/engine/internal/timer"
)

// PostgresStore is a PostgreSQL implementation of the timer store.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore creates a new PostgreSQL timer store.
func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

// CreateTimer creates a new timer.
func (s *PostgresStore) CreateTimer(ctx context.Context, t *timer.Timer) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO timers (
			shard_id, namespace_id, workflow_id, run_id, timer_id,
			fire_time, status, version, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`,
		t.ShardID,
		t.NamespaceID,
		t.WorkflowID,
		t.RunID,
		t.TimerID,
		t.FireTime,
		int16(t.Status),
		t.Version,
		t.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to create timer: %w", err)
	}
	return nil
}

// GetTimer retrieves a timer by ID.
func (s *PostgresStore) GetTimer(ctx context.Context, namespaceID, workflowID, runID, timerID string) (*timer.Timer, error) {
	var t timer.Timer
	var status int16
	var firedAt *time.Time

	err := s.pool.QueryRow(ctx, `
		SELECT shard_id, namespace_id, workflow_id, run_id, timer_id,
			   fire_time, status, version, created_at, fired_at
		FROM timers
		WHERE namespace_id = $1 AND workflow_id = $2 AND run_id = $3 AND timer_id = $4
	`, namespaceID, workflowID, runID, timerID).Scan(
		&t.ShardID,
		&t.NamespaceID,
		&t.WorkflowID,
		&t.RunID,
		&t.TimerID,
		&t.FireTime,
		&status,
		&t.Version,
		&t.CreatedAt,
		&firedAt,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, timer.ErrTimerNotFound
		}
		return nil, fmt.Errorf("failed to get timer: %w", err)
	}

	t.Status = timer.TimerStatus(status)
	if firedAt != nil {
		t.FiredAt = *firedAt
	}
	return &t, nil
}

// UpdateTimer updates a timer.
func (s *PostgresStore) UpdateTimer(ctx context.Context, t *timer.Timer) error {
	var firedAt *time.Time
	if !t.FiredAt.IsZero() {
		firedAt = &t.FiredAt
	}

	tag, err := s.pool.Exec(ctx, `
		UPDATE timers
		SET status = $1, version = $2, fired_at = $3
		WHERE namespace_id = $4 AND workflow_id = $5 AND run_id = $6 AND timer_id = $7 AND version = $8
	`,
		int16(t.Status),
		t.Version,
		firedAt,
		t.NamespaceID,
		t.WorkflowID,
		t.RunID,
		t.TimerID,
		t.Version-1, // Expected version
	)
	if err != nil {
		return fmt.Errorf("failed to update timer: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return timer.ErrOptimisticLockConflict
	}
	return nil
}

// DeleteTimer deletes a timer.
func (s *PostgresStore) DeleteTimer(ctx context.Context, namespaceID, workflowID, runID, timerID string) error {
	_, err := s.pool.Exec(ctx, `
		DELETE FROM timers
		WHERE namespace_id = $1 AND workflow_id = $2 AND run_id = $3 AND timer_id = $4
	`, namespaceID, workflowID, runID, timerID)
	if err != nil {
		return fmt.Errorf("failed to delete timer: %w", err)
	}
	return nil
}

// GetDueTimers returns all timers that are due for firing.
func (s *PostgresStore) GetDueTimers(ctx context.Context, shardID int32, fireTime time.Time, limit int) ([]*timer.Timer, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT shard_id, namespace_id, workflow_id, run_id, timer_id,
			   fire_time, status, version, created_at, fired_at
		FROM timers
		WHERE shard_id = $1 AND status = $2 AND fire_time <= $3
		ORDER BY fire_time ASC
		LIMIT $4
		FOR UPDATE SKIP LOCKED
	`, shardID, int16(timer.TimerStatusPending), fireTime, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get due timers: %w", err)
	}
	defer rows.Close()

	var timers []*timer.Timer
	for rows.Next() {
		var t timer.Timer
		var status int16
		var firedAt *time.Time
		if err := rows.Scan(
			&t.ShardID,
			&t.NamespaceID,
			&t.WorkflowID,
			&t.RunID,
			&t.TimerID,
			&t.FireTime,
			&status,
			&t.Version,
			&t.CreatedAt,
			&firedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan timer: %w", err)
		}
		t.Status = timer.TimerStatus(status)
		if firedAt != nil {
			t.FiredAt = *firedAt
		}
		timers = append(timers, &t)
	}

	return timers, rows.Err()
}

// GetTimersByExecution returns all timers for an execution.
func (s *PostgresStore) GetTimersByExecution(ctx context.Context, namespaceID, workflowID, runID string) ([]*timer.Timer, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT shard_id, namespace_id, workflow_id, run_id, timer_id,
			   fire_time, status, version, created_at, fired_at
		FROM timers
		WHERE namespace_id = $1 AND workflow_id = $2 AND run_id = $3
		ORDER BY fire_time ASC
	`, namespaceID, workflowID, runID)
	if err != nil {
		return nil, fmt.Errorf("failed to get timers: %w", err)
	}
	defer rows.Close()

	var timers []*timer.Timer
	for rows.Next() {
		var t timer.Timer
		var status int16
		var firedAt *time.Time
		if err := rows.Scan(
			&t.ShardID,
			&t.NamespaceID,
			&t.WorkflowID,
			&t.RunID,
			&t.TimerID,
			&t.FireTime,
			&status,
			&t.Version,
			&t.CreatedAt,
			&firedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan timer: %w", err)
		}
		t.Status = timer.TimerStatus(status)
		if firedAt != nil {
			t.FiredAt = *firedAt
		}
		timers = append(timers, &t)
	}

	return timers, rows.Err()
}

// CleanupFiredTimers removes fired timers older than the specified retention period.
func (s *PostgresStore) CleanupFiredTimers(ctx context.Context, olderThan time.Time) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM timers
		WHERE status = $1 AND fired_at < $2
	`, int16(timer.TimerStatusFired), olderThan)
	if err != nil {
		return 0, fmt.Errorf("failed to cleanup timers: %w", err)
	}
	return tag.RowsAffected(), nil
}
