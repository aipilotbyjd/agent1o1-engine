package replay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/linkflow/engine/internal/history/engine"
	"github.com/linkflow/engine/internal/history/store"
	"github.com/linkflow/engine/internal/history/types"
)

var (
	ErrReplayFailed    = errors.New("replay failed")
	ErrEventMismatch   = errors.New("event mismatch during replay")
	ErrVersionMismatch = errors.New("version mismatch during replay")
)

// Replayer replays workflow executions from history.
type Replayer struct {
	eventStore store.EventStore
	stateStore store.MutableStateStore
	logger     *slog.Logger
}

// NewReplayer creates a new replayer.
func NewReplayer(eventStore store.EventStore, stateStore store.MutableStateStore, logger *slog.Logger) *Replayer {
	if logger == nil {
		logger = slog.Default()
	}
	return &Replayer{
		eventStore: eventStore,
		stateStore: stateStore,
		logger:     logger,
	}
}

// ReplayResult contains the result of a replay operation.
type ReplayResult struct {
	ExecutionID    string
	MutableState   *engine.MutableState
	EventsReplayed int64
	Duration       time.Duration
	Errors         []ReplayError
}

// ReplayError represents an error during replay.
type ReplayError struct {
	EventID int64
	Error   error
}

// Replay replays an execution from its event history.
func (r *Replayer) Replay(ctx context.Context, key types.ExecutionKey, targetEventID int64) (*ReplayResult, error) {
	start := time.Now()

	r.logger.Info("starting replay",
		slog.String("workflow_id", key.WorkflowID),
		slog.String("run_id", key.RunID),
		slog.Int64("target_event_id", targetEventID),
	)

	// Fetch history events
	events, err := r.eventStore.GetEvents(ctx, key, 0, targetEventID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch history: %w", err)
	}

	if len(events) == 0 {
		return nil, fmt.Errorf("no events found for execution: %s", key.WorkflowID)
	}

	// Get initial state from first event
	firstEvent := events[0]
	if firstEvent.EventType != types.EventTypeExecutionStarted {
		return nil, errors.New("first event must be ExecutionStarted")
	}

	// Initialize mutable state
	execInfo := &types.ExecutionInfo{
		NamespaceID: key.NamespaceID,
		WorkflowID:  key.WorkflowID,
		RunID:       key.RunID,
	}
	ms := engine.NewMutableState(execInfo)

	result := &ReplayResult{
		ExecutionID:  key.WorkflowID,
		MutableState: ms,
	}

	// Replay each event
	for _, event := range events {
		if err := ms.ApplyEvent(event); err != nil {
			result.Errors = append(result.Errors, ReplayError{
				EventID: event.EventID,
				Error:   err,
			})
			r.logger.Warn("error applying event during replay",
				slog.Int64("event_id", event.EventID),
				slog.String("event_type", event.EventType.String()),
				slog.String("error", err.Error()),
			)
		}
		result.EventsReplayed++
	}

	result.Duration = time.Since(start)

	r.logger.Info("replay completed",
		slog.String("workflow_id", key.WorkflowID),
		slog.Int64("events_replayed", result.EventsReplayed),
		slog.Duration("duration", result.Duration),
		slog.Int("errors", len(result.Errors)),
	)

	return result, nil
}

// ReplayToPoint replays to a specific point in time.
func (r *Replayer) ReplayToPoint(ctx context.Context, key types.ExecutionKey, timestamp time.Time) (*ReplayResult, error) {
	// Fetch all events
	events, err := r.eventStore.GetEvents(ctx, key, 0, 10000)
	if err != nil {
		return nil, err
	}

	// Find the last event before the timestamp
	var targetEventID int64 = 0
	for _, event := range events {
		if event.Timestamp.Before(timestamp) || event.Timestamp.Equal(timestamp) {
			targetEventID = event.EventID
		} else {
			break
		}
	}

	if targetEventID == 0 {
		return nil, errors.New("no events found before specified timestamp")
	}

	return r.Replay(ctx, key, targetEventID)
}

// Compare compares the replay result with stored mutable state.
func (r *Replayer) Compare(ctx context.Context, key types.ExecutionKey) (*ComparisonResult, error) {
	// Get stored state
	stored, err := r.stateStore.GetMutableState(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get stored state: %w", err)
	}

	// Replay from history
	replayResult, err := r.Replay(ctx, key, stored.NextEventID-1)
	if err != nil {
		return nil, fmt.Errorf("replay failed: %w", err)
	}

	// Compare states
	result := &ComparisonResult{
		ExecutionID: key.WorkflowID,
		Match:       true,
	}

	replayed := replayResult.MutableState

	// Compare next event IDs
	if stored.NextEventID != replayed.NextEventID {
		result.Match = false
		result.Differences = append(result.Differences, Difference{
			Field:    "NextEventID",
			Stored:   stored.NextEventID,
			Replayed: replayed.NextEventID,
		})
	}

	// Compare pending activities count
	if len(stored.PendingActivities) != len(replayed.PendingActivities) {
		result.Match = false
		result.Differences = append(result.Differences, Difference{
			Field:    "PendingActivitiesCount",
			Stored:   len(stored.PendingActivities),
			Replayed: len(replayed.PendingActivities),
		})
	}

	// Compare pending timers count
	if len(stored.PendingTimers) != len(replayed.PendingTimers) {
		result.Match = false
		result.Differences = append(result.Differences, Difference{
			Field:    "PendingTimersCount",
			Stored:   len(stored.PendingTimers),
			Replayed: len(replayed.PendingTimers),
		})
	}

	return result, nil
}

// ComparisonResult contains the result of comparing stored vs replayed state.
type ComparisonResult struct {
	ExecutionID string
	Match       bool
	Differences []Difference
}

// Difference represents a single difference between stored and replayed state.
type Difference struct {
	Field    string
	Stored   interface{}
	Replayed interface{}
}

// ValidateHistoryIntegrity validates the integrity of execution history.
func (r *Replayer) ValidateHistoryIntegrity(ctx context.Context, key types.ExecutionKey) error {
	// Fetch all events
	events, err := r.eventStore.GetEvents(ctx, key, 0, 100000)
	if err != nil {
		return err
	}

	// Validate event ordering
	var lastEventID int64 = 0
	for _, event := range events {
		if event.EventID != lastEventID+1 {
			return fmt.Errorf("event ordering gap: expected %d, got %d", lastEventID+1, event.EventID)
		}
		lastEventID = event.EventID
	}

	// Validate first event is execution started
	if len(events) > 0 && events[0].EventType != types.EventTypeExecutionStarted {
		return errors.New("first event must be ExecutionStarted")
	}

	// Validate terminal state
	if len(events) > 0 {
		lastEvent := events[len(events)-1]
		switch lastEvent.EventType {
		case types.EventTypeExecutionCompleted,
			types.EventTypeExecutionFailed,
			types.EventTypeExecutionTerminated:
			// Valid terminal state
		default:
			// Execution still in progress - validate no terminal state in middle
			for i := 0; i < len(events)-1; i++ {
				switch events[i].EventType {
				case types.EventTypeExecutionCompleted,
					types.EventTypeExecutionFailed,
					types.EventTypeExecutionTerminated:
					return fmt.Errorf("terminal event found at position %d, not at end", i)
				}
			}
		}
	}

	r.logger.Info("history integrity validated",
		slog.String("workflow_id", key.WorkflowID),
		slog.Int("event_count", len(events)),
	)

	return nil
}
