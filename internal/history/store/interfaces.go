package store

import (
	"context"

	"github.com/linkflow/engine/internal/history/engine"
	"github.com/linkflow/engine/internal/history/types"
)

type EventStore interface {
	AppendEvents(ctx context.Context, key types.ExecutionKey, events []*types.HistoryEvent, expectedVersion int64) error
	GetEvents(ctx context.Context, key types.ExecutionKey, firstEventID, lastEventID int64) ([]*types.HistoryEvent, error)
	GetEventCount(ctx context.Context, key types.ExecutionKey) (int64, error)
}

type MutableStateStore interface {
	GetMutableState(ctx context.Context, key types.ExecutionKey) (*engine.MutableState, error)
	UpdateMutableState(ctx context.Context, key types.ExecutionKey, state *engine.MutableState, expectedVersion int64) error
	ListRunningExecutions(ctx context.Context) ([]types.ExecutionKey, error)
}
