package engine

import (
	"context"
	"time"

	"github.com/linkflow/engine/internal/history/types"
)

// Snapshot represents a point-in-time checkpoint of execution state.
type Snapshot struct {
	ExecutionKey types.ExecutionKey
	State        *MutableState
	LastEventID  int64
	CreatedAt    time.Time
	Checksum     []byte
}

// SnapshotStore defines the interface for storing and retrieving state snapshots.
type SnapshotStore interface {
	SaveSnapshot(ctx context.Context, snapshot *Snapshot) error
	GetLatestSnapshot(ctx context.Context, key types.ExecutionKey) (*Snapshot, error)
	DeleteSnapshots(ctx context.Context, key types.ExecutionKey) error
}
