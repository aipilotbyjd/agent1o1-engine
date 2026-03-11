package ndc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/linkflow/engine/internal/history/types"
)

var (
	ErrConflictDetected  = errors.New("conflict detected during replication")
	ErrBranchDiverged    = errors.New("branch has diverged")
	ErrReplicationFailed = errors.New("replication failed")
)

// Replicator handles N-DC (multi-datacenter) replication.
type Replicator struct {
	localClusterID  string
	remoteClients   map[string]ReplicationClient
	conflictHandler ConflictHandler
	logger          *slog.Logger

	mu sync.RWMutex
}

// ReplicationClient is the interface for remote cluster communication.
type ReplicationClient interface {
	SendReplicationTask(ctx context.Context, task *ReplicationTask) error
	FetchMissingEvents(ctx context.Context, executionID string, fromEventID int64) ([]*types.HistoryEvent, error)
	GetClusterInfo(ctx context.Context) (*ClusterInfo, error)
}

// ClusterInfo contains information about a remote cluster.
type ClusterInfo struct {
	ClusterID   string
	ClusterName string
	Address     string
	IsActive    bool
	LastSync    time.Time
}

// ReplicationTask represents a task to replicate events.
type ReplicationTask struct {
	TaskID        string
	SourceCluster string
	TargetCluster string
	ExecutionID   string
	Events        []*types.HistoryEvent
	Version       int64
	CreatedAt     time.Time
}

// Config holds replicator configuration.
type Config struct {
	LocalClusterID   string
	ReplicationDelay time.Duration
	MaxBatchSize     int
	RetryAttempts    int
	RetryBackoff     time.Duration
}

// DefaultConfig returns default replicator config.
func DefaultConfig() Config {
	return Config{
		LocalClusterID:   "cluster-1",
		ReplicationDelay: 100 * time.Millisecond,
		MaxBatchSize:     100,
		RetryAttempts:    3,
		RetryBackoff:     1 * time.Second,
	}
}

// NewReplicator creates a new replicator.
func NewReplicator(config Config, conflictHandler ConflictHandler, logger *slog.Logger) *Replicator {
	if logger == nil {
		logger = slog.Default()
	}
	if conflictHandler == nil {
		conflictHandler = &LastWriterWinsHandler{}
	}

	return &Replicator{
		localClusterID:  config.LocalClusterID,
		remoteClients:   make(map[string]ReplicationClient),
		conflictHandler: conflictHandler,
		logger:          logger,
	}
}

// AddRemoteCluster adds a remote cluster for replication.
func (r *Replicator) AddRemoteCluster(clusterID string, client ReplicationClient) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.remoteClients[clusterID] = client
}

// RemoveRemoteCluster removes a remote cluster.
func (r *Replicator) RemoveRemoteCluster(clusterID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.remoteClients, clusterID)
}

// ReplicateEvents replicates events to remote clusters.
func (r *Replicator) ReplicateEvents(ctx context.Context, executionID string, events []*types.HistoryEvent) error {
	r.mu.RLock()
	clients := make(map[string]ReplicationClient)
	for id, client := range r.remoteClients {
		clients[id] = client
	}
	r.mu.RUnlock()

	if len(clients) == 0 {
		return nil // No remote clusters to replicate to
	}

	task := &ReplicationTask{
		TaskID:        fmt.Sprintf("rep-%s-%d", executionID, time.Now().UnixNano()),
		SourceCluster: r.localClusterID,
		ExecutionID:   executionID,
		Events:        events,
		CreatedAt:     time.Now(),
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(clients))

	for clusterID, client := range clients {
		wg.Add(1)
		go func(id string, c ReplicationClient) {
			defer wg.Done()

			taskCopy := *task
			taskCopy.TargetCluster = id

			if err := c.SendReplicationTask(ctx, &taskCopy); err != nil {
				errCh <- fmt.Errorf("failed to replicate to %s: %w", id, err)
			}
		}(clusterID, client)
	}

	wg.Wait()
	close(errCh)

	// Collect errors
	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		r.logger.Warn("some replications failed",
			slog.String("execution_id", executionID),
			slog.Int("failed", len(errs)),
			slog.Int("total", len(clients)),
		)
		return errs[0]
	}

	r.logger.Debug("events replicated successfully",
		slog.String("execution_id", executionID),
		slog.Int("event_count", len(events)),
		slog.Int("clusters", len(clients)),
	)

	return nil
}

// ApplyReplicationTask applies incoming replication events.
func (r *Replicator) ApplyReplicationTask(ctx context.Context, task *ReplicationTask, localEvents []*types.HistoryEvent) ([]*types.HistoryEvent, error) {
	// Check for conflicts
	conflicts := r.detectConflicts(localEvents, task.Events)

	if len(conflicts) > 0 {
		r.logger.Warn("conflicts detected during replication",
			slog.String("execution_id", task.ExecutionID),
			slog.Int("conflict_count", len(conflicts)),
		)

		// Resolve conflicts
		resolved, err := r.conflictHandler.Resolve(ctx, conflicts)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve conflicts: %w", err)
		}

		return resolved, nil
	}

	// No conflicts, merge events
	merged := r.mergeEvents(localEvents, task.Events)
	return merged, nil
}

// Conflict represents a conflict between local and remote events.
type Conflict struct {
	LocalEvent  *types.HistoryEvent
	RemoteEvent *types.HistoryEvent
	Type        ConflictType
}

// ConflictType represents the type of conflict.
type ConflictType int

const (
	ConflictTypeOverlap ConflictType = iota
	ConflictTypeDivergence
	ConflictTypeVersion
)

func (r *Replicator) detectConflicts(local, remote []*types.HistoryEvent) []Conflict {
	var conflicts []Conflict

	localByID := make(map[int64]*types.HistoryEvent)
	for _, e := range local {
		localByID[e.EventID] = e
	}

	for _, remoteEvent := range remote {
		if localEvent, exists := localByID[remoteEvent.EventID]; exists {
			// Same event ID exists locally
			if localEvent.EventType != remoteEvent.EventType {
				conflicts = append(conflicts, Conflict{
					LocalEvent:  localEvent,
					RemoteEvent: remoteEvent,
					Type:        ConflictTypeDivergence,
				})
			}
		}
	}

	return conflicts
}

func (r *Replicator) mergeEvents(local, remote []*types.HistoryEvent) []*types.HistoryEvent {
	eventMap := make(map[int64]*types.HistoryEvent)

	// Add local events
	for _, e := range local {
		eventMap[e.EventID] = e
	}

	// Add remote events (newer wins based on timestamp)
	for _, e := range remote {
		if existing, exists := eventMap[e.EventID]; exists {
			if e.Timestamp.After(existing.Timestamp) {
				eventMap[e.EventID] = e
			}
		} else {
			eventMap[e.EventID] = e
		}
	}

	// Convert back to slice and sort by event ID
	result := make([]*types.HistoryEvent, 0, len(eventMap))
	for _, e := range eventMap {
		result = append(result, e)
	}

	// Sort by event ID
	for i := 1; i < len(result); i++ {
		for j := i; j > 0 && result[j].EventID < result[j-1].EventID; j-- {
			result[j], result[j-1] = result[j-1], result[j]
		}
	}

	return result
}

// ConflictHandler resolves conflicts between events.
type ConflictHandler interface {
	Resolve(ctx context.Context, conflicts []Conflict) ([]*types.HistoryEvent, error)
}

// LastWriterWinsHandler resolves conflicts using last-writer-wins.
type LastWriterWinsHandler struct{}

func (h *LastWriterWinsHandler) Resolve(ctx context.Context, conflicts []Conflict) ([]*types.HistoryEvent, error) {
	result := make([]*types.HistoryEvent, 0, len(conflicts))

	for _, c := range conflicts {
		// Choose the event with the later timestamp
		if c.RemoteEvent.Timestamp.After(c.LocalEvent.Timestamp) {
			result = append(result, c.RemoteEvent)
		} else {
			result = append(result, c.LocalEvent)
		}
	}

	return result, nil
}
