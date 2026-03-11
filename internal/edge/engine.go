package edge

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

var (
	ErrOfflineMode      = errors.New("edge engine is in offline mode")
	ErrSyncRequired     = errors.New("synchronization required")
	ErrExecutionPending = errors.New("execution pending sync")
)

// ExecutionMode represents the edge execution mode.
type ExecutionMode int

const (
	ExecutionModeOnline ExecutionMode = iota
	ExecutionModeOffline
	ExecutionModeHybrid
)

// SyncStatus represents synchronization status.
type SyncStatus int

const (
	SyncStatusSynced SyncStatus = iota
	SyncStatusPending
	SyncStatusConflict
	SyncStatusFailed
)

// EdgeExecution represents an execution on the edge.
type EdgeExecution struct {
	ID           string
	NamespaceID  string
	WorkflowID   string
	RunID        string
	Status       ExecutionStatus
	Input        json.RawMessage
	Output       json.RawMessage
	Events       []*EdgeEvent
	StartTime    time.Time
	EndTime      time.Time
	SyncStatus   SyncStatus
	LastSyncTime time.Time
	Version      int64
}

// ExecutionStatus for edge.
type ExecutionStatus int

const (
	ExecutionStatusPending ExecutionStatus = iota
	ExecutionStatusRunning
	ExecutionStatusCompleted
	ExecutionStatusFailed
)

// EdgeEvent represents an event captured at the edge.
type EdgeEvent struct {
	ID         int64
	Type       string
	Timestamp  time.Time
	Data       json.RawMessage
	SyncStatus SyncStatus
}

// CentralClient interface for communicating with central cluster.
type CentralClient interface {
	SyncExecution(ctx context.Context, exec *EdgeExecution) error
	GetWorkflowDefinition(ctx context.Context, namespaceID, workflowID string) (json.RawMessage, error)
	SendHeartbeat(ctx context.Context, edgeID string) error
}

// LocalStore interface for local edge storage.
type LocalStore interface {
	SaveExecution(ctx context.Context, exec *EdgeExecution) error
	GetExecution(ctx context.Context, id string) (*EdgeExecution, error)
	ListPendingSyncs(ctx context.Context) ([]*EdgeExecution, error)
	GetWorkflowDefinition(ctx context.Context, namespaceID, workflowID string) (json.RawMessage, error)
	CacheWorkflowDefinition(ctx context.Context, namespaceID, workflowID string, def json.RawMessage) error
}

// Config holds edge engine configuration.
type Config struct {
	EdgeID             string
	Region             string
	CentralEndpoint    string
	SyncInterval       time.Duration
	OfflineGracePeriod time.Duration
	MaxOfflineEvents   int
	Logger             *slog.Logger
}

// DefaultConfig returns default edge configuration.
func DefaultConfig() Config {
	return Config{
		SyncInterval:       30 * time.Second,
		OfflineGracePeriod: 24 * time.Hour,
		MaxOfflineEvents:   10000,
	}
}

// Engine is the edge execution engine.
type Engine struct {
	config        Config
	logger        *slog.Logger
	centralClient CentralClient
	localStore    LocalStore

	mode               ExecutionMode
	lastCentralContact time.Time

	executions  map[string]*EdgeExecution
	pendingSync []*EdgeExecution

	mu      sync.RWMutex
	stopCh  chan struct{}
	running bool
}

// NewEngine creates a new edge engine.
func NewEngine(config Config, centralClient CentralClient, localStore LocalStore) *Engine {
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	if config.SyncInterval == 0 {
		config.SyncInterval = 30 * time.Second
	}
	if config.OfflineGracePeriod == 0 {
		config.OfflineGracePeriod = 24 * time.Hour
	}

	return &Engine{
		config:        config,
		logger:        config.Logger,
		centralClient: centralClient,
		localStore:    localStore,
		mode:          ExecutionModeOnline,
		executions:    make(map[string]*EdgeExecution),
		pendingSync:   make([]*EdgeExecution, 0),
		stopCh:        make(chan struct{}),
	}
}

// Start starts the edge engine.
func (e *Engine) Start(ctx context.Context) error {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return errors.New("edge engine already running")
	}
	e.running = true
	e.stopCh = make(chan struct{})
	e.mu.Unlock()

	// Load pending syncs from local store
	if pending, err := e.localStore.ListPendingSyncs(ctx); err == nil {
		e.pendingSync = pending
	}

	// Start background tasks
	go e.runSyncLoop(ctx)
	go e.runHealthCheck(ctx)

	e.logger.Info("edge engine started",
		slog.String("edge_id", e.config.EdgeID),
		slog.String("region", e.config.Region),
	)

	return nil
}

// Stop stops the edge engine.
func (e *Engine) Stop(ctx context.Context) error {
	e.mu.Lock()
	if !e.running {
		e.mu.Unlock()
		return nil
	}
	e.running = false
	close(e.stopCh)
	e.mu.Unlock()

	// Attempt final sync
	e.syncPending(ctx)

	e.logger.Info("edge engine stopped")
	return nil
}

// StartExecution starts a new execution on the edge.
func (e *Engine) StartExecution(ctx context.Context, namespaceID, workflowID string, input json.RawMessage) (*EdgeExecution, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	exec := &EdgeExecution{
		ID:          generateEdgeExecutionID(),
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       generateEdgeExecutionID(),
		Status:      ExecutionStatusPending,
		Input:       input,
		Events:      make([]*EdgeEvent, 0),
		StartTime:   time.Now(),
		SyncStatus:  SyncStatusPending,
		Version:     1,
	}

	e.executions[exec.ID] = exec

	// Save to local store
	if err := e.localStore.SaveExecution(ctx, exec); err != nil {
		e.logger.Error("failed to save execution locally", slog.String("error", err.Error()))
	}

	// Try to sync immediately if online
	if e.mode == ExecutionModeOnline {
		go e.syncExecution(context.Background(), exec)
	}

	e.logger.Info("execution started on edge",
		slog.String("execution_id", exec.ID),
		slog.String("workflow_id", workflowID),
		slog.String("mode", e.mode.String()),
	)

	return exec, nil
}

// CompleteExecution completes an execution.
func (e *Engine) CompleteExecution(ctx context.Context, executionID string, output json.RawMessage) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	exec, exists := e.executions[executionID]
	if !exists {
		return errors.New("execution not found")
	}

	exec.Status = ExecutionStatusCompleted
	exec.Output = output
	exec.EndTime = time.Now()
	exec.SyncStatus = SyncStatusPending
	exec.Version++

	// Add completion event
	exec.Events = append(exec.Events, &EdgeEvent{
		ID:         int64(len(exec.Events) + 1),
		Type:       "execution_completed",
		Timestamp:  time.Now(),
		Data:       output,
		SyncStatus: SyncStatusPending,
	})

	// Save to local store
	if err := e.localStore.SaveExecution(ctx, exec); err != nil {
		e.logger.Error("failed to save execution", slog.String("error", err.Error()))
	}

	// Queue for sync
	e.pendingSync = append(e.pendingSync, exec)

	return nil
}

// FailExecution fails an execution.
func (e *Engine) FailExecution(ctx context.Context, executionID string, reason string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	exec, exists := e.executions[executionID]
	if !exists {
		return errors.New("execution not found")
	}

	exec.Status = ExecutionStatusFailed
	exec.EndTime = time.Now()
	exec.SyncStatus = SyncStatusPending
	exec.Version++

	// Add failure event
	failureData, _ := json.Marshal(map[string]string{"reason": reason})
	exec.Events = append(exec.Events, &EdgeEvent{
		ID:         int64(len(exec.Events) + 1),
		Type:       "execution_failed",
		Timestamp:  time.Now(),
		Data:       failureData,
		SyncStatus: SyncStatusPending,
	})

	// Save and queue for sync
	e.localStore.SaveExecution(ctx, exec)
	e.pendingSync = append(e.pendingSync, exec)

	return nil
}

// RecordEvent records an event for an execution.
func (e *Engine) RecordEvent(ctx context.Context, executionID, eventType string, data json.RawMessage) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	exec, exists := e.executions[executionID]
	if !exists {
		return errors.New("execution not found")
	}

	event := &EdgeEvent{
		ID:         int64(len(exec.Events) + 1),
		Type:       eventType,
		Timestamp:  time.Now(),
		Data:       data,
		SyncStatus: SyncStatusPending,
	}

	exec.Events = append(exec.Events, event)
	exec.SyncStatus = SyncStatusPending

	return e.localStore.SaveExecution(ctx, exec)
}

// GetExecution retrieves an execution.
func (e *Engine) GetExecution(ctx context.Context, executionID string) (*EdgeExecution, error) {
	e.mu.RLock()
	exec, exists := e.executions[executionID]
	e.mu.RUnlock()

	if exists {
		return exec, nil
	}

	return e.localStore.GetExecution(ctx, executionID)
}

// GetMode returns the current execution mode.
func (e *Engine) GetMode() ExecutionMode {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.mode
}

// SetMode sets the execution mode.
func (e *Engine) SetMode(mode ExecutionMode) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.mode = mode
	e.logger.Info("execution mode changed", slog.String("mode", mode.String()))
}

// GetPendingSyncCount returns count of executions pending sync.
func (e *Engine) GetPendingSyncCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.pendingSync)
}

func (e *Engine) runSyncLoop(ctx context.Context) {
	ticker := time.NewTicker(e.config.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-e.stopCh:
			return
		case <-ticker.C:
			e.syncPending(ctx)
		}
	}
}

func (e *Engine) syncPending(ctx context.Context) {
	e.mu.Lock()
	pending := make([]*EdgeExecution, len(e.pendingSync))
	copy(pending, e.pendingSync)
	e.mu.Unlock()

	if len(pending) == 0 {
		return
	}

	e.logger.Info("syncing pending executions", slog.Int("count", len(pending)))

	var synced []string
	for _, exec := range pending {
		if err := e.syncExecution(ctx, exec); err != nil {
			e.logger.Error("failed to sync execution",
				slog.String("execution_id", exec.ID),
				slog.String("error", err.Error()),
			)
			continue
		}
		synced = append(synced, exec.ID)
	}

	// Remove synced executions from pending
	e.mu.Lock()
	newPending := make([]*EdgeExecution, 0)
	for _, exec := range e.pendingSync {
		isSynced := false
		for _, id := range synced {
			if exec.ID == id {
				isSynced = true
				break
			}
		}
		if !isSynced {
			newPending = append(newPending, exec)
		}
	}
	e.pendingSync = newPending
	e.mu.Unlock()
}

func (e *Engine) syncExecution(ctx context.Context, exec *EdgeExecution) error {
	if e.centralClient == nil {
		return ErrOfflineMode
	}

	if err := e.centralClient.SyncExecution(ctx, exec); err != nil {
		return err
	}

	exec.SyncStatus = SyncStatusSynced
	exec.LastSyncTime = time.Now()

	for _, event := range exec.Events {
		event.SyncStatus = SyncStatusSynced
	}

	return e.localStore.SaveExecution(ctx, exec)
}

func (e *Engine) runHealthCheck(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-e.stopCh:
			return
		case <-ticker.C:
			e.checkCentralConnection(ctx)
		}
	}
}

func (e *Engine) checkCentralConnection(ctx context.Context) {
	if e.centralClient == nil {
		e.SetMode(ExecutionModeOffline)
		return
	}

	if err := e.centralClient.SendHeartbeat(ctx, e.config.EdgeID); err != nil {
		e.logger.Warn("failed to contact central cluster", slog.String("error", err.Error()))

		// Check if we should switch to offline mode
		if time.Since(e.lastCentralContact) > e.config.OfflineGracePeriod {
			e.SetMode(ExecutionModeOffline)
		} else {
			e.SetMode(ExecutionModeHybrid)
		}
	} else {
		e.mu.Lock()
		e.lastCentralContact = time.Now()
		e.mu.Unlock()
		e.SetMode(ExecutionModeOnline)
	}
}

func generateEdgeExecutionID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40 // UUID version 4
	b[8] = (b[8] & 0x3f) | 0x80 // UUID variant
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func (m ExecutionMode) String() string {
	switch m {
	case ExecutionModeOnline:
		return "online"
	case ExecutionModeOffline:
		return "offline"
	case ExecutionModeHybrid:
		return "hybrid"
	default:
		return "unknown"
	}
}
