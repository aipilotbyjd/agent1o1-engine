package edge

import (
	"context"
	"encoding/json"
	"sync"
)

// MemoryStore is an in-memory implementation of LocalStore for edge.
type MemoryStore struct {
	executions  map[string]*EdgeExecution
	definitions map[string]json.RawMessage // key: namespace/workflow
	mu          sync.RWMutex
}

// NewMemoryStore creates a new in-memory edge store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		executions:  make(map[string]*EdgeExecution),
		definitions: make(map[string]json.RawMessage),
	}
}

// SaveExecution saves an execution.
func (s *MemoryStore) SaveExecution(ctx context.Context, exec *EdgeExecution) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Deep copy
	clone := *exec
	clone.Events = make([]*EdgeEvent, len(exec.Events))
	for i, e := range exec.Events {
		eventCopy := *e
		clone.Events[i] = &eventCopy
	}

	s.executions[exec.ID] = &clone
	return nil
}

// GetExecution retrieves an execution.
func (s *MemoryStore) GetExecution(ctx context.Context, id string) (*EdgeExecution, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	exec, exists := s.executions[id]
	if !exists {
		return nil, ErrExecutionPending
	}

	clone := *exec
	return &clone, nil
}

// ListPendingSyncs returns all executions pending sync.
func (s *MemoryStore) ListPendingSyncs(ctx context.Context) ([]*EdgeExecution, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var pending []*EdgeExecution
	for _, exec := range s.executions {
		if exec.SyncStatus == SyncStatusPending {
			clone := *exec
			pending = append(pending, &clone)
		}
	}

	return pending, nil
}

// GetWorkflowDefinition retrieves a cached workflow definition.
func (s *MemoryStore) GetWorkflowDefinition(ctx context.Context, namespaceID, workflowID string) (json.RawMessage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := namespaceID + "/" + workflowID
	def, exists := s.definitions[key]
	if !exists {
		return nil, ErrSyncRequired
	}

	return def, nil
}

// CacheWorkflowDefinition caches a workflow definition.
func (s *MemoryStore) CacheWorkflowDefinition(ctx context.Context, namespaceID, workflowID string, def json.RawMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := namespaceID + "/" + workflowID
	s.definitions[key] = def

	return nil
}

// Clear removes all data (for testing).
func (s *MemoryStore) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.executions = make(map[string]*EdgeExecution)
	s.definitions = make(map[string]json.RawMessage)
}
