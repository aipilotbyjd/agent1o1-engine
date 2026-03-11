package store

import (
	"context"
	"fmt"
	"sync"

	"github.com/linkflow/engine/internal/history/engine"
	"github.com/linkflow/engine/internal/history/types"
)

type executionKeyString string

func keyToString(key types.ExecutionKey) executionKeyString {
	return executionKeyString(fmt.Sprintf("%s/%s/%s", key.NamespaceID, key.WorkflowID, key.RunID))
}

type MemoryEventStore struct {
	mu     sync.RWMutex
	events map[executionKeyString][]*types.HistoryEvent
}

func NewMemoryEventStore() *MemoryEventStore {
	return &MemoryEventStore{
		events: make(map[executionKeyString][]*types.HistoryEvent),
	}
}

func (s *MemoryEventStore) AppendEvents(ctx context.Context, key types.ExecutionKey, events []*types.HistoryEvent, expectedVersion int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := keyToString(key)
	s.events[k] = append(s.events[k], events...)
	return nil
}

func (s *MemoryEventStore) GetEvents(ctx context.Context, key types.ExecutionKey, firstEventID, lastEventID int64) ([]*types.HistoryEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	k := keyToString(key)
	allEvents := s.events[k]
	var result []*types.HistoryEvent

	for _, e := range allEvents {
		if e.EventID >= firstEventID && e.EventID <= lastEventID {
			result = append(result, e)
		}
	}

	return result, nil
}

func (s *MemoryEventStore) GetEventCount(ctx context.Context, key types.ExecutionKey) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	k := keyToString(key)
	return int64(len(s.events[k])), nil
}

type MemoryMutableStateStore struct {
	mu     sync.RWMutex
	states map[executionKeyString]*engine.MutableState
}

func NewMemoryMutableStateStore() *MemoryMutableStateStore {
	return &MemoryMutableStateStore{
		states: make(map[executionKeyString]*engine.MutableState),
	}
}

func (s *MemoryMutableStateStore) GetMutableState(ctx context.Context, key types.ExecutionKey) (*engine.MutableState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	k := keyToString(key)
	state, ok := s.states[k]
	if !ok {
		// Return specific error if desired, or let caller handle it.
		// Service expects ErrExecutionNotFound or similar if implementing proper handling.
		// For now returning error is enough.
		return nil, fmt.Errorf("execution not found")
	}
	return state.Clone(), nil
}

func (s *MemoryMutableStateStore) UpdateMutableState(ctx context.Context, key types.ExecutionKey, state *engine.MutableState, expectedVersion int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := keyToString(key)
	s.states[k] = state.Clone()
	return nil
}

func (s *MemoryMutableStateStore) ListRunningExecutions(ctx context.Context) ([]types.ExecutionKey, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var keys []types.ExecutionKey
	for _, state := range s.states {
		if state.ExecutionInfo != nil && state.ExecutionInfo.Status == types.ExecutionStatusRunning {
			keys = append(keys, types.ExecutionKey{
				NamespaceID: state.ExecutionInfo.NamespaceID,
				WorkflowID:  state.ExecutionInfo.WorkflowID,
				RunID:       state.ExecutionInfo.RunID,
			})
		}
	}
	return keys, nil
}
