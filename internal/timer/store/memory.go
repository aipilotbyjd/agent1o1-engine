package store

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/linkflow/engine/internal/timer"
)

// MemoryStore is an in-memory implementation of the timer store.
type MemoryStore struct {
	timers map[string]*timer.Timer
	mu     sync.RWMutex
}

// NewMemoryStore creates a new in-memory timer store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		timers: make(map[string]*timer.Timer),
	}
}

func (s *MemoryStore) makeKey(namespaceID, workflowID, runID, timerID string) string {
	return namespaceID + "/" + workflowID + "/" + runID + "/" + timerID
}

// CreateTimer creates a new timer.
func (s *MemoryStore) CreateTimer(ctx context.Context, t *timer.Timer) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := s.makeKey(t.NamespaceID, t.WorkflowID, t.RunID, t.TimerID)
	if _, exists := s.timers[key]; exists {
		return timer.ErrTimerAlreadyExists
	}

	// Clone the timer to avoid external modifications
	clone := *t
	s.timers[key] = &clone

	return nil
}

// GetTimer retrieves a timer by ID.
func (s *MemoryStore) GetTimer(ctx context.Context, namespaceID, workflowID, runID, timerID string) (*timer.Timer, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := s.makeKey(namespaceID, workflowID, runID, timerID)
	t, exists := s.timers[key]
	if !exists {
		return nil, timer.ErrTimerNotFound
	}

	// Return a clone
	clone := *t
	return &clone, nil
}

// UpdateTimer updates a timer.
func (s *MemoryStore) UpdateTimer(ctx context.Context, t *timer.Timer) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := s.makeKey(t.NamespaceID, t.WorkflowID, t.RunID, t.TimerID)
	existing, exists := s.timers[key]
	if !exists {
		return timer.ErrTimerNotFound
	}

	if existing.Version != t.Version-1 {
		return timer.ErrOptimisticLockConflict
	}

	clone := *t
	s.timers[key] = &clone

	return nil
}

// DeleteTimer deletes a timer.
func (s *MemoryStore) DeleteTimer(ctx context.Context, namespaceID, workflowID, runID, timerID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := s.makeKey(namespaceID, workflowID, runID, timerID)
	if _, exists := s.timers[key]; !exists {
		return timer.ErrTimerNotFound
	}

	delete(s.timers, key)
	return nil
}

// GetDueTimers returns all timers that are due for firing.
func (s *MemoryStore) GetDueTimers(ctx context.Context, shardID int32, fireTime time.Time, limit int) ([]*timer.Timer, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var dueTimers []*timer.Timer

	for _, t := range s.timers {
		if t.ShardID == shardID && t.Status == timer.TimerStatusPending && !t.FireTime.After(fireTime) {
			clone := *t
			dueTimers = append(dueTimers, &clone)
		}
	}

	// Sort by fire time
	sort.Slice(dueTimers, func(i, j int) bool {
		return dueTimers[i].FireTime.Before(dueTimers[j].FireTime)
	})

	// Apply limit
	if limit > 0 && len(dueTimers) > limit {
		dueTimers = dueTimers[:limit]
	}

	return dueTimers, nil
}

// GetTimersByExecution returns all timers for an execution.
func (s *MemoryStore) GetTimersByExecution(ctx context.Context, namespaceID, workflowID, runID string) ([]*timer.Timer, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	prefix := namespaceID + "/" + workflowID + "/" + runID + "/"
	var timers []*timer.Timer

	for key, t := range s.timers {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			clone := *t
			timers = append(timers, &clone)
		}
	}

	return timers, nil
}

// Clear removes all timers (used for testing).
func (s *MemoryStore) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.timers = make(map[string]*timer.Timer)
}

// Count returns the number of timers.
func (s *MemoryStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.timers)
}
