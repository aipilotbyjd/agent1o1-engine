package engine

import (
	"container/list"
	"context"
	"sync"
	"time"
)

const (
	numPriorityLevels = 10
	defaultPriority   = 5
)

type priorityEntry struct {
	priority int
	element  *list.Element
}

// PriorityTaskStore implements TaskStore with priority ordering.
// Uses separate lists per priority level (0 = highest, 9 = lowest, default 5).
type PriorityTaskStore struct {
	buckets   [numPriorityLevels]*list.List
	taskIndex map[string]priorityEntry
	mu        sync.Mutex
}

// NewPriorityTaskStore creates a new PriorityTaskStore.
func NewPriorityTaskStore() *PriorityTaskStore {
	s := &PriorityTaskStore{
		taskIndex: make(map[string]priorityEntry),
	}
	for i := 0; i < numPriorityLevels; i++ {
		s.buckets[i] = list.New()
	}
	return s
}

func (s *PriorityTaskStore) normalizePriority(p int32) int {
	prio := int(p)
	if prio < 0 {
		prio = 0
	}
	if prio >= numPriorityLevels {
		prio = numPriorityLevels - 1
	}
	return prio
}

func (s *PriorityTaskStore) AddTask(ctx context.Context, task *Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.taskIndex[task.ID]; exists {
		return ErrTaskExists
	}

	prio := s.normalizePriority(task.Priority)
	elem := s.buckets[prio].PushBack(task)
	s.taskIndex[task.ID] = priorityEntry{priority: prio, element: elem}
	return nil
}

// PollTask returns the highest-priority pending task (lowest priority number first).
func (s *PriorityTaskStore) PollTask(ctx context.Context, timeout time.Duration) (*Task, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for i := 0; i < numPriorityLevels; i++ {
		elem := s.buckets[i].Front()
		if elem == nil {
			continue
		}
		task := elem.Value.(*Task)
		s.buckets[i].Remove(elem)
		delete(s.taskIndex, task.ID)
		return task, nil
	}

	return nil, nil
}

func (s *PriorityTaskStore) AckTask(ctx context.Context, taskID string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.taskIndex[taskID]
	if !exists {
		return false, nil
	}

	s.buckets[entry.priority].Remove(entry.element)
	delete(s.taskIndex, taskID)
	return true, nil
}

func (s *PriorityTaskStore) Len(ctx context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(len(s.taskIndex)), nil
}
