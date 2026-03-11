package engine

import (
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// DLQEntry represents a task that has been moved to the dead letter queue
// after exceeding the maximum number of retry attempts.
type DLQEntry struct {
	Task      *Task
	Reason    string
	FailedAt  time.Time
	Attempts  int32
	LastError string
}

// DeadLetterQueue holds tasks that have exceeded the retry limit.
type DeadLetterQueue struct {
	entries []*DLQEntry
	maxSize int
	mu      sync.Mutex
	logger  *slog.Logger
}

// NewDeadLetterQueue creates a new DeadLetterQueue with the given maximum size.
func NewDeadLetterQueue(maxSize int, logger *slog.Logger) *DeadLetterQueue {
	if logger == nil {
		logger = slog.Default()
	}
	return &DeadLetterQueue{
		entries: make([]*DLQEntry, 0),
		maxSize: maxSize,
		logger:  logger,
	}
}

// Add adds an entry to the dead letter queue. Returns an error if the DLQ is full.
func (dlq *DeadLetterQueue) Add(entry *DLQEntry) error {
	dlq.mu.Lock()
	defer dlq.mu.Unlock()

	if len(dlq.entries) >= dlq.maxSize {
		return fmt.Errorf("dead letter queue is full (max %d)", dlq.maxSize)
	}

	dlq.entries = append(dlq.entries, entry)
	dlq.logger.Warn("task moved to DLQ",
		slog.String("task_id", entry.Task.ID),
		slog.String("reason", entry.Reason),
		slog.Int("attempts", int(entry.Attempts)),
	)
	return nil
}

// List returns a copy of all entries in the dead letter queue.
func (dlq *DeadLetterQueue) List() []*DLQEntry {
	dlq.mu.Lock()
	defer dlq.mu.Unlock()

	result := make([]*DLQEntry, len(dlq.entries))
	copy(result, dlq.entries)
	return result
}

// Retry removes a task from the DLQ by taskID and returns it for re-processing.
func (dlq *DeadLetterQueue) Retry(taskID string) (*Task, error) {
	dlq.mu.Lock()
	defer dlq.mu.Unlock()

	for i, entry := range dlq.entries {
		if entry.Task.ID == taskID {
			task := entry.Task
			task.Attempt = 0
			dlq.entries = append(dlq.entries[:i], dlq.entries[i+1:]...)
			dlq.logger.Info("task retried from DLQ",
				slog.String("task_id", taskID),
			)
			return task, nil
		}
	}

	return nil, fmt.Errorf("task %s not found in DLQ", taskID)
}

// Purge removes all entries from the dead letter queue and returns the count removed.
func (dlq *DeadLetterQueue) Purge() int {
	dlq.mu.Lock()
	defer dlq.mu.Unlock()

	count := len(dlq.entries)
	dlq.entries = make([]*DLQEntry, 0)
	dlq.logger.Info("DLQ purged", slog.Int("count", count))
	return count
}

// Remove removes a single entry from the DLQ by taskID. Returns true if found and removed.
func (dlq *DeadLetterQueue) Remove(taskID string) bool {
	dlq.mu.Lock()
	defer dlq.mu.Unlock()

	for i, entry := range dlq.entries {
		if entry.Task.ID == taskID {
			dlq.entries = append(dlq.entries[:i], dlq.entries[i+1:]...)
			return true
		}
	}
	return false
}

// Len returns the number of entries in the dead letter queue.
func (dlq *DeadLetterQueue) Len() int {
	dlq.mu.Lock()
	defer dlq.mu.Unlock()
	return len(dlq.entries)
}
