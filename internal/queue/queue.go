// Package queue provides an in-memory priority queue used by the standalone
// DAG scheduler for direct execution mode.
//
// The primary Temporal-style execution path does NOT use this queue — it uses
// the Matching service (gRPC) for durable, persistent task queuing.
//
// WARNING: Tasks in this queue are NOT persisted to disk or database.
// If the engine process crashes, all queued tasks are lost. For production
// workloads requiring durability, use the Matching service path instead.
package queue

import (
	"container/heap"
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrQueueClosed = errors.New("queue is closed")
	ErrQueueFull   = errors.New("queue is full")
	ErrNoTask      = errors.New("no task available")
)

// Priority levels.
const (
	PriorityLow    = 0
	PriorityNormal = 5
	PriorityHigh   = 10
)

// Task represents a task in the queue.
type Task struct {
	ID          string
	NamespaceID string
	WorkflowID  string
	RunID       string
	NodeID      string
	TaskType    string
	Priority    int
	Payload     []byte
	ScheduledAt time.Time
	VisibleAt   time.Time
	Attempts    int
	MaxAttempts int
	Timeout     time.Duration
	index       int // for heap
}

// PriorityQueue implements a priority queue for tasks.
type PriorityQueue struct {
	items    []*Task
	capacity int
	mu       sync.RWMutex
}

// NewPriorityQueue creates a new priority queue.
func NewPriorityQueue(capacity int) *PriorityQueue {
	pq := &PriorityQueue{
		items:    make([]*Task, 0, capacity),
		capacity: capacity,
	}
	heap.Init(pq)
	return pq
}

// Len returns the number of items in the queue.
func (pq *PriorityQueue) Len() int {
	return len(pq.items)
}

// Less compares two items (higher priority first, then earlier scheduled).
func (pq *PriorityQueue) Less(i, j int) bool {
	if pq.items[i].Priority != pq.items[j].Priority {
		return pq.items[i].Priority > pq.items[j].Priority
	}
	return pq.items[i].ScheduledAt.Before(pq.items[j].ScheduledAt)
}

// Swap swaps two items.
func (pq *PriorityQueue) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.items[i].index = i
	pq.items[j].index = j
}

// Push adds an item to the queue.
func (pq *PriorityQueue) Push(x interface{}) {
	n := len(pq.items)
	task := x.(*Task)
	task.index = n
	pq.items = append(pq.items, task)
}

// Pop removes and returns the highest priority item.
func (pq *PriorityQueue) Pop() interface{} {
	old := pq.items
	n := len(old)
	task := old[n-1]
	old[n-1] = nil
	task.index = -1
	pq.items = old[0 : n-1]
	return task
}

// Enqueue adds a task to the queue.
func (pq *PriorityQueue) Enqueue(task *Task) error {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if pq.capacity > 0 && len(pq.items) >= pq.capacity {
		return ErrQueueFull
	}

	if task.ScheduledAt.IsZero() {
		task.ScheduledAt = time.Now()
	}
	if task.VisibleAt.IsZero() {
		task.VisibleAt = task.ScheduledAt
	}

	heap.Push(pq, task)
	return nil
}

// Dequeue removes and returns the highest priority visible task.
func (pq *PriorityQueue) Dequeue() (*Task, error) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if len(pq.items) == 0 {
		return nil, ErrNoTask
	}

	now := time.Now()

	// Find first visible task
	for i := 0; i < len(pq.items); i++ {
		if !pq.items[i].VisibleAt.After(now) {
			task := heap.Remove(pq, i).(*Task)
			return task, nil
		}
	}

	return nil, ErrNoTask
}

// Peek returns the highest priority task without removing it.
func (pq *PriorityQueue) Peek() (*Task, error) {
	pq.mu.RLock()
	defer pq.mu.RUnlock()

	if len(pq.items) == 0 {
		return nil, ErrNoTask
	}

	return pq.items[0], nil
}

// Size returns the queue size.
func (pq *PriorityQueue) Size() int {
	pq.mu.RLock()
	defer pq.mu.RUnlock()
	return len(pq.items)
}

// Clear removes all items from the queue.
func (pq *PriorityQueue) Clear() {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	pq.items = pq.items[:0]
}

// TaskQueue provides a managed task queue with workers.
type TaskQueue struct {
	name       string
	queue      *PriorityQueue
	workers    int
	workerFunc func(context.Context, *Task) error
	stopCh     chan struct{}
	taskCh     chan *Task
	running    bool
	mu         sync.RWMutex
	wg         sync.WaitGroup
}

// TaskQueueConfig holds task queue configuration.
type TaskQueueConfig struct {
	Name       string
	Capacity   int
	Workers    int
	WorkerFunc func(context.Context, *Task) error
}

// NewTaskQueue creates a new task queue.
func NewTaskQueue(config TaskQueueConfig) *TaskQueue {
	if config.Workers <= 0 {
		config.Workers = 1
	}
	if config.Capacity <= 0 {
		config.Capacity = 10000
	}

	return &TaskQueue{
		name:       config.Name,
		queue:      NewPriorityQueue(config.Capacity),
		workers:    config.Workers,
		workerFunc: config.WorkerFunc,
		stopCh:     make(chan struct{}),
		taskCh:     make(chan *Task, config.Workers*2),
	}
}

// Start starts the task queue workers.
func (tq *TaskQueue) Start(ctx context.Context) error {
	tq.mu.Lock()
	if tq.running {
		tq.mu.Unlock()
		return errors.New("task queue already running")
	}
	tq.running = true
	tq.stopCh = make(chan struct{})
	tq.mu.Unlock()

	// Start dispatcher
	tq.wg.Add(1)
	go tq.runDispatcher(ctx)

	// Start workers
	for i := 0; i < tq.workers; i++ {
		tq.wg.Add(1)
		go tq.runWorker(ctx, i)
	}

	return nil
}

// Stop stops the task queue.
func (tq *TaskQueue) Stop(ctx context.Context) error {
	tq.mu.Lock()
	if !tq.running {
		tq.mu.Unlock()
		return nil
	}
	tq.running = false
	close(tq.stopCh)
	tq.mu.Unlock()

	// Wait for workers to finish
	done := make(chan struct{})
	go func() {
		tq.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

// Enqueue adds a task to the queue.
func (tq *TaskQueue) Enqueue(task *Task) error {
	tq.mu.RLock()
	running := tq.running
	tq.mu.RUnlock()

	if !running {
		return ErrQueueClosed
	}

	return tq.queue.Enqueue(task)
}

// Size returns the queue size.
func (tq *TaskQueue) Size() int {
	return tq.queue.Size()
}

func (tq *TaskQueue) runDispatcher(ctx context.Context) {
	defer tq.wg.Done()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-tq.stopCh:
			return
		case <-ticker.C:
			task, err := tq.queue.Dequeue()
			if err != nil {
				continue
			}

			select {
			case tq.taskCh <- task:
			case <-ctx.Done():
				return
			case <-tq.stopCh:
				return
			}
		}
	}
}

func (tq *TaskQueue) runWorker(ctx context.Context, id int) {
	defer tq.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-tq.stopCh:
			return
		case task := <-tq.taskCh:
			if tq.workerFunc != nil {
				taskCtx := ctx
				if task.Timeout > 0 {
					var cancel context.CancelFunc
					taskCtx, cancel = context.WithTimeout(ctx, task.Timeout)
					defer cancel()
				}

				task.Attempts++
				if err := tq.workerFunc(taskCtx, task); err != nil {
					// Requeue if attempts remaining
					if task.Attempts < task.MaxAttempts {
						task.VisibleAt = time.Now().Add(backoff(task.Attempts))
						tq.queue.Enqueue(task)
					}
				}
			}
		}
	}
}

func backoff(attempt int) time.Duration {
	delays := []time.Duration{
		time.Second,
		2 * time.Second,
		5 * time.Second,
		10 * time.Second,
		30 * time.Second,
		time.Minute,
	}
	if attempt < len(delays) {
		return delays[attempt]
	}
	return delays[len(delays)-1]
}
