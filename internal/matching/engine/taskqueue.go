package engine

import (
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/time/rate"
)

const (
	DefaultLeaseTimeout = 60 * time.Second
	DefaultMaxRetries   = 3
)

var ErrTaskExists = errors.New("task already exists")

// TaskStore defines the interface for task persistence.
type TaskStore interface {
	AddTask(ctx context.Context, task *Task) error
	PollTask(ctx context.Context, timeout time.Duration) (*Task, error)
	AckTask(ctx context.Context, taskID string) (bool, error)
	Len(ctx context.Context) (int64, error)
}

// MemoryTaskStore is an in-memory implementation of TaskStore.
type MemoryTaskStore struct {
	tasks    *list.List
	tasksMap map[string]*list.Element
	mu       sync.Mutex
}

func NewMemoryTaskStore() *MemoryTaskStore {
	return &MemoryTaskStore{
		tasks:    list.New(),
		tasksMap: make(map[string]*list.Element),
	}
}

func (s *MemoryTaskStore) AddTask(ctx context.Context, task *Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tasksMap[task.ID]; exists {
		return ErrTaskExists
	}

	elem := s.tasks.PushBack(task)
	s.tasksMap[task.ID] = elem
	return nil
}

func (s *MemoryTaskStore) PollTask(ctx context.Context, timeout time.Duration) (*Task, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	elem := s.tasks.Front()
	if elem == nil {
		return nil, nil // Or wait if we implement condition variable
	}

	task := elem.Value.(*Task)
	s.tasks.Remove(elem)
	delete(s.tasksMap, task.ID)
	return task, nil
}

func (s *MemoryTaskStore) AckTask(ctx context.Context, taskID string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Support removing pending tasks by ID for idempotent completion paths.
	if elem, exists := s.tasksMap[taskID]; exists {
		s.tasks.Remove(elem)
		delete(s.tasksMap, taskID)
		return true, nil
	}

	return false, nil
}

func (s *MemoryTaskStore) Len(ctx context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(s.tasks.Len()), nil
}

// RedisTaskStore is a Redis-backed implementation of TaskStore.
type RedisTaskStore struct {
	client        *redis.Client
	queueKey      string
	processingKey string
}

func NewRedisTaskStore(client *redis.Client, queueName string) *RedisTaskStore {
	return &RedisTaskStore{
		client:        client,
		queueKey:      fmt.Sprintf("taskqueue:%s", queueName),
		processingKey: fmt.Sprintf("taskqueue:%s:processing", queueName),
	}
}

func (s *RedisTaskStore) AddTask(ctx context.Context, task *Task) error {
	data, err := json.Marshal(task)
	if err != nil {
		return err
	}
	return s.client.RPush(ctx, s.queueKey, data).Err()
}

func (s *RedisTaskStore) PollTask(ctx context.Context, timeout time.Duration) (*Task, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// LMOVE atomically moves a task from the main queue to the processing queue.
	// If the worker crashes, the task remains in the processing queue for redelivery.
	result, err := s.client.LMove(ctx, s.queueKey, s.processingKey, "LEFT", "RIGHT").Result()
	if err != nil {
		if err == redis.Nil {
			// No tasks available; sleep briefly to avoid busy-spinning
			select {
			case <-time.After(timeout):
				return nil, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return nil, err
	}

	var task Task
	if err := json.Unmarshal([]byte(result), &task); err != nil {
		return nil, err
	}
	return &task, nil
}

func (s *RedisTaskStore) AckTask(ctx context.Context, taskID string) (bool, error) {
	// Remove the acknowledged task from the processing queue.
	// We scan the processing list for the task with this ID and remove it.
	items, err := s.client.LRange(ctx, s.processingKey, 0, -1).Result()
	if err != nil {
		return false, err
	}
	for _, item := range items {
		var t Task
		if err := json.Unmarshal([]byte(item), &t); err != nil {
			continue
		}
		if t.ID == taskID {
			// LREM removes the first occurrence
			removed, err := s.client.LRem(ctx, s.processingKey, 1, item).Result()
			if err != nil {
				return false, err
			}
			return removed > 0, nil
		}
	}
	return false, nil
}

func (s *RedisTaskStore) Len(ctx context.Context) (int64, error) {
	return s.client.LLen(ctx, s.queueKey).Result()
}

// TaskQueueConfig holds optional configuration for NewTaskQueue.
type TaskQueueConfig struct {
	DLQ            *DeadLetterQueue
	MaxRetries     int32
	Backpressure   *Backpressure
	WAL            *WAL
	StickyAffinity *StickyAffinity
	Logger         *slog.Logger
}

type TaskQueue struct {
	name           string
	kind           TaskQueueKind
	store          TaskStore
	pollers        *list.List
	rateLimiter    *rate.Limiter
	metrics        *Metrics
	mu             sync.Mutex
	inFlight       map[string]*Task
	inFlightExpiry map[string]time.Time
	leaseTimeout   time.Duration

	// DLQ support
	dlq        *DeadLetterQueue
	maxRetries int32

	// Backpressure support
	backpressure *Backpressure

	// WAL support
	wal *WAL

	// Sticky queue support
	stickyAffinity *StickyAffinity

	logger *slog.Logger
}

// NewTaskQueue creates a new TaskQueue. Maintains backward compatibility.
func NewTaskQueue(name string, kind TaskQueueKind, rateLimit float64, burst int, redisClient *redis.Client) *TaskQueue {
	return NewTaskQueueWithConfig(name, kind, rateLimit, burst, redisClient, TaskQueueConfig{})
}

// NewTaskQueueWithConfig creates a new TaskQueue with extended configuration.
func NewTaskQueueWithConfig(name string, kind TaskQueueKind, rateLimit float64, burst int, redisClient *redis.Client, cfg TaskQueueConfig) *TaskQueue {
	var store TaskStore
	if redisClient != nil {
		store = NewRedisTaskStore(redisClient, name)
	} else {
		store = NewPriorityTaskStore()
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	maxRetries := cfg.MaxRetries
	if maxRetries <= 0 {
		maxRetries = DefaultMaxRetries
	}

	bp := cfg.Backpressure
	if bp == nil {
		bp = NewBackpressure(DefaultSoftLimit, DefaultHardLimit, logger)
	}

	var sa *StickyAffinity
	if kind == TaskQueueKindSticky {
		sa = cfg.StickyAffinity
		if sa == nil {
			sa = NewStickyAffinity()
		}
	}

	return &TaskQueue{
		name:           name,
		kind:           kind,
		store:          store,
		pollers:        list.New(),
		rateLimiter:    rate.NewLimiter(rate.Limit(rateLimit), burst),
		metrics:        NewMetrics(),
		inFlight:       make(map[string]*Task),
		inFlightExpiry: make(map[string]time.Time),
		leaseTimeout:   DefaultLeaseTimeout,
		dlq:            cfg.DLQ,
		maxRetries:     maxRetries,
		backpressure:   bp,
		wal:            cfg.WAL,
		stickyAffinity: sa,
		logger:         logger,
	}
}

func (tq *TaskQueue) Name() string {
	return tq.name
}

func (tq *TaskQueue) Kind() TaskQueueKind {
	return tq.kind
}

func (tq *TaskQueue) Metrics() *Metrics {
	return tq.metrics
}

func (tq *TaskQueue) AddTask(task *Task) error {
	tq.mu.Lock()
	defer tq.mu.Unlock()

	// Backpressure check
	depth, _ := tq.store.Len(context.Background())
	if tq.backpressure != nil && tq.backpressure.ShouldReject(int(depth)) {
		tq.metrics.TaskRejected()
		return ErrBackpressure
	}

	tq.metrics.TaskAdded()

	// Sticky affinity: bind workflow to any existing worker, or leave unbound
	if tq.kind == TaskQueueKindSticky && tq.stickyAffinity != nil {
		// If there's no existing affinity, the task is available to any worker.
		// Affinity is established when Poll() dispatches the task.
	}

	// Try dispatch directly to waiting poller first (optimization)
	if tq.tryDispatchLocked(task) {
		return nil
	}

	// Persist to Store FIRST
	if err := tq.store.AddTask(context.Background(), task); err != nil {
		return err
	}

	// Write to WAL AFTER successful enqueue
	if tq.wal != nil {
		if err := tq.wal.WriteAdd(task); err != nil {
			tq.logger.Error("failed to write WAL", slog.String("task_id", task.ID), slog.String("error", err.Error()))
		}
	}

	// Update gauge metrics
	newDepth, _ := tq.store.Len(context.Background())
	tq.metrics.SetQueueDepth(newDepth)

	return nil
}

func (tq *TaskQueue) Poll(ctx context.Context, identity string) (*Task, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// First check rate limit
	tq.mu.Lock()
	if !tq.rateLimiter.Allow() {
		tq.mu.Unlock()
		return nil, ErrRateLimited
	}
	tq.mu.Unlock()

	for {
		task, err := tq.store.PollTask(ctx, time.Second)
		if err != nil {
			return nil, err
		}

		if task != nil {
			// Sticky queue: check affinity
			if tq.kind == TaskQueueKindSticky && tq.stickyAffinity != nil {
				boundIdentity, hasBind := tq.stickyAffinity.GetIdentity(task.WorkflowID)
				if hasBind && boundIdentity != identity {
					// Check if the affinity has expired (worker didn't poll in time)
					if !tq.stickyAffinity.IsExpired(task.WorkflowID, tq.leaseTimeout) {
						// Put task back and continue polling
						_ = tq.store.AddTask(ctx, task)
						if err := ctx.Err(); err != nil {
							return nil, err
						}
						// Brief backoff to avoid busy-spinning
						time.Sleep(100 * time.Millisecond)
						continue
					}
					// Affinity expired, allow any worker
					tq.stickyAffinity.Remove(task.WorkflowID)
				}
				// Bind or refresh affinity
				tq.stickyAffinity.Bind(task.WorkflowID, identity)
			}

			tq.mu.Lock()
			tq.inFlight[task.ID] = task
			tq.inFlightExpiry[task.ID] = time.Now().Add(tq.leaseTimeout)
			tq.metrics.SetInFlightCount(int64(len(tq.inFlight)))
			tq.mu.Unlock()

			// Update queue depth gauge
			depth, _ := tq.store.Len(context.Background())
			tq.metrics.SetQueueDepth(depth)

			tq.metrics.TaskDispatched()
			tq.metrics.RecordLatency(time.Since(task.ScheduledTime))
			return task, nil
		}

		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
}

func (tq *TaskQueue) CompleteTask(taskID string) bool {
	tq.mu.Lock()
	defer tq.mu.Unlock()

	if _, exists := tq.inFlight[taskID]; exists {
		delete(tq.inFlight, taskID)
		delete(tq.inFlightExpiry, taskID)
		tq.metrics.SetInFlightCount(int64(len(tq.inFlight)))

		// Write completion to WAL
		if tq.wal != nil {
			if err := tq.wal.WriteComplete(taskID); err != nil {
				tq.logger.Error("failed to write WAL completion", slog.String("task_id", taskID), slog.String("error", err.Error()))
			}
		}

		return true
	}
	// Task might be in store but not in flight?
	// AckTask logic might be needed for Redis if we used RPOPLPUSH
	acked, err := tq.store.AckTask(context.Background(), taskID)
	if err != nil {
		return false
	}

	if acked && tq.wal != nil {
		if err := tq.wal.WriteComplete(taskID); err != nil {
			tq.logger.Error("failed to write WAL completion", slog.String("task_id", taskID), slog.String("error", err.Error()))
		}
	}

	return acked
}

// FailTask records a task failure in metrics.
func (tq *TaskQueue) FailTask(taskID string) {
	tq.metrics.TaskFailed()
}

func (tq *TaskQueue) tryDispatchLocked(task *Task) bool {
	elem := tq.pollers.Front()
	if elem == nil {
		return false
	}

	poller := elem.Value.(*Poller)
	tq.pollers.Remove(elem)

	// Sticky affinity: bind the workflow to this poller's identity
	if tq.kind == TaskQueueKindSticky && tq.stickyAffinity != nil {
		tq.stickyAffinity.Bind(task.WorkflowID, poller.Identity)
	}

	task.StartedTime = time.Now()
	tq.inFlight[task.ID] = task
	tq.inFlightExpiry[task.ID] = time.Now().Add(tq.leaseTimeout)
	tq.metrics.SetInFlightCount(int64(len(tq.inFlight)))
	poller.ResultCh <- task

	tq.metrics.TaskDispatched()
	tq.metrics.RecordLatency(time.Since(task.ScheduledTime))
	return true
}

func (tq *TaskQueue) PendingTaskCount() int {
	len, _ := tq.store.Len(context.Background())
	return int(len)
}

func (tq *TaskQueue) PollerCount() int {
	tq.mu.Lock()
	defer tq.mu.Unlock()
	return tq.pollers.Len()
}

func (tq *TaskQueue) RequeueExpiredTasks() int {
	tq.mu.Lock()
	defer tq.mu.Unlock()

	now := time.Now()
	requeued := 0

	for taskID, expiry := range tq.inFlightExpiry {
		if now.After(expiry) {
			task := tq.inFlight[taskID]
			delete(tq.inFlight, taskID)
			delete(tq.inFlightExpiry, taskID)

			tq.metrics.TaskTimedOut()

			// Check if task has exceeded max retries
			if tq.dlq != nil && task.Attempt >= tq.maxRetries {
				entry := &DLQEntry{
					Task:      task,
					Reason:    "max retries exceeded",
					FailedAt:  now,
					Attempts:  task.Attempt,
					LastError: "lease timeout",
				}
				if err := tq.dlq.Add(entry); err != nil {
					tq.logger.Error("failed to add task to DLQ",
						slog.String("task_id", taskID),
						slog.String("error", err.Error()),
					)
				} else {
					tq.metrics.TaskSentToDLQ()
				}
				continue
			}

			// Increment attempt on a copy and requeue synchronously
			requeueTask := *task
			requeueTask.Attempt++
			if err := tq.store.AddTask(context.Background(), &requeueTask); err != nil {
				tq.logger.Error("failed to requeue expired task",
					slog.String("task_id", taskID),
					slog.String("error", err.Error()),
				)
			}
			requeued++
		}
	}

	tq.metrics.SetInFlightCount(int64(len(tq.inFlight)))

	return requeued
}

var ErrRateLimited = errRateLimited{}

type errRateLimited struct{}

func (errRateLimited) Error() string { return "rate limited" }
