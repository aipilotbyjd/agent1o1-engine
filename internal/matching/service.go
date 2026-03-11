package matching

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/linkflow/engine/internal/matching/engine"
	"github.com/linkflow/engine/internal/matching/partition"
	"github.com/redis/go-redis/v9"
)

const (
	defaultRateLimit = 1000.0
	defaultBurst     = 100
)

type Service struct {
	partitionMgr *partition.Manager
	taskQueues   map[string]*engine.TaskQueue
	logger       *slog.Logger
	mu           sync.RWMutex

	stopCh  chan struct{}
	wg      sync.WaitGroup
	running bool

	// DLQ shared across all queues
	dlq *engine.DeadLetterQueue

	// WAL for crash recovery
	wal    *engine.WAL
	walDir string
}

type Config struct {
	NumPartitions int32
	Replicas      int
	Logger        *slog.Logger
	RedisClient   *redis.Client
	WALDir        string
}

func NewService(cfg Config) *Service {
	if cfg.NumPartitions <= 0 {
		cfg.NumPartitions = 4
	}
	if cfg.Replicas <= 0 {
		cfg.Replicas = 100
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	return &Service{
		partitionMgr: partition.NewManager(cfg.NumPartitions, cfg.Replicas, cfg.RedisClient),
		taskQueues:   make(map[string]*engine.TaskQueue),
		logger:       cfg.Logger,
		dlq:          engine.NewDeadLetterQueue(10000, cfg.Logger),
		walDir:       cfg.WALDir,
	}
}

func (s *Service) AddTask(ctx context.Context, taskQueueName string, task *engine.Task) error {
	tq := s.GetOrCreateTaskQueue(taskQueueName, engine.TaskQueueKindNormal)
	if err := tq.AddTask(task); err != nil {
		if errors.Is(err, engine.ErrTaskExists) {
			s.logger.Warn("task already exists",
				slog.String("task_id", task.ID),
				slog.String("task_queue", taskQueueName),
			)
			return nil
		}

		if errors.Is(err, engine.ErrBackpressure) {
			s.logger.Warn("task rejected by backpressure",
				slog.String("task_id", task.ID),
				slog.String("task_queue", taskQueueName),
			)
			return err
		}

		s.logger.Error("failed to add task",
			slog.String("task_id", task.ID),
			slog.String("task_queue", taskQueueName),
			slog.String("error", err.Error()),
		)
		return err
	}

	return nil
}

func (s *Service) CompleteTaskByID(ctx context.Context, taskID string) error {
	s.mu.RLock()
	queues := make([]*engine.TaskQueue, 0, len(s.taskQueues))
	for _, tq := range s.taskQueues {
		queues = append(queues, tq)
	}
	s.mu.RUnlock()

	for _, tq := range queues {
		if tq.CompleteTask(taskID) {
			return nil
		}
	}

	return ErrTaskNotFound
}

func (s *Service) CompleteTask(ctx context.Context, taskQueueName string, taskID string) error {
	s.mu.RLock()
	tq, exists := s.taskQueues[taskQueueName]
	s.mu.RUnlock()

	if !exists {
		return ErrTaskQueueNotFound
	}

	if !tq.CompleteTask(taskID) {
		return ErrTaskNotFound
	}

	return nil
}

func (s *Service) PollTask(ctx context.Context, taskQueueName string, identity string) (*engine.Task, error) {
	s.mu.RLock()
	tq, exists := s.taskQueues[taskQueueName]
	s.mu.RUnlock()

	if !exists {
		// If queue is not in memory, we might still have it in Redis if persistence is enabled.
		// But s.taskQueues tracks active queues.
		// Let's try to "create" (load) it.
		tq = s.GetOrCreateTaskQueue(taskQueueName, engine.TaskQueueKindNormal)
	}

	task, err := tq.Poll(ctx, identity)
	if err != nil {
		return nil, err
	}

	return task, nil
}

func (s *Service) GetOrCreateTaskQueue(name string, kind engine.TaskQueueKind) *engine.TaskQueue {
	s.mu.RLock()
	tq, exists := s.taskQueues[name]
	s.mu.RUnlock()

	if exists {
		return tq
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if tq, exists = s.taskQueues[name]; exists {
		return tq
	}

	partition := s.partitionMgr.GetPartitionForTaskQueue(name)
	tq = partition.GetOrCreateTaskQueueWithConfig(name, kind, defaultRateLimit, defaultBurst, engine.TaskQueueConfig{
		DLQ:    s.dlq,
		WAL:    s.wal,
		Logger: s.logger,
	})
	s.taskQueues[name] = tq

	s.logger.Info("created task queue",
		slog.String("name", name),
		slog.Int("kind", int(kind)),
		slog.Int("partition", int(partition.ID)),
	)

	return tq
}

// GetOrCreateStickyQueue creates or retrieves a sticky task queue for a specific worker identity.
func (s *Service) GetOrCreateStickyQueue(workerIdentity string) *engine.TaskQueue {
	name := "sticky:" + workerIdentity
	return s.GetOrCreateTaskQueue(name, engine.TaskQueueKindSticky)
}

func (s *Service) GetTaskQueue(name string) (*engine.TaskQueue, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tq, exists := s.taskQueues[name]
	if !exists {
		return nil, ErrTaskQueueNotFound
	}
	return tq, nil
}

func (s *Service) PartitionManager() *partition.Manager {
	return s.partitionMgr
}

// GetDLQEntries returns all entries in the dead letter queue.
func (s *Service) GetDLQEntries() []*engine.DLQEntry {
	return s.dlq.List()
}

// RetryDLQTask removes a task from the DLQ and re-adds it to its original queue.
func (s *Service) RetryDLQTask(ctx context.Context, taskID string) error {
	task, err := s.dlq.Retry(taskID)
	if err != nil {
		return err
	}

	// Determine the queue name from the task's namespace or use default
	queueName := "default"
	if task.Namespace != "" {
		queueName = task.Namespace
	}

	tq := s.GetOrCreateTaskQueue(queueName, engine.TaskQueueKindNormal)
	return tq.AddTask(task)
}

// PurgeDLQ removes all entries from the dead letter queue and returns the count removed.
func (s *Service) PurgeDLQ() int {
	return s.dlq.Purge()
}

// GetAllMetrics returns a snapshot of metrics for every active task queue.
func (s *Service) GetAllMetrics() map[string]*engine.MetricsSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]*engine.MetricsSnapshot, len(s.taskQueues))
	for name, tq := range s.taskQueues {
		snap := tq.Metrics().Snapshot()
		result[name] = &snap
	}
	return result
}

// GetQueueStats returns a metrics snapshot for a specific queue.
func (s *Service) GetQueueStats(queueName string) (*engine.MetricsSnapshot, error) {
	s.mu.RLock()
	tq, exists := s.taskQueues[queueName]
	s.mu.RUnlock()

	if !exists {
		return nil, ErrTaskQueueNotFound
	}

	snap := tq.Metrics().Snapshot()
	return &snap, nil
}

func (s *Service) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return nil
	}
	s.running = true
	s.stopCh = make(chan struct{})

	// Initialize WAL if configured
	if s.walDir != "" {
		wal, err := engine.NewWAL(s.walDir, s.logger)
		if err != nil {
			s.running = false
			s.mu.Unlock()
			return err
		}

		// Recover pending tasks BEFORE setting s.wal to avoid re-WAL'ing recovered tasks
		tasks, err := wal.Recover()
		if err != nil {
			s.logger.Error("WAL recovery failed", slog.String("error", err.Error()))
		} else if len(tasks) > 0 {
			s.mu.Unlock()
			for _, task := range tasks {
				queueName := "default"
				if task.Namespace != "" {
					queueName = task.Namespace
				}
				tq := s.GetOrCreateTaskQueue(queueName, engine.TaskQueueKindNormal)
				if err := tq.AddTask(task); err != nil && !errors.Is(err, engine.ErrTaskExists) {
					s.logger.Error("failed to recover task",
						slog.String("task_id", task.ID),
						slog.String("error", err.Error()),
					)
				}
			}
			s.logger.Info("recovered tasks from WAL", slog.Int("count", len(tasks)))
			s.mu.Lock()
			if !s.running {
				s.mu.Unlock()
				return errors.New("service stopped during WAL recovery")
			}
		}

		// Now set WAL so future task additions are logged
		s.wal = wal
	}
	s.mu.Unlock()

	s.wg.Add(1)
	go s.runLeaseReaper(ctx)

	s.logger.Info("matching service started")
	return nil
}

func (s *Service) Stop() error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return nil
	}
	s.running = false
	close(s.stopCh)
	s.mu.Unlock()

	s.wg.Wait()

	// Close WAL
	if s.wal != nil {
		if err := s.wal.Close(); err != nil {
			s.logger.Error("failed to close WAL", slog.String("error", err.Error()))
		}
	}

	s.logger.Info("matching service stopped")
	return nil
}

func (s *Service) runLeaseReaper(ctx context.Context) {
	defer s.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.requeueExpiredTasks()
		}
	}
}

func (s *Service) requeueExpiredTasks() {
	s.mu.RLock()
	queues := make([]*engine.TaskQueue, 0, len(s.taskQueues))
	for _, tq := range s.taskQueues {
		queues = append(queues, tq)
	}
	s.mu.RUnlock()

	totalRequeued := 0
	for _, tq := range queues {
		requeued := tq.RequeueExpiredTasks()
		totalRequeued += requeued
	}

	if totalRequeued > 0 {
		s.logger.Info("requeued expired tasks", slog.Int("count", totalRequeued))
	}
}
