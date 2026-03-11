package pool

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrPoolClosed    = errors.New("worker pool is closed")
	ErrPoolExhausted = errors.New("worker pool exhausted")
)

// Config holds pool configuration.
type Config struct {
	MinWorkers    int
	MaxWorkers    int
	IdleTimeout   time.Duration
	TaskTimeout   time.Duration
	QueueSize     int
	ScaleUpStep   int
	ScaleDownStep int
}

// DefaultConfig returns default pool config.
func DefaultConfig() Config {
	return Config{
		MinWorkers:    2,
		MaxWorkers:    50,
		IdleTimeout:   60 * time.Second,
		TaskTimeout:   30 * time.Second,
		QueueSize:     1000,
		ScaleUpStep:   5,
		ScaleDownStep: 2,
	}
}

// Task represents a task to be executed.
type Task struct {
	ID       string
	Execute  func(context.Context) error
	Timeout  time.Duration
	Priority int
}

// Manager manages a pool of workers.
type Manager struct {
	name   string
	config Config
	logger *slog.Logger

	tasks     chan *Task
	workers   int32
	active    int32
	completed int64
	failed    int64

	stopCh  chan struct{}
	running bool
	mu      sync.RWMutex
	wg      sync.WaitGroup
}

// NewManager creates a new pool manager.
func NewManager(name string, config Config, logger *slog.Logger) *Manager {
	if config.MinWorkers <= 0 {
		config.MinWorkers = 2
	}
	if config.MaxWorkers <= 0 {
		config.MaxWorkers = 50
	}
	if config.QueueSize <= 0 {
		config.QueueSize = 1000
	}
	if logger == nil {
		logger = slog.Default()
	}

	return &Manager{
		name:   name,
		config: config,
		logger: logger,
		tasks:  make(chan *Task, config.QueueSize),
		stopCh: make(chan struct{}),
	}
}

// Start starts the worker pool.
func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	if m.running {
		m.mu.Unlock()
		return errors.New("pool already running")
	}
	m.running = true
	m.stopCh = make(chan struct{})
	m.mu.Unlock()

	// Start minimum workers
	for i := 0; i < m.config.MinWorkers; i++ {
		m.startWorker(ctx)
	}

	// Start auto-scaler
	go m.runAutoScaler(ctx)

	m.logger.Info("worker pool started",
		slog.String("name", m.name),
		slog.Int("min_workers", m.config.MinWorkers),
		slog.Int("max_workers", m.config.MaxWorkers),
	)

	return nil
}

// Stop stops the worker pool.
func (m *Manager) Stop(ctx context.Context) error {
	m.mu.Lock()
	if !m.running {
		m.mu.Unlock()
		return nil
	}
	m.running = false
	close(m.stopCh)
	m.mu.Unlock()

	// Wait for workers to finish
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}

	m.logger.Info("worker pool stopped", slog.String("name", m.name))
	return nil
}

// Submit submits a task to the pool.
func (m *Manager) Submit(task *Task) error {
	m.mu.RLock()
	if !m.running {
		m.mu.RUnlock()
		return ErrPoolClosed
	}
	m.mu.RUnlock()

	select {
	case m.tasks <- task:
		return nil
	default:
		return ErrPoolExhausted
	}
}

// SubmitWait submits a task and waits for completion.
func (m *Manager) SubmitWait(ctx context.Context, task *Task) error {
	done := make(chan error, 1)

	originalExecute := task.Execute
	task.Execute = func(taskCtx context.Context) error {
		err := originalExecute(taskCtx)
		done <- err
		return err
	}

	if err := m.Submit(task); err != nil {
		return err
	}

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Metrics returns pool metrics.
func (m *Manager) Metrics() Metrics {
	return Metrics{
		Name:          m.name,
		Workers:       int(atomic.LoadInt32(&m.workers)),
		ActiveWorkers: int(atomic.LoadInt32(&m.active)),
		QueueSize:     len(m.tasks),
		QueueCapacity: cap(m.tasks),
		Completed:     atomic.LoadInt64(&m.completed),
		Failed:        atomic.LoadInt64(&m.failed),
	}
}

// Metrics holds pool metrics.
type Metrics struct {
	Name          string
	Workers       int
	ActiveWorkers int
	QueueSize     int
	QueueCapacity int
	Completed     int64
	Failed        int64
}

func (m *Manager) startWorker(ctx context.Context) {
	atomic.AddInt32(&m.workers, 1)
	m.wg.Add(1)

	go func() {
		defer m.wg.Done()
		defer atomic.AddInt32(&m.workers, -1)

		m.runWorker(ctx)
	}()
}

func (m *Manager) runWorker(ctx context.Context) {
	idleTimer := time.NewTimer(m.config.IdleTimeout)
	defer idleTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-m.stopCh:
			return

		case task := <-m.tasks:
			// Reset idle timer
			if !idleTimer.Stop() {
				select {
				case <-idleTimer.C:
				default:
				}
			}
			idleTimer.Reset(m.config.IdleTimeout)

			m.executeTask(ctx, task)

		case <-idleTimer.C:
			// Check if we can scale down
			currentWorkers := atomic.LoadInt32(&m.workers)
			if int(currentWorkers) > m.config.MinWorkers {
				return // Exit this worker
			}
			idleTimer.Reset(m.config.IdleTimeout)
		}
	}
}

func (m *Manager) executeTask(ctx context.Context, task *Task) {
	atomic.AddInt32(&m.active, 1)
	defer atomic.AddInt32(&m.active, -1)

	timeout := task.Timeout
	if timeout == 0 {
		timeout = m.config.TaskTimeout
	}

	taskCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if err := task.Execute(taskCtx); err != nil {
		atomic.AddInt64(&m.failed, 1)
		m.logger.Error("task failed",
			slog.String("task_id", task.ID),
			slog.String("error", err.Error()),
		)
	} else {
		atomic.AddInt64(&m.completed, 1)
	}
}

func (m *Manager) runAutoScaler(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.autoScale()
		}
	}
}

func (m *Manager) autoScale() {
	queueSize := len(m.tasks)
	currentWorkers := int(atomic.LoadInt32(&m.workers))
	activeWorkers := int(atomic.LoadInt32(&m.active))

	// Scale up if queue is backing up
	if queueSize > 0 && activeWorkers == currentWorkers && currentWorkers < m.config.MaxWorkers {
		toAdd := m.config.ScaleUpStep
		if currentWorkers+toAdd > m.config.MaxWorkers {
			toAdd = m.config.MaxWorkers - currentWorkers
		}

		for i := 0; i < toAdd; i++ {
			m.startWorker(context.Background())
		}

		m.logger.Info("scaled up workers",
			slog.String("name", m.name),
			slog.Int("added", toAdd),
			slog.Int("total", currentWorkers+toAdd),
		)
	}
}

// DynamicConfig represents dynamic pool configuration.
type DynamicConfig struct {
	Category    string
	MinWorkers  int
	MaxWorkers  int
	TaskTimeout time.Duration
}

// CategoryManager manages multiple worker pools by category.
type CategoryManager struct {
	pools  map[string]*Manager
	logger *slog.Logger
	mu     sync.RWMutex
}

// NewCategoryManager creates a new category manager.
func NewCategoryManager(logger *slog.Logger) *CategoryManager {
	return &CategoryManager{
		pools:  make(map[string]*Manager),
		logger: logger,
	}
}

// GetOrCreate gets or creates a pool for a category.
func (cm *CategoryManager) GetOrCreate(category string, config Config) *Manager {
	cm.mu.RLock()
	if pool, exists := cm.pools[category]; exists {
		cm.mu.RUnlock()
		return pool
	}
	cm.mu.RUnlock()

	cm.mu.Lock()
	defer cm.mu.Unlock()

	if pool, exists := cm.pools[category]; exists {
		return pool
	}

	pool := NewManager(category, config, cm.logger)
	cm.pools[category] = pool
	return pool
}

// StartAll starts all pools.
func (cm *CategoryManager) StartAll(ctx context.Context) error {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	for _, pool := range cm.pools {
		if err := pool.Start(ctx); err != nil {
			return err
		}
	}
	return nil
}

// StopAll stops all pools.
func (cm *CategoryManager) StopAll(ctx context.Context) error {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	for _, pool := range cm.pools {
		if err := pool.Stop(ctx); err != nil {
			return err
		}
	}
	return nil
}

// AllMetrics returns metrics for all pools.
func (cm *CategoryManager) AllMetrics() map[string]Metrics {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	metrics := make(map[string]Metrics)
	for name, pool := range cm.pools {
		metrics[name] = pool.Metrics()
	}
	return metrics
}
