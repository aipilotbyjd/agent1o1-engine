package timer

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

var (
	ErrServiceNotRunning      = errors.New("timer service is not running")
	ErrTimerNotFound          = errors.New("timer not found")
	ErrTimerAlreadyExists     = errors.New("timer already exists")
	ErrOptimisticLockConflict = errors.New("optimistic lock conflict: version mismatch")
)

// TimerStatus represents the status of a timer.
type TimerStatus int32

const (
	TimerStatusPending TimerStatus = iota
	TimerStatusFired
	TimerStatusCanceled
)

// Timer represents a scheduled timer.
type Timer struct {
	ID          string
	ShardID     int32
	NamespaceID string
	WorkflowID  string
	RunID       string
	TimerID     string
	FireTime    time.Time
	Status      TimerStatus
	Version     int64
	CreatedAt   time.Time
	FiredAt     time.Time
}

// TimerCallback is called when a timer fires.
type TimerCallback func(ctx context.Context, timer *Timer) error

// Store defines the interface for timer persistence.
type Store interface {
	// CreateTimer creates a new timer
	CreateTimer(ctx context.Context, timer *Timer) error
	// GetTimer retrieves a timer by ID
	GetTimer(ctx context.Context, namespaceID, workflowID, runID, timerID string) (*Timer, error)
	// UpdateTimer updates a timer
	UpdateTimer(ctx context.Context, timer *Timer) error
	// DeleteTimer deletes a timer
	DeleteTimer(ctx context.Context, namespaceID, workflowID, runID, timerID string) error
	// GetDueTimers returns all timers that are due for firing
	GetDueTimers(ctx context.Context, shardID int32, fireTime time.Time, limit int) ([]*Timer, error)
	// GetTimersByExecution returns all timers for an execution
	GetTimersByExecution(ctx context.Context, namespaceID, workflowID, runID string) ([]*Timer, error)
}

// HistoryClient defines the interface for history service communication.
type HistoryClient interface {
	// RecordTimerFired records that a timer has fired
	RecordTimerFired(ctx context.Context, namespaceID, workflowID, runID, timerID string) error
}

// Config holds the configuration for the timer service.
type Config struct {
	NumShards      int32
	ScanInterval   time.Duration
	BatchSize      int
	ProcessorCount int
	LockDuration   time.Duration
	MaxFireDelay   time.Duration
	Logger         *slog.Logger
}

// DefaultConfig returns the default configuration.
func DefaultConfig() Config {
	return Config{
		NumShards:      16,
		ScanInterval:   time.Second,
		BatchSize:      100,
		ProcessorCount: 4,
		LockDuration:   30 * time.Second,
		MaxFireDelay:   time.Minute,
	}
}

// Service is the timer service that handles scheduled timers.
type Service struct {
	store         Store
	historyClient HistoryClient
	config        Config
	logger        *slog.Logger

	// Shard assignment for this instance
	assignedShards []int32

	// Channels for coordination
	stopCh  chan struct{}
	timerCh chan *Timer

	running bool
	mu      sync.RWMutex
	wg      sync.WaitGroup
}

// NewService creates a new timer service.
func NewService(store Store, historyClient HistoryClient, config Config) *Service {
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	if config.NumShards <= 0 {
		config.NumShards = 16
	}
	if config.ScanInterval <= 0 {
		config.ScanInterval = time.Second
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 100
	}
	if config.ProcessorCount <= 0 {
		config.ProcessorCount = 4
	}

	return &Service{
		store:         store,
		historyClient: historyClient,
		config:        config,
		logger:        config.Logger,
		stopCh:        make(chan struct{}),
		timerCh:       make(chan *Timer, config.BatchSize*config.ProcessorCount),
	}
}

// AssignShards assigns shards to this instance for processing.
func (s *Service) AssignShards(shards []int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.assignedShards = shards
	s.logger.Info("assigned shards", slog.Any("shards", shards))
}

// Start starts the timer service.
func (s *Service) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return errors.New("timer service is already running")
	}
	s.running = true
	s.stopCh = make(chan struct{})
	s.mu.Unlock()

	s.logger.Info("starting timer service",
		slog.Int("processor_count", s.config.ProcessorCount),
		slog.Duration("scan_interval", s.config.ScanInterval),
	)

	// Start scanner goroutines for each assigned shard
	s.wg.Add(1)
	go s.runScanner(ctx)

	// Start processor goroutines
	for i := 0; i < s.config.ProcessorCount; i++ {
		s.wg.Add(1)
		go s.runProcessor(ctx, i)
	}

	return nil
}

// Stop stops the timer service.
func (s *Service) Stop(ctx context.Context) error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return nil
	}
	s.running = false
	close(s.stopCh)
	s.mu.Unlock()

	s.logger.Info("stopping timer service")

	// Wait for all goroutines to finish
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.logger.Info("timer service stopped")
	case <-ctx.Done():
		s.logger.Warn("timer service stop timed out")
	}

	return nil
}

// CreateTimer creates a new timer.
func (s *Service) CreateTimer(ctx context.Context, timer *Timer) error {
	s.mu.RLock()
	running := s.running
	s.mu.RUnlock()

	if !running {
		return ErrServiceNotRunning
	}

	timer.Status = TimerStatusPending
	timer.CreatedAt = time.Now()
	timer.ShardID = s.getShardID(timer.NamespaceID, timer.WorkflowID)

	s.logger.Debug("creating timer",
		slog.String("timer_id", timer.TimerID),
		slog.String("workflow_id", timer.WorkflowID),
		slog.Time("fire_time", timer.FireTime),
	)

	return s.store.CreateTimer(ctx, timer)
}

// CancelTimer cancels a timer.
func (s *Service) CancelTimer(ctx context.Context, namespaceID, workflowID, runID, timerID string) error {
	s.mu.RLock()
	running := s.running
	s.mu.RUnlock()

	if !running {
		return ErrServiceNotRunning
	}

	timer, err := s.store.GetTimer(ctx, namespaceID, workflowID, runID, timerID)
	if err != nil {
		return err
	}

	if timer.Status != TimerStatusPending {
		return nil // Timer already fired or canceled
	}

	timer.Status = TimerStatusCanceled
	timer.Version++

	s.logger.Debug("canceling timer",
		slog.String("timer_id", timerID),
		slog.String("workflow_id", workflowID),
	)

	return s.store.UpdateTimer(ctx, timer)
}

// GetTimer retrieves a timer.
func (s *Service) GetTimer(ctx context.Context, namespaceID, workflowID, runID, timerID string) (*Timer, error) {
	return s.store.GetTimer(ctx, namespaceID, workflowID, runID, timerID)
}

// runScanner scans for due timers and sends them to the processor.
func (s *Service) runScanner(ctx context.Context) {
	defer s.wg.Done()

	ticker := time.NewTicker(s.config.ScanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.scanDueTimers(ctx)
		}
	}
}

func (s *Service) scanDueTimers(ctx context.Context) {
	s.mu.RLock()
	shards := s.assignedShards
	s.mu.RUnlock()

	if len(shards) == 0 {
		// If no shards assigned, scan all shards
		shards = make([]int32, s.config.NumShards)
		for i := int32(0); i < s.config.NumShards; i++ {
			shards[i] = i
		}
	}

	now := time.Now()

	for _, shardID := range shards {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		default:
		}

		timers, err := s.store.GetDueTimers(ctx, shardID, now, s.config.BatchSize)
		if err != nil {
			s.logger.Error("failed to get due timers",
				slog.Int("shard_id", int(shardID)),
				slog.String("error", err.Error()),
			)
			continue
		}

		for _, timer := range timers {
			select {
			case <-ctx.Done():
				return
			case <-s.stopCh:
				return
			case s.timerCh <- timer:
			}
		}
	}
}

// runProcessor processes due timers.
func (s *Service) runProcessor(ctx context.Context, id int) {
	defer s.wg.Done()

	s.logger.Debug("timer processor started", slog.Int("processor_id", id))

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case timer := <-s.timerCh:
			s.processTimer(ctx, timer)
		}
	}
}

func (s *Service) processTimer(ctx context.Context, timer *Timer) {
	current, err := s.store.GetTimer(ctx, timer.NamespaceID, timer.WorkflowID, timer.RunID, timer.TimerID)
	if err != nil {
		s.logger.Error("failed to get timer for processing",
			slog.String("timer_id", timer.TimerID),
			slog.String("error", err.Error()),
		)
		return
	}

	if current.Status != TimerStatusPending {
		s.logger.Debug("timer already processed",
			slog.String("timer_id", timer.TimerID),
			slog.Int("status", int(current.Status)),
		)
		return
	}

	delay := time.Since(timer.FireTime)
	if delay > s.config.MaxFireDelay {
		s.logger.Warn("timer fire delayed significantly",
			slog.String("timer_id", timer.TimerID),
			slog.Duration("delay", delay),
		)
	}

	current.Status = TimerStatusFired
	current.FiredAt = time.Now()
	current.Version++

	if err := s.store.UpdateTimer(ctx, current); err != nil {
		if errors.Is(err, ErrOptimisticLockConflict) {
			s.logger.Debug("timer already claimed by another processor",
				slog.String("timer_id", timer.TimerID))
			return
		}
		s.logger.Error("failed to claim timer",
			slog.String("timer_id", timer.TimerID),
			slog.String("error", err.Error()),
		)
		return
	}

	if err := s.historyClient.RecordTimerFired(ctx, current.NamespaceID, current.WorkflowID, current.RunID, current.TimerID); err != nil {
		s.logger.Error("failed to record timer fired",
			slog.String("timer_id", timer.TimerID),
			slog.String("error", err.Error()),
		)
		current.Status = TimerStatusPending
		current.FiredAt = time.Time{}
		current.Version++
		if rollbackErr := s.store.UpdateTimer(ctx, current); rollbackErr != nil {
			s.logger.Error("failed to rollback timer status",
				slog.String("timer_id", timer.TimerID),
				slog.String("error", rollbackErr.Error()),
			)
		}
		return
	}

	s.logger.Info("timer fired",
		slog.String("timer_id", timer.TimerID),
		slog.String("workflow_id", timer.WorkflowID),
		slog.Duration("delay", delay),
	)
}

// getShardID calculates the shard ID for a timer.
func (s *Service) getShardID(namespaceID, workflowID string) int32 {
	data := namespaceID + "/" + workflowID
	var hash uint32
	for i := 0; i < len(data); i++ {
		hash = 31*hash + uint32(data[i])
	}
	return int32(hash % uint32(s.config.NumShards))
}

// IsRunning returns whether the service is running.
func (s *Service) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.running
}
