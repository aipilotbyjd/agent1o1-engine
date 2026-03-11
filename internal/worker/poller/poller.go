package poller

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

type Task struct {
	TaskToken        []byte                 `json:"task_token"`
	TaskID           string                 `json:"task_id"`
	WorkflowID       string                 `json:"workflow_id"`
	RunID            string                 `json:"run_id"`
	Namespace        string                 `json:"namespace"`
	NodeType         string                 `json:"node_type"`
	NodeID           string                 `json:"node_id"`
	Config           []byte                 `json:"config"`
	Input            []byte                 `json:"input"`
	Deterministic    map[string]interface{} `json:"deterministic"`
	Attempt          int32                  `json:"attempt"`
	TimeoutSec       int32                  `json:"timeout_sec"`
	ScheduledEventID int64                  `json:"scheduled_event_id"`
}

type TaskResult struct {
	TaskID    string `json:"task_id"`
	Output    []byte `json:"output"`
	Error     string `json:"error"`
	ErrorType string `json:"error_type"`
	Logs      []byte `json:"logs"`
}

type TaskHandler func(ctx context.Context, task *Task) (*TaskResult, error)

type MatchingClient interface {
	PollTask(ctx context.Context, taskQueue string, identity string) (*Task, error)
	CompleteTask(ctx context.Context, task *Task, identity string) error
}

type Poller struct {
	client       MatchingClient
	taskQueue    string
	identity     string
	pollInterval time.Duration
	logger       *slog.Logger

	handler TaskHandler
	wg      sync.WaitGroup
	stopCh  chan struct{}
	running bool
	mu      sync.Mutex
	pollCtx context.Context
	cancel  context.CancelFunc
}

type Config struct {
	Client       MatchingClient
	TaskQueue    string
	Identity     string
	PollInterval time.Duration
	Logger       *slog.Logger
}

func New(cfg Config) *Poller {
	if cfg.PollInterval == 0 {
		cfg.PollInterval = time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	return &Poller{
		client:       cfg.Client,
		taskQueue:    cfg.TaskQueue,
		identity:     cfg.Identity,
		pollInterval: cfg.PollInterval,
		logger:       cfg.Logger,
		stopCh:       make(chan struct{}),
	}
}

func (p *Poller) SetHandler(handler TaskHandler) {
	p.handler = handler
}

func (p *Poller) Start(ctx context.Context) error {
	p.mu.Lock()
	if p.running {
		p.mu.Unlock()
		return nil
	}
	p.running = true
	p.stopCh = make(chan struct{})
	p.pollCtx, p.cancel = context.WithCancel(ctx)
	p.mu.Unlock()

	p.wg.Add(1)
	go p.pollLoop(p.pollCtx)

	p.logger.Info("poller started",
		slog.String("task_queue", p.taskQueue),
		slog.String("identity", p.identity),
	)

	return nil
}

func (p *Poller) Stop() {
	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()
		return
	}
	p.running = false
	if p.cancel != nil {
		p.cancel()
	}
	close(p.stopCh)
	p.mu.Unlock()

	p.wg.Wait()
	p.logger.Info("poller stopped")
}

func (p *Poller) pollLoop(ctx context.Context) {
	defer p.wg.Done()

	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopCh:
			return
		case <-ticker.C:
			task, err := p.Poll(ctx)
			if err != nil {
				p.logger.Error("poll failed", slog.String("error", err.Error()))
				continue
			}
			if task == nil {
				continue
			}

			if p.handler != nil {
				result, err := p.handler(ctx, task)
				if err != nil {
					p.logger.Error("task handler failed",
						slog.String("task_id", task.TaskID),
						slog.String("error", err.Error()),
					)
				} else {
					if err := p.client.CompleteTask(ctx, task, p.identity); err != nil {
						p.logger.Error("failed to complete task",
							slog.String("task_id", task.TaskID),
							slog.String("error", err.Error()),
						)
					}

					p.logger.Debug("task completed",
						slog.String("task_id", task.TaskID),
						slog.String("error_type", result.ErrorType),
					)
				}
			}
		}
	}
}

func (p *Poller) Poll(ctx context.Context) (*Task, error) {
	return p.client.PollTask(ctx, p.taskQueue, p.identity)
}

func (p *Poller) IsRunning() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.running
}
