package frontend

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"time"

	"github.com/linkflow/engine/internal/frontend/namespace"
	"github.com/linkflow/engine/internal/frontend/ratelimit"
)

type HistoryClient interface {
	RecordEvent(ctx context.Context, req *RecordEventRequest) error
	GetHistory(ctx context.Context, req *GetHistoryRequest) (*GetHistoryResponse, error)
	GetMutableState(ctx context.Context, key ExecutionKey) (*MutableState, error)
}

type MatchingClient interface {
	AddTask(ctx context.Context, req *AddTaskRequest) error
	PollTask(ctx context.Context, req *PollTaskRequest) (*Task, error)
}

type Service struct {
	historyClient  HistoryClient
	matchingClient MatchingClient
	namespaceCache *namespace.Cache
	rateLimiter    *ratelimit.Limiter
	logger         *slog.Logger
}

type ServiceConfig struct {
	RateLimitConfig ratelimit.Config
}

func DefaultServiceConfig() ServiceConfig {
	return ServiceConfig{
		RateLimitConfig: ratelimit.DefaultConfig(),
	}
}

func NewService(
	historyClient HistoryClient,
	matchingClient MatchingClient,
	logger *slog.Logger,
	cfg ServiceConfig,
) *Service {
	return &Service{
		historyClient:  historyClient,
		matchingClient: matchingClient,
		namespaceCache: namespace.NewCache(),
		rateLimiter:    ratelimit.NewLimiter(cfg.RateLimitConfig),
		logger:         logger,
	}
}

func (s *Service) HistoryClient() HistoryClient {
	return s.historyClient
}

func (s *Service) MatchingClient() MatchingClient {
	return s.matchingClient
}

func (s *Service) NamespaceCache() *namespace.Cache {
	return s.namespaceCache
}

func (s *Service) RateLimiter() *ratelimit.Limiter {
	return s.rateLimiter
}

func (s *Service) Logger() *slog.Logger {
	return s.logger
}

func (s *Service) StartWorkflowExecution(ctx context.Context, req *StartWorkflowExecutionRequest) (*StartWorkflowExecutionResponse, error) {
	runID := req.RequestID
	if runID == "" {
		runID = generateRunID()
	}

	eventReq := &RecordEventRequest{
		NamespaceID: req.Namespace,
		WorkflowID:  req.WorkflowID,
		RunID:       runID,
		EventType:   "WorkflowExecutionStarted",
		Attributes: &ExecutionStartedAttributes{
			WorkflowType: req.WorkflowType,
			TaskQueue:    req.TaskQueue,
			Input:        req.Input,
		},
	}
	if err := s.historyClient.RecordEvent(ctx, eventReq); err != nil {
		return nil, err
	}

	taskReq := &AddTaskRequest{
		NamespaceID:      req.Namespace,
		WorkflowID:       req.WorkflowID,
		RunID:            runID,
		TaskQueue:        req.TaskQueue,
		TaskType:         TaskTypeWorkflow,
		TaskInfo:         nil,
		ScheduledEventID: 1,
	}
	if err := s.matchingClient.AddTask(ctx, taskReq); err != nil {
		return nil, err
	}

	return &StartWorkflowExecutionResponse{
		RunID: runID,
	}, nil
}

func (s *Service) SignalWorkflowExecution(ctx context.Context, req *SignalWorkflowExecutionRequest) error {
	eventReq := &RecordEventRequest{
		NamespaceID: req.Namespace,
		WorkflowID:  req.WorkflowID,
		RunID:       req.RunID,
		EventType:   "WorkflowExecutionSignaled",
		Attributes:  req.Input,
	}
	return s.historyClient.RecordEvent(ctx, eventReq)
}

func (s *Service) TerminateWorkflowExecution(ctx context.Context, req *TerminateWorkflowExecutionRequest) error {
	eventReq := &RecordEventRequest{
		NamespaceID: req.Namespace,
		WorkflowID:  req.WorkflowID,
		RunID:       req.RunID,
		EventType:   "WorkflowExecutionTerminated",
		Attributes:  req.Details,
	}
	return s.historyClient.RecordEvent(ctx, eventReq)
}

func (s *Service) QueryWorkflow(ctx context.Context, req *QueryWorkflowRequest) (*QueryWorkflowResponse, error) {
	key := ExecutionKey{
		NamespaceID: req.Namespace,
		WorkflowID:  req.WorkflowID,
		RunID:       req.RunID,
	}

	state, err := s.historyClient.GetMutableState(ctx, key)
	if err != nil {
		return nil, err
	}

	_ = state

	return &QueryWorkflowResponse{
		QueryResult: nil,
	}, nil
}

func (s *Service) GetExecution(ctx context.Context, req *GetExecutionRequest) (*GetExecutionResponse, error) {
	key := ExecutionKey{
		NamespaceID: req.Namespace,
		WorkflowID:  req.WorkflowID,
		RunID:       req.RunID,
	}

	state, err := s.historyClient.GetMutableState(ctx, key)
	if err != nil {
		return nil, err
	}

	return &GetExecutionResponse{
		Execution: state.ExecutionInfo,
	}, nil
}

func (s *Service) ListExecutions(ctx context.Context, req *ListExecutionsRequest) (*ListExecutionsResponse, error) {
	return &ListExecutionsResponse{
		Executions:    []*WorkflowExecution{},
		NextPageToken: nil,
	}, nil
}

func (s *Service) DescribeExecution(ctx context.Context, req *DescribeExecutionRequest) (*DescribeExecutionResponse, error) {
	key := ExecutionKey{
		NamespaceID: req.Namespace,
		WorkflowID:  req.WorkflowID,
		RunID:       req.RunID,
	}

	state, err := s.historyClient.GetMutableState(ctx, key)
	if err != nil {
		return nil, err
	}

	pendingActivities := make([]*PendingActivity, 0, len(state.ActivityInfos))
	for _, info := range state.ActivityInfos {
		pendingActivities = append(pendingActivities, &PendingActivity{
			ActivityID:    info.ActivityID,
			ActivityType:  info.ActivityType,
			ScheduledTime: info.ScheduledTime,
			Attempt:       info.Attempt,
		})
	}

	pendingChildren := make([]*PendingChildExecution, 0, len(state.ChildExecutions))
	for _, child := range state.ChildExecutions {
		pendingChildren = append(pendingChildren, &PendingChildExecution{
			WorkflowID:   child.WorkflowID,
			RunID:        child.RunID,
			WorkflowType: child.WorkflowType,
			InitiatedID:  child.InitiatedID,
		})
	}

	return &DescribeExecutionResponse{
		Execution:         state.ExecutionInfo,
		PendingActivities: pendingActivities,
		PendingChildExecs: pendingChildren,
	}, nil
}

func generateRunID() string {
	return "run-" + secureRandomString(32)
}

func secureRandomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// Fallback to a UUID-like format if crypto/rand fails
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	for i := range b {
		b[i] = letters[int(b[i])%len(letters)]
	}
	return string(b)
}
