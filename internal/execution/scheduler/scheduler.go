package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/linkflow/engine/internal/execution/graph"
)

var (
	ErrExecutionFailed  = errors.New("execution failed")
	ErrNodeFailed       = errors.New("node execution failed")
	ErrExecutionTimeout = errors.New("execution timed out")
)

// NodeExecutor is the interface for node execution.
type NodeExecutor interface {
	Execute(ctx context.Context, nodeType string, input json.RawMessage, config json.RawMessage) (*NodeResult, error)
}

// Scheduler schedules and coordinates node execution.
type Scheduler struct {
	dag         *graph.DAG
	executor    NodeExecutor
	concurrency int
	timeout     time.Duration
	logger      *slog.Logger

	state       *ExecutionState
	taskQueue   chan *NodeTask
	resultQueue chan *NodeResult
	errorQueue  chan *NodeError

	wg sync.WaitGroup
}

// Config holds scheduler configuration.
type Config struct {
	Concurrency int
	Timeout     time.Duration
}

// DefaultConfig returns default scheduler config.
func DefaultConfig() Config {
	return Config{
		Concurrency: 10,
		Timeout:     5 * time.Minute,
	}
}

// NewScheduler creates a new scheduler.
func NewScheduler(dag *graph.DAG, executor NodeExecutor, config Config, logger *slog.Logger) *Scheduler {
	if logger == nil {
		logger = slog.Default()
	}

	return &Scheduler{
		dag:         dag,
		executor:    executor,
		concurrency: config.Concurrency,
		timeout:     config.Timeout,
		logger:      logger,
		taskQueue:   make(chan *NodeTask, 100),
		resultQueue: make(chan *NodeResult, 100),
		errorQueue:  make(chan *NodeError, 100),
	}
}

// ExecutionState tracks the state of an execution.
type ExecutionState struct {
	ExecutionID string
	Status      ExecutionStatus

	NodeStates     map[string]*NodeState
	NodeOutputs    map[string]json.RawMessage
	CompletedNodes map[string]bool
	FailedNodes    map[string]*NodeError
	SkippedNodes   map[string]bool
	ScheduledNodes map[string]bool

	StartedAt   time.Time
	CompletedAt time.Time

	mu sync.RWMutex
}

// ExecutionStatus represents execution status.
type ExecutionStatus int

const (
	ExecutionStatusPending ExecutionStatus = iota
	ExecutionStatusRunning
	ExecutionStatusCompleted
	ExecutionStatusFailed
	ExecutionStatusCanceled
	ExecutionStatusTimedOut
)

// NodeState represents node state.
type NodeState struct {
	NodeID      string
	Status      NodeStatus
	StartedAt   time.Time
	CompletedAt time.Time
	Attempt     int
	Error       *NodeError
}

// NodeStatus represents node status.
type NodeStatus int

const (
	NodeStatusPending NodeStatus = iota
	NodeStatusRunning
	NodeStatusCompleted
	NodeStatusFailed
	NodeStatusSkipped
)

// NodeTask represents a task to execute a node.
type NodeTask struct {
	NodeID   string
	NodeType string
	Input    json.RawMessage
	Config   json.RawMessage
	Attempt  int
}

// NodeResult represents the result of node execution.
type NodeResult struct {
	NodeID  string
	Output  json.RawMessage
	Logs    []LogEntry
	Metrics NodeMetrics
}

// NodeError represents a node execution error.
type NodeError struct {
	NodeID    string
	Error     error
	Attempt   int
	Retryable bool
}

// LogEntry represents a log entry.
type LogEntry struct {
	Timestamp time.Time
	Level     string
	Message   string
}

// NodeMetrics holds node execution metrics.
type NodeMetrics struct {
	Duration   time.Duration
	MemoryPeak int64
	CPUTime    time.Duration
}

// ExecutionResult is the final result of execution.
type ExecutionResult struct {
	ExecutionID string
	Status      ExecutionStatus
	Outputs     map[string]json.RawMessage
	Duration    time.Duration
	NodeMetrics map[string]NodeMetrics
}

// Execute executes the workflow.
func (s *Scheduler) Execute(ctx context.Context, executionID string, input json.RawMessage) (*ExecutionResult, error) {
	// Apply timeout
	if s.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.timeout)
		defer cancel()
	}

	// Initialize state
	s.state = &ExecutionState{
		ExecutionID:    executionID,
		Status:         ExecutionStatusRunning,
		NodeStates:     make(map[string]*NodeState),
		NodeOutputs:    make(map[string]json.RawMessage),
		CompletedNodes: make(map[string]bool),
		FailedNodes:    make(map[string]*NodeError),
		SkippedNodes:   make(map[string]bool),
		ScheduledNodes: make(map[string]bool),
		StartedAt:      time.Now(),
	}

	s.logger.Info("starting workflow execution",
		slog.String("execution_id", executionID),
		slog.Int("node_count", len(s.dag.Nodes)),
	)

	// Store trigger data as entry node output
	for _, entryID := range s.dag.EntryNodes {
		s.state.NodeOutputs[entryID] = input
	}

	// Start worker pool
	for i := 0; i < s.concurrency; i++ {
		s.wg.Add(1)
		go s.worker(ctx)
	}

	// Schedule entry nodes
	for _, entryID := range s.dag.EntryNodes {
		s.scheduleNode(ctx, entryID, input)
	}

	// Process results until complete
	err := s.processUntilComplete(ctx)

	// Cleanup - rely on context cancellation, do not close taskQueue
	s.wg.Wait()

	s.state.CompletedAt = time.Now()

	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			s.state.Status = ExecutionStatusTimedOut
		} else if errors.Is(err, context.Canceled) {
			s.state.Status = ExecutionStatusCanceled
		} else {
			s.state.Status = ExecutionStatusFailed
		}
		return nil, err
	}

	s.state.Status = ExecutionStatusCompleted

	s.logger.Info("workflow execution completed",
		slog.String("execution_id", executionID),
		slog.Duration("duration", s.state.CompletedAt.Sub(s.state.StartedAt)),
	)

	return &ExecutionResult{
		ExecutionID: s.state.ExecutionID,
		Status:      s.state.Status,
		Outputs:     s.collectOutputs(),
		Duration:    s.state.CompletedAt.Sub(s.state.StartedAt),
	}, nil
}

func (s *Scheduler) worker(ctx context.Context) {
	defer s.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return

		case task, ok := <-s.taskQueue:
			if !ok {
				return
			}

			result, err := s.executeNode(ctx, task)
			if err != nil {
				nodeErr := &NodeError{
					NodeID:    task.NodeID,
					Error:     err,
					Attempt:   task.Attempt,
					Retryable: isRetryableError(err),
				}
				select {
				case s.errorQueue <- nodeErr:
				case <-ctx.Done():
					return
				}
			} else {
				select {
				case s.resultQueue <- result:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

func (s *Scheduler) scheduleNode(ctx context.Context, nodeID string, input json.RawMessage) {
	node := s.dag.Nodes[nodeID]

	s.state.mu.Lock()
	s.state.NodeStates[nodeID] = &NodeState{
		NodeID:    nodeID,
		Status:    NodeStatusRunning,
		StartedAt: time.Now(),
		Attempt:   1,
	}
	s.state.mu.Unlock()

	select {
	case s.taskQueue <- &NodeTask{
		NodeID:   nodeID,
		NodeType: node.Type,
		Input:    input,
		Config:   node.Config,
		Attempt:  1,
	}:
	case <-ctx.Done():
	}
}

func (s *Scheduler) executeNode(ctx context.Context, task *NodeTask) (*NodeResult, error) {
	s.logger.Debug("executing node",
		slog.String("node_id", task.NodeID),
		slog.String("node_type", task.NodeType),
	)

	return s.executor.Execute(ctx, task.NodeType, task.Input, task.Config)
}

func (s *Scheduler) processUntilComplete(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case result := <-s.resultQueue:
			s.handleNodeCompleted(ctx, result)

			// Check if execution is complete
			if s.isExecutionComplete() {
				return nil
			}

		case nodeErr := <-s.errorQueue:
			if nodeErr.Retryable && nodeErr.Attempt < 3 {
				s.scheduleRetry(ctx, nodeErr)
			} else {
				return s.handleNodeFailed(nodeErr)
			}
		}
	}
}

func (s *Scheduler) handleNodeCompleted(ctx context.Context, result *NodeResult) {
	s.state.mu.Lock()

	// Update state
	s.state.CompletedNodes[result.NodeID] = true
	s.state.NodeOutputs[result.NodeID] = result.Output
	s.state.NodeStates[result.NodeID].Status = NodeStatusCompleted
	s.state.NodeStates[result.NodeID].CompletedAt = time.Now()

	s.logger.Debug("node completed",
		slog.String("node_id", result.NodeID),
	)

	// Check if the completed node is a condition node (for logging)
	completedNode := s.dag.Nodes[result.NodeID]
	if completedNode.Type == "condition" {
		// Parse and log the condition output for debugging
		var condResult struct {
			Output string `json:"output"`
		}
		if err := json.Unmarshal(result.Output, &condResult); err == nil {
			s.logger.Debug("condition node output",
				slog.String("node_id", result.NodeID),
				slog.String("selected_branch", condResult.Output),
			)
		}
	}

	// Find and schedule next nodes
	nextNodes := s.dag.GetNextNodes(s.state.CompletedNodes)

	// Collect nodes to schedule (check if already scheduled)
	var nodesToSchedule []string
	for _, nextID := range nextNodes {
		if s.state.ScheduledNodes[nextID] {
			continue
		}

		// Check if this node should be skipped due to conditional branching
		shouldSchedule := true

		// Check all upstream nodes to see if any are condition nodes
		for _, upstreamID := range s.dag.ReverseEdges[nextID] {
			upstreamNode := s.dag.Nodes[upstreamID]
			if upstreamNode.Type == "condition" {
				// Get the edge info to check sourceHandle
				edgeInfo := s.dag.GetEdgeInfo(upstreamID, nextID)
				if edgeInfo != nil && edgeInfo.SourceHandle != "" {
					// This is a conditional edge - check if it matches the condition output
					upstreamOutput := s.state.NodeOutputs[upstreamID]
					var upstreamCondResult struct {
						Output string `json:"output"`
					}
					if err := json.Unmarshal(upstreamOutput, &upstreamCondResult); err == nil {
						// Only schedule if the edge sourceHandle matches the condition output
						if edgeInfo.SourceHandle != upstreamCondResult.Output {
							shouldSchedule = false
							s.logger.Debug("skipping node due to unmatched condition branch",
								slog.String("node_id", nextID),
								slog.String("edge_handle", edgeInfo.SourceHandle),
								slog.String("condition_output", upstreamCondResult.Output),
							)
							// Mark as skipped
							s.state.SkippedNodes[nextID] = true
							break
						}
					}
				}
			}
		}

		if shouldSchedule {
			s.state.ScheduledNodes[nextID] = true
			nodesToSchedule = append(nodesToSchedule, nextID)
		}
	}

	s.state.mu.Unlock()

	for _, nextID := range nodesToSchedule {
		// Merge inputs from all upstream nodes
		input := s.mergeInputs(nextID)
		s.scheduleNode(ctx, nextID, input)
	}
}

func (s *Scheduler) handleNodeFailed(nodeErr *NodeError) error {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()

	s.state.FailedNodes[nodeErr.NodeID] = nodeErr
	s.state.NodeStates[nodeErr.NodeID].Status = NodeStatusFailed
	s.state.NodeStates[nodeErr.NodeID].Error = nodeErr

	s.logger.Error("node failed",
		slog.String("node_id", nodeErr.NodeID),
		slog.String("error", nodeErr.Error.Error()),
	)

	return fmt.Errorf("%w: %s: %w", ErrNodeFailed, nodeErr.NodeID, nodeErr.Error)
}

func (s *Scheduler) scheduleRetry(ctx context.Context, nodeErr *NodeError) {
	s.state.mu.Lock()
	s.state.NodeStates[nodeErr.NodeID].Attempt++
	attempt := s.state.NodeStates[nodeErr.NodeID].Attempt
	input := s.state.NodeOutputs[nodeErr.NodeID]
	s.state.mu.Unlock()

	node := s.dag.Nodes[nodeErr.NodeID]

	s.logger.Info("retrying node",
		slog.String("node_id", nodeErr.NodeID),
		slog.Int("attempt", attempt),
	)

	select {
	case s.taskQueue <- &NodeTask{
		NodeID:   nodeErr.NodeID,
		NodeType: node.Type,
		Input:    input,
		Config:   node.Config,
		Attempt:  attempt,
	}:
	case <-ctx.Done():
	}
}

func (s *Scheduler) mergeInputs(nodeID string) json.RawMessage {
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()

	merged := make(map[string]json.RawMessage)

	for _, upstream := range s.dag.ReverseEdges[nodeID] {
		if output, ok := s.state.NodeOutputs[upstream]; ok {
			merged[upstream] = output
		}
	}

	result, _ := json.Marshal(merged)
	return result
}

func (s *Scheduler) isExecutionComplete() bool {
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()

	// Check if all exit nodes are completed
	for _, exitID := range s.dag.ExitNodes {
		if !s.state.CompletedNodes[exitID] && !s.state.SkippedNodes[exitID] {
			return false
		}
	}
	return true
}

func (s *Scheduler) collectOutputs() map[string]json.RawMessage {
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()

	// Collect outputs from exit nodes
	outputs := make(map[string]json.RawMessage)
	for _, exitID := range s.dag.ExitNodes {
		if output, ok := s.state.NodeOutputs[exitID]; ok {
			outputs[exitID] = output
		}
	}
	return outputs
}

func isRetryableError(err error) bool {
	// Add logic to determine if error is retryable
	return true // Default to retryable for now
}
