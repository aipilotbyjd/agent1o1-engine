package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// Registry manages all available node executors.
type Registry struct {
	executors map[string]Executor
	mu        sync.RWMutex
}

// NewRegistry creates a new executor registry.
func NewRegistry() *Registry {
	return &Registry{
		executors: make(map[string]Executor),
	}
}

// Register registers an executor for a node type.
func (r *Registry) Register(executor Executor) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	nodeType := executor.NodeType()
	if _, exists := r.executors[nodeType]; exists {
		return fmt.Errorf("executor for node type '%s' is already registered", nodeType)
	}

	r.executors[nodeType] = executor
	return nil
}

// MustRegister registers an executor, panicking on error.
func (r *Registry) MustRegister(executor Executor) {
	if err := r.Register(executor); err != nil {
		panic(err)
	}
}

// Get retrieves an executor by node type.
func (r *Registry) Get(nodeType string) (Executor, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	executor, exists := r.executors[nodeType]
	return executor, exists
}

// Execute executes a request using the appropriate executor.
func (r *Registry) Execute(ctx context.Context, req *ExecuteRequest) (*ExecuteResponse, error) {
	executor, exists := r.Get(req.NodeType)
	if !exists {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("no executor found for node type: %s", req.NodeType),
				Type:    ErrorTypeNonRetryable,
			},
		}, nil
	}

	return executor.Execute(ctx, req)
}

// NodeTypes returns all registered node types.
func (r *Registry) NodeTypes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	types := make([]string, 0, len(r.executors))
	for nodeType := range r.executors {
		types = append(types, nodeType)
	}
	return types
}

// Count returns the number of registered executors.
func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.executors)
}

// DefaultRegistry is the global default registry.
var DefaultRegistry = NewRegistry()

// DefaultRegistryInit initializes the default registry with all built-in executors.
func DefaultRegistryInit() *Registry {
	registry := NewRegistry()

	// Register all built-in executors
	registry.MustRegister(NewHTTPExecutor())
	registry.MustRegister(NewCodeExecutor())
	registry.MustRegister(NewEmailExecutor())
	registry.MustRegister(NewConditionExecutor())
	registry.MustRegister(NewSlackExecutor())
	registry.MustRegister(NewDelayExecutor())
	registry.MustRegister(NewDatabaseExecutor())
	registry.MustRegister(NewAIExecutor())
	registry.MustRegister(NewWebhookExecutor())
	registry.MustRegister(NewTransformExecutor())
	registry.MustRegister(NewLoopExecutor())
	registry.MustRegister(NewDiscordExecutor())
	registry.MustRegister(NewTwilioExecutor())
	registry.MustRegister(NewStorageExecutor())
	registry.MustRegister(NewScriptExecutor())
	registry.MustRegister(NewOutputExecutor())
	registry.MustRegister(NewApprovalExecutor())
	registry.MustRegister(NewLogicConditionExecutor())
	registry.MustRegister(NewManualExecutor())
	registry.MustRegister(NewAliasExecutor("trigger_schedule", NewManualExecutor()))

	return registry
}

// TransformExecutor handles data transformation nodes.
type TransformExecutor struct{}

// TransformConfig represents the configuration for a transform node.
type TransformConfig struct {
	// Transformation type
	Type string `json:"type"` // map, filter, reduce, pick, omit, merge, flatten, group

	// For map/filter/reduce operations
	Expression string `json:"expression"`

	// For pick/omit
	Fields []string `json:"fields"`

	// For merge
	Sources []string `json:"sources"`

	// Input data (if not provided, uses req.Input)
	Data interface{} `json:"data"`
}

// NewTransformExecutor creates a new transform executor.
func NewTransformExecutor() *TransformExecutor {
	return &TransformExecutor{}
}

func (e *TransformExecutor) NodeType() string {
	return "transform"
}

func (e *TransformExecutor) Execute(ctx context.Context, req *ExecuteRequest) (*ExecuteResponse, error) {
	start := time.Now()
	logs := make([]LogEntry, 0)

	logs = append(logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "info",
		Message:   "starting transform operation",
	})

	// Parse transform configuration
	var config struct {
		Operation string      `json:"operation"`
		Field     string      `json:"field"`
		Value     interface{} `json:"value"`
		FromField string      `json:"from_field"`
		ToField   string      `json:"to_field"`
	}

	if err := json.Unmarshal(req.Config, &config); err != nil {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("failed to parse transform config: %v", err),
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	// Parse input data
	var inputData map[string]interface{}
	if err := json.Unmarshal(req.Input, &inputData); err != nil {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("failed to parse input data: %v", err),
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	// Perform transformation based on operation
	switch config.Operation {
	case "set":
		inputData[config.Field] = config.Value
		logs = append(logs, LogEntry{
			Timestamp: time.Now(),
			Level:     "info",
			Message:   fmt.Sprintf("set field '%s' to value '%v'", config.Field, config.Value),
		})

	case "rename":
		if val, exists := inputData[config.FromField]; exists {
			inputData[config.ToField] = val
			delete(inputData, config.FromField)
			logs = append(logs, LogEntry{
				Timestamp: time.Now(),
				Level:     "info",
				Message:   fmt.Sprintf("renamed field '%s' to '%s'", config.FromField, config.ToField),
			})
		}

	case "delete":
		delete(inputData, config.Field)
		logs = append(logs, LogEntry{
			Timestamp: time.Now(),
			Level:     "info",
			Message:   fmt.Sprintf("deleted field '%s'", config.Field),
		})

	case "filter":
		// Simple array filtering
		if arr, ok := inputData[config.Field].([]interface{}); ok {
			filtered := make([]interface{}, 0)
			for _, item := range arr {
				if item != nil {
					filtered = append(filtered, item)
				}
			}
			inputData[config.Field] = filtered
			logs = append(logs, LogEntry{
				Timestamp: time.Now(),
				Level:     "info",
				Message:   fmt.Sprintf("filtered array '%s', removed %d null items", config.Field, len(arr)-len(filtered)),
			})
		}

	default:
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("unsupported transform operation: %s", config.Operation),
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	// Marshal transformed data
	output, err := json.Marshal(inputData)
	if err != nil {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("failed to marshal output: %v", err),
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	logs = append(logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "info",
		Message:   "transform operation completed successfully",
	})

	return &ExecuteResponse{
		Output:   output,
		Logs:     logs,
		Duration: time.Since(start),
	}, nil
}

// LoopExecutor handles loop/iteration nodes.
type LoopExecutor struct{}

// LoopConfig represents the configuration for a loop node.
type LoopConfig struct {
	// Loop type
	Type string `json:"type"` // forEach, while, repeat, parallel

	// For forEach
	Collection string `json:"collection"` // JSONPath to collection
	ItemVar    string `json:"item_var"`   // Variable name for current item
	IndexVar   string `json:"index_var"`  // Variable name for index

	// For while
	Condition string `json:"condition"`

	// For repeat
	Count int `json:"count"`

	// For parallel
	MaxConcurrency int `json:"max_concurrency"`

	// Nested actions (node IDs to execute in loop)
	Actions []string `json:"actions"`
}

// NewLoopExecutor creates a new loop executor.
func NewLoopExecutor() *LoopExecutor {
	return &LoopExecutor{}
}

func (e *LoopExecutor) NodeType() string {
	return "loop"
}

func (e *LoopExecutor) Execute(ctx context.Context, req *ExecuteRequest) (*ExecuteResponse, error) {
	start := time.Now()
	logs := make([]LogEntry, 0)

	logs = append(logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "info",
		Message:   "starting loop operation",
	})

	// Parse loop configuration
	var config struct {
		ItemsField string `json:"items_field"`
		ItemAlias  string `json:"item_alias"`
		IndexAlias string `json:"index_alias"`
		MaxItems   int    `json:"max_items"`
	}

	if err := json.Unmarshal(req.Config, &config); err != nil {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("failed to parse loop config: %v", err),
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	// Set defaults
	if config.ItemAlias == "" {
		config.ItemAlias = "item"
	}
	if config.IndexAlias == "" {
		config.IndexAlias = "index"
	}
	if config.MaxItems == 0 {
		config.MaxItems = 100 // Safety limit
	}

	// Parse input data
	var inputData map[string]interface{}
	if err := json.Unmarshal(req.Input, &inputData); err != nil {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("failed to parse input data: %v", err),
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	// Get items to iterate over
	itemsInterface, exists := inputData[config.ItemsField]
	if !exists {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("field '%s' not found in input data", config.ItemsField),
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	items, ok := itemsInterface.([]interface{})
	if !ok {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("field '%s' is not an array", config.ItemsField),
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	// Limit items for safety
	if len(items) > config.MaxItems {
		items = items[:config.MaxItems]
		logs = append(logs, LogEntry{
			Timestamp: time.Now(),
			Level:     "warning",
			Message:   fmt.Sprintf("limited iteration to %d items (original: %d)", config.MaxItems, len(items)),
		})
	}

	// Process each item
	results := make([]interface{}, 0, len(items))
	for i, item := range items {
		// Create item context
		itemContext := make(map[string]interface{})
		for k, v := range inputData {
			itemContext[k] = v
		}
		itemContext[config.ItemAlias] = item
		itemContext[config.IndexAlias] = i

		// In a real implementation, this would trigger sub-workflow execution
		// For now, we'll just collect the processed items
		results = append(results, itemContext)

		logs = append(logs, LogEntry{
			Timestamp: time.Now(),
			Level:     "info",
			Message:   fmt.Sprintf("processed item %d/%d", i+1, len(items)),
		})
	}

	// Add results to output
	inputData["loop_results"] = results
	inputData["loop_count"] = len(results)

	// Marshal output data
	output, err := json.Marshal(inputData)
	if err != nil {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("failed to marshal output: %v", err),
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	logs = append(logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "info",
		Message:   fmt.Sprintf("loop completed successfully with %d iterations", len(results)),
	})

	return &ExecuteResponse{
		Output:   output,
		Logs:     logs,
		Duration: time.Since(start),
	}, nil
}
