package executor

import (
	"context"
)

// AliasExecutor wraps an existing executor under a different node type name.
// This allows the same executor logic to handle multiple node type names.
type AliasExecutor struct {
	aliasType string
	wrapped   Executor
}

// NewAliasExecutor creates an executor alias.
func NewAliasExecutor(aliasType string, wrapped Executor) *AliasExecutor {
	return &AliasExecutor{
		aliasType: aliasType,
		wrapped:   wrapped,
	}
}

func (e *AliasExecutor) NodeType() string {
	return e.aliasType
}

func (e *AliasExecutor) Execute(ctx context.Context, req *ExecuteRequest) (*ExecuteResponse, error) {
	return e.wrapped.Execute(ctx, req)
}

// LogicConditionExecutor is an alias for ConditionExecutor.
// Handles "logic_condition" node type using the same logic as "condition".
type LogicConditionExecutor struct {
	*ConditionExecutor
}

// NewLogicConditionExecutor creates a new logic_condition executor.
func NewLogicConditionExecutor() *LogicConditionExecutor {
	return &LogicConditionExecutor{
		ConditionExecutor: NewConditionExecutor(),
	}
}

func (e *LogicConditionExecutor) NodeType() string {
	return "logic_condition"
}
