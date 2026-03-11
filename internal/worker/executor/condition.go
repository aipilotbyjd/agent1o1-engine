package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// ConditionExecutor handles conditional logic nodes (if/else, switch).
type ConditionExecutor struct{}

// ConditionConfig represents the configuration for a condition node.
type ConditionConfig struct {
	// Mode can be "if", "switch", or "expression"
	Mode string `json:"mode"`

	// For "if" mode
	Conditions []Condition `json:"conditions"`

	// For "switch" mode
	SwitchValue   string       `json:"switch_value"`
	Cases         []SwitchCase `json:"cases"`
	DefaultOutput string       `json:"default_output"`

	// For "expression" mode - CEL-like expression
	Expression string `json:"expression"`
}

// Condition represents a single conditional check.
type Condition struct {
	Field    string      `json:"field"`    // JSONPath-like field selector
	Operator string      `json:"operator"` // eq, ne, gt, lt, gte, lte, contains, startsWith, endsWith, matches, in, empty, exists
	Value    interface{} `json:"value"`    // Value to compare against
	Output   string      `json:"output"`   // Which output port to use
}

// SwitchCase represents a case in a switch statement.
type SwitchCase struct {
	Value  interface{} `json:"value"`
	Output string      `json:"output"`
}

// ConditionResponse represents the result of a condition evaluation.
type ConditionResponse struct {
	Matched     bool         `json:"matched"`
	Output      string       `json:"output"`       // Which output branch to take
	MatchedRule int          `json:"matched_rule"` // Index of matched condition (-1 for default)
	EvalResults []EvalResult `json:"eval_results"`
}

// EvalResult represents the result of evaluating a single condition.
type EvalResult struct {
	Index   int    `json:"index"`
	Field   string `json:"field"`
	Result  bool   `json:"result"`
	Message string `json:"message"`
}

// NewConditionExecutor creates a new condition executor.
func NewConditionExecutor() *ConditionExecutor {
	return &ConditionExecutor{}
}

func (e *ConditionExecutor) NodeType() string {
	return "condition"
}

func (e *ConditionExecutor) Execute(ctx context.Context, req *ExecuteRequest) (*ExecuteResponse, error) {
	start := time.Now()
	logs := make([]LogEntry, 0)

	logs = append(logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "INFO",
		Message:   fmt.Sprintf("Starting condition evaluation for node %s", req.NodeID),
	})

	var config ConditionConfig
	if err := json.Unmarshal(req.Config, &config); err != nil {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("failed to parse condition config: %v", err),
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	// Parse input data
	var inputData map[string]interface{}
	if len(req.Input) > 0 {
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
	} else {
		inputData = make(map[string]interface{})
	}

	var response ConditionResponse
	var evalError error

	switch config.Mode {
	case "if", "":
		response, evalError = e.evaluateIfConditions(config.Conditions, inputData, &logs)
	case "switch":
		response, evalError = e.evaluateSwitch(config.SwitchValue, config.Cases, config.DefaultOutput, inputData, &logs)
	case "expression":
		response, evalError = e.evaluateExpression(config.Expression, inputData, &logs)
	default:
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("unknown condition mode: %s", config.Mode),
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	if evalError != nil {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: evalError.Error(),
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	logs = append(logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "INFO",
		Message:   fmt.Sprintf("Condition evaluation complete: matched=%v, output=%s", response.Matched, response.Output),
	})

	output, err := json.Marshal(response)
	if err != nil {
		return &ExecuteResponse{
			Error: &ExecutionError{
				Message: fmt.Sprintf("failed to marshal response: %v", err),
				Type:    ErrorTypeNonRetryable,
			},
			Logs:     logs,
			Duration: time.Since(start),
		}, nil
	}

	return &ExecuteResponse{
		Output:   output,
		Logs:     logs,
		Duration: time.Since(start),
	}, nil
}

func (e *ConditionExecutor) evaluateIfConditions(conditions []Condition, data map[string]interface{}, logs *[]LogEntry) (ConditionResponse, error) {
	response := ConditionResponse{
		MatchedRule: -1,
		EvalResults: make([]EvalResult, len(conditions)),
	}

	for i, cond := range conditions {
		fieldValue := getFieldValue(data, cond.Field)
		result, message := evaluateCondition(fieldValue, cond.Operator, cond.Value)

		response.EvalResults[i] = EvalResult{
			Index:   i,
			Field:   cond.Field,
			Result:  result,
			Message: message,
		}

		*logs = append(*logs, LogEntry{
			Timestamp: time.Now(),
			Level:     "DEBUG",
			Message:   fmt.Sprintf("Condition %d: %s %s %v => %v", i, cond.Field, cond.Operator, cond.Value, result),
		})

		if result {
			response.Matched = true
			response.Output = cond.Output
			response.MatchedRule = i
			return response, nil
		}
	}

	// No condition matched - use "else" or "false" output
	response.Output = "else"
	return response, nil
}

func (e *ConditionExecutor) evaluateSwitch(switchValue string, cases []SwitchCase, defaultOutput string, data map[string]interface{}, logs *[]LogEntry) (ConditionResponse, error) {
	response := ConditionResponse{
		MatchedRule: -1,
		EvalResults: make([]EvalResult, len(cases)),
	}

	actualValue := getFieldValue(data, switchValue)

	*logs = append(*logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "DEBUG",
		Message:   fmt.Sprintf("Switch value: %s = %v", switchValue, actualValue),
	})

	for i, c := range cases {
		matches := compareValues(actualValue, c.Value)
		response.EvalResults[i] = EvalResult{
			Index:   i,
			Field:   switchValue,
			Result:  matches,
			Message: fmt.Sprintf("case %v: %v", c.Value, matches),
		}

		if matches {
			response.Matched = true
			response.Output = c.Output
			response.MatchedRule = i
			return response, nil
		}
	}

	// Use default
	response.Output = defaultOutput
	if defaultOutput == "" {
		response.Output = "default"
	}
	return response, nil
}

func (e *ConditionExecutor) evaluateExpression(expr string, data map[string]interface{}, logs *[]LogEntry) (ConditionResponse, error) {
	response := ConditionResponse{
		MatchedRule: -1,
	}

	// Simple expression evaluation (basic boolean expressions)
	result, err := evaluateSimpleExpression(expr, data)
	if err != nil {
		return response, fmt.Errorf("expression evaluation failed: %w", err)
	}

	response.Matched = result
	if result {
		response.Output = "true"
	} else {
		response.Output = "false"
	}

	*logs = append(*logs, LogEntry{
		Timestamp: time.Now(),
		Level:     "DEBUG",
		Message:   fmt.Sprintf("Expression '%s' evaluated to %v", expr, result),
	})

	return response, nil
}

// getFieldValue extracts a value from nested data using dot notation.
func getFieldValue(data map[string]interface{}, field string) interface{} {
	if field == "" {
		return data
	}

	parts := strings.Split(field, ".")
	var current interface{} = data

	for _, part := range parts {
		switch v := current.(type) {
		case map[string]interface{}:
			current = v[part]
		case []interface{}:
			// Handle array index
			if idx, err := strconv.Atoi(part); err == nil && idx >= 0 && idx < len(v) {
				current = v[idx]
			} else {
				return nil
			}
		default:
			return nil
		}
	}

	return current
}

// evaluateCondition evaluates a single condition.
func evaluateCondition(fieldValue interface{}, operator string, compareValue interface{}) (result bool, message string) {
	switch operator {
	case "eq", "==", "equals":
		result := compareValues(fieldValue, compareValue)
		return result, fmt.Sprintf("%v == %v: %v", fieldValue, compareValue, result)

	case "ne", "!=", "not_equals":
		result := !compareValues(fieldValue, compareValue)
		return result, fmt.Sprintf("%v != %v: %v", fieldValue, compareValue, result)

	case "gt", ">":
		result := compareNumeric(fieldValue, compareValue) > 0
		return result, fmt.Sprintf("%v > %v: %v", fieldValue, compareValue, result)

	case "gte", ">=":
		result := compareNumeric(fieldValue, compareValue) >= 0
		return result, fmt.Sprintf("%v >= %v: %v", fieldValue, compareValue, result)

	case "lt", "<":
		result := compareNumeric(fieldValue, compareValue) < 0
		return result, fmt.Sprintf("%v < %v: %v", fieldValue, compareValue, result)

	case "lte", "<=":
		result := compareNumeric(fieldValue, compareValue) <= 0
		return result, fmt.Sprintf("%v <= %v: %v", fieldValue, compareValue, result)

	case "contains":
		result := strings.Contains(toString(fieldValue), toString(compareValue))
		return result, fmt.Sprintf("'%v' contains '%v': %v", fieldValue, compareValue, result)

	case "startsWith", "starts_with":
		result := strings.HasPrefix(toString(fieldValue), toString(compareValue))
		return result, fmt.Sprintf("'%v' starts with '%v': %v", fieldValue, compareValue, result)

	case "endsWith", "ends_with":
		result := strings.HasSuffix(toString(fieldValue), toString(compareValue))
		return result, fmt.Sprintf("'%v' ends with '%v': %v", fieldValue, compareValue, result)

	case "matches", "regex":
		pattern := toString(compareValue)
		re, err := regexp.Compile(pattern)
		if err != nil {
			return false, fmt.Sprintf("invalid regex pattern: %v", err)
		}
		result := re.MatchString(toString(fieldValue))
		return result, fmt.Sprintf("'%v' matches '%v': %v", fieldValue, pattern, result)

	case "in":
		if arr, ok := compareValue.([]interface{}); ok {
			for _, item := range arr {
				if compareValues(fieldValue, item) {
					return true, fmt.Sprintf("%v in %v: true", fieldValue, compareValue)
				}
			}
		}
		return false, fmt.Sprintf("%v in %v: false", fieldValue, compareValue)

	case "empty", "is_empty":
		result := isEmpty(fieldValue)
		return result, fmt.Sprintf("%v is empty: %v", fieldValue, result)

	case "not_empty", "is_not_empty":
		result := !isEmpty(fieldValue)
		return result, fmt.Sprintf("%v is not empty: %v", fieldValue, result)

	case "exists":
		result := fieldValue != nil
		return result, fmt.Sprintf("field exists: %v", result)

	case "not_exists":
		result := fieldValue == nil
		return result, fmt.Sprintf("field not exists: %v", result)

	default:
		return false, fmt.Sprintf("unknown operator: %s", operator)
	}
}

func compareValues(a, b interface{}) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return reflect.DeepEqual(a, b) || toString(a) == toString(b)
}

func compareNumeric(a, b interface{}) int {
	aNum := toFloat64(a)
	bNum := toFloat64(b)
	if aNum < bNum {
		return -1
	}
	if aNum > bNum {
		return 1
	}
	return 0
}

func toString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case fmt.Stringer:
		return val.String()
	default:
		return fmt.Sprintf("%v", v)
	}
}

func toFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int:
		return float64(val)
	case int32:
		return float64(val)
	case int64:
		return float64(val)
	case string:
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f
		}
	}
	return 0
}

func isEmpty(v interface{}) bool {
	if v == nil {
		return true
	}
	switch val := v.(type) {
	case string:
		return val == ""
	case []interface{}:
		return len(val) == 0
	case map[string]interface{}:
		return len(val) == 0
	}
	return false
}

func evaluateSimpleExpression(expr string, data map[string]interface{}) (bool, error) {
	// Simple expression parser for basic conditions
	// Supports: field == value, field != value, field > value, etc.
	// Also supports: AND (&&), OR (||)

	expr = strings.TrimSpace(expr)

	// Handle OR expressions
	if strings.Contains(expr, "||") {
		parts := strings.SplitN(expr, "||", 2)
		left, err := evaluateSimpleExpression(strings.TrimSpace(parts[0]), data)
		if err != nil {
			return false, err
		}
		if left {
			return true, nil
		}
		return evaluateSimpleExpression(strings.TrimSpace(parts[1]), data)
	}

	// Handle AND expressions
	if strings.Contains(expr, "&&") {
		parts := strings.SplitN(expr, "&&", 2)
		left, err := evaluateSimpleExpression(strings.TrimSpace(parts[0]), data)
		if err != nil {
			return false, err
		}
		if !left {
			return false, nil
		}
		return evaluateSimpleExpression(strings.TrimSpace(parts[1]), data)
	}

	// Parse simple comparison: field operator value
	operators := []string{"==", "!=", ">=", "<=", ">", "<"}
	for _, op := range operators {
		if strings.Contains(expr, op) {
			parts := strings.SplitN(expr, op, 2)
			if len(parts) == 2 {
				field := strings.TrimSpace(parts[0])
				valueStr := strings.TrimSpace(parts[1])

				fieldValue := getFieldValue(data, field)
				var compareValue interface{} = valueStr

				// Try to parse as number
				if f, err := strconv.ParseFloat(valueStr, 64); err == nil {
					compareValue = f
				} else {
					switch {
					case valueStr == "true":
						compareValue = true
					case valueStr == "false":
						compareValue = false
					case valueStr == "null" || valueStr == "nil":
						compareValue = nil
					default:
						// Remove quotes if present
						if len(valueStr) >= 2 && ((valueStr[0] == '"' && valueStr[len(valueStr)-1] == '"') ||
							(valueStr[0] == '\'' && valueStr[len(valueStr)-1] == '\'')) {
							compareValue = valueStr[1 : len(valueStr)-1]
						}
					}
				}

				result, _ := evaluateCondition(fieldValue, op, compareValue)
				return result, nil
			}
		}
	}

	return false, fmt.Errorf("unable to parse expression: %s", expr)
}
