package expression

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

var (
	ErrInvalidExpression = errors.New("invalid expression")
	ErrUnsupportedType   = errors.New("unsupported type")
	ErrPathNotFound      = errors.New("path not found")
)

// Engine evaluates expressions against data.
type Engine struct {
	functions map[string]Function
}

// Function represents a custom function.
type Function func(args ...interface{}) (interface{}, error)

// NewEngine creates a new expression engine.
func NewEngine() *Engine {
	e := &Engine{
		functions: make(map[string]Function),
	}
	e.registerBuiltins()
	return e
}

// RegisterFunction registers a custom function.
func (e *Engine) RegisterFunction(name string, fn Function) {
	e.functions[name] = fn
}

// Evaluate evaluates an expression against data.
func (e *Engine) Evaluate(expr string, data interface{}) (interface{}, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, ErrInvalidExpression
	}

	// Handle different expression types
	if strings.HasPrefix(expr, "$.") || strings.HasPrefix(expr, "$[") {
		// JSONPath expression
		return e.evaluateJSONPath(expr, data)
	}

	if strings.Contains(expr, "{{") && strings.Contains(expr, "}}") {
		// Template expression
		return e.evaluateTemplate(expr, data)
	}

	// Check if it's a comparison or logical expression
	if containsOperator(expr) {
		return e.evaluateComparison(expr, data)
	}

	// Simple path expression
	return e.evaluatePath(expr, data)
}

// EvaluateBool evaluates an expression and returns a boolean.
func (e *Engine) EvaluateBool(expr string, data interface{}) (bool, error) {
	result, err := e.Evaluate(expr, data)
	if err != nil {
		return false, err
	}

	switch v := result.(type) {
	case bool:
		return v, nil
	case string:
		return v != "", nil
	case float64:
		return v != 0, nil
	case int:
		return v != 0, nil
	case nil:
		return false, nil
	default:
		return true, nil
	}
}

// evaluateJSONPath evaluates a JSONPath expression.
func (e *Engine) evaluateJSONPath(expr string, data interface{}) (interface{}, error) {
	// Remove leading $
	path := strings.TrimPrefix(expr, "$")

	return e.resolvePath(path, data)
}

// evaluatePath evaluates a simple path expression.
func (e *Engine) evaluatePath(expr string, data interface{}) (interface{}, error) {
	return e.resolvePath("."+expr, data)
}

// resolvePath resolves a path in the data.
func (e *Engine) resolvePath(path string, data interface{}) (interface{}, error) {
	if path == "" || path == "." {
		return data, nil
	}

	current := data
	parts := parsePath(path)

	for _, part := range parts {
		if part == "" {
			continue
		}

		var err error
		current, err = e.resolvePathPart(current, part)
		if err != nil {
			return nil, err
		}
	}

	return current, nil
}

func (e *Engine) resolvePathPart(data interface{}, part string) (interface{}, error) {
	// Handle array index
	if strings.HasPrefix(part, "[") && strings.HasSuffix(part, "]") {
		indexStr := part[1 : len(part)-1]

		// Handle wildcard
		if indexStr == "*" {
			return data, nil
		}

		// Handle filters
		if strings.HasPrefix(indexStr, "?") {
			return e.applyFilter(data, indexStr[1:])
		}

		// Handle numeric index
		index, err := strconv.Atoi(indexStr)
		if err != nil {
			// Try as a quoted string key
			if strings.HasPrefix(indexStr, "'") && strings.HasSuffix(indexStr, "'") {
				key := indexStr[1 : len(indexStr)-1]
				return e.resolvePathPart(data, key)
			}
			return nil, fmt.Errorf("invalid index: %s", indexStr)
		}

		// Get from array
		switch v := data.(type) {
		case []interface{}:
			if index < 0 {
				index = len(v) + index
			}
			if index < 0 || index >= len(v) {
				return nil, ErrPathNotFound
			}
			return v[index], nil
		case []map[string]interface{}:
			if index < 0 {
				index = len(v) + index
			}
			if index < 0 || index >= len(v) {
				return nil, ErrPathNotFound
			}
			return v[index], nil
		default:
			return nil, fmt.Errorf("cannot index into %T", data)
		}
	}

	// Handle object key
	switch v := data.(type) {
	case map[string]interface{}:
		val, exists := v[part]
		if !exists {
			return nil, ErrPathNotFound
		}
		return val, nil
	case map[string]string:
		val, exists := v[part]
		if !exists {
			return nil, ErrPathNotFound
		}
		return val, nil
	default:
		// Try reflection for struct fields
		rv := reflect.ValueOf(data)
		if rv.Kind() == reflect.Ptr {
			rv = rv.Elem()
		}
		if rv.Kind() == reflect.Struct {
			field := rv.FieldByName(part)
			if field.IsValid() {
				return field.Interface(), nil
			}
			// Try case-insensitive
			for i := 0; i < rv.NumField(); i++ {
				if strings.EqualFold(rv.Type().Field(i).Name, part) {
					return rv.Field(i).Interface(), nil
				}
			}
		}
		return nil, fmt.Errorf("cannot access field %s on %T", part, data)
	}
}

func (e *Engine) applyFilter(data interface{}, filter string) (interface{}, error) {
	arr, ok := data.([]interface{})
	if !ok {
		return nil, fmt.Errorf("filter can only be applied to arrays")
	}

	// Parse filter expression (simplified)
	// Format: (@.field == value) or (@.field > value)
	filter = strings.TrimPrefix(filter, "(")
	filter = strings.TrimSuffix(filter, ")")

	var results []interface{}
	for _, item := range arr {
		match, err := e.evaluateFilterCondition(filter, item)
		if err != nil {
			continue
		}
		if match {
			results = append(results, item)
		}
	}

	return results, nil
}

func (e *Engine) evaluateFilterCondition(condition string, data interface{}) (bool, error) {
	// Replace @ with the actual data reference
	condition = strings.ReplaceAll(condition, "@", "$")
	result, err := e.Evaluate(condition, data)
	if err != nil {
		return false, err
	}

	b, ok := result.(bool)
	return ok && b, nil
}

// evaluateTemplate evaluates a template expression.
func (e *Engine) evaluateTemplate(template string, data interface{}) (interface{}, error) {
	result := template

	// Find all {{ ... }} patterns
	re := regexp.MustCompile(`\{\{([^}]+)\}\}`)
	matches := re.FindAllStringSubmatch(template, -1)

	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		expr := strings.TrimSpace(match[1])
		val, err := e.Evaluate(expr, data)
		if err != nil {
			val = ""
		}
		result = strings.Replace(result, match[0], fmt.Sprintf("%v", val), 1)
	}

	// If the entire template was a single expression, return the typed value
	if len(matches) == 1 && matches[0][0] == template {
		return e.Evaluate(strings.TrimSpace(matches[0][1]), data)
	}

	return result, nil
}

// evaluateComparison evaluates a comparison expression.
func (e *Engine) evaluateComparison(expr string, data interface{}) (interface{}, error) {
	// Handle AND/OR
	if idx := strings.Index(strings.ToUpper(expr), " AND "); idx > 0 {
		left := strings.TrimSpace(expr[:idx])
		right := strings.TrimSpace(expr[idx+5:])

		leftResult, err := e.EvaluateBool(left, data)
		if err != nil {
			return false, err
		}
		if !leftResult {
			return false, nil
		}
		return e.EvaluateBool(right, data)
	}

	if idx := strings.Index(strings.ToUpper(expr), " OR "); idx > 0 {
		left := strings.TrimSpace(expr[:idx])
		right := strings.TrimSpace(expr[idx+4:])

		leftResult, err := e.EvaluateBool(left, data)
		if err == nil && leftResult {
			return true, nil
		}
		return e.EvaluateBool(right, data)
	}

	// Handle comparison operators
	operators := []struct {
		op   string
		eval func(left, right interface{}) bool
	}{
		{"===", func(l, r interface{}) bool { return reflect.DeepEqual(l, r) }},
		{"!==", func(l, r interface{}) bool { return !reflect.DeepEqual(l, r) }},
		{"==", func(l, r interface{}) bool { return compareEqual(l, r) }},
		{"!=", func(l, r interface{}) bool { return !compareEqual(l, r) }},
		{">=", func(l, r interface{}) bool { return compareNum(l, r) >= 0 }},
		{"<=", func(l, r interface{}) bool { return compareNum(l, r) <= 0 }},
		{">", func(l, r interface{}) bool { return compareNum(l, r) > 0 }},
		{"<", func(l, r interface{}) bool { return compareNum(l, r) < 0 }},
	}

	for _, op := range operators {
		if idx := strings.Index(expr, op.op); idx > 0 {
			left := strings.TrimSpace(expr[:idx])
			right := strings.TrimSpace(expr[idx+len(op.op):])

			leftVal, err := e.evaluateOperand(left, data)
			if err != nil {
				return nil, err
			}
			rightVal, err := e.evaluateOperand(right, data)
			if err != nil {
				return nil, err
			}

			return op.eval(leftVal, rightVal), nil
		}
	}

	return nil, ErrInvalidExpression
}

func (e *Engine) evaluateOperand(operand string, data interface{}) (interface{}, error) {
	operand = strings.TrimSpace(operand)

	// Check for string literal
	if (strings.HasPrefix(operand, "'") && strings.HasSuffix(operand, "'")) ||
		(strings.HasPrefix(operand, "\"") && strings.HasSuffix(operand, "\"")) {
		return operand[1 : len(operand)-1], nil
	}

	// Check for number
	if num, err := strconv.ParseFloat(operand, 64); err == nil {
		return num, nil
	}

	// Check for boolean
	if operand == "true" {
		return true, nil
	}
	if operand == "false" {
		return false, nil
	}

	// Check for null
	if operand == "null" || operand == "nil" {
		return nil, nil
	}

	// Evaluate as path
	return e.Evaluate(operand, data)
}

func (e *Engine) registerBuiltins() {
	e.functions["len"] = func(args ...interface{}) (interface{}, error) {
		if len(args) != 1 {
			return nil, errors.New("len requires exactly 1 argument")
		}
		rv := reflect.ValueOf(args[0])
		switch rv.Kind() {
		case reflect.String, reflect.Slice, reflect.Array, reflect.Map:
			return rv.Len(), nil
		default:
			return nil, ErrUnsupportedType
		}
	}

	e.functions["upper"] = func(args ...interface{}) (interface{}, error) {
		if len(args) != 1 {
			return nil, errors.New("upper requires exactly 1 argument")
		}
		s, ok := args[0].(string)
		if !ok {
			return nil, ErrUnsupportedType
		}
		return strings.ToUpper(s), nil
	}

	e.functions["lower"] = func(args ...interface{}) (interface{}, error) {
		if len(args) != 1 {
			return nil, errors.New("lower requires exactly 1 argument")
		}
		s, ok := args[0].(string)
		if !ok {
			return nil, ErrUnsupportedType
		}
		return strings.ToLower(s), nil
	}

	e.functions["trim"] = func(args ...interface{}) (interface{}, error) {
		if len(args) != 1 {
			return nil, errors.New("trim requires exactly 1 argument")
		}
		s, ok := args[0].(string)
		if !ok {
			return nil, ErrUnsupportedType
		}
		return strings.TrimSpace(s), nil
	}

	e.functions["contains"] = func(args ...interface{}) (interface{}, error) {
		if len(args) != 2 {
			return nil, errors.New("contains requires exactly 2 arguments")
		}
		s, ok1 := args[0].(string)
		substr, ok2 := args[1].(string)
		if !ok1 || !ok2 {
			return nil, ErrUnsupportedType
		}
		return strings.Contains(s, substr), nil
	}

	e.functions["startsWith"] = func(args ...interface{}) (interface{}, error) {
		if len(args) != 2 {
			return nil, errors.New("startsWith requires exactly 2 arguments")
		}
		s, ok1 := args[0].(string)
		prefix, ok2 := args[1].(string)
		if !ok1 || !ok2 {
			return nil, ErrUnsupportedType
		}
		return strings.HasPrefix(s, prefix), nil
	}

	e.functions["endsWith"] = func(args ...interface{}) (interface{}, error) {
		if len(args) != 2 {
			return nil, errors.New("endsWith requires exactly 2 arguments")
		}
		s, ok1 := args[0].(string)
		suffix, ok2 := args[1].(string)
		if !ok1 || !ok2 {
			return nil, ErrUnsupportedType
		}
		return strings.HasSuffix(s, suffix), nil
	}

	e.functions["split"] = func(args ...interface{}) (interface{}, error) {
		if len(args) != 2 {
			return nil, errors.New("split requires exactly 2 arguments")
		}
		s, ok1 := args[0].(string)
		sep, ok2 := args[1].(string)
		if !ok1 || !ok2 {
			return nil, ErrUnsupportedType
		}
		parts := strings.Split(s, sep)
		result := make([]interface{}, len(parts))
		for i, p := range parts {
			result[i] = p
		}
		return result, nil
	}

	e.functions["join"] = func(args ...interface{}) (interface{}, error) {
		if len(args) != 2 {
			return nil, errors.New("join requires exactly 2 arguments")
		}
		arr, ok := args[0].([]interface{})
		sep, ok2 := args[1].(string)
		if !ok || !ok2 {
			return nil, ErrUnsupportedType
		}
		strs := make([]string, len(arr))
		for i, v := range arr {
			strs[i] = fmt.Sprintf("%v", v)
		}
		return strings.Join(strs, sep), nil
	}

	e.functions["toJson"] = func(args ...interface{}) (interface{}, error) {
		if len(args) != 1 {
			return nil, errors.New("toJson requires exactly 1 argument")
		}
		data, err := json.Marshal(args[0])
		if err != nil {
			return nil, err
		}
		return string(data), nil
	}

	e.functions["fromJson"] = func(args ...interface{}) (interface{}, error) {
		if len(args) != 1 {
			return nil, errors.New("fromJson requires exactly 1 argument")
		}
		s, ok := args[0].(string)
		if !ok {
			return nil, ErrUnsupportedType
		}
		var result interface{}
		if err := json.Unmarshal([]byte(s), &result); err != nil {
			return nil, err
		}
		return result, nil
	}
}

func parsePath(path string) []string {
	var parts []string
	var current strings.Builder
	inBracket := 0

	for _, ch := range path {
		switch ch {
		case '.':
			if inBracket == 0 {
				if current.Len() > 0 {
					parts = append(parts, current.String())
					current.Reset()
				}
				continue
			}
		case '[':
			if inBracket == 0 && current.Len() > 0 {
				parts = append(parts, current.String())
				current.Reset()
			}
			inBracket++
		case ']':
			inBracket--
			if inBracket == 0 {
				current.WriteRune(ch)
				parts = append(parts, current.String())
				current.Reset()
				continue
			}
		}
		current.WriteRune(ch)
	}

	if current.Len() > 0 {
		parts = append(parts, current.String())
	}

	return parts
}

func containsOperator(expr string) bool {
	operators := []string{"===", "!==", "==", "!=", ">=", "<=", ">", "<", " AND ", " OR ", " and ", " or "}
	for _, op := range operators {
		if strings.Contains(expr, op) {
			return true
		}
	}
	return false
}

func compareEqual(a, b interface{}) bool {
	// Convert to comparable types
	switch av := a.(type) {
	case string:
		if bv, ok := b.(string); ok {
			return av == bv
		}
		return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
	case float64:
		return toFloat(a) == toFloat(b)
	case int:
		return toFloat(a) == toFloat(b)
	case bool:
		if bv, ok := b.(bool); ok {
			return av == bv
		}
		return false
	}
	return reflect.DeepEqual(a, b)
}

func compareNum(a, b interface{}) int {
	af := toFloat(a)
	bf := toFloat(b)
	if af < bf {
		return -1
	}
	if af > bf {
		return 1
	}
	return 0
}

func toFloat(v interface{}) float64 {
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
