package validator

import (
	"errors"
	"fmt"
	"regexp"
	"time"
	"unicode/utf8"

	"github.com/linkflow/engine/internal/frontend"
)

const (
	maxWorkflowIDLength   = 1000
	maxTaskQueueLength    = 1000
	maxNamespaceLength    = 256
	maxWorkflowTypeLength = 1000
	maxSignalNameLength   = 256
	maxQueryTypeLength    = 256
)

var (
	ErrWorkflowIDEmpty     = errors.New("workflow id is required")
	ErrWorkflowIDTooLong   = errors.New("workflow id exceeds maximum length")
	ErrWorkflowIDInvalid   = errors.New("workflow id contains invalid characters")
	ErrTaskQueueEmpty      = errors.New("task queue is required")
	ErrTaskQueueTooLong    = errors.New("task queue exceeds maximum length")
	ErrTaskQueueInvalid    = errors.New("task queue contains invalid characters")
	ErrNamespaceEmpty      = errors.New("namespace is required")
	ErrNamespaceTooLong    = errors.New("namespace exceeds maximum length")
	ErrNamespaceInvalid    = errors.New("namespace contains invalid characters")
	ErrWorkflowTypeEmpty   = errors.New("workflow type is required")
	ErrWorkflowTypeTooLong = errors.New("workflow type exceeds maximum length")
	ErrSignalNameEmpty     = errors.New("signal name is required")
	ErrSignalNameTooLong   = errors.New("signal name exceeds maximum length")
	ErrQueryTypeEmpty      = errors.New("query type is required")
	ErrQueryTypeTooLong    = errors.New("query type exceeds maximum length")
	ErrInvalidTimeout      = errors.New("timeout must be positive")
	ErrInvalidRetryPolicy  = errors.New("invalid retry policy")
)

var (
	validIDPattern        = regexp.MustCompile(`^[a-zA-Z0-9_\-\./:]+$`)
	validNamespacePattern = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_\-]*$`)
)

func ValidateStartWorkflowRequest(req *frontend.StartWorkflowExecutionRequest) error {
	if err := ValidateNamespace(req.Namespace); err != nil {
		return fmt.Errorf("namespace: %w", err)
	}

	if err := ValidateWorkflowID(req.WorkflowID); err != nil {
		return fmt.Errorf("workflow_id: %w", err)
	}

	if err := ValidateWorkflowType(req.WorkflowType); err != nil {
		return fmt.Errorf("workflow_type: %w", err)
	}

	if err := ValidateTaskQueue(req.TaskQueue); err != nil {
		return fmt.Errorf("task_queue: %w", err)
	}

	if req.WorkflowExecutionTimeout < 0 {
		return fmt.Errorf("workflow_execution_timeout: %w", ErrInvalidTimeout)
	}

	if req.WorkflowRunTimeout < 0 {
		return fmt.Errorf("workflow_run_timeout: %w", ErrInvalidTimeout)
	}

	if req.WorkflowTaskTimeout < 0 {
		return fmt.Errorf("workflow_task_timeout: %w", ErrInvalidTimeout)
	}

	if req.RetryPolicy != nil {
		if err := ValidateRetryPolicy(req.RetryPolicy); err != nil {
			return fmt.Errorf("retry_policy: %w", err)
		}
	}

	return nil
}

func ValidateSignalWorkflowRequest(req *frontend.SignalWorkflowExecutionRequest) error {
	if err := ValidateNamespace(req.Namespace); err != nil {
		return fmt.Errorf("namespace: %w", err)
	}

	if err := ValidateWorkflowID(req.WorkflowID); err != nil {
		return fmt.Errorf("workflow_id: %w", err)
	}

	if err := ValidateSignalName(req.SignalName); err != nil {
		return fmt.Errorf("signal_name: %w", err)
	}

	return nil
}

func ValidateTerminateWorkflowRequest(req *frontend.TerminateWorkflowExecutionRequest) error {
	if err := ValidateNamespace(req.Namespace); err != nil {
		return fmt.Errorf("namespace: %w", err)
	}

	if err := ValidateWorkflowID(req.WorkflowID); err != nil {
		return fmt.Errorf("workflow_id: %w", err)
	}

	return nil
}

func ValidateQueryWorkflowRequest(req *frontend.QueryWorkflowRequest) error {
	if err := ValidateNamespace(req.Namespace); err != nil {
		return fmt.Errorf("namespace: %w", err)
	}

	if err := ValidateWorkflowID(req.WorkflowID); err != nil {
		return fmt.Errorf("workflow_id: %w", err)
	}

	if err := ValidateQueryType(req.QueryType); err != nil {
		return fmt.Errorf("query_type: %w", err)
	}

	return nil
}

func ValidateGetExecutionRequest(req *frontend.GetExecutionRequest) error {
	if err := ValidateNamespace(req.Namespace); err != nil {
		return fmt.Errorf("namespace: %w", err)
	}

	if err := ValidateWorkflowID(req.WorkflowID); err != nil {
		return fmt.Errorf("workflow_id: %w", err)
	}

	return nil
}

func ValidateListExecutionsRequest(req *frontend.ListExecutionsRequest) error {
	if err := ValidateNamespace(req.Namespace); err != nil {
		return fmt.Errorf("namespace: %w", err)
	}

	if req.PageSize < 0 {
		return errors.New("page_size must be non-negative")
	}

	return nil
}

func ValidateDescribeExecutionRequest(req *frontend.DescribeExecutionRequest) error {
	if err := ValidateNamespace(req.Namespace); err != nil {
		return fmt.Errorf("namespace: %w", err)
	}

	if err := ValidateWorkflowID(req.WorkflowID); err != nil {
		return fmt.Errorf("workflow_id: %w", err)
	}

	return nil
}

func ValidateWorkflowID(id string) error {
	if id == "" {
		return ErrWorkflowIDEmpty
	}

	if utf8.RuneCountInString(id) > maxWorkflowIDLength {
		return ErrWorkflowIDTooLong
	}

	if !validIDPattern.MatchString(id) {
		return ErrWorkflowIDInvalid
	}

	return nil
}

func ValidateTaskQueue(name string) error {
	if name == "" {
		return ErrTaskQueueEmpty
	}

	if utf8.RuneCountInString(name) > maxTaskQueueLength {
		return ErrTaskQueueTooLong
	}

	if !validIDPattern.MatchString(name) {
		return ErrTaskQueueInvalid
	}

	return nil
}

func ValidateNamespace(name string) error {
	if name == "" {
		return ErrNamespaceEmpty
	}

	if utf8.RuneCountInString(name) > maxNamespaceLength {
		return ErrNamespaceTooLong
	}

	if !validNamespacePattern.MatchString(name) {
		return ErrNamespaceInvalid
	}

	return nil
}

func ValidateWorkflowType(wfType string) error {
	if wfType == "" {
		return ErrWorkflowTypeEmpty
	}

	if utf8.RuneCountInString(wfType) > maxWorkflowTypeLength {
		return ErrWorkflowTypeTooLong
	}

	return nil
}

func ValidateSignalName(name string) error {
	if name == "" {
		return ErrSignalNameEmpty
	}

	if utf8.RuneCountInString(name) > maxSignalNameLength {
		return ErrSignalNameTooLong
	}

	return nil
}

func ValidateQueryType(queryType string) error {
	if queryType == "" {
		return ErrQueryTypeEmpty
	}

	if utf8.RuneCountInString(queryType) > maxQueryTypeLength {
		return ErrQueryTypeTooLong
	}

	return nil
}

func ValidateRetryPolicy(policy *frontend.RetryPolicy) error {
	if policy.InitialInterval < 0 {
		return fmt.Errorf("%w: initial interval must be non-negative", ErrInvalidRetryPolicy)
	}

	if policy.BackoffCoefficient < 1 {
		return fmt.Errorf("%w: backoff coefficient must be >= 1", ErrInvalidRetryPolicy)
	}

	if policy.MaximumInterval < 0 {
		return fmt.Errorf("%w: maximum interval must be non-negative", ErrInvalidRetryPolicy)
	}

	if policy.MaximumInterval > 0 && policy.InitialInterval > 0 && policy.MaximumInterval < policy.InitialInterval {
		return fmt.Errorf("%w: maximum interval must be >= initial interval", ErrInvalidRetryPolicy)
	}

	if policy.MaximumAttempts < 0 {
		return fmt.Errorf("%w: maximum attempts must be non-negative", ErrInvalidRetryPolicy)
	}

	return nil
}

func ValidateTimeout(d time.Duration, fieldName string) error {
	if d < 0 {
		return fmt.Errorf("%s: %w", fieldName, ErrInvalidTimeout)
	}
	return nil
}
