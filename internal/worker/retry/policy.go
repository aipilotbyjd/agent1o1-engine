package retry

import (
	"slices"
	"time"
)

type Policy struct {
	InitialInterval    time.Duration
	BackoffCoefficient float64
	MaximumInterval    time.Duration
	MaximumAttempts    int32
	NonRetryableErrors []string
}

func DefaultPolicy() *Policy {
	return &Policy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2.0,
		MaximumInterval:    time.Minute,
		MaximumAttempts:    3,
		NonRetryableErrors: []string{},
	}
}

func (p *Policy) NextRetryDelay(attempt int32) time.Duration {
	return CalculateBackoff(p, attempt)
}

func (p *Policy) ShouldRetry(attempt int32, errorType, errorMessage string) bool {
	if attempt >= p.MaximumAttempts {
		return false
	}

	if errorType == "NON_RETRYABLE" || errorType == "TIMEOUT" {
		return false
	}

	if slices.Contains(p.NonRetryableErrors, errorMessage) {
		return false
	}

	return true
}

func (p *Policy) WithInitialInterval(d time.Duration) *Policy {
	p.InitialInterval = d
	return p
}

func (p *Policy) WithBackoffCoefficient(c float64) *Policy {
	p.BackoffCoefficient = c
	return p
}

func (p *Policy) WithMaximumInterval(d time.Duration) *Policy {
	p.MaximumInterval = d
	return p
}

func (p *Policy) WithMaximumAttempts(n int32) *Policy {
	p.MaximumAttempts = n
	return p
}

func (p *Policy) WithNonRetryableErrors(errors []string) *Policy {
	p.NonRetryableErrors = errors
	return p
}
