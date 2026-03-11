package circuit

import (
	"context"
	"errors"
	"time"
)

var (
	ErrTimeout = errors.New("execution timed out")
)

// Timeout provides timeout wrapper for operations.
type Timeout struct {
	defaultTimeout time.Duration
}

// NewTimeout creates a new timeout wrapper.
func NewTimeout(defaultTimeout time.Duration) *Timeout {
	return &Timeout{defaultTimeout: defaultTimeout}
}

// Execute executes a function with timeout.
func (t *Timeout) Execute(ctx context.Context, timeout time.Duration, fn func(context.Context) error) error {
	if timeout == 0 {
		timeout = t.defaultTimeout
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	done := make(chan error, 1)

	go func() {
		done <- fn(ctx)
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return ErrTimeout
		}
		return ctx.Err()
	}
}

// ExecuteWithResult executes a function with timeout and returns result.
func ExecuteWithResult[T any](ctx context.Context, timeout time.Duration, fn func(context.Context) (T, error)) (T, error) {
	var zero T

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	type result struct {
		value T
		err   error
	}

	done := make(chan result, 1)

	go func() {
		v, err := fn(ctx)
		done <- result{value: v, err: err}
	}()

	select {
	case r := <-done:
		return r.value, r.err
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return zero, ErrTimeout
		}
		return zero, ctx.Err()
	}
}

// ResilienceConfig holds all resilience configuration.
type ResilienceConfig struct {
	CircuitBreaker Config
	Bulkhead       BulkheadConfig
	DefaultTimeout time.Duration
}

// DefaultResilienceConfig returns default resilience config.
func DefaultResilienceConfig() ResilienceConfig {
	return ResilienceConfig{
		CircuitBreaker: DefaultConfig(),
		Bulkhead:       DefaultBulkheadConfig(),
		DefaultTimeout: 30 * time.Second,
	}
}

// Resilience combines circuit breaker, bulkhead, and timeout.
type Resilience struct {
	breaker  *Breaker
	bulkhead *Bulkhead
	timeout  *Timeout
}

// NewResilience creates a new resilience wrapper.
func NewResilience(name string, config ResilienceConfig) *Resilience {
	return &Resilience{
		breaker:  NewBreaker(name, config.CircuitBreaker),
		bulkhead: NewBulkhead(name, config.Bulkhead),
		timeout:  NewTimeout(config.DefaultTimeout),
	}
}

// Execute executes with all resilience patterns applied.
func (r *Resilience) Execute(ctx context.Context, timeout time.Duration, fn func(context.Context) error) error {
	// Check circuit breaker first
	if !r.breaker.Allow() {
		return ErrCircuitOpen
	}

	// Execute with bulkhead and timeout
	err := r.bulkhead.Execute(ctx, func() error {
		return r.timeout.Execute(ctx, timeout, fn)
	})

	// Record result in circuit breaker
	if err != nil {
		r.breaker.RecordFailure()
	} else {
		r.breaker.RecordSuccess()
	}

	return err
}

// Metrics returns combined resilience metrics.
func (r *Resilience) Metrics() ResilienceMetrics {
	return ResilienceMetrics{
		CircuitBreaker: r.breaker.Metrics(),
		Bulkhead:       r.bulkhead.Metrics(),
	}
}

// ResilienceMetrics holds combined resilience metrics.
type ResilienceMetrics struct {
	CircuitBreaker BreakerMetrics
	Bulkhead       BulkheadMetrics
}
