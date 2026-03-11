package circuit

import (
	"errors"
	"testing"
	"time"
)

func TestBreaker_InitialState(t *testing.T) {
	b := NewBreaker("test", DefaultConfig())

	if b.State() != StateClosed {
		t.Errorf("Initial state = %v, want StateClosed", b.State())
	}

	if !b.Allow() {
		t.Error("Closed breaker should allow requests")
	}
}

func TestBreaker_OpenAfterFailures(t *testing.T) {
	cfg := Config{
		FailureThreshold:    3,
		SuccessThreshold:    1,
		HalfOpenRequests:    1,
		OpenTimeout:         time.Hour,
		FailureRateWindow:   time.Hour,
		MinRequestsInWindow: 100,
	}
	b := NewBreaker("test", cfg)

	// Record failures up to threshold
	for i := 0; i < cfg.FailureThreshold; i++ {
		if b.State() != StateClosed {
			t.Errorf("Should still be closed after %d failures", i)
		}
		b.RecordFailure()
	}

	if b.State() != StateOpen {
		t.Errorf("State = %v, want StateOpen after %d failures", b.State(), cfg.FailureThreshold)
	}

	if b.Allow() {
		t.Error("Open breaker should not allow requests")
	}
}

func TestBreaker_TransitionToHalfOpen(t *testing.T) {
	cfg := Config{
		FailureThreshold:    2,
		SuccessThreshold:    1,
		HalfOpenRequests:    1,
		OpenTimeout:         10 * time.Millisecond,
		FailureRateWindow:   time.Hour,
		MinRequestsInWindow: 100,
	}
	b := NewBreaker("test", cfg)

	// Open the breaker
	b.RecordFailure()
	b.RecordFailure()

	if b.State() != StateOpen {
		t.Fatalf("State = %v, want StateOpen", b.State())
	}

	// Wait for timeout
	time.Sleep(20 * time.Millisecond)

	// Next Allow should transition to half-open
	if !b.Allow() {
		t.Error("Should allow request after open timeout")
	}

	if b.State() != StateHalfOpen {
		t.Errorf("State = %v, want StateHalfOpen", b.State())
	}
}

func TestBreaker_CloseFromHalfOpen(t *testing.T) {
	cfg := Config{
		FailureThreshold:    2,
		SuccessThreshold:    2,
		HalfOpenRequests:    3,
		OpenTimeout:         10 * time.Millisecond,
		FailureRateWindow:   time.Hour,
		MinRequestsInWindow: 100,
	}
	b := NewBreaker("test", cfg)

	// Open the breaker
	b.RecordFailure()
	b.RecordFailure()

	// Wait and transition to half-open
	time.Sleep(20 * time.Millisecond)
	b.Allow()

	// Record successes to close
	b.RecordSuccess()
	if b.State() != StateHalfOpen {
		t.Errorf("Should still be half-open after 1 success")
	}

	b.RecordSuccess()
	if b.State() != StateClosed {
		t.Errorf("State = %v, want StateClosed after %d successes", b.State(), cfg.SuccessThreshold)
	}
}

func TestBreaker_OpenFromHalfOpenOnFailure(t *testing.T) {
	cfg := Config{
		FailureThreshold:    2,
		SuccessThreshold:    3,
		HalfOpenRequests:    5,
		OpenTimeout:         10 * time.Millisecond,
		FailureRateWindow:   time.Hour,
		MinRequestsInWindow: 100,
	}
	b := NewBreaker("test", cfg)

	// Open the breaker
	b.RecordFailure()
	b.RecordFailure()

	// Wait and transition to half-open
	time.Sleep(20 * time.Millisecond)
	b.Allow()

	// A failure in half-open should immediately open
	b.RecordFailure()

	if b.State() != StateOpen {
		t.Errorf("State = %v, want StateOpen after failure in half-open", b.State())
	}
}

func TestBreaker_Execute(t *testing.T) {
	b := NewBreaker("test", DefaultConfig())

	// Successful execution
	err := b.Execute(func() error {
		return nil
	})
	if err != nil {
		t.Errorf("Execute() error = %v", err)
	}

	// Failed execution
	expectedErr := errors.New("test error")
	err = b.Execute(func() error {
		return expectedErr
	})
	if err != expectedErr {
		t.Errorf("Execute() error = %v, want %v", err, expectedErr)
	}
}

func TestBreaker_ExecuteCircuitOpen(t *testing.T) {
	cfg := Config{
		FailureThreshold:    1,
		SuccessThreshold:    1,
		HalfOpenRequests:    1,
		OpenTimeout:         time.Hour,
		FailureRateWindow:   time.Hour,
		MinRequestsInWindow: 100,
	}
	b := NewBreaker("test", cfg)

	// Open the breaker
	b.RecordFailure()

	// Execute should return ErrCircuitOpen
	err := b.Execute(func() error {
		t.Error("Function should not be called when circuit is open")
		return nil
	})

	if err != ErrCircuitOpen {
		t.Errorf("Execute() error = %v, want ErrCircuitOpen", err)
	}
}

func TestBreaker_Metrics(t *testing.T) {
	b := NewBreaker("test-breaker", DefaultConfig())

	b.RecordSuccess()
	b.RecordFailure()
	b.RecordSuccess()

	metrics := b.Metrics()

	if metrics.Name != "test-breaker" {
		t.Errorf("Metrics.Name = %q, want %q", metrics.Name, "test-breaker")
	}

	if metrics.TotalRequests != 3 {
		t.Errorf("Metrics.TotalRequests = %d, want 3", metrics.TotalRequests)
	}
}

func TestBreakerRegistry_GetOrCreate(t *testing.T) {
	r := NewBreakerRegistry(DefaultConfig())

	b1 := r.Get("service-a")
	b1.RecordFailure()

	b2 := r.Get("service-a")

	// Should be the same breaker
	if b2.Metrics().Failures != 1 {
		t.Error("Registry should return same breaker")
	}
}

func TestBreakerRegistry_DifferentBreakers(t *testing.T) {
	r := NewBreakerRegistry(DefaultConfig())

	b1 := r.Get("service-a")
	b2 := r.Get("service-b")

	b1.RecordFailure()

	if b2.Metrics().Failures != 0 {
		t.Error("Different names should have separate breakers")
	}
}
