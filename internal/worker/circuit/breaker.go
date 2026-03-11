package circuit

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrCircuitOpen = errors.New("circuit breaker is open")
)

// State represents circuit breaker state.
type State int

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// Config holds circuit breaker configuration.
type Config struct {
	FailureThreshold    int           // Number of failures before opening
	SuccessThreshold    int           // Successes needed in half-open to close
	HalfOpenRequests    int           // Max requests in half-open state
	OpenTimeout         time.Duration // Time to wait before half-open
	FailureRateWindow   time.Duration // Window for calculating failure rate
	MinRequestsInWindow int           // Min requests before calculating rate
}

// DefaultConfig returns default circuit breaker config.
func DefaultConfig() Config {
	return Config{
		FailureThreshold:    5,
		SuccessThreshold:    3,
		HalfOpenRequests:    3,
		OpenTimeout:         30 * time.Second,
		FailureRateWindow:   60 * time.Second,
		MinRequestsInWindow: 10,
	}
}

// Breaker is a circuit breaker implementation.
type Breaker struct {
	name   string
	config Config

	state           State
	failures        int
	successes       int
	requests        int
	lastFailure     time.Time
	lastStateChange time.Time

	// Sliding window for failure rate
	requestTimes []time.Time
	failureTimes []time.Time

	mu sync.RWMutex
}

// NewBreaker creates a new circuit breaker.
func NewBreaker(name string, config Config) *Breaker {
	return &Breaker{
		name:            name,
		config:          config,
		state:           StateClosed,
		lastStateChange: time.Now(),
		requestTimes:    make([]time.Time, 0),
		failureTimes:    make([]time.Time, 0),
	}
}

// Allow checks if a request is allowed.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()

	switch b.state {
	case StateClosed:
		return true

	case StateOpen:
		// Check if we should transition to half-open
		if now.Sub(b.lastStateChange) > b.config.OpenTimeout {
			b.transitionTo(StateHalfOpen)
			return true
		}
		return false

	case StateHalfOpen:
		// Allow limited requests
		if b.requests < b.config.HalfOpenRequests {
			b.requests++
			return true
		}
		return false
	}

	return false
}

// Execute executes a function with circuit breaker protection.
func (b *Breaker) Execute(fn func() error) error {
	if !b.Allow() {
		return ErrCircuitOpen
	}

	err := fn()
	if err != nil {
		b.RecordFailure()
		return err
	}

	b.RecordSuccess()
	return nil
}

// RecordSuccess records a successful request.
func (b *Breaker) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.requestTimes = append(b.requestTimes, time.Now())
	b.cleanupWindows()

	switch b.state {
	case StateHalfOpen:
		b.successes++
		if b.successes >= b.config.SuccessThreshold {
			b.transitionTo(StateClosed)
		}
	case StateClosed:
		b.failures = 0 // Reset consecutive failures
	}
}

// RecordFailure records a failed request.
func (b *Breaker) RecordFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	b.requestTimes = append(b.requestTimes, now)
	b.failureTimes = append(b.failureTimes, now)
	b.lastFailure = now
	b.cleanupWindows()

	switch b.state {
	case StateClosed:
		b.failures++
		if b.failures >= b.config.FailureThreshold {
			b.transitionTo(StateOpen)
		} else if b.shouldOpenByRate() {
			b.transitionTo(StateOpen)
		}
	case StateHalfOpen:
		b.transitionTo(StateOpen)
	}
}

// State returns the current state.
func (b *Breaker) State() State {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.state
}

// Metrics returns circuit breaker metrics.
func (b *Breaker) Metrics() BreakerMetrics {
	b.mu.RLock()
	defer b.mu.RUnlock()

	b.cleanupWindows()

	return BreakerMetrics{
		Name:            b.name,
		State:           b.state.String(),
		Failures:        b.failures,
		Successes:       b.successes,
		TotalRequests:   len(b.requestTimes),
		FailureRate:     b.calculateFailureRate(),
		LastFailure:     b.lastFailure,
		LastStateChange: b.lastStateChange,
	}
}

// BreakerMetrics holds circuit breaker metrics.
type BreakerMetrics struct {
	Name            string
	State           string
	Failures        int
	Successes       int
	TotalRequests   int
	FailureRate     float64
	LastFailure     time.Time
	LastStateChange time.Time
}

func (b *Breaker) transitionTo(state State) {
	b.state = state
	b.lastStateChange = time.Now()
	b.failures = 0
	b.successes = 0
	b.requests = 0
}

func (b *Breaker) cleanupWindows() {
	cutoff := time.Now().Add(-b.config.FailureRateWindow)

	// Clean request times
	newRequests := make([]time.Time, 0, len(b.requestTimes))
	for _, t := range b.requestTimes {
		if t.After(cutoff) {
			newRequests = append(newRequests, t)
		}
	}
	b.requestTimes = newRequests

	// Clean failure times
	newFailures := make([]time.Time, 0, len(b.failureTimes))
	for _, t := range b.failureTimes {
		if t.After(cutoff) {
			newFailures = append(newFailures, t)
		}
	}
	b.failureTimes = newFailures
}

func (b *Breaker) shouldOpenByRate() bool {
	if len(b.requestTimes) < b.config.MinRequestsInWindow {
		return false
	}

	rate := b.calculateFailureRate()
	return rate > 0.5 // Open if more than 50% failure rate
}

func (b *Breaker) calculateFailureRate() float64 {
	if len(b.requestTimes) == 0 {
		return 0
	}
	return float64(len(b.failureTimes)) / float64(len(b.requestTimes))
}

// BreakerRegistry manages multiple circuit breakers.
type BreakerRegistry struct {
	breakers map[string]*Breaker
	config   Config
	mu       sync.RWMutex
}

// NewBreakerRegistry creates a new breaker registry.
func NewBreakerRegistry(defaultConfig Config) *BreakerRegistry {
	return &BreakerRegistry{
		breakers: make(map[string]*Breaker),
		config:   defaultConfig,
	}
}

// Get gets or creates a circuit breaker by name.
func (r *BreakerRegistry) Get(name string) *Breaker {
	r.mu.RLock()
	if b, exists := r.breakers[name]; exists {
		r.mu.RUnlock()
		return b
	}
	r.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()

	// Double check
	if b, exists := r.breakers[name]; exists {
		return b
	}

	b := NewBreaker(name, r.config)
	r.breakers[name] = b
	return b
}

// List returns all breakers.
func (r *BreakerRegistry) List() []*Breaker {
	r.mu.RLock()
	defer r.mu.RUnlock()

	breakers := make([]*Breaker, 0, len(r.breakers))
	for _, b := range r.breakers {
		breakers = append(breakers, b)
	}
	return breakers
}

// AllMetrics returns metrics for all breakers.
func (r *BreakerRegistry) AllMetrics() []BreakerMetrics {
	r.mu.RLock()
	defer r.mu.RUnlock()

	metrics := make([]BreakerMetrics, 0, len(r.breakers))
	for _, b := range r.breakers {
		metrics = append(metrics, b.Metrics())
	}
	return metrics
}
