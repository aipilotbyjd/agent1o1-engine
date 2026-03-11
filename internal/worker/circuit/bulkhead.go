package circuit

import (
	"context"
	"sync"
	"time"
)

// Bulkhead provides resource isolation using the bulkhead pattern.
type Bulkhead struct {
	name           string
	maxConcurrency int
	maxWait        time.Duration

	current int
	waiting int
	sem     chan struct{}

	mu sync.Mutex
}

// BulkheadConfig holds bulkhead configuration.
type BulkheadConfig struct {
	MaxConcurrency int
	MaxWait        time.Duration
}

// DefaultBulkheadConfig returns default bulkhead config.
func DefaultBulkheadConfig() BulkheadConfig {
	return BulkheadConfig{
		MaxConcurrency: 10,
		MaxWait:        30 * time.Second,
	}
}

// NewBulkhead creates a new bulkhead.
func NewBulkhead(name string, config BulkheadConfig) *Bulkhead {
	return &Bulkhead{
		name:           name,
		maxConcurrency: config.MaxConcurrency,
		maxWait:        config.MaxWait,
		sem:            make(chan struct{}, config.MaxConcurrency),
	}
}

// Acquire acquires a slot in the bulkhead.
func (b *Bulkhead) Acquire(ctx context.Context) error {
	// Check if we can acquire immediately
	select {
	case b.sem <- struct{}{}:
		b.mu.Lock()
		b.current++
		b.mu.Unlock()
		return nil
	default:
	}

	// Wait with timeout
	waitCtx := ctx
	if b.maxWait > 0 {
		var cancel context.CancelFunc
		waitCtx, cancel = context.WithTimeout(ctx, b.maxWait)
		defer cancel()
	}

	b.mu.Lock()
	b.waiting++
	b.mu.Unlock()

	defer func() {
		b.mu.Lock()
		b.waiting--
		b.mu.Unlock()
	}()

	select {
	case b.sem <- struct{}{}:
		b.mu.Lock()
		b.current++
		b.mu.Unlock()
		return nil
	case <-waitCtx.Done():
		return waitCtx.Err()
	}
}

// Release releases a slot in the bulkhead.
func (b *Bulkhead) Release() {
	select {
	case <-b.sem:
		b.mu.Lock()
		b.current--
		b.mu.Unlock()
	default:
		// Should not happen, but don't panic
	}
}

// Execute executes a function within the bulkhead.
func (b *Bulkhead) Execute(ctx context.Context, fn func() error) error {
	if err := b.Acquire(ctx); err != nil {
		return err
	}
	defer b.Release()
	return fn()
}

// Metrics returns bulkhead metrics.
func (b *Bulkhead) Metrics() BulkheadMetrics {
	b.mu.Lock()
	defer b.mu.Unlock()

	return BulkheadMetrics{
		Name:           b.name,
		MaxConcurrency: b.maxConcurrency,
		Current:        b.current,
		Waiting:        b.waiting,
		Available:      b.maxConcurrency - b.current,
	}
}

// BulkheadMetrics holds bulkhead metrics.
type BulkheadMetrics struct {
	Name           string
	MaxConcurrency int
	Current        int
	Waiting        int
	Available      int
}

// BulkheadRegistry manages multiple bulkheads.
type BulkheadRegistry struct {
	bulkheads map[string]*Bulkhead
	config    BulkheadConfig
	mu        sync.RWMutex
}

// NewBulkheadRegistry creates a new bulkhead registry.
func NewBulkheadRegistry(config BulkheadConfig) *BulkheadRegistry {
	return &BulkheadRegistry{
		bulkheads: make(map[string]*Bulkhead),
		config:    config,
	}
}

// Get gets or creates a bulkhead by name.
func (r *BulkheadRegistry) Get(name string) *Bulkhead {
	r.mu.RLock()
	if b, exists := r.bulkheads[name]; exists {
		r.mu.RUnlock()
		return b
	}
	r.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()

	if b, exists := r.bulkheads[name]; exists {
		return b
	}

	b := NewBulkhead(name, r.config)
	r.bulkheads[name] = b
	return b
}
