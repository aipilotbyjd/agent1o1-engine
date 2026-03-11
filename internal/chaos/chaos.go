package chaos

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

// FaultType represents a type of fault injection.
type FaultType int

const (
	FaultTypeLatency FaultType = iota
	FaultTypeError
	FaultTypeTimeout
	FaultTypePanic
	FaultTypeResourceExhaustion
)

// Fault represents a fault injection configuration.
type Fault struct {
	ID          string
	Type        FaultType
	Probability float64 // 0.0 to 1.0
	Duration    time.Duration
	Target      FaultTarget
	Enabled     bool
	StartTime   time.Time
	EndTime     time.Time
}

// FaultTarget specifies what the fault affects.
type FaultTarget struct {
	Service     string
	Method      string
	NodeType    string
	WorkflowID  string
	NamespaceID string
}

// Engine handles fault injection for chaos engineering.
type Engine struct {
	faults  map[string]*Fault
	logger  *slog.Logger
	enabled bool
	mu      sync.RWMutex
	rng     *rand.Rand
}

// Config holds chaos engine configuration.
type Config struct {
	Enabled bool
	Logger  *slog.Logger
	Seed    int64
}

// NewEngine creates a new chaos engine.
func NewEngine(config Config) *Engine {
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	seed := config.Seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}

	return &Engine{
		faults:  make(map[string]*Fault),
		logger:  config.Logger,
		enabled: config.Enabled,
		rng:     rand.New(rand.NewSource(seed)),
	}
}

// Enable enables the chaos engine.
func (e *Engine) Enable() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.enabled = true
	e.logger.Info("chaos engine enabled")
}

// Disable disables the chaos engine.
func (e *Engine) Disable() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.enabled = false
	e.logger.Info("chaos engine disabled")
}

// IsEnabled returns whether the chaos engine is enabled.
func (e *Engine) IsEnabled() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.enabled
}

// RegisterFault registers a fault.
func (e *Engine) RegisterFault(fault *Fault) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if fault.ID == "" {
		return errors.New("fault ID is required")
	}
	if fault.Probability < 0 || fault.Probability > 1 {
		return errors.New("probability must be between 0 and 1")
	}

	e.faults[fault.ID] = fault
	e.logger.Info("fault registered",
		slog.String("fault_id", fault.ID),
		slog.Int("type", int(fault.Type)),
		slog.Float64("probability", fault.Probability),
	)

	return nil
}

// UnregisterFault removes a fault.
func (e *Engine) UnregisterFault(faultID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.faults, faultID)
}

// EnableFault enables a specific fault.
func (e *Engine) EnableFault(faultID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if fault, exists := e.faults[faultID]; exists {
		fault.Enabled = true
	}
}

// DisableFault disables a specific fault.
func (e *Engine) DisableFault(faultID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if fault, exists := e.faults[faultID]; exists {
		fault.Enabled = false
	}
}

// ListFaults returns all registered faults.
func (e *Engine) ListFaults() []*Fault {
	e.mu.RLock()
	defer e.mu.RUnlock()

	faults := make([]*Fault, 0, len(e.faults))
	for _, f := range e.faults {
		faults = append(faults, f)
	}
	return faults
}

// Apply applies applicable faults to a context.
func (e *Engine) Apply(ctx context.Context, target FaultTarget) (context.Context, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.enabled {
		return ctx, nil
	}

	now := time.Now()

	for _, fault := range e.faults {
		if !fault.Enabled {
			continue
		}

		// Check time bounds
		if !fault.StartTime.IsZero() && now.Before(fault.StartTime) {
			continue
		}
		if !fault.EndTime.IsZero() && now.After(fault.EndTime) {
			continue
		}

		// Check if target matches
		if !e.targetMatches(fault.Target, target) {
			continue
		}

		// Check probability
		if e.rng.Float64() > fault.Probability {
			continue
		}

		// Apply fault
		err := e.applyFault(ctx, fault)
		if err != nil {
			e.logger.Info("fault applied",
				slog.String("fault_id", fault.ID),
				slog.Int("type", int(fault.Type)),
			)
			return ctx, err
		}
	}

	return ctx, nil
}

func (e *Engine) targetMatches(pattern, target FaultTarget) bool {
	if pattern.Service != "" && pattern.Service != target.Service {
		return false
	}
	if pattern.Method != "" && pattern.Method != target.Method {
		return false
	}
	if pattern.NodeType != "" && pattern.NodeType != target.NodeType {
		return false
	}
	if pattern.WorkflowID != "" && pattern.WorkflowID != target.WorkflowID {
		return false
	}
	if pattern.NamespaceID != "" && pattern.NamespaceID != target.NamespaceID {
		return false
	}
	return true
}

func (e *Engine) applyFault(ctx context.Context, fault *Fault) error {
	switch fault.Type {
	case FaultTypeLatency:
		time.Sleep(fault.Duration)
		return nil

	case FaultTypeError:
		return errors.New("chaos: injected error")

	case FaultTypeTimeout:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(fault.Duration):
			return context.DeadlineExceeded
		}

	case FaultTypePanic:
		panic("chaos: injected panic")

	case FaultTypeResourceExhaustion:
		// Simulate resource exhaustion by sleeping
		time.Sleep(fault.Duration)
		return errors.New("chaos: resource exhaustion")
	}

	return nil
}

// InjectLatency creates a latency injection fault.
func InjectLatency(id string, target FaultTarget, duration time.Duration, probability float64) *Fault {
	return &Fault{
		ID:          id,
		Type:        FaultTypeLatency,
		Duration:    duration,
		Probability: probability,
		Target:      target,
		Enabled:     true,
	}
}

// InjectError creates an error injection fault.
func InjectError(id string, target FaultTarget, probability float64) *Fault {
	return &Fault{
		ID:          id,
		Type:        FaultTypeError,
		Probability: probability,
		Target:      target,
		Enabled:     true,
	}
}

// InjectTimeout creates a timeout injection fault.
func InjectTimeout(id string, target FaultTarget, duration time.Duration, probability float64) *Fault {
	return &Fault{
		ID:          id,
		Type:        FaultTypeTimeout,
		Duration:    duration,
		Probability: probability,
		Target:      target,
		Enabled:     true,
	}
}

// Middleware returns a middleware that applies chaos faults.
func (e *Engine) Middleware(next func(ctx context.Context) error) func(ctx context.Context, target FaultTarget) error {
	return func(ctx context.Context, target FaultTarget) error {
		ctx, err := e.Apply(ctx, target)
		if err != nil {
			return err
		}
		return next(ctx)
	}
}
