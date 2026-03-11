package engine

import (
	"errors"
	"log/slog"
	"sync/atomic"
)

// ErrBackpressure is returned when a task is rejected due to queue backpressure.
var ErrBackpressure = errors.New("backpressure: queue depth exceeds hard limit")

// BackpressureState indicates the current pressure level of a queue.
type BackpressureState int

const (
	BackpressureNormal   BackpressureState = iota
	BackpressureWarning                    // queue depth > soft limit
	BackpressureCritical                   // queue depth > hard limit, reject new tasks
)

const (
	DefaultSoftLimit = 10000
	DefaultHardLimit = 50000
)

// Backpressure tracks queue pressure and rejects tasks when the hard limit is exceeded.
type Backpressure struct {
	softLimit     int
	hardLimit     int
	state         atomic.Int32
	rejectedCount atomic.Int64
	logger        *slog.Logger
}

// NewBackpressure creates a new Backpressure with the given limits.
func NewBackpressure(softLimit, hardLimit int, logger *slog.Logger) *Backpressure {
	if logger == nil {
		logger = slog.Default()
	}
	return &Backpressure{
		softLimit: softLimit,
		hardLimit: hardLimit,
		logger:    logger,
	}
}

// Check evaluates the current queue depth and returns the backpressure state.
func (bp *Backpressure) Check(currentDepth int) BackpressureState {
	var state BackpressureState
	switch {
	case currentDepth >= bp.hardLimit:
		state = BackpressureCritical
	case currentDepth >= bp.softLimit:
		state = BackpressureWarning
	default:
		state = BackpressureNormal
	}

	prev := BackpressureState(bp.state.Swap(int32(state)))
	if state != prev {
		bp.logger.Info("backpressure state changed",
			slog.Int("depth", currentDepth),
			slog.Int("state", int(state)),
			slog.Int("soft_limit", bp.softLimit),
			slog.Int("hard_limit", bp.hardLimit),
		)
	}

	return state
}

// ShouldReject returns true if the current depth exceeds the hard limit.
func (bp *Backpressure) ShouldReject(currentDepth int) bool {
	state := bp.Check(currentDepth)
	if state == BackpressureCritical {
		bp.rejectedCount.Add(1)
		return true
	}
	return false
}

// State returns the last evaluated backpressure state.
func (bp *Backpressure) State() BackpressureState {
	return BackpressureState(bp.state.Load())
}

// RejectedCount returns the total number of rejected tasks.
func (bp *Backpressure) RejectedCount() int64 {
	return bp.rejectedCount.Load()
}
