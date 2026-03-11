package retry

import (
	"math"
	"math/rand/v2"
	"time"
)

// CalculateBackoff calculates exponential backoff with jitter for retry attempts.
// Uses math/rand/v2 which is safe for non-cryptographic purposes like backoff jitter.
func CalculateBackoff(policy *Policy, attempt int32) time.Duration {
	if attempt <= 0 {
		return policy.InitialInterval
	}

	multiplier := math.Pow(policy.BackoffCoefficient, float64(attempt-1))
	backoff := float64(policy.InitialInterval) * multiplier

	jitterFactor := 0.8 + rand.Float64()*0.4
	backoff *= jitterFactor

	if backoff > float64(policy.MaximumInterval) {
		backoff = float64(policy.MaximumInterval)
	}

	return time.Duration(backoff)
}

// CalculateBackoffWithJitter calculates exponential backoff with configurable jitter percentage.
func CalculateBackoffWithJitter(policy *Policy, attempt int32, jitterPercent float64) time.Duration {
	if attempt <= 0 {
		return policy.InitialInterval
	}

	multiplier := math.Pow(policy.BackoffCoefficient, float64(attempt-1))
	backoff := float64(policy.InitialInterval) * multiplier

	if jitterPercent > 0 {
		jitterRange := backoff * jitterPercent
		jitter := (rand.Float64() * 2 * jitterRange) - jitterRange
		backoff += jitter
	}

	if backoff > float64(policy.MaximumInterval) {
		backoff = float64(policy.MaximumInterval)
	}

	if backoff < 0 {
		backoff = float64(policy.InitialInterval)
	}

	return time.Duration(backoff)
}
