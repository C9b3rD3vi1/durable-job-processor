// Package retry implements failure classification and bounded exponential
// backoff with jitter for durable job attempts.
package retry

import (
	"math/rand"
	"time"
)

const (
	// DefaultBaseDelay is the first-attempt ceiling when no policy is supplied.
	DefaultBaseDelay = 5 * time.Second

	// DefaultMaxDelay bounds the exponential ceiling.
	DefaultMaxDelay = 15 * time.Minute
)

// Policy computes retry delays. The exponential ceiling grows as
// BaseDelay * 2^(attempt-1) and is capped at MaxDelay. The returned delay uses
// full jitter, uniformly distributed between zero and the ceiling.
type Policy struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
}

// NewPolicy normalizes a policy. Non-positive values fall back to the defaults,
// and MaxDelay is never allowed below BaseDelay.
func NewPolicy(baseDelay, maxDelay time.Duration) Policy {
	if baseDelay <= 0 {
		baseDelay = DefaultBaseDelay
	}
	if maxDelay <= 0 {
		maxDelay = DefaultMaxDelay
	}
	if maxDelay < baseDelay {
		maxDelay = baseDelay
	}
	return Policy{BaseDelay: baseDelay, MaxDelay: maxDelay}
}

// Ceiling returns min(BaseDelay * 2^(attempt-1), MaxDelay). Attempts are
// 1-based; values below one are treated as the first attempt.
func (p Policy) Ceiling(attempt int) time.Duration {
	base := p.BaseDelay
	if base <= 0 {
		base = DefaultBaseDelay
	}
	max := p.MaxDelay
	if max <= 0 {
		max = DefaultMaxDelay
	}
	if base > max {
		base = max
	}
	if attempt < 1 {
		attempt = 1
	}

	delay := base
	for i := 1; i < attempt; i++ {
		if delay > max/2 {
			return max
		}
		delay *= 2
	}
	if delay > max {
		return max
	}
	return delay
}

// Delay returns a full-jitter retry delay in [0, Ceiling(attempt)].
func (p Policy) Delay(attempt int) time.Duration {
	return p.DelayWithJitter(attempt, rand.Float64())
}

// DelayWithJitter returns a deterministic delay for tests and simulations.
// jitter is clamped to [0, 1], where 0 returns zero and 1 returns the ceiling.
func (p Policy) DelayWithJitter(attempt int, jitter float64) time.Duration {
	ceiling := p.Ceiling(attempt)
	if ceiling <= 0 {
		return 0
	}
	if jitter <= 0 {
		return 0
	}
	if jitter > 1 {
		jitter = 1
	}
	return time.Duration(float64(ceiling) * jitter)
}
