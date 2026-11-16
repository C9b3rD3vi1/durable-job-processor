package retry

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestPolicyCeiling(t *testing.T) {
	policy := NewPolicy(time.Second, 15*time.Second)

	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{0, time.Second},
		{1, time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
		{5, 15 * time.Second},
		{50, 15 * time.Second},
	}
	for _, tt := range tests {
		if got := policy.Ceiling(tt.attempt); got != tt.want {
			t.Errorf("Ceiling(%d) = %v, want %v", tt.attempt, got, tt.want)
		}
	}
}

func TestPolicyDelayWithJitter(t *testing.T) {
	policy := NewPolicy(time.Second, 10*time.Second)

	if got := policy.DelayWithJitter(3, 0); got != 0 {
		t.Errorf("jitter 0 = %v, want 0", got)
	}
	if got := policy.DelayWithJitter(3, 1); got != 4*time.Second {
		t.Errorf("jitter 1 = %v, want 4s", got)
	}
	if got := policy.DelayWithJitter(3, 0.5); got != 2*time.Second {
		t.Errorf("jitter 0.5 = %v, want 2s", got)
	}
	if got := policy.DelayWithJitter(3, 2); got != 4*time.Second {
		t.Errorf("jitter above 1 = %v, want 4s", got)
	}
}

func TestPolicyDelayStaysWithinCeiling(t *testing.T) {
	policy := NewPolicy(500*time.Millisecond, 8*time.Second)
	for attempt := 1; attempt <= 12; attempt++ {
		ceiling := policy.Ceiling(attempt)
		for i := 0; i < 50; i++ {
			delay := policy.Delay(attempt)
			if delay < 0 || delay > ceiling {
				t.Fatalf("Delay(%d) = %v, want within [0, %v]", attempt, delay, ceiling)
			}
		}
	}
}

func TestNewPolicyNormalizesValues(t *testing.T) {
	policy := NewPolicy(0, 0)
	if policy.BaseDelay != DefaultBaseDelay || policy.MaxDelay != DefaultMaxDelay {
		t.Errorf("NewPolicy(0,0) = %+v, want defaults", policy)
	}

	policy = NewPolicy(10*time.Second, time.Second)
	if policy.MaxDelay != 10*time.Second {
		t.Errorf("MaxDelay = %v, want 10s", policy.MaxDelay)
	}
}

func TestClassify(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want Class
	}{
		{"nil", nil, ClassUnknown},
		{"plain", errors.New("boom"), ClassUnknown},
		{"transient", Transient(errors.New("boom")), ClassTransient},
		{"permanent", Permanent(errors.New("boom")), ClassPermanent},
		{"cancelled", Cancelled(errors.New("boom")), ClassCancelled},
		{"wrapped transient", fmt.Errorf("outer: %w", Transient(errors.New("boom"))), ClassTransient},
		{"context canceled", context.Canceled, ClassCancelled},
		{"deadline", context.DeadlineExceeded, ClassTransient},
		{"wrapped deadline", fmt.Errorf("timeout: %w", context.DeadlineExceeded), ClassTransient},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Classify(tt.err); got != tt.want {
				t.Errorf("Classify() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClassifiedErrorPreservesMessageAndUnwrap(t *testing.T) {
	sentinel := errors.New("underlying")
	err := Transient(sentinel)
	if err.Error() != "underlying" {
		t.Errorf("Error() = %q, want underlying", err.Error())
	}
	if !errors.Is(err, sentinel) {
		t.Error("classified error does not unwrap to the sentinel")
	}
}

func TestShouldRetry(t *testing.T) {
	tests := []struct {
		class     Class
		attempt   int
		max       int
		wantRetry bool
	}{
		{ClassTransient, 1, 3, true},
		{ClassTransient, 2, 3, true},
		{ClassTransient, 3, 3, false},
		{ClassUnknown, 1, 1, false},
		{ClassCancelled, 1, 2, true},
		{ClassPermanent, 1, 5, false},
	}
	for _, tt := range tests {
		if got := ShouldRetry(tt.class, tt.attempt, tt.max); got != tt.wantRetry {
			t.Errorf("ShouldRetry(%v, %d, %d) = %v, want %v",
				tt.class, tt.attempt, tt.max, got, tt.wantRetry)
		}
	}
}
