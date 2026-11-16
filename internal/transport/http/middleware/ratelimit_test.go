package middleware

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestRateLimiterFixedWindow(t *testing.T) {
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	limiter := NewRateLimiter(2, time.Minute)
	limiter.now = func() time.Time { return now }

	tenantID := uuid.New()
	otherTenantID := uuid.New()

	for i := 0; i < 2; i++ {
		if allowed, _ := limiter.Allow(tenantID); !allowed {
			t.Fatalf("request %d rejected, want allowed", i)
		}
	}
	allowed, retryAfter := limiter.Allow(tenantID)
	if allowed {
		t.Fatal("third request allowed, want rejected")
	}
	if retryAfter != time.Minute {
		t.Errorf("retry after = %v, want 1m", retryAfter)
	}

	// Another tenant has an independent budget.
	if allowed, _ := limiter.Allow(otherTenantID); !allowed {
		t.Fatal("other tenant rejected, want allowed")
	}

	// The window resets after its duration.
	now = now.Add(time.Minute)
	if allowed, _ := limiter.Allow(tenantID); !allowed {
		t.Fatal("request after window reset rejected, want allowed")
	}
}

func TestRateLimiterDisabled(t *testing.T) {
	limiter := NewRateLimiter(0, time.Minute)
	tenantID := uuid.New()
	for i := 0; i < 1000; i++ {
		if allowed, _ := limiter.Allow(tenantID); !allowed {
			t.Fatal("disabled limiter rejected a request")
		}
	}

	var nilLimiter *RateLimiter
	if allowed, _ := nilLimiter.Allow(tenantID); !allowed {
		t.Fatal("nil limiter rejected a request")
	}
}
