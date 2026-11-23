package middleware

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/auth"
	"github.com/google/uuid"
)

func TestRateLimiterFixedWindow(t *testing.T) {
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	limiter := NewRateLimiter(2, time.Minute)
	limiter.now = func() time.Time { return now }

	tenantID := uuid.New()
	otherTenantID := uuid.New()

	for i := 0; i < 2; i++ {
		if allowed, _, _ := limiter.AllowContext(context.Background(), tenantID); !allowed {
			t.Fatalf("request %d rejected, want allowed", i)
		}
	}
	allowed, retryAfter, err := limiter.AllowContext(context.Background(), tenantID)
	if err != nil {
		t.Fatalf("AllowContext() error = %v", err)
	}
	if allowed {
		t.Fatal("third request allowed, want rejected")
	}
	if retryAfter != time.Minute {
		t.Errorf("retry after = %v, want 1m", retryAfter)
	}

	// Another tenant has an independent budget.
	if allowed, _, _ := limiter.AllowContext(context.Background(), otherTenantID); !allowed {
		t.Fatal("other tenant rejected, want allowed")
	}

	// The window resets after its duration.
	now = now.Add(time.Minute)
	if allowed, _, _ := limiter.AllowContext(context.Background(), tenantID); !allowed {
		t.Fatal("request after window reset rejected, want allowed")
	}
}

func TestRateLimiterDisabled(t *testing.T) {
	limiter := NewRateLimiter(0, time.Minute)
	tenantID := uuid.New()
	for i := 0; i < 1000; i++ {
		if allowed, _, _ := limiter.AllowContext(context.Background(), tenantID); !allowed {
			t.Fatal("disabled limiter rejected a request")
		}
	}

	var nilLimiter *RateLimiter
	if allowed, _, _ := nilLimiter.AllowContext(context.Background(), tenantID); !allowed {
		t.Fatal("nil limiter rejected a request")
	}
}

type fakeRateLimitStore struct {
	calls int
	allow func(context.Context, string, uuid.UUID, int, time.Duration) (bool, time.Duration, error)
}

func (s *fakeRateLimitStore) Allow(ctx context.Context, scope string, tenantID uuid.UUID, limit int, window time.Duration) (bool, time.Duration, error) {
	s.calls++
	if s.allow == nil {
		return true, 0, nil
	}
	return s.allow(ctx, scope, tenantID, limit, window)
}

func TestStoreRateLimiterPassesPolicyToStore(t *testing.T) {
	tenantID := uuid.New()
	store := &fakeRateLimitStore{
		allow: func(_ context.Context, scope string, gotTenant uuid.UUID, limit int, window time.Duration) (bool, time.Duration, error) {
			if scope != "submission" {
				t.Errorf("scope = %q, want submission", scope)
			}
			if gotTenant != tenantID {
				t.Errorf("tenant = %s, want %s", gotTenant, tenantID)
			}
			if limit != 7 {
				t.Errorf("limit = %d, want 7", limit)
			}
			if window != 2*time.Minute {
				t.Errorf("window = %v, want 2m", window)
			}
			return false, 1500 * time.Millisecond, nil
		},
	}

	limiter := NewStoreRateLimiter(store, "submission", 7, 2*time.Minute)
	if !limiter.Enabled() {
		t.Fatal("store limiter is disabled")
	}
	allowed, retryAfter, err := limiter.AllowContext(context.Background(), tenantID)
	if err != nil {
		t.Fatalf("AllowContext() error = %v", err)
	}
	if allowed {
		t.Fatal("AllowContext() = true, want false")
	}
	if retryAfter != 1500*time.Millisecond {
		t.Errorf("retryAfter = %v, want 1.5s", retryAfter)
	}
	if store.calls != 1 {
		t.Errorf("store calls = %d, want 1", store.calls)
	}
}

func TestStoreRateLimiterDisabledDoesNotTouchStore(t *testing.T) {
	store := &fakeRateLimitStore{}
	limiter := NewStoreRateLimiter(store, "request", 0, time.Minute)
	if limiter.Enabled() {
		t.Fatal("zero-limit limiter must be disabled")
	}
	allowed, retryAfter, err := limiter.AllowContext(context.Background(), uuid.New())
	if err != nil {
		t.Fatalf("AllowContext() error = %v", err)
	}
	if !allowed || retryAfter != 0 {
		t.Fatalf("AllowContext() = %v/%v, want true/0", allowed, retryAfter)
	}
	if store.calls != 0 {
		t.Errorf("store calls = %d, want 0", store.calls)
	}
}

func TestRateLimitStoreErrorFailsClosed(t *testing.T) {
	store := &fakeRateLimitStore{
		allow: func(context.Context, string, uuid.UUID, int, time.Duration) (bool, time.Duration, error) {
			return false, 0, errors.New("database unavailable")
		},
	}
	handler := RateLimit(NewStoreRateLimiter(store, "request", 5, time.Minute), nil)(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
	req = req.WithContext(context.WithValue(req.Context(), identityContextKey{},
		auth.Identity{TenantID: uuid.New()}))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "rate_limiter_unavailable") {
		t.Errorf("body = %q, want rate_limiter_unavailable", rec.Body.String())
	}
}
