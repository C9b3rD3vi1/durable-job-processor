package middleware

import (
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// RateLimiter is a per-tenant fixed-window request limiter. It is intentionally
// in-memory: the deployment runs one API instance, while durable per-tenant job
// quotas are enforced in PostgreSQL.
type RateLimiter struct {
	limit  int
	window time.Duration
	now    func() time.Time

	mu      sync.Mutex
	tenants map[uuid.UUID]*rateWindow
}

type rateWindow struct {
	start time.Time
	count int
}

// NewRateLimiter creates a limiter that allows limit requests per tenant per
// window. A non-positive limit disables limiting.
func NewRateLimiter(limit int, window time.Duration) *RateLimiter {
	if window <= 0 {
		window = time.Minute
	}
	return &RateLimiter{
		limit:   limit,
		window:  window,
		now:     time.Now,
		tenants: make(map[uuid.UUID]*rateWindow),
	}
}

// Allow records one request for tenantID. When the tenant is over its limit it
// returns false and the remaining time until the window resets.
func (l *RateLimiter) Allow(tenantID uuid.UUID) (bool, time.Duration) {
	if l == nil || l.limit <= 0 {
		return true, 0
	}

	now := l.now()

	l.mu.Lock()
	defer l.mu.Unlock()

	l.pruneLocked(now)

	window, ok := l.tenants[tenantID]
	if !ok || now.Sub(window.start) >= l.window {
		l.tenants[tenantID] = &rateWindow{start: now, count: 1}
		return true, 0
	}
	if window.count >= l.limit {
		retryAfter := l.window - now.Sub(window.start)
		if retryAfter < 0 {
			retryAfter = 0
		}
		return false, retryAfter
	}
	window.count++
	return true, 0
}

// pruneLocked discards windows that no longer affect any tenant. It runs only
// when the map has grown enough to be worth traversing.
func (l *RateLimiter) pruneLocked(now time.Time) {
	if len(l.tenants) < 1024 {
		return
	}
	for tenantID, window := range l.tenants {
		if now.Sub(window.start) >= l.window {
			delete(l.tenants, tenantID)
		}
	}
}

// RateLimit rejects requests over a tenant's fixed-window budget. It must run
// after RequireAuth so the tenant is a verified identity rather than a header.
// An optional scope distinguishes independent budgets (for example submissions
// from general API traffic) in log messages and error responses.
func RateLimit(limiter *RateLimiter, logger *slog.Logger, scope ...string) func(http.Handler) http.Handler {
	if limiter == nil || limiter.limit <= 0 {
		return func(next http.Handler) http.Handler {
			return next
		}
	}

	label := "request"
	if len(scope) > 0 && strings.TrimSpace(scope[0]) != "" {
		label = strings.TrimSpace(scope[0])
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			identity, ok := IdentityFrom(r.Context())
			if !ok {
				// RequireAuth normally handles this; keep the middleware safe if
				// it is composed in a different order.
				next.ServeHTTP(w, r)
				return
			}

			allowed, retryAfter := limiter.Allow(identity.TenantID)
			if allowed {
				next.ServeHTTP(w, r)
				return
			}

			seconds := int(math.Ceil(retryAfter.Seconds()))
			if seconds < 1 {
				seconds = 1
			}
			w.Header().Set("Retry-After", strconv.Itoa(seconds))
			if logger != nil {
				logger.Warn("tenant rate limit exceeded",
					"tenant_id", identity.TenantID,
					"api_key_id", identity.KeyID,
					"scope", label,
					"retry_after_seconds", seconds,
					"path", r.URL.Path,
				)
			}
			writeError(w, http.StatusTooManyRequests, "rate_limited",
				"tenant "+label+" rate limit exceeded")
		})
	}
}
