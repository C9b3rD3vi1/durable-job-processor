package middleware

import (
	"context"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/observability"
	"github.com/google/uuid"
)

// RateLimiter is the in-memory per-tenant fixed-window backend. It is suitable
// for tests and single-process use; multi-replica deployments should use
// NewStoreRateLimiter with a shared store. Durable per-tenant job quotas are
// always enforced in PostgreSQL.
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

// RateLimitStore stores fixed-window request counters shared by every API
// process. Implementations must be safe for concurrent use. The middleware
// owns the policy (limit and window); the store only records counters.
type RateLimitStore interface {
	Allow(ctx context.Context, scope string, tenantID uuid.UUID, limit int, window time.Duration) (allowed bool, retryAfter time.Duration, err error)
}

// Limiter decides whether one verified tenant request is within budget.
// AllowContext is named distinctly so the in-memory backend can keep its
// simpler Allow method for local tests.
type Limiter interface {
	AllowContext(ctx context.Context, tenantID uuid.UUID) (allowed bool, retryAfter time.Duration, err error)
	Enabled() bool
}

// storeLimiter binds a shared store to one scope and budget.
type storeLimiter struct {
	store  RateLimitStore
	scope  string
	limit  int
	window time.Duration
}

// NewStoreRateLimiter creates a limiter backed by a shared counter store. A
// non-positive limit disables limiting.
func NewStoreRateLimiter(store RateLimitStore, scope string, limit int, window time.Duration) Limiter {
	if window <= 0 {
		window = time.Minute
	}
	return &storeLimiter{store: store, scope: scope, limit: limit, window: window}
}

func (l *storeLimiter) Enabled() bool {
	return l != nil && l.store != nil && l.limit > 0
}

func (l *storeLimiter) AllowContext(ctx context.Context, tenantID uuid.UUID) (bool, time.Duration, error) {
	if !l.Enabled() {
		return true, 0, nil
	}
	return l.store.Allow(ctx, l.scope, tenantID, l.limit, l.window)
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

// AllowContext adapts the in-memory limiter to the Limiter interface. The
// in-memory backend cannot fail, so it never returns an error.
func (l *RateLimiter) AllowContext(_ context.Context, tenantID uuid.UUID) (bool, time.Duration, error) {
	allowed, retryAfter := l.Allow(tenantID)
	return allowed, retryAfter, nil
}

// Enabled reports whether this limiter enforces a budget.
func (l *RateLimiter) Enabled() bool { return l != nil && l.limit > 0 }

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
func RateLimit(limiter Limiter, logger *slog.Logger, scope ...string) func(http.Handler) http.Handler {
	return RateLimitWithMetrics(limiter, logger, nil, scope...)
}

// RateLimitWithMetrics is RateLimit with a counter for rejected requests. The
// scope label distinguishes the general and submission budgets. A limiter
// storage error fails closed with 503 rather than silently dropping the
// tenant's budget.
func RateLimitWithMetrics(limiter Limiter, logger *slog.Logger, metrics *observability.Metrics, scope ...string) func(http.Handler) http.Handler {
	if limiter == nil || !limiter.Enabled() {
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

			allowed, retryAfter, err := limiter.AllowContext(r.Context(), identity.TenantID)
			if err != nil {
				if logger != nil {
					logger.Error("rate limit check failed",
						"tenant_id", identity.TenantID,
						"scope", label,
						"error", err,
						"path", r.URL.Path,
					)
				}
				writeError(w, http.StatusServiceUnavailable, "rate_limiter_unavailable",
					"rate limit check is temporarily unavailable")
				return
			}
			if allowed {
				next.ServeHTTP(w, r)
				return
			}

			metrics.Inc(observability.MetricRateLimitedTotal, observability.L("scope", label))

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
