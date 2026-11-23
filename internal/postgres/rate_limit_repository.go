package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// RateLimitRepository stores fixed-window request counters in PostgreSQL so
// every API process enforces the same per-tenant budget.
type RateLimitRepository struct {
	pool *pgxpool.Pool
}

// NewRateLimitRepository creates a shared rate-limit counter store.
func NewRateLimitRepository(pool *pgxpool.Pool) *RateLimitRepository {
	return &RateLimitRepository{pool: pool}
}

// Allow records one request and reports whether it is within limit. The
// counter is capped at limit+1: the request that reaches the cap is the first
// rejected one, and further rejected requests do not grow the value. The
// ON CONFLICT DO UPDATE clause locks the existing row, so concurrent API
// processes increment the same counter atomically.
func (r *RateLimitRepository) Allow(ctx context.Context, scope string, tenantID uuid.UUID, limit int, window time.Duration) (bool, time.Duration, error) {
	if limit <= 0 {
		return true, 0, nil
	}
	if r == nil || r.pool == nil {
		return false, 0, fmt.Errorf("rate limit repository has no database pool")
	}
	if window <= 0 {
		window = time.Minute
	}

	now := time.Now()
	windowStart := now.Truncate(window)
	cap := int64(limit) + 1

	var count int64
	var storedWindow time.Time
	err := r.pool.QueryRow(ctx, `
		INSERT INTO rate_limit_counters AS c (scope, tenant_id, window_start, count, updated_at)
		VALUES ($1, $2, $3, 1, now())
		ON CONFLICT (scope, tenant_id) DO UPDATE
		SET window_start = CASE
		        WHEN c.window_start < EXCLUDED.window_start THEN EXCLUDED.window_start
		        ELSE c.window_start
		    END,
		    count = CASE
		        WHEN c.window_start < EXCLUDED.window_start THEN 1
		        WHEN c.count >= $4 THEN c.count
		        ELSE c.count + 1
		    END,
		    updated_at = now()
		RETURNING count, window_start`,
		scope, tenantID, windowStart, cap,
	).Scan(&count, &storedWindow)
	if err != nil {
		return false, 0, fmt.Errorf("update rate limit counter: %w", err)
	}

	if count <= int64(limit) {
		return true, 0, nil
	}
	retryAfter := storedWindow.Add(window).Sub(now)
	if retryAfter < 0 {
		retryAfter = 0
	}
	return false, retryAfter, nil
}
