package integration_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/postgres"
	"github.com/google/uuid"
)

func TestSharedRateLimitCounter(t *testing.T) {
	pool := mustPool(t, testDatabaseURL(t))
	defer pool.Close()
	applyMigrations(t, pool)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	scope := "test-shared-" + uuid.NewString()
	otherScope := scope + "-other"
	defer func() {
		_, _ = pool.Exec(context.Background(),
			`DELETE FROM rate_limit_counters WHERE scope IN ($1, $2)`, scope, otherScope)
	}()

	tenantID := uuid.New()
	// Two repository instances model two API processes sharing one budget.
	first := postgres.NewRateLimitRepository(pool)
	second := postgres.NewRateLimitRepository(pool)

	for i := 0; i < 2; i++ {
		allowed, _, err := first.Allow(ctx, scope, tenantID, 2, time.Minute)
		if err != nil {
			t.Fatalf("Allow() error = %v", err)
		}
		if !allowed {
			t.Fatalf("request %d rejected, want allowed", i)
		}
	}

	allowed, retryAfter, err := second.Allow(ctx, scope, tenantID, 2, time.Minute)
	if err != nil {
		t.Fatalf("Allow() from second process error = %v", err)
	}
	if allowed {
		t.Fatal("request from second process allowed, want rejected")
	}
	if retryAfter <= 0 || retryAfter > time.Minute {
		t.Errorf("retryAfter = %v, want between 0 and 1m", retryAfter)
	}

	// The cap must not grow on rejected requests.
	allowed, _, err = second.Allow(ctx, scope, tenantID, 2, time.Minute)
	if err != nil || allowed {
		t.Fatalf("repeated rejected request = %v/%v, want false/nil", allowed, err)
	}

	// Another tenant and another scope have independent budgets.
	if allowed, _, err = second.Allow(ctx, scope, uuid.New(), 2, time.Minute); err != nil || !allowed {
		t.Fatalf("other tenant = %v/%v, want true/nil", allowed, err)
	}
	if allowed, _, err = second.Allow(ctx, otherScope, tenantID, 2, time.Minute); err != nil || !allowed {
		t.Fatalf("other scope = %v/%v, want true/nil", allowed, err)
	}

	// A non-positive limit disables the budget.
	for i := 0; i < 3; i++ {
		allowed, _, err = first.Allow(ctx, scope, tenantID, 0, time.Minute)
		if err != nil || !allowed {
			t.Fatalf("disabled limiter = %v/%v, want true/nil", allowed, err)
		}
	}
}

func TestSharedRateLimitCounterConcurrent(t *testing.T) {
	pool := mustPool(t, testDatabaseURL(t))
	defer pool.Close()
	applyMigrations(t, pool)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	scope := "test-concurrent-" + uuid.NewString()
	defer func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM rate_limit_counters WHERE scope = $1`, scope)
	}()

	const limit = 20
	tenantID := uuid.New()
	repositories := []*postgres.RateLimitRepository{
		postgres.NewRateLimitRepository(pool),
		postgres.NewRateLimitRepository(pool),
	}

	var allowed atomic.Int32
	var wg sync.WaitGroup
	errs := make(chan error, 40)
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			allowedRequest, _, err := repositories[i%len(repositories)].Allow(ctx, scope, tenantID, limit, time.Minute)
			if err != nil {
				errs <- err
				return
			}
			if allowedRequest {
				allowed.Add(1)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("Allow() error = %v", err)
	}
	if got := allowed.Load(); got != limit {
		t.Errorf("allowed = %d, want exactly %d", got, limit)
	}
}
