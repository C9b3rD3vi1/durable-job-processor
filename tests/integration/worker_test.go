package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/domain"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/handlers"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/postgres"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/retry"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/worker"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// mustIsolatedPool creates a throwaway PostgreSQL schema and returns a pool
// whose search_path points at it. Claim and recovery tests operate on the whole
// table, so schema isolation keeps them independent from development data and
// from the HTTP integration suite.
func mustIsolatedPool(t *testing.T, databaseURL string) *pgxpool.Pool {
	t.Helper()

	schema := "test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	admin := mustPool(t, databaseURL)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	quotedSchema := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quotedSchema); err != nil {
		admin.Close()
		t.Fatalf("create test schema: %v", err)
	}

	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		admin.Close()
		t.Fatalf("parse database URL: %v", err)
	}
	poolConfig.ConnConfig.RuntimeParams["search_path"] = schema

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		admin.Close()
		t.Fatalf("create isolated pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		admin.Close()
		t.Fatalf("ping isolated pool: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, err := admin.Exec(cleanupCtx, "DROP SCHEMA "+quotedSchema+" CASCADE"); err != nil {
			t.Errorf("drop test schema: %v", err)
		}
		admin.Close()
	})

	applyMigrations(t, pool)
	return pool
}

func createWorkerJob(
	t *testing.T,
	ctx context.Context,
	repository *postgres.JobRepository,
	tenantID uuid.UUID,
	jobType string,
	payload string,
	priority int16,
	runAt time.Time,
	attempts int,
	maxAttempts int,
) *domain.Job {
	t.Helper()
	created, isNew, err := repository.CreateJob(ctx, &domain.Job{
		ID:          uuid.New(),
		TenantID:    tenantID,
		JobType:     jobType,
		Payload:     json.RawMessage(payload),
		Status:      domain.StatusQueued,
		Priority:    priority,
		RunAt:       runAt,
		Attempts:    attempts,
		MaxAttempts: maxAttempts,
	}, 0)
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	if !isNew {
		t.Fatal("expected a new job")
	}
	return created
}

func newWorkerRunner(t *testing.T, repository *postgres.JobRepository, registry *handlers.Registry, mutate func(*worker.Config)) *worker.Runner {
	t.Helper()
	cfg := worker.Config{
		Store:           repository,
		Registry:        registry,
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		WorkerID:        "integration-worker",
		Concurrency:     1,
		PollInterval:    5 * time.Millisecond,
		LeaseDuration:   30 * time.Second,
		HandlerTimeout:  5 * time.Second,
		RetryBaseDelay:  10 * time.Millisecond,
		ShutdownTimeout: 2 * time.Second,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	runner, err := worker.New(cfg)
	if err != nil {
		t.Fatalf("create worker: %v", err)
	}
	return runner
}

func startWorker(t *testing.T, runner *worker.Runner) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("worker Run() error = %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Error("worker did not stop")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func jobStatus(t *testing.T, pool *pgxpool.Pool, id uuid.UUID) domain.JobStatus {
	t.Helper()
	var status string
	if err := pool.QueryRow(context.Background(),
		`SELECT status::text FROM jobs WHERE id = $1`, id.String(),
	).Scan(&status); err != nil {
		t.Fatalf("read job status: %v", err)
	}
	return domain.JobStatus(status)
}

func waitForJobStatus(t *testing.T, pool *pgxpool.Pool, id uuid.UUID, want domain.JobStatus, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last domain.JobStatus
	for time.Now().Before(deadline) {
		last = jobStatus(t, pool, id)
		if last == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("job %s status = %q, want %q", id, last, want)
}

func waitForSucceededJobs(t *testing.T, pool *pgxpool.Pool, tenantID uuid.UUID, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var count int
	for time.Now().Before(deadline) {
		if err := pool.QueryRow(context.Background(),
			`SELECT count(*) FROM jobs WHERE tenant_id = $1 AND status = 'succeeded'`,
			tenantID.String(),
		).Scan(&count); err != nil {
			t.Fatalf("count succeeded jobs: %v", err)
		}
		if count == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("succeeded jobs = %d, want %d", count, want)
}

func TestWorkerExecutesJobToCompletion(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := mustIsolatedPool(t, databaseURL)

	tenantID := uuid.New()

	repository := postgres.NewJobRepository(pool)
	job := createWorkerJob(t, ctx, repository, tenantID, "integration.ok", `{"value":"x"}`, 0, time.Now().Add(-time.Second), 0, 3)

	registry := handlers.New()
	if err := registry.Register("integration.ok", handlers.HandlerFunc(func(context.Context, domain.Job) (json.RawMessage, error) {
		return json.RawMessage(`{"status":"done"}`), nil
	})); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	startWorker(t, newWorkerRunner(t, repository, registry, nil))
	waitForJobStatus(t, pool, job.ID, domain.StatusSucceeded, 5*time.Second)

	got, err := repository.GetJobByID(ctx, tenantID, job.ID)
	if err != nil {
		t.Fatalf("get completed job: %v", err)
	}
	if got.Status != domain.StatusSucceeded {
		t.Errorf("status = %q, want succeeded", got.Status)
	}
	if got.Attempts != 1 {
		t.Errorf("attempts = %d, want 1", got.Attempts)
	}
	if got.StartedAt == nil || got.FinishedAt == nil {
		t.Error("started_at and finished_at must be set")
	}
	if got.LockedBy != nil || got.LeaseToken != nil || got.LockedUntil != nil || got.HeartbeatAt != nil {
		t.Errorf("lease fields not cleared: %+v", got)
	}
	if got.LastError != nil {
		t.Errorf("last_error = %q, want nil", *got.LastError)
	}

	var result map[string]any
	if err := json.Unmarshal(got.Result, &result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if result["status"] != "done" {
		t.Errorf("result status = %v, want done", result["status"])
	}
}

func TestWorkerRetriesThenDeadLetters(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := mustIsolatedPool(t, databaseURL)

	tenantID := uuid.New()

	repository := postgres.NewJobRepository(pool)
	job := createWorkerJob(t, ctx, repository, tenantID, "integration.fail", `{}`, 0, time.Now().Add(-time.Second), 0, 2)

	registry := handlers.New()
	if err := registry.Register("integration.fail", handlers.HandlerFunc(func(context.Context, domain.Job) (json.RawMessage, error) {
		return nil, errors.New("handler exploded")
	})); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	runner := newWorkerRunner(t, repository, registry, func(cfg *worker.Config) {
		cfg.RetryBaseDelay = 10 * time.Millisecond
	})
	startWorker(t, runner)
	waitForJobStatus(t, pool, job.ID, domain.StatusDead, 5*time.Second)

	got, err := repository.GetJobByID(ctx, tenantID, job.ID)
	if err != nil {
		t.Fatalf("get dead job: %v", err)
	}
	if got.Attempts != 2 {
		t.Errorf("attempts = %d, want 2", got.Attempts)
	}
	if got.LastError == nil || *got.LastError != "handler exploded" {
		t.Errorf("last_error = %v, want handler exploded", got.LastError)
	}
	if got.FinishedAt == nil {
		t.Error("finished_at is nil for a dead job")
	}
	if got.LockedBy != nil || got.LeaseToken != nil {
		t.Error("lease fields were not cleared")
	}
}

func TestClaimIsAtomicAcrossWorkers(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := mustIsolatedPool(t, databaseURL)

	tenantID := uuid.New()

	repository := postgres.NewJobRepository(pool)
	const (
		jobCount = 20
		claimers = 10
	)
	for i := 0; i < jobCount; i++ {
		createWorkerJob(t, ctx, repository, tenantID, "integration.claim", `{}`, 0, time.Now().Add(-time.Second), 0, 1)
	}

	var wg sync.WaitGroup
	claimed := make(chan uuid.UUID, jobCount)
	errs := make(chan error, claimers)
	for i := 0; i < claimers; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for {
				job, err := repository.ClaimJob(ctx, fmt.Sprintf("claimer-%d", n), time.Minute)
				if err != nil {
					errs <- err
					return
				}
				if job == nil {
					return
				}
				claimed <- job.ID
			}
		}(i)
	}
	wg.Wait()
	close(claimed)
	close(errs)

	for err := range errs {
		t.Fatalf("claim error: %v", err)
	}

	seen := make(map[uuid.UUID]int, jobCount)
	for id := range claimed {
		seen[id]++
	}
	if len(seen) != jobCount {
		t.Fatalf("distinct claims = %d, want %d", len(seen), jobCount)
	}
	for id, count := range seen {
		if count != 1 {
			t.Errorf("job %s was claimed %d times", id, count)
		}
	}

	var running int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM jobs WHERE tenant_id = $1 AND status = 'running' AND attempts = 1`,
		tenantID.String(),
	).Scan(&running); err != nil {
		t.Fatalf("count running jobs: %v", err)
	}
	if running != jobCount {
		t.Errorf("running jobs = %d, want %d", running, jobCount)
	}
}

func TestClaimOrdering(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := mustIsolatedPool(t, databaseURL)

	tenantID := uuid.New()

	repository := postgres.NewJobRepository(pool)
	due := time.Now().Add(-time.Minute)
	low := createWorkerJob(t, ctx, repository, tenantID, "integration.order", `{}`, 0, due, 0, 1)
	high := createWorkerJob(t, ctx, repository, tenantID, "integration.order", `{}`, 10, due, 0, 1)
	early := createWorkerJob(t, ctx, repository, tenantID, "integration.order", `{}`, 5, due.Add(-time.Hour), 0, 1)

	want := []uuid.UUID{high.ID, early.ID, low.ID}
	for i, wantID := range want {
		job, err := repository.ClaimJob(ctx, "order", time.Minute)
		if err != nil {
			t.Fatalf("claim %d: %v", i, err)
		}
		if job == nil {
			t.Fatalf("claim %d returned no job", i)
		}
		if job.ID != wantID {
			t.Fatalf("claim %d got %s, want %s", i, job.ID, wantID)
		}
	}
}

func TestClaimSkipsFutureAndExhaustedJobs(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := mustIsolatedPool(t, databaseURL)

	tenantID := uuid.New()

	repository := postgres.NewJobRepository(pool)
	createWorkerJob(t, ctx, repository, tenantID, "integration.skip", `{}`, 0, time.Now().Add(time.Hour), 0, 3)
	createWorkerJob(t, ctx, repository, tenantID, "integration.skip", `{}`, 0, time.Now().Add(-time.Second), 1, 1)

	job, err := repository.ClaimJob(ctx, "skip", time.Minute)
	if err != nil {
		t.Fatalf("ClaimJob() error = %v", err)
	}
	if job != nil {
		t.Fatalf("ClaimJob() = %s, want nil", job.ID)
	}
}

func TestCompleteAndFailAreFenced(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := mustIsolatedPool(t, databaseURL)

	tenantID := uuid.New()

	repository := postgres.NewJobRepository(pool)
	job := createWorkerJob(t, ctx, repository, tenantID, "integration.fence", `{}`, 0, time.Now().Add(-time.Second), 0, 1)
	claimed, err := repository.ClaimJob(ctx, "fence", time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("claim job = %v, %v", claimed, err)
	}
	if claimed.LeaseToken == nil {
		t.Fatal("claimed job has no lease token")
	}

	completed, err := repository.CompleteJob(ctx, claimed.ID, uuid.New(), json.RawMessage(`{"stale":true}`))
	if err != nil || completed {
		t.Fatalf("stale CompleteJob() = %v, %v; want false, nil", completed, err)
	}
	if got := jobStatus(t, pool, job.ID); got != domain.StatusRunning {
		t.Fatalf("status after stale completion = %q, want running", got)
	}

	completed, err = repository.CompleteJob(ctx, claimed.ID, *claimed.LeaseToken, json.RawMessage(`{"ok":true}`))
	if err != nil || !completed {
		t.Fatalf("CompleteJob() = %v, %v; want true, nil", completed, err)
	}
	if got := jobStatus(t, pool, job.ID); got != domain.StatusSucceeded {
		t.Fatalf("status after completion = %q, want succeeded", got)
	}

	other := createWorkerJob(t, ctx, repository, tenantID, "integration.fence", `{}`, 0, time.Now().Add(-time.Second), 0, 2)
	claimedOther, err := repository.ClaimJob(ctx, "fence", time.Minute)
	if err != nil || claimedOther == nil {
		t.Fatalf("claim other job = %v, %v", claimedOther, err)
	}
	failed, err := repository.FailJob(ctx, claimedOther.ID, uuid.New(), "stale", 0)
	if err != nil || failed {
		t.Fatalf("stale FailJob() = %v, %v; want false, nil", failed, err)
	}
	if got := jobStatus(t, pool, other.ID); got != domain.StatusRunning {
		t.Fatalf("status after stale failure = %q, want running", got)
	}

	failed, err = repository.FailJob(ctx, claimedOther.ID, *claimedOther.LeaseToken, "terminal", 0)
	if err != nil || !failed {
		t.Fatalf("FailJob() = %v, %v; want true, nil", failed, err)
	}
	if got := jobStatus(t, pool, other.ID); got != domain.StatusDead {
		t.Fatalf("status after failure = %q, want dead", got)
	}
}

func TestFailJobSchedulesRetryThenDeadLetters(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := mustIsolatedPool(t, databaseURL)

	tenantID := uuid.New()

	repository := postgres.NewJobRepository(pool)
	job := createWorkerJob(t, ctx, repository, tenantID, "integration.retry", `{}`, 0, time.Now().Add(-time.Second), 0, 2)

	claimed, err := repository.ClaimJob(ctx, "retry", time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("first claim = %v, %v", claimed, err)
	}
	retried, err := repository.FailJob(ctx, claimed.ID, *claimed.LeaseToken, "transient", time.Hour)
	if err != nil || !retried {
		t.Fatalf("FailJob() = %v, %v; want true, nil", retried, err)
	}

	got, err := repository.GetJobByID(ctx, tenantID, job.ID)
	if err != nil {
		t.Fatalf("get retry job: %v", err)
	}
	if got.Status != domain.StatusRetryWait {
		t.Fatalf("status = %q, want retry_wait", got.Status)
	}
	if !got.RunAt.After(time.Now()) {
		t.Errorf("run_at = %v, want in the future", got.RunAt)
	}
	if got.FinishedAt != nil {
		t.Error("finished_at must be nil while waiting to retry")
	}
	if got.LastError == nil || *got.LastError != "transient" {
		t.Errorf("last_error = %v, want transient", got.LastError)
	}

	if _, err := pool.Exec(ctx, `UPDATE jobs SET run_at = now() WHERE id = $1`, job.ID.String()); err != nil {
		t.Fatalf("make retry due: %v", err)
	}
	claimedAgain, err := repository.ClaimJob(ctx, "retry", time.Minute)
	if err != nil || claimedAgain == nil {
		t.Fatalf("second claim = %v, %v", claimedAgain, err)
	}
	if claimedAgain.Attempts != 2 {
		t.Errorf("attempts = %d, want 2", claimedAgain.Attempts)
	}

	dead, err := repository.FailJob(ctx, claimedAgain.ID, *claimedAgain.LeaseToken, "exhausted", time.Hour)
	if err != nil || !dead {
		t.Fatalf("second FailJob() = %v, %v; want true, nil", dead, err)
	}
	got, err = repository.GetJobByID(ctx, tenantID, job.ID)
	if err != nil {
		t.Fatalf("get dead job: %v", err)
	}
	if got.Status != domain.StatusDead {
		t.Fatalf("status = %q, want dead", got.Status)
	}
	if got.FinishedAt == nil {
		t.Error("finished_at is nil for a dead job")
	}
}

func TestWorkerGracefulShutdownDrainsInFlight(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := mustIsolatedPool(t, databaseURL)

	tenantID := uuid.New()
	repository := postgres.NewJobRepository(pool)
	job := createWorkerJob(t, ctx, repository, tenantID, "integration.block", `{}`, 0, time.Now().Add(-time.Second), 0, 2)

	started := make(chan struct{})
	release := make(chan struct{})
	registry := handlers.New()
	if err := registry.Register("integration.block", handlers.HandlerFunc(func(ctx context.Context, _ domain.Job) (json.RawMessage, error) {
		close(started)
		select {
		case <-release:
			return json.RawMessage(`{"ok":true}`), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	runner := newWorkerRunner(t, repository, registry, func(cfg *worker.Config) {
		cfg.HandlerTimeout = 5 * time.Second
		cfg.ShutdownTimeout = 2 * time.Second
	})

	runCtx, runCancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	var runErr error
	go func() {
		runErr = runner.Run(runCtx)
		close(finished)
	}()
	t.Cleanup(func() {
		runCancel()
		select {
		case <-finished:
		case <-time.After(10 * time.Second):
			t.Error("worker did not stop")
		}
	})

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not start")
	}

	runCancel()
	select {
	case <-finished:
		t.Fatal("worker returned before the in-flight handler drained")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	select {
	case <-finished:
		if runErr != nil {
			t.Fatalf("Run() error = %v", runErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop after the handler finished")
	}

	waitForJobStatus(t, pool, job.ID, domain.StatusSucceeded, 5*time.Second)
}

func TestWorkerHonorsConcurrency(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := mustIsolatedPool(t, databaseURL)

	tenantID := uuid.New()

	repository := postgres.NewJobRepository(pool)
	const (
		jobCount    = 12
		concurrency = 3
	)
	for i := 0; i < jobCount; i++ {
		createWorkerJob(t, ctx, repository, tenantID, "integration.slow", `{}`, 0, time.Now().Add(-time.Second), 0, 1)
	}

	var (
		mu        sync.Mutex
		active    int
		maxActive int
	)
	registry := handlers.New()
	if err := registry.Register("integration.slow", handlers.HandlerFunc(func(context.Context, domain.Job) (json.RawMessage, error) {
		mu.Lock()
		active++
		if active > maxActive {
			maxActive = active
		}
		mu.Unlock()

		time.Sleep(20 * time.Millisecond)

		mu.Lock()
		active--
		mu.Unlock()
		return json.RawMessage(`{"ok":true}`), nil
	})); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	runner := newWorkerRunner(t, repository, registry, func(cfg *worker.Config) {
		cfg.Concurrency = concurrency
	})
	startWorker(t, runner)
	waitForSucceededJobs(t, pool, tenantID, jobCount, 10*time.Second)

	mu.Lock()
	peak := maxActive
	mu.Unlock()
	if peak > concurrency {
		t.Errorf("peak concurrency = %d, want at most %d", peak, concurrency)
	}
	if peak < 2 {
		t.Errorf("peak concurrency = %d, want at least 2", peak)
	}
}

func TestHeartbeatKeepsLeaseAliveBeyondLeaseDuration(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := mustIsolatedPool(t, databaseURL)

	tenantID := uuid.New()

	repository := postgres.NewJobRepository(pool)
	job := createWorkerJob(t, ctx, repository, tenantID, "integration.heartbeat", `{}`, 0, time.Now().Add(-time.Second), 0, 1)

	started := make(chan struct{})
	release := make(chan struct{})
	registry := handlers.New()
	if err := registry.Register("integration.heartbeat", handlers.HandlerFunc(func(context.Context, domain.Job) (json.RawMessage, error) {
		close(started)
		<-release
		return json.RawMessage(`{"ok":true}`), nil
	})); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	runner := newWorkerRunner(t, repository, registry, func(cfg *worker.Config) {
		// The handler deliberately outlives the original lease. Heartbeats must
		// renew it, and the handler timeout is allowed to exceed the lease.
		cfg.LeaseDuration = 200 * time.Millisecond
		cfg.HeartbeatInterval = 20 * time.Millisecond
		cfg.HandlerTimeout = 2 * time.Second
		cfg.RecoveryInterval = time.Hour
	})
	startWorker(t, runner)

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not start")
	}

	// Wait beyond the original lease, then ask recovery to scan. A live
	// heartbeat must prevent the row from being treated as abandoned.
	time.Sleep(400 * time.Millisecond)
	result, err := repository.RecoverExpiredJobs(ctx, 10)
	if err != nil {
		t.Fatalf("RecoverExpiredJobs() error = %v", err)
	}
	if result.Requeued != 0 || result.Dead != 0 {
		t.Fatalf("recovery touched a live lease: %+v", result)
	}

	close(release)
	waitForJobStatus(t, pool, job.ID, domain.StatusSucceeded, 5*time.Second)

	got, err := repository.GetJobByID(ctx, tenantID, job.ID)
	if err != nil {
		t.Fatalf("get completed job: %v", err)
	}
	if got.Attempts != 1 {
		t.Errorf("attempts = %d, want 1", got.Attempts)
	}
}

func TestRecoveryRequeuesExpiredLease(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := mustIsolatedPool(t, databaseURL)

	tenantID := uuid.New()

	repository := postgres.NewJobRepository(pool)
	job := createWorkerJob(t, ctx, repository, tenantID, "integration.recover", `{}`, 0, time.Now().Add(-time.Second), 0, 3)

	claimed, err := repository.ClaimJob(ctx, "crashed-worker", 50*time.Millisecond)
	if err != nil || claimed == nil {
		t.Fatalf("claim job = %v, %v", claimed, err)
	}

	time.Sleep(150 * time.Millisecond)

	result, err := repository.RecoverExpiredJobs(ctx, 10)
	if err != nil {
		t.Fatalf("RecoverExpiredJobs() error = %v", err)
	}
	if result.Requeued != 1 || result.Dead != 0 {
		t.Fatalf("recovery result = %+v, want 1 requeued and 0 dead", result)
	}

	got, err := repository.GetJobByID(ctx, tenantID, job.ID)
	if err != nil {
		t.Fatalf("get recovered job: %v", err)
	}
	if got.Status != domain.StatusRetryWait {
		t.Errorf("status = %q, want retry_wait", got.Status)
	}
	if got.Attempts != 1 {
		t.Errorf("attempts = %d, want 1 (recovery must not increment)", got.Attempts)
	}
	if got.LockedBy != nil || got.LeaseToken != nil || got.LockedUntil != nil || got.HeartbeatAt != nil {
		t.Errorf("lease fields not cleared: %+v", got)
	}
	if got.LastError == nil || !strings.Contains(*got.LastError, "lease expired") {
		t.Errorf("last_error = %v, want lease expiry context", got.LastError)
	}
	if got.FinishedAt != nil {
		t.Error("finished_at must be nil for a requeued job")
	}

	claimedAgain, err := repository.ClaimJob(ctx, "healthy-worker", time.Minute)
	if err != nil || claimedAgain == nil {
		t.Fatalf("reclaim job = %v, %v", claimedAgain, err)
	}
	if claimedAgain.ID != job.ID {
		t.Errorf("reclaimed job = %s, want %s", claimedAgain.ID, job.ID)
	}
	if claimedAgain.Attempts != 2 {
		t.Errorf("attempts after reclaim = %d, want 2", claimedAgain.Attempts)
	}
}

func TestRecoveryDeadLettersExhaustedLease(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := mustIsolatedPool(t, databaseURL)

	tenantID := uuid.New()

	repository := postgres.NewJobRepository(pool)
	job := createWorkerJob(t, ctx, repository, tenantID, "integration.recover", `{}`, 0, time.Now().Add(-time.Second), 0, 1)

	claimed, err := repository.ClaimJob(ctx, "crashed-worker", 50*time.Millisecond)
	if err != nil || claimed == nil {
		t.Fatalf("claim job = %v, %v", claimed, err)
	}

	time.Sleep(150 * time.Millisecond)

	result, err := repository.RecoverExpiredJobs(ctx, 10)
	if err != nil {
		t.Fatalf("RecoverExpiredJobs() error = %v", err)
	}
	if result.Requeued != 0 || result.Dead != 1 {
		t.Fatalf("recovery result = %+v, want 0 requeued and 1 dead", result)
	}

	got, err := repository.GetJobByID(ctx, tenantID, job.ID)
	if err != nil {
		t.Fatalf("get dead job: %v", err)
	}
	if got.Status != domain.StatusDead {
		t.Errorf("status = %q, want dead", got.Status)
	}
	if got.FinishedAt == nil {
		t.Error("finished_at is nil for a job dead-lettered by recovery")
	}
	if got.LastError == nil || !strings.Contains(*got.LastError, "lease expired") {
		t.Errorf("last_error = %v, want lease expiry context", got.LastError)
	}
	if got.LockedBy != nil || got.LeaseToken != nil || got.LockedUntil != nil {
		t.Error("lease fields were not cleared")
	}
}

func TestRecoveryLeavesLiveLeaseAlone(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := mustIsolatedPool(t, databaseURL)

	tenantID := uuid.New()

	repository := postgres.NewJobRepository(pool)
	job := createWorkerJob(t, ctx, repository, tenantID, "integration.live", `{}`, 0, time.Now().Add(-time.Second), 0, 3)

	claimed, err := repository.ClaimJob(ctx, "live-worker", time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("claim job = %v, %v", claimed, err)
	}

	result, err := repository.RecoverExpiredJobs(ctx, 10)
	if err != nil {
		t.Fatalf("RecoverExpiredJobs() error = %v", err)
	}
	if result.Requeued != 0 || result.Dead != 0 {
		t.Fatalf("recovery touched a live lease: %+v", result)
	}

	if other, err := repository.ClaimJob(ctx, "other-worker", time.Minute); err != nil || other != nil {
		t.Fatalf("second claim of a live lease = %v, %v; want nil, nil", other, err)
	}

	completed, err := repository.CompleteJob(ctx, job.ID, *claimed.LeaseToken, json.RawMessage(`{"ok":true}`))
	if err != nil || !completed {
		t.Fatalf("CompleteJob() after recovery scan = %v, %v; want true, nil", completed, err)
	}
}

func TestWorkerPermanentFailureDeadLettersImmediately(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := mustIsolatedPool(t, databaseURL)

	tenantID := uuid.New()

	repository := postgres.NewJobRepository(pool)
	job := createWorkerJob(t, ctx, repository, tenantID, "integration.permanent", `{}`, 0, time.Now().Add(-time.Second), 0, 5)

	registry := handlers.New()
	if err := registry.Register("integration.permanent", handlers.HandlerFunc(func(context.Context, domain.Job) (json.RawMessage, error) {
		return nil, retry.Permanent(errors.New("payload cannot be processed"))
	})); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	startWorker(t, newWorkerRunner(t, repository, registry, nil))
	waitForJobStatus(t, pool, job.ID, domain.StatusDead, 5*time.Second)

	got, err := repository.GetJobByID(ctx, tenantID, job.ID)
	if err != nil {
		t.Fatalf("get dead job: %v", err)
	}
	if got.Attempts != 1 {
		t.Errorf("attempts = %d, want 1 (permanent failures must not retry)", got.Attempts)
	}
	if got.LastError == nil || *got.LastError != "payload cannot be processed" {
		t.Errorf("last_error = %v, want payload cannot be processed", got.LastError)
	}
}

func TestWorkerTransientFailureRetriesThenSucceeds(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := mustIsolatedPool(t, databaseURL)

	tenantID := uuid.New()

	repository := postgres.NewJobRepository(pool)
	job := createWorkerJob(t, ctx, repository, tenantID, "integration.transient", `{}`, 0, time.Now().Add(-time.Second), 0, 3)

	var calls atomic.Int32
	registry := handlers.New()
	if err := registry.Register("integration.transient", handlers.HandlerFunc(func(context.Context, domain.Job) (json.RawMessage, error) {
		if calls.Add(1) == 1 {
			return nil, retry.Transient(errors.New("upstream unavailable"))
		}
		return json.RawMessage(`{"ok":true}`), nil
	})); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	// A short ceiling keeps the assertion focused on the retry outcome. The
	// exact jittered schedule is covered deterministically at the repository
	// and policy layers.
	runner := newWorkerRunner(t, repository, registry, func(cfg *worker.Config) {
		cfg.RetryBaseDelay = 10 * time.Millisecond
		cfg.RetryMaxDelay = 10 * time.Millisecond
	})
	startWorker(t, runner)

	waitForJobStatus(t, pool, job.ID, domain.StatusSucceeded, 10*time.Second)

	got, err := repository.GetJobByID(ctx, tenantID, job.ID)
	if err != nil {
		t.Fatalf("get retried job: %v", err)
	}
	if got.Attempts != 2 {
		t.Errorf("attempts = %d, want 2 (one transient retry)", got.Attempts)
	}
	if calls.Load() != 2 {
		t.Errorf("handler calls = %d, want 2", calls.Load())
	}
	if got.LastError != nil {
		t.Errorf("last_error = %v, want cleared after success", got.LastError)
	}
}
