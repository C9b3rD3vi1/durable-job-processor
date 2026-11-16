package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/auth"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/domain"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/handlers"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/jobs"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/postgres"
	httpapi "github.com/C9b3rD3vi1/durable-job-processor/internal/transport/http"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/transport/http/dto"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/worker"
	"github.com/google/uuid"
)

func TestListJobsCursorPaginationAndFilters(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := mustIsolatedPool(t, databaseURL)
	tenantID := uuid.New()
	otherTenantID := uuid.New()
	repository := postgres.NewJobRepository(pool)

	created := []*domain.Job{
		createWorkerJob(t, ctx, repository, tenantID, "report.generate", `{}`, 0, time.Now().Add(-time.Second), 0, 3),
		createWorkerJob(t, ctx, repository, tenantID, "email.send", `{}`, 0, time.Now().Add(-time.Second), 0, 3),
		createWorkerJob(t, ctx, repository, tenantID, "report.generate", `{}`, 0, time.Now().Add(-time.Second), 0, 3),
	}
	other := createWorkerJob(t, ctx, repository, otherTenantID, "report.generate", `{}`, 0, time.Now().Add(-time.Second), 0, 3)

	server := newAPI(t, pool)
	token := createAPIKey(t, ctx, pool, tenantID, "list-client", auth.RoleClient)
	otherToken := createAPIKey(t, ctx, pool, otherTenantID, "other-list-client", auth.RoleClient)

	// The database is the source of truth for the expected newest-first order.
	rows, err := pool.Query(ctx,
		`SELECT id::text FROM jobs WHERE tenant_id = $1 ORDER BY created_at DESC, id DESC`,
		tenantID.String(),
	)
	if err != nil {
		t.Fatalf("query expected order: %v", err)
	}
	var expectedOrder []uuid.UUID
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			t.Fatalf("scan expected id: %v", err)
		}
		id, err := uuid.Parse(raw)
		if err != nil {
			t.Fatalf("parse expected id: %v", err)
		}
		expectedOrder = append(expectedOrder, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate expected order: %v", err)
	}
	rows.Close()
	if len(expectedOrder) != len(created) {
		t.Fatalf("expected order has %d jobs, want %d", len(expectedOrder), len(created))
	}

	resp, body := doRequest(t, http.MethodGet, server.URL+"/v1/jobs?limit=2", token, "", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first page status = %d, want 200; body=%s", resp.StatusCode, body)
	}
	var firstPage dto.ListJobsResponse
	if err := json.Unmarshal(body, &firstPage); err != nil {
		t.Fatalf("decode first page: %v", err)
	}
	if len(firstPage.Jobs) != 2 || firstPage.NextCursor == "" {
		t.Fatalf("first page = %d jobs, cursor %q; want 2 jobs and a cursor", len(firstPage.Jobs), firstPage.NextCursor)
	}
	for i, job := range firstPage.Jobs {
		if job.ID != expectedOrder[i] {
			t.Errorf("first page job %d = %v, want %v", i, job.ID, expectedOrder[i])
		}
	}

	resp, body = doRequest(t, http.MethodGet,
		server.URL+"/v1/jobs?limit=2&cursor="+firstPage.NextCursor, token, "", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("second page status = %d, want 200; body=%s", resp.StatusCode, body)
	}
	var secondPage dto.ListJobsResponse
	if err := json.Unmarshal(body, &secondPage); err != nil {
		t.Fatalf("decode second page: %v", err)
	}
	if len(secondPage.Jobs) != 1 || secondPage.NextCursor != "" {
		t.Fatalf("second page = %d jobs, cursor %q; want 1 job and no cursor",
			len(secondPage.Jobs), secondPage.NextCursor)
	}
	if secondPage.Jobs[0].ID != expectedOrder[2] {
		t.Errorf("second page job = %v, want %v", secondPage.Jobs[0].ID, expectedOrder[2])
	}

	// Status and job_type filters narrow the tenant-scoped page.
	if _, err := pool.Exec(ctx, `UPDATE jobs SET status = 'dead' WHERE id = $1`, created[0].ID.String()); err != nil {
		t.Fatalf("mark job dead: %v", err)
	}
	resp, body = doRequest(t, http.MethodGet, server.URL+"/v1/jobs?status=dead&job_type=report.generate", token, "", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("filtered list status = %d, want 200; body=%s", resp.StatusCode, body)
	}
	var filtered dto.ListJobsResponse
	if err := json.Unmarshal(body, &filtered); err != nil {
		t.Fatalf("decode filtered page: %v", err)
	}
	if len(filtered.Jobs) != 1 || filtered.Jobs[0].ID != created[0].ID {
		t.Fatalf("filtered jobs = %+v, want only %v", filtered.Jobs, created[0].ID)
	}

	// A different tenant sees only its own jobs even with the same filters.
	resp, body = doRequest(t, http.MethodGet, server.URL+"/v1/jobs", otherToken, "", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("other tenant list status = %d, want 200; body=%s", resp.StatusCode, body)
	}
	var otherPage dto.ListJobsResponse
	if err := json.Unmarshal(body, &otherPage); err != nil {
		t.Fatalf("decode other tenant page: %v", err)
	}
	if len(otherPage.Jobs) != 1 || otherPage.Jobs[0].ID != other.ID {
		t.Fatalf("other tenant jobs = %+v, want only %v", otherPage.Jobs, other.ID)
	}

	// An unknown cursor is a client error, not a server error.
	resp, body = doRequest(t, http.MethodGet, server.URL+"/v1/jobs?cursor=bogus", token, "", "")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid cursor status = %d, want 400; body=%s", resp.StatusCode, body)
	}
}

func TestCancelQueuedAndTerminalJobs(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := mustIsolatedPool(t, databaseURL)
	tenantID := uuid.New()
	otherTenantID := uuid.New()
	repository := postgres.NewJobRepository(pool)

	server := newAPI(t, pool)
	token := createAPIKey(t, ctx, pool, tenantID, "cancel-client", auth.RoleClient)
	otherToken := createAPIKey(t, ctx, pool, otherTenantID, "other-cancel-client", auth.RoleClient)

	job := createWorkerJob(t, ctx, repository, tenantID, "report.generate", `{}`, 0, time.Now().Add(time.Hour), 0, 3)

	// Another tenant cannot cancel the job and must not observe it.
	resp, body := doRequest(t, http.MethodPost, server.URL+"/v1/jobs/"+job.ID.String()+"/cancel", otherToken, "", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("cross-tenant cancel status = %d, want 404; body=%s", resp.StatusCode, body)
	}

	resp, body = doRequest(t, http.MethodPost, server.URL+"/v1/jobs/"+job.ID.String()+"/cancel", token, "", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cancel status = %d, want 200; body=%s", resp.StatusCode, body)
	}
	var cancelled dto.JobResponse
	if err := json.Unmarshal(body, &cancelled); err != nil {
		t.Fatalf("decode cancel response: %v", err)
	}
	if cancelled.Status != domain.StatusCancelled {
		t.Errorf("status = %q, want cancelled", cancelled.Status)
	}
	if cancelled.CancelRequestedAt == nil || cancelled.FinishedAt == nil {
		t.Errorf("cancelled job missing timestamps: cancel_requested_at=%v finished_at=%v",
			cancelled.CancelRequestedAt, cancelled.FinishedAt)
	}
	if got := jobStatus(t, pool, job.ID); got != domain.StatusCancelled {
		t.Fatalf("durable status = %q, want cancelled", got)
	}

	// Cancelling again is idempotent.
	resp, body = doRequest(t, http.MethodPost, server.URL+"/v1/jobs/"+job.ID.String()+"/cancel", token, "", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("repeat cancel status = %d, want 200; body=%s", resp.StatusCode, body)
	}

	// Terminal jobs cannot be cancelled.
	terminal := createWorkerJob(t, ctx, repository, tenantID, "report.generate", `{}`, 0, time.Now().Add(-time.Second), 0, 3)
	if _, err := pool.Exec(ctx,
		`UPDATE jobs SET status = 'succeeded', finished_at = now() WHERE id = $1`,
		terminal.ID.String(),
	); err != nil {
		t.Fatalf("mark job succeeded: %v", err)
	}
	resp, body = doRequest(t, http.MethodPost, server.URL+"/v1/jobs/"+terminal.ID.String()+"/cancel", token, "", "")
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("terminal cancel status = %d, want 409; body=%s", resp.StatusCode, body)
	}
	var errResp dto.ErrorResponse
	if err := json.Unmarshal(body, &errResp); err != nil {
		t.Fatalf("decode conflict response: %v", err)
	}
	if errResp.Error.Code != "invalid_transition" {
		t.Errorf("error code = %q, want invalid_transition", errResp.Error.Code)
	}
}

func TestCancelRunningJobIsCooperative(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := mustIsolatedPool(t, databaseURL)
	tenantID := uuid.New()
	repository := postgres.NewJobRepository(pool)

	job := createWorkerJob(t, ctx, repository, tenantID, "integration.cancel", `{}`, 0, time.Now().Add(-time.Second), 0, 3)

	started := make(chan struct{})
	registry := handlers.New()
	if err := registry.Register("integration.cancel", handlers.HandlerFunc(func(handlerCtx context.Context, _ domain.Job) (json.RawMessage, error) {
		close(started)
		<-handlerCtx.Done()
		return nil, handlerCtx.Err()
	})); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	runner := newWorkerRunner(t, repository, registry, func(cfg *worker.Config) {
		cfg.LeaseDuration = 500 * time.Millisecond
		cfg.HeartbeatInterval = 20 * time.Millisecond
		cfg.HandlerTimeout = 10 * time.Second
	})
	startWorker(t, runner)

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not start")
	}

	server := newAPI(t, pool)
	token := createAPIKey(t, ctx, pool, tenantID, "running-cancel-client", auth.RoleClient)
	resp, body := doRequest(t, http.MethodPost, server.URL+"/v1/jobs/"+job.ID.String()+"/cancel", token, "", "")
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("running cancel status = %d, want 202; body=%s", resp.StatusCode, body)
	}
	if got := resp.Header.Get("Cancel-Requested"); got != "true" {
		t.Errorf("Cancel-Requested = %q, want true", got)
	}

	waitForJobStatus(t, pool, job.ID, domain.StatusCancelled, 5*time.Second)
	got, err := repository.GetJobByID(ctx, tenantID, job.ID)
	if err != nil {
		t.Fatalf("get cancelled job: %v", err)
	}
	if got.CancelRequestedAt == nil {
		t.Error("cancel_requested_at is nil after cooperative cancellation")
	}
	if got.FinishedAt == nil {
		t.Error("finished_at is nil after cooperative cancellation")
	}
	if got.LockedBy != nil || got.LeaseToken != nil || got.LockedUntil != nil || got.HeartbeatAt != nil {
		t.Errorf("lease fields not cleared after cancellation: %+v", got)
	}
}

func TestRecoveryCancelsExpiredJobWithCancellationRequest(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := mustIsolatedPool(t, databaseURL)
	tenantID := uuid.New()
	repository := postgres.NewJobRepository(pool)

	job := createWorkerJob(t, ctx, repository, tenantID, "integration.recover-cancel", `{}`, 0, time.Now().Add(-time.Second), 0, 3)
	if _, err := repository.ClaimJob(ctx, "crashed-worker", 50*time.Millisecond); err != nil {
		t.Fatalf("claim job: %v", err)
	}
	if _, _, err := repository.CancelJob(ctx, tenantID, job.ID); err != nil {
		t.Fatalf("request cancellation: %v", err)
	}

	time.Sleep(150 * time.Millisecond)

	result, err := repository.RecoverExpiredJobs(ctx, 10)
	if err != nil {
		t.Fatalf("RecoverExpiredJobs() error = %v", err)
	}
	if result.Cancelled != 1 || result.Requeued != 0 || result.Dead != 0 {
		t.Fatalf("recovery result = %+v, want 1 cancelled and nothing else", result)
	}
	if got := jobStatus(t, pool, job.ID); got != domain.StatusCancelled {
		t.Fatalf("status = %q, want cancelled", got)
	}
}

func TestTenantOutstandingQuota(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := mustIsolatedPool(t, databaseURL)
	tenantID := uuid.New()
	repository := postgres.NewJobRepository(pool)

	newJob := func(key *string) *domain.Job {
		return &domain.Job{
			ID:             uuid.New(),
			TenantID:       tenantID,
			JobType:        "report.generate",
			Payload:        json.RawMessage(`{}`),
			Status:         domain.StatusQueued,
			Priority:       0,
			RunAt:          time.Now().Add(-time.Second),
			MaxAttempts:    3,
			IdempotencyKey: key,
		}
	}

	key := "quota-key"
	if _, _, err := repository.CreateJob(ctx, newJob(&key), 2); err != nil {
		t.Fatalf("create first job: %v", err)
	}
	if _, _, err := repository.CreateJob(ctx, newJob(nil), 2); err != nil {
		t.Fatalf("create second job: %v", err)
	}

	// The quota is reached; a distinct submission is rejected.
	if _, _, err := repository.CreateJob(ctx, newJob(nil), 2); !errors.Is(err, domain.ErrQuotaExceeded) {
		t.Fatalf("third CreateJob() error = %v, want ErrQuotaExceeded", err)
	}

	// An idempotent replay is not a new job and must not be rejected by quota.
	replayed, created, err := repository.CreateJob(ctx, newJob(&key), 2)
	if err != nil {
		t.Fatalf("idempotent CreateJob() error = %v", err)
	}
	if created {
		t.Error("idempotent replay reported created=true")
	}
	if replayed == nil || replayed.IdempotencyKey == nil || *replayed.IdempotencyKey != key {
		t.Errorf("idempotent replay = %+v, want the existing key", replayed)
	}

	// A terminal job no longer consumes quota.
	if _, err := pool.Exec(ctx, `UPDATE jobs SET status = 'succeeded' WHERE idempotency_key = $1`, key); err != nil {
		t.Fatalf("mark job succeeded: %v", err)
	}
	if _, _, err := repository.CreateJob(ctx, newJob(nil), 2); err != nil {
		t.Fatalf("CreateJob() after freeing quota = %v", err)
	}
}

func TestTenantOutstandingQuotaIsAtomic(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := mustIsolatedPool(t, databaseURL)
	tenantID := uuid.New()
	repository := postgres.NewJobRepository(pool)

	const (
		attempts = 12
		quota    = 3
	)
	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		succeeded int
		rejected  int
	)
	start := make(chan struct{})
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, _, err := repository.CreateJob(ctx, &domain.Job{
				ID:          uuid.New(),
				TenantID:    tenantID,
				JobType:     "report.generate",
				Payload:     json.RawMessage(`{}`),
				Status:      domain.StatusQueued,
				RunAt:       time.Now().Add(-time.Second),
				MaxAttempts: 3,
			}, quota)

			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				succeeded++
			case errors.Is(err, domain.ErrQuotaExceeded):
				rejected++
			default:
				t.Errorf("CreateJob() error = %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	if succeeded != quota || rejected != attempts-quota {
		t.Fatalf("succeeded=%d rejected=%d, want %d and %d", succeeded, rejected, quota, attempts-quota)
	}
	var count int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM jobs WHERE tenant_id = $1`,
		tenantID.String(),
	).Scan(&count); err != nil {
		t.Fatalf("count jobs: %v", err)
	}
	if count != quota {
		t.Fatalf("durable jobs = %d, want %d", count, quota)
	}
}

func TestSubmitQuotaExceededViaAPI(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := mustIsolatedPool(t, databaseURL)
	tenantID := uuid.New()
	repository := postgres.NewJobRepository(pool)
	validator := jobs.NewValidator(handlers.DefaultRegistry(), 256*1024, 5)
	service := jobs.NewService(repository, validator, jobs.WithMaxOutstandingJobs(1))
	authenticator := auth.NewAuthenticator(postgres.NewAPIKeyRepository(pool))
	server := httptest.NewServer(httpapi.NewRouter(httpapi.Options{
		JobService:    service,
		Pinger:        pool,
		Authenticator: authenticator,
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxBodyBytes:  1 << 20,
	}))
	t.Cleanup(server.Close)

	token := createAPIKey(t, ctx, pool, tenantID, "quota-client", auth.RoleClient)
	requestBody := `{"job_type":"report.generate","payload":{"report_id":"r-1"}}`

	resp, body := doRequest(t, http.MethodPost, server.URL+"/v1/jobs", token, "quota-first", requestBody)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("first submit status = %d, want 202; body=%s", resp.StatusCode, body)
	}

	resp, body = doRequest(t, http.MethodPost, server.URL+"/v1/jobs", token, "quota-second", requestBody)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("second submit status = %d, want 429; body=%s", resp.StatusCode, body)
	}
	var errResp dto.ErrorResponse
	if err := json.Unmarshal(body, &errResp); err != nil {
		t.Fatalf("decode quota response: %v", err)
	}
	if errResp.Error.Code != "quota_exceeded" {
		t.Errorf("error code = %q, want quota_exceeded", errResp.Error.Code)
	}
}
