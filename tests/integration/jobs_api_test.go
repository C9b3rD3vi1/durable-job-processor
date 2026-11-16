package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
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
	"github.com/C9b3rD3vi1/durable-job-processor/migrations"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testDatabaseURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		url = os.Getenv("DATABASE_URL")
	}
	if url == "" {
		t.Skip("set TEST_DATABASE_URL or DATABASE_URL to run PostgreSQL integration tests")
	}
	return url
}

func mustPool(t *testing.T, url string) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := postgres.Connect(ctx, postgres.Config{
		URL:            url,
		MaxConns:       5,
		ConnectTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("connect to PostgreSQL: %v", err)
	}
	return pool
}

func applyMigrations(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := postgres.Migrate(ctx, pool, migrations.FS); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	// Running again must be a no-op and proves version tracking works.
	applied, err := postgres.Migrate(ctx, pool, migrations.FS)
	if err != nil {
		t.Fatalf("re-apply migrations: %v", err)
	}
	if applied != 0 {
		t.Fatalf("second Migrate() applied %d migrations, want 0", applied)
	}
}

func newAPI(t *testing.T, pool *pgxpool.Pool) *httptest.Server {
	t.Helper()
	repository := postgres.NewJobRepository(pool)
	validator := jobs.NewValidator(handlers.DefaultRegistry(), 256*1024, 5)
	service := jobs.NewService(repository, validator)
	authenticator := auth.NewAuthenticator(postgres.NewAPIKeyRepository(pool))
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	router := httpapi.NewRouter(httpapi.Options{
		JobService:    service,
		Pinger:        pool,
		Authenticator: authenticator,
		Logger:        logger,
		MaxBodyBytes:  1 << 20,
	})
	server := httptest.NewServer(router)
	t.Cleanup(server.Close)
	return server
}

// createAPIKey provisions a key directly through the repository and returns its
// one-time plaintext token. Tests never store plaintext anywhere else.
func createAPIKey(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tenantID uuid.UUID, name string, role auth.Role) string {
	t.Helper()
	token, hash, err := auth.GenerateToken()
	if err != nil {
		t.Fatalf("generate API key: %v", err)
	}
	if _, err := postgres.NewAPIKeyRepository(pool).CreateAPIKey(ctx, tenantID, name, role, hash); err != nil {
		t.Fatalf("create API key: %v", err)
	}
	return token
}

func doRequest(t *testing.T, method, url, token, idempotencyKey string, body string) (*http.Response, []byte) {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = bytes.NewBufferString(body)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("perform request: %v", err)
	}
	data, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return resp, data
}

func TestJobSubmissionPersistsAcrossRestart(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	tenantID := uuid.New()
	otherTenantID := uuid.New()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool1 := mustPool(t, databaseURL)
	applyMigrations(t, pool1)

	server1 := newAPI(t, pool1)

	tenantToken := createAPIKey(t, ctx, pool1, tenantID, "client", auth.RoleClient)
	otherTenantToken := createAPIKey(t, ctx, pool1, otherTenantID, "other-client", auth.RoleClient)

	requestBody := `{"job_type":"report.generate","payload":{"report_id":"r-1","format":"pdf"},"priority":3,"max_attempts":4}`
	resp, body := doRequest(t, http.MethodPost, server1.URL+"/v1/jobs", tenantToken, "submit-once", requestBody)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("POST status = %d, want 202; body=%s", resp.StatusCode, body)
	}
	if location := resp.Header.Get("Location"); location == "" {
		t.Fatal("POST Location header is empty")
	}

	var created dto.JobResponse
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatalf("decode created job: %v", err)
	}
	if created.ID == uuid.Nil || created.Status != "queued" {
		t.Fatalf("created job = %+v, want queued job with id", created)
	}
	if created.Priority != 3 || created.MaxAttempts != 4 {
		t.Fatalf("created limits = priority %d, max_attempts %d; want 3, 4", created.Priority, created.MaxAttempts)
	}

	// Simulate an API restart: close the server and the entire connection pool,
	// then build a new pool and router against the same database.
	server1.Close()
	pool1.Close()

	pool2 := mustPool(t, databaseURL)
	defer pool2.Close()
	server2 := newAPI(t, pool2)
	defer func() {
		_, _ = pool2.Exec(context.Background(), `DELETE FROM jobs WHERE tenant_id = $1`, tenantID.String())
		_, _ = pool2.Exec(context.Background(), `DELETE FROM api_keys WHERE tenant_id IN ($1, $2)`, tenantID.String(), otherTenantID.String())
	}()

	resp, body = doRequest(t, http.MethodGet, server2.URL+"/v1/jobs/"+created.ID.String(), tenantToken, "", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET after restart status = %d, want 200; body=%s", resp.StatusCode, body)
	}
	var fetched dto.JobResponse
	if err := json.Unmarshal(body, &fetched); err != nil {
		t.Fatalf("decode fetched job: %v", err)
	}
	// A worker may claim the job between submission and this read, so the test
	// asserts durable identity rather than freezing the lifecycle status.
	if fetched.ID != created.ID || fetched.JobType != created.JobType || fetched.TenantID != created.TenantID {
		t.Fatalf("fetched job = %+v, want the original job %v", fetched, created.ID)
	}
	if fetched.CreatedAt.IsZero() {
		t.Fatal("fetched job has no created_at timestamp")
	}

	// The same idempotency key must return the existing durable job.
	resp, body = doRequest(t, http.MethodPost, server2.URL+"/v1/jobs", tenantToken, "submit-once", requestBody)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("duplicate POST status = %d, want 200; body=%s", resp.StatusCode, body)
	}
	if replayed := resp.Header.Get("Idempotent-Replayed"); replayed != "true" {
		t.Fatalf("Idempotent-Replayed = %q, want true", replayed)
	}
	var replayedJob dto.JobResponse
	if err := json.Unmarshal(body, &replayedJob); err != nil {
		t.Fatalf("decode replayed job: %v", err)
	}
	if replayedJob.ID != created.ID {
		t.Fatalf("replayed job id = %v, want %v", replayedJob.ID, created.ID)
	}

	var count int
	if err := pool2.QueryRow(ctx,
		`SELECT count(*) FROM jobs WHERE tenant_id = $1`,
		tenantID.String(),
	).Scan(&count); err != nil {
		t.Fatalf("count jobs: %v", err)
	}
	if count != 1 {
		t.Fatalf("job count = %d, want 1", count)
	}

	// Tenant scoping must hide the job from another tenant.
	resp, body = doRequest(t, http.MethodGet, server2.URL+"/v1/jobs/"+created.ID.String(), otherTenantToken, "", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("cross-tenant GET status = %d, want 404; body=%s", resp.StatusCode, body)
	}

	// Identity is required at the edge.
	resp, body = doRequest(t, http.MethodGet, server2.URL+"/v1/jobs/"+created.ID.String(), "", "", "")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("missing-tenant GET status = %d, want 401; body=%s", resp.StatusCode, body)
	}

	// Invalid payloads must be rejected before persistence.
	invalidBody := `{"job_type":"report.generate","payload":[]}`
	resp, body = doRequest(t, http.MethodPost, server2.URL+"/v1/jobs", tenantToken, "invalid", invalidBody)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid payload status = %d, want 400; body=%s", resp.StatusCode, body)
	}
	if resp.StatusCode == http.StatusBadRequest {
		var errResp dto.ErrorResponse
		if err := json.Unmarshal(body, &errResp); err != nil {
			t.Fatalf("decode error response: %v", err)
		}
		if errResp.Error.Code != "invalid_job" {
			t.Fatalf("error code = %q, want invalid_job", errResp.Error.Code)
		}
	}

	// The failed submission must not have created a second row.
	if err := pool2.QueryRow(ctx,
		`SELECT count(*) FROM jobs WHERE tenant_id = $1`,
		tenantID.String(),
	).Scan(&count); err != nil {
		t.Fatalf("count jobs after invalid submission: %v", err)
	}
	if count != 1 {
		t.Fatalf("job count after invalid submission = %d, want 1", count)
	}

	var applied int
	if err := pool2.QueryRow(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&applied); err != nil {
		t.Fatalf("count schema_migrations: %v", err)
	}
	if applied == 0 {
		t.Fatal("schema_migrations is empty")
	}
}

func TestRepositoryTenantIsolation(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := mustPool(t, databaseURL)
	defer pool.Close()
	applyMigrations(t, pool)

	repository := postgres.NewJobRepository(pool)
	tenantID := uuid.New()
	otherTenantID := uuid.New()
	defer func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM jobs WHERE tenant_id = $1`, tenantID.String())
	}()

	validator := jobs.NewValidator(handlers.DefaultRegistry(), 256*1024, 5)
	service := jobs.NewService(repository, validator)
	job, _, err := service.Submit(ctx, jobs.SubmitInput{
		TenantID: tenantID,
		JobType:  "email.send",
		Payload:  json.RawMessage(`{"to":"person@example.com"}`),
	})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	if _, err := repository.GetJobByID(ctx, otherTenantID, job.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("cross-tenant GetJobByID() error = %v, want ErrNotFound", err)
	}

	got, err := repository.GetJobByID(ctx, tenantID, job.ID)
	if err != nil {
		t.Fatalf("GetJobByID() error = %v", err)
	}
	if got.ID != job.ID || got.TenantID != tenantID {
		t.Fatalf("got job = %+v, want id %v tenant %v", got, job.ID, tenantID)
	}
}

// TestConcurrentIdempotentSubmissions proves that parallel submissions sharing
// an idempotency key converge on exactly one durable row.
func TestConcurrentIdempotentSubmissions(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := mustPool(t, databaseURL)
	defer pool.Close()
	applyMigrations(t, pool)

	tenantID := uuid.New()
	defer func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM jobs WHERE tenant_id = $1`, tenantID.String())
	}()

	server := newAPI(t, pool)
	token := createAPIKey(t, ctx, pool, tenantID, "concurrent-client", auth.RoleClient)
	const requestCount = 8
	requestBody := `{"job_type":"webhook.deliver","payload":{"url":"https://example.com/hook"}}`

	type result struct {
		status int
		replay string
		job    dto.JobResponse
		err    error
	}

	results := make(chan result, requestCount)
	var wg sync.WaitGroup
	for i := 0; i < requestCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			req, err := http.NewRequest(http.MethodPost, server.URL+"/v1/jobs", bytes.NewBufferString(requestBody))
			if err != nil {
				results <- result{err: err}
				return
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+token)
			req.Header.Set("Idempotency-Key", "concurrent-key")

			resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
			if err != nil {
				results <- result{err: err}
				return
			}
			data, readErr := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if readErr != nil {
				results <- result{err: readErr}
				return
			}

			var job dto.JobResponse
			if err := json.Unmarshal(data, &job); err != nil {
				results <- result{err: err}
				return
			}
			results <- result{
				status: resp.StatusCode,
				replay: resp.Header.Get("Idempotent-Replayed"),
				job:    job,
			}
		}()
	}
	wg.Wait()
	close(results)

	created := 0
	ids := make(map[uuid.UUID]int, 1)
	for res := range results {
		if res.err != nil {
			t.Fatalf("concurrent submission failed: %v", res.err)
		}
		switch res.status {
		case http.StatusAccepted:
			created++
			if res.replay != "" {
				t.Errorf("fresh submission has Idempotent-Replayed = %q, want empty", res.replay)
			}
		case http.StatusOK:
			if res.replay != "true" {
				t.Errorf("replayed submission has Idempotent-Replayed = %q, want true", res.replay)
			}
		default:
			t.Errorf("submission status = %d, want 200 or 202", res.status)
		}
		ids[res.job.ID]++
	}

	if created != 1 {
		t.Errorf("created rows = %d, want 1", created)
	}
	if len(ids) != 1 {
		t.Fatalf("distinct job ids = %d, want 1 (%v)", len(ids), ids)
	}

	var count int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM jobs WHERE tenant_id = $1`,
		tenantID.String(),
	).Scan(&count); err != nil {
		t.Fatalf("count jobs: %v", err)
	}
	if count != 1 {
		t.Fatalf("job count = %d, want 1", count)
	}
}
