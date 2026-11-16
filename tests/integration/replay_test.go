package integration_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/auth"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/domain"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/postgres"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/transport/http/dto"
	"github.com/google/uuid"
)

func TestReplayDeadJobViaAPI(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := mustIsolatedPool(t, databaseURL)

	tenantID := uuid.New()
	repository := postgres.NewJobRepository(pool)
	job := createWorkerJob(t, ctx, repository, tenantID, "report.generate", `{"report_id":"r-1"}`, 0, time.Now().Add(-time.Second), 0, 3)

	if _, err := pool.Exec(ctx,
		`UPDATE jobs SET status = 'dead', attempts = 2, finished_at = now(), last_error = 'boom' WHERE id = $1`,
		job.ID.String(),
	); err != nil {
		t.Fatalf("mark job dead: %v", err)
	}

	server := newAPI(t, pool)
	operatorToken := createAPIKey(t, ctx, pool, tenantID, "ops-frank", auth.RoleOperator)
	resp, body := doRequest(t, http.MethodPost, server.URL+"/v1/jobs/"+job.ID.String()+"/replay", operatorToken, "", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("replay status = %d, want 200; body=%s", resp.StatusCode, body)
	}

	var replayed dto.JobResponse
	if err := json.Unmarshal(body, &replayed); err != nil {
		t.Fatalf("decode replay response: %v", err)
	}
	if replayed.Status != domain.StatusRetryWait {
		t.Errorf("status = %q, want retry_wait", replayed.Status)
	}
	if replayed.Attempts != 0 {
		t.Errorf("attempts = %d, want 0", replayed.Attempts)
	}
	if replayed.ReplayCount != 1 {
		t.Errorf("replay_count = %d, want 1", replayed.ReplayCount)
	}
	if replayed.ReplayedAt == nil {
		t.Error("replayed_at is nil")
	}
	if replayed.LastError != nil {
		t.Errorf("last_error = %v, want nil", *replayed.LastError)
	}
	if replayed.FinishedAt != nil {
		t.Errorf("finished_at = %v, want nil", *replayed.FinishedAt)
	}

	var (
		replayCount int
		replayedBy  *string
		replayedAt  *time.Time
	)
	if err := pool.QueryRow(ctx,
		`SELECT replay_count, replayed_by, replayed_at FROM jobs WHERE id = $1`,
		job.ID.String(),
	).Scan(&replayCount, &replayedBy, &replayedAt); err != nil {
		t.Fatalf("read replay audit columns: %v", err)
	}
	if replayCount != 1 {
		t.Errorf("durable replay_count = %d, want 1", replayCount)
	}
	if replayedBy == nil || *replayedBy != "ops-frank" {
		t.Errorf("durable replayed_by = %v, want ops-frank", replayedBy)
	}
	if replayedAt == nil {
		t.Error("durable replayed_at is nil")
	}

	// The job is no longer dead, so a second replay is a conflict.
	resp, body = doRequest(t, http.MethodPost, server.URL+"/v1/jobs/"+job.ID.String()+"/replay", operatorToken, "", "")
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("second replay status = %d, want 409; body=%s", resp.StatusCode, body)
	}
	var errResp dto.ErrorResponse
	if err := json.Unmarshal(body, &errResp); err != nil {
		t.Fatalf("decode conflict response: %v", err)
	}
	if errResp.Error.Code != "invalid_transition" {
		t.Errorf("error code = %q, want invalid_transition", errResp.Error.Code)
	}
}

func TestReplayDeadJobIsTenantScoped(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := mustIsolatedPool(t, databaseURL)

	tenantID := uuid.New()
	otherTenantID := uuid.New()
	repository := postgres.NewJobRepository(pool)
	job := createWorkerJob(t, ctx, repository, tenantID, "report.generate", `{"report_id":"r-1"}`, 0, time.Now().Add(-time.Second), 0, 3)

	if _, err := pool.Exec(ctx,
		`UPDATE jobs SET status = 'dead', attempts = 2, finished_at = now(), last_error = 'boom' WHERE id = $1`,
		job.ID.String(),
	); err != nil {
		t.Fatalf("mark job dead: %v", err)
	}

	server := newAPI(t, pool)
	otherOperatorToken := createAPIKey(t, ctx, pool, otherTenantID, "other-ops", auth.RoleOperator)
	resp, body := doRequest(t, http.MethodPost, server.URL+"/v1/jobs/"+job.ID.String()+"/replay", otherOperatorToken, "", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("cross-tenant replay status = %d, want 404; body=%s", resp.StatusCode, body)
	}

	got, err := repository.GetJobByID(ctx, tenantID, job.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if got.Status != domain.StatusDead {
		t.Errorf("status after cross-tenant replay = %q, want dead", got.Status)
	}
	if got.ReplayCount != 0 {
		t.Errorf("replay_count after cross-tenant replay = %d, want 0", got.ReplayCount)
	}
}

func TestReplayRequiresOperatorRole(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := mustIsolatedPool(t, databaseURL)

	tenantID := uuid.New()
	repository := postgres.NewJobRepository(pool)
	job := createWorkerJob(t, ctx, repository, tenantID, "report.generate", `{"report_id":"r-1"}`, 0, time.Now().Add(-time.Second), 0, 3)

	if _, err := pool.Exec(ctx,
		`UPDATE jobs SET status = 'dead', attempts = 2, finished_at = now(), last_error = 'boom' WHERE id = $1`,
		job.ID.String(),
	); err != nil {
		t.Fatalf("mark job dead: %v", err)
	}

	server := newAPI(t, pool)
	clientToken := createAPIKey(t, ctx, pool, tenantID, "client", auth.RoleClient)
	resp, body := doRequest(t, http.MethodPost, server.URL+"/v1/jobs/"+job.ID.String()+"/replay", clientToken, "", "")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("client replay status = %d, want 403; body=%s", resp.StatusCode, body)
	}
	var errResp dto.ErrorResponse
	if err := json.Unmarshal(body, &errResp); err != nil {
		t.Fatalf("decode forbidden response: %v", err)
	}
	if errResp.Error.Code != "forbidden" {
		t.Errorf("error code = %q, want forbidden", errResp.Error.Code)
	}

	got, err := repository.GetJobByID(ctx, tenantID, job.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if got.Status != domain.StatusDead {
		t.Errorf("status after forbidden replay = %q, want dead", got.Status)
	}
	if got.ReplayCount != 0 {
		t.Errorf("replay_count after forbidden replay = %d, want 0", got.ReplayCount)
	}
}
