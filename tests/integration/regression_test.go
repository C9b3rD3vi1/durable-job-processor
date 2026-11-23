package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func jobReportID(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id uuid.UUID) string {
	t.Helper()
	var reportID string
	if err := pool.QueryRow(ctx, `SELECT payload->>'report_id' FROM jobs WHERE id = $1`, id).Scan(&reportID); err != nil {
		t.Fatalf("read job payload: %v", err)
	}
	return reportID
}

func TestWithTxRollsBackOnErrorAndPanic(t *testing.T) {
	pool := mustIsolatedPool(t, testDatabaseURL(t))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	repository := postgres.NewJobRepository(pool)
	tenantID := uuid.New()
	job := createWorkerJob(t, ctx, repository, tenantID, "report.generate",
		`{"report_id":"tx-original"}`, 0, time.Now().Add(-time.Second), 0, 1)

	const changed = `{"report_id":"tx-changed"}`

	err := postgres.WithTx(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE jobs SET payload = $1::jsonb WHERE id = $2`, changed, job.ID); err != nil {
			return err
		}
		return errors.New("rollback")
	})
	if err == nil {
		t.Fatal("WithTx() error = nil, want the injected error")
	}
	if got := jobReportID(t, ctx, pool, job.ID); got != "tx-original" {
		t.Fatalf("report_id after error = %q, want tx-original", got)
	}

	func() {
		defer func() {
			if recovered := recover(); recovered == nil {
				t.Error("WithTx() did not propagate the injected panic")
			}
		}()
		_ = postgres.WithTx(ctx, pool, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, `UPDATE jobs SET payload = $1::jsonb WHERE id = $2`, changed, job.ID); err != nil {
				return err
			}
			panic("boom")
		})
	}()
	if got := jobReportID(t, ctx, pool, job.ID); got != "tx-original" {
		t.Fatalf("report_id after panic = %q, want tx-original", got)
	}
}

func TestHeartbeatDoesNotRenewExpiredLease(t *testing.T) {
	pool := mustIsolatedPool(t, testDatabaseURL(t))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	repository := postgres.NewJobRepository(pool)
	tenantID := uuid.New()
	createWorkerJob(t, ctx, repository, tenantID, "report.generate",
		`{"report_id":"heartbeat-expired"}`, 0, time.Now().Add(-time.Second), 0, 1)

	claimed, err := repository.ClaimJob(ctx, "expired-lease-worker", time.Minute)
	if err != nil {
		t.Fatalf("ClaimJob() error = %v", err)
	}
	if claimed == nil || claimed.LeaseToken == nil {
		t.Fatalf("ClaimJob() = %+v, want a claimed job with a lease token", claimed)
	}

	if _, err := pool.Exec(ctx,
		`UPDATE jobs SET locked_until = now() - interval '1 second' WHERE id = $1`, claimed.ID,
	); err != nil {
		t.Fatalf("expire lease: %v", err)
	}

	result, err := repository.HeartbeatJob(ctx, claimed.ID, *claimed.LeaseToken, time.Minute)
	if err != nil {
		t.Fatalf("HeartbeatJob() error = %v", err)
	}
	if result.Renewed {
		t.Error("HeartbeatJob() renewed an expired lease, want fenced")
	}
	if result.CancelRequested {
		t.Error("HeartbeatJob() reported cancellation for an expired lease")
	}

	var stillExpired bool
	if err := pool.QueryRow(ctx,
		`SELECT locked_until < now() FROM jobs WHERE id = $1`, claimed.ID,
	).Scan(&stillExpired); err != nil {
		t.Fatalf("read locked_until: %v", err)
	}
	if !stillExpired {
		t.Error("expired lease was extended by a rejected heartbeat")
	}
}
