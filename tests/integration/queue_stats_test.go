package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/postgres"
	"github.com/google/uuid"
)

func TestQueueStatsReportsDueBacklogAndAge(t *testing.T) {
	databaseURL := testDatabaseURL(t)
	pool := mustIsolatedPool(t, databaseURL)
	ctx := context.Background()
	repository := postgres.NewJobRepository(pool)
	tenantID := uuid.New()

	initial, err := repository.QueueStats(ctx)
	if err != nil {
		t.Fatalf("QueueStats() error = %v", err)
	}
	if initial.Depth != 0 || initial.OldestDueSeconds != 0 {
		t.Fatalf("empty queue stats = %+v, want zero", initial)
	}

	createWorkerJob(t, ctx, repository, tenantID, "report.generate", `{"report_id":"old"}`, 0,
		time.Now().Add(-2*time.Minute), 0, 5)
	// Not due yet, attempts exhausted, and a future lease are all excluded.
	createWorkerJob(t, ctx, repository, tenantID, "report.generate", `{"report_id":"future"}`, 0,
		time.Now().Add(time.Hour), 0, 5)
	createWorkerJob(t, ctx, repository, tenantID, "report.generate", `{"report_id":"exhausted"}`, 0,
		time.Now().Add(-time.Minute), 3, 3)

	stats, err := repository.QueueStats(ctx)
	if err != nil {
		t.Fatalf("QueueStats() error = %v", err)
	}
	if stats.Depth != 1 {
		t.Errorf("Depth = %d, want 1", stats.Depth)
	}
	if stats.OldestDueSeconds < 110 || stats.OldestDueSeconds > 130 {
		t.Errorf("OldestDueSeconds = %v, want about 120", stats.OldestDueSeconds)
	}

	claimed, err := repository.ClaimJob(ctx, "stats-worker", time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("ClaimJob() = %v, %v, want one job", claimed, err)
	}
	afterClaim, err := repository.QueueStats(ctx)
	if err != nil {
		t.Fatalf("QueueStats() error = %v", err)
	}
	if afterClaim.Depth != 0 || afterClaim.OldestDueSeconds != 0 {
		t.Errorf("stats after claim = %+v, want zero", afterClaim)
	}
}
