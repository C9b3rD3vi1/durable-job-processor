package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/domain"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/queue"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ClaimJob atomically claims one due job using FOR UPDATE SKIP LOCKED.
//
// The claim is a single statement, so no transaction or row lock is held while
// the handler runs. attempts increments only when the claim succeeds, and the
// database clock decides which jobs are due and when the lease expires.
func (r *JobRepository) ClaimJob(ctx context.Context, workerID string, leaseDuration time.Duration) (*domain.Job, error) {
	if workerID == "" {
		return nil, fmt.Errorf("%w: worker id is required", domain.ErrInvalidArgument)
	}
	if leaseDuration <= 0 {
		return nil, fmt.Errorf("%w: lease duration must be positive", domain.ErrInvalidArgument)
	}

	const query = `
		WITH candidate AS (
			SELECT id
			FROM jobs
			WHERE status IN ('queued', 'retry_wait')
			  AND run_at <= now()
			  AND attempts < max_attempts
			  AND cancel_requested_at IS NULL
			ORDER BY priority DESC, run_at ASC, created_at ASC
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		),
		claimed AS (
			UPDATE jobs AS j
			SET status       = 'running',
			    attempts     = j.attempts + 1,
			    locked_by    = $1,
			    lease_token  = $2,
			    locked_until = now() + make_interval(secs => $3),
			    heartbeat_at = now(),
			    started_at   = COALESCE(j.started_at, now()),
			    updated_at   = now()
			FROM candidate
			WHERE j.id = candidate.id
			RETURNING j.*
		)
		SELECT ` + jobColumns + ` FROM claimed`

	job, err := scanJob(r.pool.QueryRow(ctx, query,
		workerID,
		uuid.NewString(),
		leaseDuration.Seconds(),
	))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("claim job: %w", err)
	}
	return job, nil
}

// QueueStats reports the due backlog using the same eligibility predicate as
// ClaimJob. The database clock computes the oldest age so worker clock skew
// cannot distort backlog alerts.
func (r *JobRepository) QueueStats(ctx context.Context) (queue.Stats, error) {
	const query = `
		SELECT count(*)::bigint,
		       COALESCE(EXTRACT(EPOCH FROM (now() - min(run_at)))::float8, 0)
		FROM jobs
		WHERE status IN ('queued', 'retry_wait')
		  AND run_at <= now()
		  AND attempts < max_attempts
		  AND cancel_requested_at IS NULL`

	var stats queue.Stats
	if err := r.pool.QueryRow(ctx, query).Scan(&stats.Depth, &stats.OldestDueSeconds); err != nil {
		return queue.Stats{}, fmt.Errorf("queue stats: %w", err)
	}
	return stats, nil
}

// CompleteJob records success only when the caller still holds the lease.
// Terminal writes (complete, fail and cancel-running) intentionally do not also
// require an unexpired lease: the lease token already fences stale owners, and
// accepting a late result avoids re-executing work that finished just after the
// lease deadline. Heartbeat is stricter because it is the signal the worker uses
// to retain ownership; once heartbeat fails, recovery owns abandoned leases.
func (r *JobRepository) CompleteJob(ctx context.Context, id, leaseToken uuid.UUID, result json.RawMessage) (bool, error) {
	if id == uuid.Nil || leaseToken == uuid.Nil {
		return false, fmt.Errorf("%w: job id and lease token are required", domain.ErrInvalidArgument)
	}
	if len(result) > 0 && !json.Valid(result) {
		return false, fmt.Errorf("%w: result must be valid JSON", domain.ErrInvalidArgument)
	}

	const query = `
		UPDATE jobs
		SET status       = 'succeeded',
		    result       = $3::jsonb,
		    last_error   = NULL,
		    finished_at  = now(),
		    updated_at   = now(),
		    locked_by    = NULL,
		    lease_token  = NULL,
		    locked_until = NULL,
		    heartbeat_at = NULL
		WHERE id = $1
		  AND status = 'running'
		  AND lease_token = $2
		  AND cancel_requested_at IS NULL`

	var resultParam any
	if len(result) > 0 {
		resultParam = string(result)
	}

	tag, err := r.pool.Exec(ctx, query, id.String(), leaseToken.String(), resultParam)
	if err != nil {
		return false, fmt.Errorf("complete job: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// FailJob records a handler failure only when the caller still holds the lease.
// A positive retryDelay reschedules the job unless attempts are exhausted.
func (r *JobRepository) FailJob(ctx context.Context, id, leaseToken uuid.UUID, cause string, retryDelay time.Duration) (bool, error) {
	if id == uuid.Nil || leaseToken == uuid.Nil {
		return false, fmt.Errorf("%w: job id and lease token are required", domain.ErrInvalidArgument)
	}

	const query = `
		UPDATE jobs
		SET status = CASE
		        WHEN $3::double precision IS NULL OR attempts >= max_attempts
		            THEN 'dead'::job_status
		        ELSE 'retry_wait'::job_status
		    END,
		    run_at = CASE
		        WHEN $3::double precision IS NOT NULL AND attempts < max_attempts
		            THEN now() + make_interval(secs => $3::double precision)
		        ELSE run_at
		    END,
		    finished_at = CASE
		        WHEN $3::double precision IS NULL OR attempts >= max_attempts
		            THEN now()
		        ELSE NULL
		    END,
		    last_error   = $4,
		    updated_at   = now(),
		    locked_by    = NULL,
		    lease_token  = NULL,
		    locked_until = NULL,
		    heartbeat_at = NULL
		WHERE id = $1
		  AND status = 'running'
		  AND lease_token = $2
		  AND cancel_requested_at IS NULL`

	var retrySeconds any
	if retryDelay > 0 {
		retrySeconds = retryDelay.Seconds()
	}

	tag, err := r.pool.Exec(ctx, query,
		id.String(),
		leaseToken.String(),
		retrySeconds,
		cause,
	)
	if err != nil {
		return false, fmt.Errorf("fail job: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// HeartbeatJob renews the lease only while the caller still holds an unexpired
// lease. A zero rows-affected result is the fencing signal that the worker has
// lost ownership and must stop state changes for this attempt. The guard also
// prevents a stale worker from renewing a lease after recovery has begun. The
// renewal reports a durable cooperative cancellation request so the worker can
// stop the handler without waiting for its lease to expire.
func (r *JobRepository) HeartbeatJob(ctx context.Context, id, leaseToken uuid.UUID, leaseDuration time.Duration) (queue.HeartbeatResult, error) {
	if id == uuid.Nil || leaseToken == uuid.Nil {
		return queue.HeartbeatResult{}, fmt.Errorf("%w: job id and lease token are required", domain.ErrInvalidArgument)
	}
	if leaseDuration <= 0 {
		return queue.HeartbeatResult{}, fmt.Errorf("%w: lease duration must be positive", domain.ErrInvalidArgument)
	}

	const query = `
		UPDATE jobs
		SET locked_until = now() + make_interval(secs => $1),
		    heartbeat_at = now(),
		    updated_at   = now()
		WHERE id = $2
		  AND status = 'running'
		  AND lease_token = $3
		  AND locked_until >= now()
		RETURNING cancel_requested_at IS NOT NULL`

	var cancelRequested bool
	err := r.pool.QueryRow(ctx, query,
		leaseDuration.Seconds(),
		id.String(),
		leaseToken.String(),
	).Scan(&cancelRequested)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return queue.HeartbeatResult{Renewed: false}, nil
		}
		return queue.HeartbeatResult{}, fmt.Errorf("heartbeat job: %w", err)
	}
	return queue.HeartbeatResult{Renewed: true, CancelRequested: cancelRequested}, nil
}

// CancelRunningJob finalizes a running job as cancelled. The update is fenced
// by the lease token and requires a durable cancellation request, so a worker
// that races with completion or loses its lease cannot cancel another attempt.
func (r *JobRepository) CancelRunningJob(ctx context.Context, id, leaseToken uuid.UUID) (bool, error) {
	if id == uuid.Nil || leaseToken == uuid.Nil {
		return false, fmt.Errorf("%w: job id and lease token are required", domain.ErrInvalidArgument)
	}

	const query = `
		UPDATE jobs
		SET status       = 'cancelled',
		    finished_at  = now(),
		    updated_at   = now(),
		    locked_by    = NULL,
		    lease_token  = NULL,
		    locked_until = NULL,
		    heartbeat_at = NULL
		WHERE id = $1
		  AND status = 'running'
		  AND lease_token = $2
		  AND cancel_requested_at IS NOT NULL`

	tag, err := r.pool.Exec(ctx, query, id.String(), leaseToken.String())
	if err != nil {
		return false, fmt.Errorf("cancel running job: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// RecoverExpiredJobs resolves running jobs whose lease has expired. The scan
// and update are one statement, so several workers can run recovery without
// claiming the same abandoned row twice. Attempts are not incremented here;
// the next claim does that. A job with a pending cancellation request is
// finalized as cancelled, and a job that already consumed its attempts is
// dead-lettered with a safe diagnostic message.
func (r *JobRepository) RecoverExpiredJobs(ctx context.Context, limit int) (queue.RecoveryResult, error) {
	if limit <= 0 {
		return queue.RecoveryResult{}, fmt.Errorf("%w: recovery limit must be positive", domain.ErrInvalidArgument)
	}

	const query = `
		WITH expired AS (
			SELECT id
			FROM jobs
			WHERE status = 'running'
			  AND (locked_until IS NULL OR locked_until < now())
			ORDER BY locked_until ASC NULLS FIRST
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		),
		updated AS (
			UPDATE jobs AS j
			SET status = CASE
			        WHEN j.cancel_requested_at IS NOT NULL THEN 'cancelled'::job_status
			        WHEN j.attempts >= j.max_attempts THEN 'dead'::job_status
			        ELSE 'retry_wait'::job_status
			    END,
			    run_at = CASE
			        WHEN j.cancel_requested_at IS NOT NULL THEN j.run_at
			        WHEN j.attempts >= j.max_attempts THEN j.run_at
			        ELSE now()
			    END,
			    finished_at = CASE
			        WHEN j.cancel_requested_at IS NOT NULL THEN now()
			        WHEN j.attempts >= j.max_attempts THEN now()
			        ELSE NULL
			    END,
			    last_error = CASE
			        WHEN j.cancel_requested_at IS NOT NULL
			            THEN 'cancellation requested; lease expired before handler stopped'
			        WHEN j.attempts >= j.max_attempts
			            THEN 'lease expired after maximum attempts'
			        ELSE 'lease expired; requeued for retry'
			    END,
			    updated_at   = now(),
			    locked_by    = NULL,
			    lease_token  = NULL,
			    locked_until = NULL,
			    heartbeat_at = NULL
			FROM expired
			WHERE j.id = expired.id
			RETURNING j.status::text AS status
		)
		SELECT
			count(*) FILTER (WHERE status = 'retry_wait'),
			count(*) FILTER (WHERE status = 'dead'),
			count(*) FILTER (WHERE status = 'cancelled')
		FROM updated`

	var requeued, dead, cancelled int64
	if err := r.pool.QueryRow(ctx, query, limit).Scan(&requeued, &dead, &cancelled); err != nil {
		return queue.RecoveryResult{}, fmt.Errorf("recover expired jobs: %w", err)
	}
	return queue.RecoveryResult{
		Requeued:  int(requeued),
		Dead:      int(dead),
		Cancelled: int(cancelled),
	}, nil
}
