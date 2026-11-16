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

// CompleteJob records success only when the caller still holds the lease.
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
		  AND lease_token = $2`

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
		  AND lease_token = $2`

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

// HeartbeatJob renews the lease only while the caller still holds it. A zero
// rows-affected result is the fencing signal that the worker has lost
// ownership and must stop state changes for this attempt.
func (r *JobRepository) HeartbeatJob(ctx context.Context, id, leaseToken uuid.UUID, leaseDuration time.Duration) (bool, error) {
	if id == uuid.Nil || leaseToken == uuid.Nil {
		return false, fmt.Errorf("%w: job id and lease token are required", domain.ErrInvalidArgument)
	}
	if leaseDuration <= 0 {
		return false, fmt.Errorf("%w: lease duration must be positive", domain.ErrInvalidArgument)
	}

	const query = `
		UPDATE jobs
		SET locked_until = now() + make_interval(secs => $1),
		    heartbeat_at = now(),
		    updated_at   = now()
		WHERE id = $2
		  AND status = 'running'
		  AND lease_token = $3`

	tag, err := r.pool.Exec(ctx, query,
		leaseDuration.Seconds(),
		id.String(),
		leaseToken.String(),
	)
	if err != nil {
		return false, fmt.Errorf("heartbeat job: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// RecoverExpiredJobs resolves running jobs whose lease has expired. The scan
// and update are one statement, so several workers can run recovery without
// claiming the same abandoned row twice. Attempts are not incremented here;
// the next claim does that. A job that already consumed its attempts is
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
			        WHEN j.attempts >= j.max_attempts THEN 'dead'::job_status
			        ELSE 'retry_wait'::job_status
			    END,
			    run_at = CASE
			        WHEN j.attempts >= j.max_attempts THEN j.run_at
			        ELSE now()
			    END,
			    finished_at = CASE
			        WHEN j.attempts >= j.max_attempts THEN now()
			        ELSE NULL
			    END,
			    last_error = CASE
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
			count(*) FILTER (WHERE status = 'dead')
		FROM updated`

	var requeued, dead int64
	if err := r.pool.QueryRow(ctx, query, limit).Scan(&requeued, &dead); err != nil {
		return queue.RecoveryResult{}, fmt.Errorf("recover expired jobs: %w", err)
	}
	return queue.RecoveryResult{Requeued: int(requeued), Dead: int(dead)}, nil
}
