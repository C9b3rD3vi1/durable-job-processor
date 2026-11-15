package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/domain"
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
