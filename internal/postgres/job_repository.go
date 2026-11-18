package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/domain"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/jobs"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// jobColumns is cast to text for UUID columns so the repository can scan into
// plain strings and convert with google/uuid explicitly.
const jobColumns = `
	id::text,
	tenant_id::text,
	job_type,
	payload,
	status::text,
	priority,
	run_at,
	attempts,
	max_attempts,
	idempotency_key,
	created_by_api_key_id::text,
	locked_by,
	lease_token::text,
	locked_until,
	heartbeat_at,
	result,
	last_error,
	cancel_requested_at,
	created_at,
	updated_at,
	started_at,
	finished_at,
	replay_count,
	replayed_by,
	replayed_at
`

// rowQuerier is the subset of pgx used by helpers shared by the pool and a
// transaction.
type rowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// JobRepository is the PostgreSQL implementation of jobs.Repository.
type JobRepository struct {
	pool *pgxpool.Pool
}

// NewJobRepository creates a repository backed by pool.
func NewJobRepository(pool *pgxpool.Pool) *JobRepository {
	return &JobRepository{pool: pool}
}

// CreateJob inserts a job. When an idempotency key collides for the same tenant,
// it returns the existing job and created=false instead of creating a duplicate.
//
// A positive maxOutstanding enforces a per-tenant quota for queued, running and
// retry_wait jobs. The quota check, idempotency lookup and insert run in one
// transaction serialized by a tenant-scoped advisory lock, so concurrent
// submissions cannot overshoot the quota.
func (r *JobRepository) CreateJob(ctx context.Context, job *domain.Job, maxOutstanding int) (*domain.Job, bool, error) {
	if job == nil {
		return nil, false, fmt.Errorf("%w: job is nil", domain.ErrInvalidJob)
	}
	if job.ID == uuid.Nil {
		job.ID = uuid.New()
	}
	if maxOutstanding < 0 {
		return nil, false, fmt.Errorf("%w: max outstanding jobs must not be negative", domain.ErrInvalidArgument)
	}
	if maxOutstanding == 0 {
		return r.createJob(ctx, r.pool, job)
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("begin job submission: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0))`,
		job.TenantID.String(),
	); err != nil {
		return nil, false, fmt.Errorf("lock tenant submission: %w", err)
	}

	// An idempotent retry returns the existing job even when the tenant is at
	// quota; the job already exists and consumes no additional capacity.
	if job.IdempotencyKey != nil {
		existing, err := getByIdempotencyKey(ctx, tx, job.TenantID, *job.IdempotencyKey)
		if err == nil {
			return existing, false, nil
		}
		if !errors.Is(err, domain.ErrNotFound) {
			return nil, false, fmt.Errorf("load existing idempotent job: %w", err)
		}
	}

	const countQuery = `
		SELECT count(*)
		FROM jobs
		WHERE tenant_id = $1
		  AND status IN ('queued', 'running', 'retry_wait')`

	var outstanding int
	if err := tx.QueryRow(ctx, countQuery, job.TenantID.String()).Scan(&outstanding); err != nil {
		return nil, false, fmt.Errorf("count outstanding jobs: %w", err)
	}
	if outstanding >= maxOutstanding {
		return nil, false, fmt.Errorf("%w: tenant has %d outstanding jobs (limit %d)",
			domain.ErrQuotaExceeded, outstanding, maxOutstanding)
	}

	stored, created, err := r.createJob(ctx, tx, job)
	if err != nil {
		return nil, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, fmt.Errorf("commit job submission: %w", err)
	}
	return stored, created, nil
}

func (r *JobRepository) createJob(ctx context.Context, q rowQuerier, job *domain.Job) (*domain.Job, bool, error) {
	const query = `
		INSERT INTO jobs (
			id, tenant_id, job_type, payload, status, priority, run_at,
			attempts, max_attempts, idempotency_key, created_by_api_key_id,
			created_at, updated_at
		)
		VALUES ($1, $2, $3, $4::jsonb, $5::job_status, $6, $7, $8, $9, $10, $11, now(), now())
		ON CONFLICT (tenant_id, idempotency_key)
			WHERE idempotency_key IS NOT NULL
			DO NOTHING
		RETURNING ` + jobColumns

	created, err := scanJob(q.QueryRow(ctx, query,
		job.ID.String(),
		job.TenantID.String(),
		job.JobType,
		string(job.Payload),
		string(job.Status),
		job.Priority,
		job.RunAt,
		job.Attempts,
		job.MaxAttempts,
		job.IdempotencyKey,
		job.CreatedByKeyID,
	))
	if err == nil {
		return created, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, false, fmt.Errorf("insert job: %w", err)
	}

	// A conflict occurred. Only a job with an idempotency key can collide.
	if job.IdempotencyKey == nil {
		return nil, false, fmt.Errorf("insert job: no row returned without idempotency key")
	}
	existing, err := getByIdempotencyKey(ctx, q, job.TenantID, *job.IdempotencyKey)
	if err != nil {
		return nil, false, fmt.Errorf("load existing idempotent job: %w", err)
	}
	return existing, false, nil
}

// GetJobByID retrieves a job only when it belongs to tenantID.
func (r *JobRepository) GetJobByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Job, error) {
	query := `SELECT ` + jobColumns + ` FROM jobs WHERE id = $1 AND tenant_id = $2`
	job, err := scanJob(r.pool.QueryRow(ctx, query, id.String(), tenantID.String()))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrNotFound
		}
		return nil, fmt.Errorf("get job: %w", err)
	}
	return job, nil
}

// ListJobs returns one tenant-scoped page ordered by (created_at DESC, id DESC).
func (r *JobRepository) ListJobs(ctx context.Context, tenantID uuid.UUID, filter jobs.ListFilter, limit int) ([]*domain.Job, error) {
	if tenantID == uuid.Nil {
		return nil, fmt.Errorf("%w: tenant id is required", domain.ErrInvalidArgument)
	}
	if limit <= 0 {
		return nil, fmt.Errorf("%w: limit must be positive", domain.ErrInvalidArgument)
	}

	args := []any{tenantID.String()}
	where := []string{"tenant_id = $1"}

	if !filter.AnyOwner {
		if filter.OwnerKeyID == nil || *filter.OwnerKeyID == uuid.Nil {
			return nil, fmt.Errorf("%w: owner key id is required for client listings", domain.ErrInvalidArgument)
		}
		args = append(args, filter.OwnerKeyID.String())
		where = append(where, fmt.Sprintf(
			"(created_by_api_key_id IS NULL OR created_by_api_key_id = $%d)", len(args)))
	}

	if filter.Status != nil {
		args = append(args, string(*filter.Status))
		where = append(where, fmt.Sprintf("status = $%d::job_status", len(args)))
	}
	if filter.JobType != "" {
		args = append(args, filter.JobType)
		where = append(where, fmt.Sprintf("job_type = $%d", len(args)))
	}
	if filter.CreatedAfter != nil {
		args = append(args, *filter.CreatedAfter)
		where = append(where, fmt.Sprintf("created_at >= $%d", len(args)))
	}
	if filter.CreatedBefore != nil {
		args = append(args, *filter.CreatedBefore)
		where = append(where, fmt.Sprintf("created_at <= $%d", len(args)))
	}
	if filter.AfterID != nil {
		if filter.AfterCreated == nil {
			return nil, fmt.Errorf("%w: cursor timestamp is required", domain.ErrInvalidArgument)
		}
		args = append(args, *filter.AfterCreated)
		createdPos := len(args)
		args = append(args, filter.AfterID.String())
		idPos := len(args)
		where = append(where, fmt.Sprintf("(created_at, id) < ($%d, $%d::uuid)", createdPos, idPos))
	}

	args = append(args, limit)
	query := `SELECT ` + jobColumns +
		` FROM jobs WHERE ` + strings.Join(where, " AND ") +
		fmt.Sprintf(` ORDER BY created_at DESC, id DESC LIMIT $%d`, len(args))

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()

	list := make([]*domain.Job, 0, limit)
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scan job: %w", err)
		}
		list = append(list, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	return list, nil
}

// CancelJob cancels queued and retry_wait jobs immediately. A running job gets
// a durable cancellation request that the worker observes through heartbeats;
// the job remains running until the handler exits.
func (r *JobRepository) CancelJob(ctx context.Context, tenantID, id uuid.UUID) (*domain.Job, bool, error) {
	if tenantID == uuid.Nil || id == uuid.Nil {
		return nil, false, fmt.Errorf("%w: tenant id and job id are required", domain.ErrInvalidArgument)
	}

	const immediate = `
		UPDATE jobs
		SET status              = 'cancelled',
		    finished_at         = now(),
		    cancel_requested_at = COALESCE(cancel_requested_at, now()),
		    updated_at          = now(),
		    locked_by           = NULL,
		    lease_token         = NULL,
		    locked_until        = NULL,
		    heartbeat_at        = NULL
		WHERE id = $1
		  AND tenant_id = $2
		  AND status IN ('queued', 'retry_wait')
		RETURNING ` + jobColumns

	job, err := scanJob(r.pool.QueryRow(ctx, immediate, id.String(), tenantID.String()))
	if err == nil {
		return job, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, false, fmt.Errorf("cancel job: %w", err)
	}

	// The job may already be running. Record a cooperative cancellation request
	// without clearing the lease; the worker remains responsible for stopping
	// the handler and finalizing the cancelled state.
	const request = `
		UPDATE jobs
		SET cancel_requested_at = COALESCE(cancel_requested_at, now()),
		    updated_at          = now()
		WHERE id = $1
		  AND tenant_id = $2
		  AND status = 'running'
		RETURNING ` + jobColumns

	job, err = scanJob(r.pool.QueryRow(ctx, request, id.String(), tenantID.String()))
	if err == nil {
		return job, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, false, fmt.Errorf("request job cancellation: %w", err)
	}

	// Neither statement matched. Distinguish an unknown tenant-scoped job, an
	// idempotent repeat, and a terminal job that cannot be cancelled.
	job, err = r.GetJobByID(ctx, tenantID, id)
	if err != nil {
		return nil, false, err
	}
	if job.Status == domain.StatusCancelled {
		return job, false, nil
	}
	return nil, false, fmt.Errorf("%w: job cannot be cancelled from status %s",
		domain.ErrInvalidTransition, job.Status)
}

// ReplayJob returns a dead job to retry_wait for an operator. Only a dead job
// owned by tenantID is eligible; attempts reset to zero and the durable audit
// columns record who replayed it and when.
func (r *JobRepository) ReplayJob(ctx context.Context, tenantID, id uuid.UUID, replayedBy string) (*domain.Job, error) {
	if tenantID == uuid.Nil || id == uuid.Nil {
		return nil, fmt.Errorf("%w: tenant id and job id are required", domain.ErrInvalidArgument)
	}
	replayedBy = strings.TrimSpace(replayedBy)
	if replayedBy == "" {
		return nil, fmt.Errorf("%w: replayed_by is required", domain.ErrInvalidArgument)
	}

	const query = `
		UPDATE jobs
		SET status       = 'retry_wait',
		    attempts     = 0,
		    run_at       = now(),
		    last_error   = NULL,
		    finished_at  = NULL,
		    locked_by    = NULL,
		    lease_token  = NULL,
		    locked_until = NULL,
		    heartbeat_at = NULL,
		    replay_count = replay_count + 1,
		    replayed_by  = $3,
		    replayed_at  = now(),
		    updated_at   = now()
		WHERE id = $1
		  AND tenant_id = $2
		  AND status = 'dead'
		RETURNING ` + jobColumns

	job, err := scanJob(r.pool.QueryRow(ctx, query, id.String(), tenantID.String(), replayedBy))
	if err == nil {
		return job, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("replay job: %w", err)
	}

	// No row was updated. Distinguish an unknown tenant-scoped job from a job
	// that exists but is not dead so the transport can return 404 or 409.
	if _, getErr := r.GetJobByID(ctx, tenantID, id); getErr != nil {
		return nil, getErr
	}
	return nil, fmt.Errorf("%w: only dead jobs can be replayed", domain.ErrInvalidTransition)
}

func getByIdempotencyKey(ctx context.Context, q rowQuerier, tenantID uuid.UUID, key string) (*domain.Job, error) {
	query := `SELECT ` + jobColumns + ` FROM jobs WHERE tenant_id = $1 AND idempotency_key = $2`
	job, err := scanJob(q.QueryRow(ctx, query, tenantID.String(), key))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrNotFound
		}
		return nil, fmt.Errorf("get job by idempotency key: %w", err)
	}
	return job, nil
}

func scanJob(row pgx.Row) (*domain.Job, error) {
	var (
		job            domain.Job
		id             string
		tenantID       string
		status         string
		payload        []byte
		leaseToken     *string
		createdByKeyID *string
		result         []byte
	)

	err := row.Scan(
		&id,
		&tenantID,
		&job.JobType,
		&payload,
		&status,
		&job.Priority,
		&job.RunAt,
		&job.Attempts,
		&job.MaxAttempts,
		&job.IdempotencyKey,
		&createdByKeyID,
		&job.LockedBy,
		&leaseToken,
		&job.LockedUntil,
		&job.HeartbeatAt,
		&result,
		&job.LastError,
		&job.CancelRequestedAt,
		&job.CreatedAt,
		&job.UpdatedAt,
		&job.StartedAt,
		&job.FinishedAt,
		&job.ReplayCount,
		&job.ReplayedBy,
		&job.ReplayedAt,
	)
	if err != nil {
		return nil, err
	}

	job.ID, err = uuid.Parse(id)
	if err != nil {
		return nil, fmt.Errorf("parse job id: %w", err)
	}
	job.TenantID, err = uuid.Parse(tenantID)
	if err != nil {
		return nil, fmt.Errorf("parse tenant id: %w", err)
	}
	job.Status = domain.JobStatus(status)
	job.Payload = json.RawMessage(payload)
	if result != nil {
		job.Result = json.RawMessage(result)
	}
	if leaseToken != nil {
		token, err := uuid.Parse(*leaseToken)
		if err != nil {
			return nil, fmt.Errorf("parse lease token: %w", err)
		}
		job.LeaseToken = &token
	}
	if createdByKeyID != nil {
		owner, err := uuid.Parse(*createdByKeyID)
		if err != nil {
			return nil, fmt.Errorf("parse created-by key id: %w", err)
		}
		job.CreatedByKeyID = &owner
	}

	return &job, nil
}
