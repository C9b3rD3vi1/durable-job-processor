package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/domain"
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
	locked_by,
	lease_token::text,
	locked_until,
	heartbeat_at,
	result,
	last_error,
	created_at,
	updated_at,
	started_at,
	finished_at
`

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
func (r *JobRepository) CreateJob(ctx context.Context, job *domain.Job) (*domain.Job, bool, error) {
	if job == nil {
		return nil, false, fmt.Errorf("%w: job is nil", domain.ErrInvalidJob)
	}
	if job.ID == uuid.Nil {
		job.ID = uuid.New()
	}

	const query = `
		INSERT INTO jobs (
			id, tenant_id, job_type, payload, status, priority, run_at,
			attempts, max_attempts, idempotency_key, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4::jsonb, $5::job_status, $6, $7, $8, $9, $10, now(), now())
		ON CONFLICT (tenant_id, idempotency_key)
			WHERE idempotency_key IS NOT NULL
			DO NOTHING
		RETURNING ` + jobColumns

	created, err := scanJob(r.pool.QueryRow(ctx, query,
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
	existing, err := r.getByIdempotencyKey(ctx, job.TenantID, *job.IdempotencyKey)
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

func (r *JobRepository) getByIdempotencyKey(ctx context.Context, tenantID uuid.UUID, key string) (*domain.Job, error) {
	query := `SELECT ` + jobColumns + ` FROM jobs WHERE tenant_id = $1 AND idempotency_key = $2`
	job, err := scanJob(r.pool.QueryRow(ctx, query, tenantID.String(), key))
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
		job        domain.Job
		id         string
		tenantID   string
		status     string
		payload    []byte
		leaseToken *string
		result     []byte
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
		&job.LockedBy,
		&leaseToken,
		&job.LockedUntil,
		&job.HeartbeatAt,
		&result,
		&job.LastError,
		&job.CreatedAt,
		&job.UpdatedAt,
		&job.StartedAt,
		&job.FinishedAt,
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

	return &job, nil
}
