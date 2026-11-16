package jobs

import (
	"context"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/domain"
	"github.com/google/uuid"
)

// Repository is the persistence boundary used by the job service.
type Repository interface {
	// CreateJob persists job. If maxOutstanding is positive and the tenant
	// already has that many queued, running or retry_wait jobs, it returns
	// domain.ErrQuotaExceeded. If an existing job has the same tenant and
	// idempotency key, it returns that job with created=false and does not
	// apply the quota check to the replay.
	CreateJob(ctx context.Context, job *domain.Job, maxOutstanding int) (stored *domain.Job, created bool, err error)

	// GetJobByID returns a tenant-scoped job or domain.ErrNotFound.
	GetJobByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Job, error)

	// ListJobs returns one tenant-scoped page of jobs ordered by
	// (created_at DESC, id DESC). The caller requests one extra row to detect
	// whether another page exists.
	ListJobs(ctx context.Context, tenantID uuid.UUID, filter ListFilter, limit int) ([]*domain.Job, error)

	// CancelJob cancels a queued or retry_wait job immediately. A running job
	// receives a durable cooperative cancellation request and the boolean is
	// true. An already cancelled job is returned unchanged. Terminal jobs
	// return domain.ErrInvalidTransition.
	CancelJob(ctx context.Context, tenantID, id uuid.UUID) (job *domain.Job, requested bool, err error)

	// ReplayJob returns an eligible dead job to retry_wait and records the
	// operator identity in the durable replay audit columns. It returns
	// domain.ErrInvalidTransition when the job exists but is not dead.
	ReplayJob(ctx context.Context, tenantID, id uuid.UUID, replayedBy string) (*domain.Job, error)
}

// ListFilter narrows a tenant-scoped job listing.
type ListFilter struct {
	Status        *domain.JobStatus
	JobType       string
	CreatedAfter  *time.Time
	CreatedBefore *time.Time
	AfterCreated  *time.Time
	AfterID       *uuid.UUID
}
