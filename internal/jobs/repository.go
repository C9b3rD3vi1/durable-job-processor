package jobs

import (
	"context"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/domain"
	"github.com/google/uuid"
)

// Repository is the persistence boundary used by the job service.
type Repository interface {
	// CreateJob persists job. If an existing job has the same tenant and
	// idempotency key, it returns that job with created=false.
	CreateJob(ctx context.Context, job *domain.Job) (stored *domain.Job, created bool, err error)

	// GetJobByID returns a tenant-scoped job or domain.ErrNotFound.
	GetJobByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Job, error)
}
