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

	// ReplayJob returns an eligible dead job to retry_wait and records the
	// operator identity in the durable replay audit columns. It returns
	// domain.ErrInvalidTransition when the job exists but is not dead.
	ReplayJob(ctx context.Context, tenantID, id uuid.UUID, replayedBy string) (*domain.Job, error)
}
