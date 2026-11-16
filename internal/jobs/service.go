package jobs

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/domain"
	"github.com/google/uuid"
)

// Service owns submission and retrieval rules.
type Service struct {
	repo               Repository
	validator          *Validator
	now                func() time.Time
	maxOutstandingJobs int
}

// Option configures a Service.
type Option func(*Service)

// WithMaxOutstandingJobs caps the number of queued, running and retry_wait
// jobs per tenant. A non-positive value disables the quota.
func WithMaxOutstandingJobs(limit int) Option {
	return func(s *Service) {
		s.maxOutstandingJobs = limit
	}
}

// NewService wires a repository and validator together.
func NewService(repo Repository, validator *Validator, opts ...Option) *Service {
	service := &Service{
		repo:      repo,
		validator: validator,
		now:       time.Now,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(service)
		}
	}
	return service
}

// Submit validates and persists a job. The boolean reports whether a new row
// was created; false means an existing idempotent job was returned.
func (s *Service) Submit(ctx context.Context, in SubmitInput) (*domain.Job, bool, error) {
	if s.repo == nil {
		return nil, false, fmt.Errorf("job repository is not configured")
	}
	if s.validator == nil {
		return nil, false, fmt.Errorf("job validator is not configured")
	}

	job, err := s.validator.Build(in, s.now())
	if err != nil {
		return nil, false, err
	}
	return s.repo.CreateJob(ctx, job, s.maxOutstandingJobs)
}

// Get returns a tenant-scoped job.
func (s *Service) Get(ctx context.Context, tenantID, id uuid.UUID) (*domain.Job, error) {
	if s.repo == nil {
		return nil, fmt.Errorf("job repository is not configured")
	}
	if tenantID == uuid.Nil {
		return nil, fmt.Errorf("%w: tenant_id is required", domain.ErrInvalidArgument)
	}
	if id == uuid.Nil {
		return nil, fmt.Errorf("%w: job id is required", domain.ErrInvalidArgument)
	}
	return s.repo.GetJobByID(ctx, tenantID, id)
}

// Replay returns an eligible dead job to retry_wait. replayedBy is the operator
// identity recorded by the durable audit columns.
func (s *Service) Replay(ctx context.Context, tenantID, id uuid.UUID, replayedBy string) (*domain.Job, error) {
	if s.repo == nil {
		return nil, fmt.Errorf("job repository is not configured")
	}
	if tenantID == uuid.Nil {
		return nil, fmt.Errorf("%w: tenant_id is required", domain.ErrInvalidArgument)
	}
	if id == uuid.Nil {
		return nil, fmt.Errorf("%w: job id is required", domain.ErrInvalidArgument)
	}
	replayedBy = strings.TrimSpace(replayedBy)
	if replayedBy == "" {
		return nil, fmt.Errorf("%w: replayed_by is required", domain.ErrInvalidArgument)
	}
	return s.repo.ReplayJob(ctx, tenantID, id, replayedBy)
}
