package jobs

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/domain"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/observability"
	"github.com/google/uuid"
)

// Service owns submission and retrieval rules.
type Service struct {
	repo               Repository
	validator          *Validator
	now                func() time.Time
	maxOutstandingJobs int
	metrics            *observability.Metrics
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

// WithMetrics enables job lifecycle counters on the service.
func WithMetrics(metrics *observability.Metrics) Option {
	return func(s *Service) {
		s.metrics = metrics
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
		// Rejected input must not become an attacker-controlled label value.
		s.metrics.Inc(observability.MetricJobsSubmittedTotal,
			observability.L("job_type", "invalid"), observability.L("result", "rejected"))
		return nil, false, err
	}

	createdJob, created, err := s.repo.CreateJob(ctx, job, s.maxOutstandingJobs)
	result := "replayed"
	if created {
		result = "created"
	}
	if err != nil {
		result = "error"
	}
	jobType := "unknown"
	if createdJob != nil {
		jobType = jobTypeLabel(createdJob.JobType)
	}
	s.metrics.Inc(observability.MetricJobsSubmittedTotal,
		observability.L("job_type", jobType), observability.L("result", result))
	return createdJob, created, err
}

// jobTypeLabel bounds the job_type label even if a repository returns a job
// whose type was not validated by this process.
func jobTypeLabel(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}
	if len(value) > domain.MaxJobTypeLength {
		return value[:domain.MaxJobTypeLength]
	}
	return value
}

// Get returns a job the caller is authorized to inspect. Jobs owned by another
// client key are hidden as 404 rather than revealing their existence.
func (s *Service) Get(ctx context.Context, actor Actor, id uuid.UUID) (*domain.Job, error) {
	if s.repo == nil {
		return nil, fmt.Errorf("job repository is not configured")
	}
	if err := actor.validate(); err != nil {
		return nil, err
	}
	if id == uuid.Nil {
		return nil, fmt.Errorf("%w: job id is required", domain.ErrInvalidArgument)
	}
	job, err := s.repo.GetJobByID(ctx, actor.TenantID, id)
	if err != nil {
		return nil, err
	}
	if !actor.canAccess(job) {
		return nil, fmt.Errorf("%w: job not found", domain.ErrNotFound)
	}
	return job, nil
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
	job, err := s.repo.ReplayJob(ctx, tenantID, id, replayedBy)
	if err == nil && job != nil {
		s.metrics.Inc(observability.MetricJobsReplayedTotal,
			observability.L("job_type", jobTypeLabel(job.JobType)))
	}
	return job, err
}
