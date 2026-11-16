package jobs

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/domain"
	"github.com/google/uuid"
)

const (
	// DefaultListLimit is the page size when the caller does not specify one.
	DefaultListLimit = 50

	// MaxListLimit is the largest page a caller may request.
	MaxListLimit = 100
)

// ListInput is the transport-independent input for GET /v1/jobs.
type ListInput struct {
	Status        *domain.JobStatus
	JobType       string
	CreatedAfter  *time.Time
	CreatedBefore *time.Time
	Cursor        string
	Limit         int
}

// ListResult is one page of tenant-scoped jobs. NextCursor is empty on the
// final page.
type ListResult struct {
	Jobs       []*domain.Job
	NextCursor string
}

// List returns a tenant-scoped page ordered newest first.
func (s *Service) List(ctx context.Context, tenantID uuid.UUID, in ListInput) (*ListResult, error) {
	if s.repo == nil {
		return nil, fmt.Errorf("job repository is not configured")
	}
	if tenantID == uuid.Nil {
		return nil, fmt.Errorf("%w: tenant_id is required", domain.ErrInvalidArgument)
	}

	limit := in.Limit
	if limit == 0 {
		limit = DefaultListLimit
	}
	if limit < 1 || limit > MaxListLimit {
		return nil, fmt.Errorf("%w: limit must be between 1 and %d", domain.ErrInvalidArgument, MaxListLimit)
	}
	if in.Status != nil && !in.Status.Valid() {
		return nil, fmt.Errorf("%w: unknown status %q", domain.ErrInvalidArgument, *in.Status)
	}

	jobType := strings.TrimSpace(in.JobType)
	if len(jobType) > domain.MaxJobTypeLength {
		return nil, fmt.Errorf("%w: job_type exceeds %d characters", domain.ErrInvalidArgument, domain.MaxJobTypeLength)
	}
	if in.CreatedAfter != nil && in.CreatedBefore != nil && in.CreatedAfter.After(*in.CreatedBefore) {
		return nil, fmt.Errorf("%w: created_after must not be later than created_before", domain.ErrInvalidArgument)
	}

	filter := ListFilter{
		Status:        in.Status,
		JobType:       jobType,
		CreatedAfter:  utcPointer(in.CreatedAfter),
		CreatedBefore: utcPointer(in.CreatedBefore),
	}

	if cursor := strings.TrimSpace(in.Cursor); cursor != "" {
		decoded, err := decodeCursor(cursor)
		if err != nil {
			return nil, err
		}
		filter.AfterCreated = &decoded.CreatedAt
		filter.AfterID = &decoded.ID
	}

	// Fetch one extra row so the presence of a next page is unambiguous.
	list, err := s.repo.ListJobs(ctx, tenantID, filter, limit+1)
	if err != nil {
		return nil, err
	}

	nextCursor := ""
	if len(list) > limit {
		list = list[:limit]
		last := list[len(list)-1]
		nextCursor = encodeCursor(last.CreatedAt, last.ID)
	}
	return &ListResult{Jobs: list, NextCursor: nextCursor}, nil
}

// Cancel requests cancellation of a tenant-scoped job. The boolean reports
// whether the job is running and cancellation is cooperative rather than
// already applied.
func (s *Service) Cancel(ctx context.Context, tenantID, id uuid.UUID) (*domain.Job, bool, error) {
	if s.repo == nil {
		return nil, false, fmt.Errorf("job repository is not configured")
	}
	if tenantID == uuid.Nil {
		return nil, false, fmt.Errorf("%w: tenant_id is required", domain.ErrInvalidArgument)
	}
	if id == uuid.Nil {
		return nil, false, fmt.Errorf("%w: job id is required", domain.ErrInvalidArgument)
	}
	return s.repo.CancelJob(ctx, tenantID, id)
}

type cursor struct {
	CreatedAt time.Time
	ID        uuid.UUID
}

// encodeCursor builds an opaque keyset cursor for (created_at DESC, id DESC).
func encodeCursor(createdAt time.Time, id uuid.UUID) string {
	value := createdAt.UTC().Format(time.RFC3339Nano) + "|" + id.String()
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}

func decodeCursor(raw string) (cursor, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(raw))
	if err != nil {
		return cursor{}, fmt.Errorf("%w: cursor is invalid", domain.ErrInvalidArgument)
	}
	parts := strings.SplitN(string(decoded), "|", 2)
	if len(parts) != 2 {
		return cursor{}, fmt.Errorf("%w: cursor is invalid", domain.ErrInvalidArgument)
	}

	createdAt, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return cursor{}, fmt.Errorf("%w: cursor is invalid", domain.ErrInvalidArgument)
	}
	id, err := uuid.Parse(parts[1])
	if err != nil || id == uuid.Nil {
		return cursor{}, fmt.Errorf("%w: cursor is invalid", domain.ErrInvalidArgument)
	}
	return cursor{CreatedAt: createdAt.UTC(), ID: id}, nil
}

func utcPointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	utc := value.UTC()
	return &utc
}
