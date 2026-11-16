package dto

import (
	"encoding/json"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/domain"
	"github.com/google/uuid"
)

// CreateJobRequest is the JSON contract for POST /v1/jobs.
type CreateJobRequest struct {
	JobType     string          `json:"job_type"`
	Payload     json.RawMessage `json:"payload"`
	ScheduledAt *time.Time      `json:"scheduled_at,omitempty"`
	Priority    *int16          `json:"priority,omitempty"`
	MaxAttempts *int            `json:"max_attempts,omitempty"`
}

// JobResponse deliberately omits payload, idempotency key and lease internals.
// The architecture requires job reads to avoid exposing sensitive payload data.
type JobResponse struct {
	ID                uuid.UUID        `json:"id"`
	TenantID          uuid.UUID        `json:"tenant_id"`
	JobType           string           `json:"job_type"`
	Status            domain.JobStatus `json:"status"`
	Priority          int16            `json:"priority"`
	RunAt             time.Time        `json:"run_at"`
	Attempts          int              `json:"attempts"`
	MaxAttempts       int              `json:"max_attempts"`
	Result            json.RawMessage  `json:"result,omitempty"`
	LastError         *string          `json:"last_error,omitempty"`
	ReplayCount       int              `json:"replay_count"`
	ReplayedAt        *time.Time       `json:"replayed_at,omitempty"`
	CancelRequestedAt *time.Time       `json:"cancel_requested_at,omitempty"`
	CreatedAt         time.Time        `json:"created_at"`
	UpdatedAt         time.Time        `json:"updated_at"`
	StartedAt         *time.Time       `json:"started_at,omitempty"`
	FinishedAt        *time.Time       `json:"finished_at,omitempty"`
}

// NewJobResponse maps a domain job to its public representation.
func NewJobResponse(job *domain.Job) JobResponse {
	return JobResponse{
		ID:                job.ID,
		TenantID:          job.TenantID,
		JobType:           job.JobType,
		Status:            job.Status,
		Priority:          job.Priority,
		RunAt:             job.RunAt.UTC(),
		Attempts:          job.Attempts,
		MaxAttempts:       job.MaxAttempts,
		Result:            job.Result,
		LastError:         job.LastError,
		ReplayCount:       job.ReplayCount,
		ReplayedAt:        utcTime(job.ReplayedAt),
		CancelRequestedAt: utcTime(job.CancelRequestedAt),
		CreatedAt:         job.CreatedAt.UTC(),
		UpdatedAt:         job.UpdatedAt.UTC(),
		StartedAt:         utcTime(job.StartedAt),
		FinishedAt:        utcTime(job.FinishedAt),
	}
}

func utcTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	utc := value.UTC()
	return &utc
}

// ListJobsResponse is one cursor-paginated page of tenant-scoped jobs.
type ListJobsResponse struct {
	Jobs       []JobResponse `json:"jobs"`
	NextCursor string        `json:"next_cursor,omitempty"`
}

// NewListJobsResponse maps a page of domain jobs to its public representation.
func NewListJobsResponse(items []*domain.Job, nextCursor string) ListJobsResponse {
	response := ListJobsResponse{
		Jobs:       make([]JobResponse, 0, len(items)),
		NextCursor: nextCursor,
	}
	for _, job := range items {
		response.Jobs = append(response.Jobs, NewJobResponse(job))
	}
	return response
}

// ErrorResponse is the stable error envelope returned by the API.
type ErrorResponse struct {
	Error ErrorBody `json:"error"`
}

// ErrorBody identifies and describes a client-visible error.
type ErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
