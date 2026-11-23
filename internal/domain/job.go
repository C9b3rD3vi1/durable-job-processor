package domain

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	MinPriority             int16 = -10
	MaxPriority             int16 = 10
	DefaultMaxAttempts            = 5
	MaxAttemptsLimit              = 10
	MaxJobTypeLength              = 100
	MaxIdempotencyKeyLength       = 255
)

// Job is the durable representation of a unit of work.
//
// Lease and execution fields are present so the repository can round-trip every
// column used by claiming, execution, recovery and replay.
type Job struct {
	ID             uuid.UUID       `json:"id"`
	TenantID       uuid.UUID       `json:"tenant_id"`
	JobType        string          `json:"job_type"`
	Payload        json.RawMessage `json:"payload"`
	Status         JobStatus       `json:"status"`
	Priority       int16           `json:"priority"`
	RunAt          time.Time       `json:"run_at"`
	Attempts       int             `json:"attempts"`
	MaxAttempts    int             `json:"max_attempts"`
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`

	// CreatedByKeyID is the verified API key that submitted the job. It is an
	// internal authorization attribute and is never exposed by the public DTO.
	CreatedByKeyID *uuid.UUID `json:"created_by_api_key_id,omitempty"`

	LockedBy    *string    `json:"locked_by,omitempty"`
	LeaseToken  *uuid.UUID `json:"lease_token,omitempty"`
	LockedUntil *time.Time `json:"locked_until,omitempty"`
	HeartbeatAt *time.Time `json:"heartbeat_at,omitempty"`

	Result            json.RawMessage `json:"result,omitempty"`
	LastError         *string         `json:"last_error,omitempty"`
	CancelRequestedAt *time.Time      `json:"cancel_requested_at,omitempty"`

	ReplayCount int        `json:"replay_count,omitempty"`
	ReplayedBy  *string    `json:"replayed_by,omitempty"`
	ReplayedAt  *time.Time `json:"replayed_at,omitempty"`

	CreatedAt  time.Time  `json:"created_at"`
	UpdatedAt  time.Time  `json:"updated_at"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

// Validate enforces structural invariants that are independent of transport.
func (j *Job) Validate() error {
	if j == nil {
		return fmt.Errorf("%w: job is nil", ErrInvalidJob)
	}
	if j.ID == uuid.Nil {
		return fmt.Errorf("%w: id is required", ErrInvalidJob)
	}
	if j.TenantID == uuid.Nil {
		return fmt.Errorf("%w: tenant_id is required", ErrInvalidJob)
	}
	if strings.TrimSpace(j.JobType) == "" {
		return fmt.Errorf("%w: job_type is required", ErrInvalidJob)
	}
	if len(j.JobType) > MaxJobTypeLength {
		return fmt.Errorf("%w: job_type exceeds %d characters", ErrInvalidJob, MaxJobTypeLength)
	}
	if len(j.Payload) == 0 {
		return fmt.Errorf("%w: payload is required", ErrInvalidJob)
	}
	if !json.Valid(j.Payload) {
		return fmt.Errorf("%w: payload must be valid JSON", ErrInvalidJob)
	}
	var payloadObject map[string]json.RawMessage
	if err := json.Unmarshal(j.Payload, &payloadObject); err != nil || payloadObject == nil {
		return fmt.Errorf("%w: payload must be a JSON object", ErrInvalidJob)
	}
	if !j.Status.Valid() {
		return fmt.Errorf("%w: unknown status %q", ErrInvalidJob, j.Status)
	}
	if j.Priority < MinPriority || j.Priority > MaxPriority {
		return fmt.Errorf("%w: priority must be between %d and %d", ErrInvalidJob, MinPriority, MaxPriority)
	}
	if j.Attempts < 0 {
		return fmt.Errorf("%w: attempts must be non-negative", ErrInvalidJob)
	}
	if j.MaxAttempts <= 0 || j.MaxAttempts > MaxAttemptsLimit {
		return fmt.Errorf("%w: max_attempts must be between 1 and %d", ErrInvalidJob, MaxAttemptsLimit)
	}
	if j.Attempts > j.MaxAttempts {
		return fmt.Errorf("%w: attempts must not exceed max_attempts", ErrInvalidJob)
	}
	if j.RunAt.IsZero() {
		return fmt.Errorf("%w: run_at is required", ErrInvalidJob)
	}
	if j.Result != nil && !json.Valid(j.Result) {
		return fmt.Errorf("%w: result must be valid JSON", ErrInvalidJob)
	}
	return nil
}
