package jobs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/domain"
	"github.com/google/uuid"
)

var jobTypePattern = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,99}$`)

// JobTypeRegistry reports which job types have registered handlers.
type JobTypeRegistry interface {
	Allows(jobType string) bool
}

// SubmitInput is the transport-independent input for submitting a job.
type SubmitInput struct {
	TenantID       uuid.UUID
	JobType        string
	Payload        json.RawMessage
	RunAt          *time.Time
	Priority       *int16
	MaxAttempts    *int
	IdempotencyKey *string
}

// Validator turns caller input into a valid queued domain job.
type Validator struct {
	registry           JobTypeRegistry
	maxPayloadBytes    int64
	defaultMaxAttempts int
}

// NewValidator creates a validator with explicit limits.
func NewValidator(registry JobTypeRegistry, maxPayloadBytes int64, defaultMaxAttempts int) *Validator {
	if registry == nil {
		registry = emptyRegistry{}
	}
	if maxPayloadBytes <= 0 {
		maxPayloadBytes = 256 * 1024
	}
	if defaultMaxAttempts < 1 || defaultMaxAttempts > domain.MaxAttemptsLimit {
		defaultMaxAttempts = domain.DefaultMaxAttempts
	}
	return &Validator{
		registry:           registry,
		maxPayloadBytes:    maxPayloadBytes,
		defaultMaxAttempts: defaultMaxAttempts,
	}
}

type emptyRegistry struct{}

func (emptyRegistry) Allows(string) bool { return false }

// Build validates input and returns a new queued job. The supplied now value is
// used when no run_at is provided.
func (v *Validator) Build(in SubmitInput, now time.Time) (*domain.Job, error) {
	if in.TenantID == uuid.Nil {
		return nil, invalid("tenant_id is required")
	}

	jobType := strings.TrimSpace(in.JobType)
	if jobType == "" {
		return nil, invalid("job_type is required")
	}
	if !jobTypePattern.MatchString(jobType) {
		return nil, invalid("job_type must match %s", jobTypePattern.String())
	}
	if !v.registry.Allows(jobType) {
		return nil, invalid("unsupported job_type %q", jobType)
	}

	payload := bytes.TrimSpace(in.Payload)
	if len(payload) == 0 {
		return nil, invalid("payload is required")
	}
	if int64(len(payload)) > v.maxPayloadBytes {
		return nil, invalid("payload exceeds %d bytes", v.maxPayloadBytes)
	}
	if !json.Valid(payload) {
		return nil, invalid("payload must be valid JSON")
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(payload, &object); err != nil || object == nil {
		return nil, invalid("payload must be a JSON object")
	}

	priority := int16(0)
	if in.Priority != nil {
		priority = *in.Priority
	}
	if priority < domain.MinPriority || priority > domain.MaxPriority {
		return nil, invalid("priority must be between %d and %d", domain.MinPriority, domain.MaxPriority)
	}

	maxAttempts := v.defaultMaxAttempts
	if in.MaxAttempts != nil {
		maxAttempts = *in.MaxAttempts
	}
	if maxAttempts < 1 || maxAttempts > domain.MaxAttemptsLimit {
		return nil, invalid("max_attempts must be between 1 and %d", domain.MaxAttemptsLimit)
	}

	runAt := now.UTC()
	if in.RunAt != nil && !in.RunAt.IsZero() {
		runAt = in.RunAt.UTC()
	}

	var idempotencyKey *string
	if in.IdempotencyKey != nil {
		key := strings.TrimSpace(*in.IdempotencyKey)
		if key != "" {
			if len(key) > domain.MaxIdempotencyKeyLength {
				return nil, invalid("Idempotency-Key exceeds %d characters", domain.MaxIdempotencyKeyLength)
			}
			idempotencyKey = &key
		}
	}

	job := &domain.Job{
		ID:             uuid.New(),
		TenantID:       in.TenantID,
		JobType:        jobType,
		Payload:        json.RawMessage(append([]byte(nil), payload...)),
		Status:         domain.StatusQueued,
		Priority:       priority,
		RunAt:          runAt,
		Attempts:       0,
		MaxAttempts:    maxAttempts,
		IdempotencyKey: idempotencyKey,
	}
	if err := job.Validate(); err != nil {
		return nil, err
	}
	return job, nil
}

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", domain.ErrInvalidJob, fmt.Sprintf(format, args...))
}
