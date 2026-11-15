package jobs

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/domain"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/handlers"
	"github.com/google/uuid"
)

func testValidator() *Validator {
	return NewValidator(handlers.NewRegistry("report.generate", "email.send"), 64, 5)
}

func validInput() SubmitInput {
	return SubmitInput{
		TenantID: uuid.New(),
		JobType:  "report.generate",
		Payload:  json.RawMessage(`{"report_id":"r-1"}`),
	}
}

func TestValidatorBuildDefaults(t *testing.T) {
	now := time.Date(2026, 11, 15, 10, 0, 0, 0, time.UTC)
	input := validInput()

	job, err := testValidator().Build(input, now)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	if job.ID == uuid.Nil {
		t.Error("ID is nil")
	}
	if job.Status != domain.StatusQueued {
		t.Errorf("Status = %q, want queued", job.Status)
	}
	if job.Priority != 0 {
		t.Errorf("Priority = %d, want 0", job.Priority)
	}
	if job.MaxAttempts != 5 {
		t.Errorf("MaxAttempts = %d, want 5", job.MaxAttempts)
	}
	if !job.RunAt.Equal(now) {
		t.Errorf("RunAt = %v, want %v", job.RunAt, now)
	}
	if job.Attempts != 0 {
		t.Errorf("Attempts = %d, want 0", job.Attempts)
	}
}

func TestValidatorBuildOverrides(t *testing.T) {
	now := time.Now().UTC()
	runAt := now.Add(time.Hour)
	priority := int16(7)
	maxAttempts := 2
	key := "  client-key  "
	input := validInput()
	input.RunAt = &runAt
	input.Priority = &priority
	input.MaxAttempts = &maxAttempts
	input.IdempotencyKey = &key

	job, err := testValidator().Build(input, now)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if !job.RunAt.Equal(runAt) {
		t.Errorf("RunAt = %v, want %v", job.RunAt, runAt)
	}
	if job.Priority != priority || job.MaxAttempts != maxAttempts {
		t.Errorf("priority/max = %d/%d, want %d/%d", job.Priority, job.MaxAttempts, priority, maxAttempts)
	}
	if job.IdempotencyKey == nil || *job.IdempotencyKey != "client-key" {
		t.Errorf("IdempotencyKey = %v, want trimmed client-key", job.IdempotencyKey)
	}
}

func TestValidatorBuildRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*SubmitInput, *Validator)
	}{
		{"missing tenant", func(in *SubmitInput, _ *Validator) { in.TenantID = uuid.Nil }},
		{"missing type", func(in *SubmitInput, _ *Validator) { in.JobType = " " }},
		{"invalid type format", func(in *SubmitInput, _ *Validator) { in.JobType = "Report Generate" }},
		{"unsupported type", func(in *SubmitInput, _ *Validator) { in.JobType = "unknown.job" }},
		{"missing payload", func(in *SubmitInput, _ *Validator) { in.Payload = nil }},
		{"invalid json", func(in *SubmitInput, _ *Validator) { in.Payload = json.RawMessage(`{`) }},
		{"array payload", func(in *SubmitInput, _ *Validator) { in.Payload = json.RawMessage(`[1,2]`) }},
		{"null payload", func(in *SubmitInput, _ *Validator) { in.Payload = json.RawMessage(`null`) }},
		{"payload too large", func(in *SubmitInput, _ *Validator) {
			in.Payload = json.RawMessage(`{"value":"` + strings.Repeat("x", 100) + `"}`)
		}},
		{"priority too high", func(in *SubmitInput, _ *Validator) { p := int16(11); in.Priority = &p }},
		{"priority too low", func(in *SubmitInput, _ *Validator) { p := int16(-11); in.Priority = &p }},
		{"zero attempts", func(in *SubmitInput, _ *Validator) { n := 0; in.MaxAttempts = &n }},
		{"too many attempts", func(in *SubmitInput, _ *Validator) { n := domain.MaxAttemptsLimit + 1; in.MaxAttempts = &n }},
		{"long idempotency key", func(in *SubmitInput, _ *Validator) {
			key := strings.Repeat("k", domain.MaxIdempotencyKeyLength+1)
			in.IdempotencyKey = &key
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := validInput()
			validator := testValidator()
			tt.mutate(&input, validator)
			if _, err := validator.Build(input, time.Now()); err == nil {
				t.Fatal("Build() error = nil, want error")
			} else if !errors.Is(err, domain.ErrInvalidJob) {
				t.Fatalf("Build() error = %v, want ErrInvalidJob", err)
			}
		})
	}
}

func TestValidatorBuildBlankIdempotencyKeyBecomesNil(t *testing.T) {
	key := "   "
	input := validInput()
	input.IdempotencyKey = &key

	job, err := testValidator().Build(input, time.Now())
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if job.IdempotencyKey != nil {
		t.Errorf("IdempotencyKey = %q, want nil", *job.IdempotencyKey)
	}
}
