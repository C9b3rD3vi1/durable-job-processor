package domain

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func validJob() *Job {
	return &Job{
		ID:          uuid.New(),
		TenantID:    uuid.New(),
		JobType:     "report.generate",
		Payload:     json.RawMessage(`{"report_id":"r-1"}`),
		Status:      StatusQueued,
		Priority:    0,
		RunAt:       time.Now().UTC(),
		Attempts:    0,
		MaxAttempts: 5,
	}
}

func TestJobValidate(t *testing.T) {
	if err := validJob().Validate(); err != nil {
		t.Fatalf("Validate() error = %v, want nil", err)
	}
}

func TestJobValidateRejectsInvalidFields(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Job)
	}{
		{"missing id", func(j *Job) { j.ID = uuid.Nil }},
		{"missing tenant", func(j *Job) { j.TenantID = uuid.Nil }},
		{"missing type", func(j *Job) { j.JobType = " " }},
		{"long type", func(j *Job) { j.JobType = strings.Repeat("a", MaxJobTypeLength+1) }},
		{"missing payload", func(j *Job) { j.Payload = nil }},
		{"invalid payload", func(j *Job) { j.Payload = json.RawMessage(`{`) }},
		{"non-object payload", func(j *Job) { j.Payload = json.RawMessage(`["x"]`) }},
		{"null payload", func(j *Job) { j.Payload = json.RawMessage(`null`) }},
		{"invalid status", func(j *Job) { j.Status = "bogus" }},
		{"priority too high", func(j *Job) { j.Priority = MaxPriority + 1 }},
		{"negative attempts", func(j *Job) { j.Attempts = -1 }},
		{"bad max attempts", func(j *Job) { j.MaxAttempts = 0 }},
		{"attempts exceed max", func(j *Job) { j.Attempts = 6 }},
		{"missing run at", func(j *Job) { j.RunAt = time.Time{} }},
		{"invalid result", func(j *Job) { j.Result = json.RawMessage(`{`) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := validJob()
			tt.mutate(job)
			if err := job.Validate(); err == nil {
				t.Fatal("Validate() error = nil, want error")
			}
		})
	}
}

func TestNilJobValidate(t *testing.T) {
	var job *Job
	if err := job.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want error")
	}
}
