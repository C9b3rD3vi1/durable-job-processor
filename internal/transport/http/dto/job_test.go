package dto

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/domain"
	"github.com/google/uuid"
)

func TestNewJobResponse(t *testing.T) {
	location := time.FixedZone("UTC+3", 3*60*60)
	started := time.Date(2026, 11, 15, 12, 0, 0, 0, location)
	finished := started.Add(time.Minute)

	job := &domain.Job{
		ID:             uuid.New(),
		TenantID:       uuid.New(),
		JobType:        "report.generate",
		Payload:        json.RawMessage(`{"secret":"value"}`),
		Status:         domain.StatusQueued,
		Priority:       1,
		RunAt:          started,
		Attempts:       0,
		MaxAttempts:    5,
		IdempotencyKey: stringPtr("private-key"),
		LockedBy:       stringPtr("worker-1"),
		Result:         json.RawMessage(`{"ok":true}`),
		CreatedAt:      started,
		UpdatedAt:      started,
		StartedAt:      &started,
		FinishedAt:     &finished,
	}

	response := NewJobResponse(job)
	if response.RunAt.Location() != time.UTC {
		t.Errorf("RunAt location = %v, want UTC", response.RunAt.Location())
	}
	if response.StartedAt == nil || response.StartedAt.Location() != time.UTC {
		t.Errorf("StartedAt = %v, want UTC", response.StartedAt)
	}
	if response.FinishedAt == nil || response.FinishedAt.Location() != time.UTC {
		t.Errorf("FinishedAt = %v, want UTC", response.FinishedAt)
	}

	encoded, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	for _, forbidden := range []string{"payload", "idempotency_key", "locked_by", "lease_token", "locked_until", "heartbeat_at"} {
		if strings.Contains(string(encoded), `"`+forbidden+`"`) {
			t.Errorf("response JSON exposes %q: %s", forbidden, encoded)
		}
	}
	if !strings.Contains(string(encoded), `"result":{"ok":true}`) {
		t.Errorf("response JSON missing result: %s", encoded)
	}
}

func stringPtr(value string) *string { return &value }
