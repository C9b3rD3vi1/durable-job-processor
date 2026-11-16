package queue

import (
	"context"
	"encoding/json"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/domain"
	"github.com/google/uuid"
)

// Store is the persistence contract used by the worker runtime. PostgreSQL is
// the source of truth; claims and transitions are fenced by the lease token.
type Store interface {
	// ClaimJob atomically claims one due job, or returns nil when none is
	// available. A fresh lease token is assigned to the claimed job.
	ClaimJob(ctx context.Context, workerID string, leaseDuration time.Duration) (*domain.Job, error)

	// CompleteJob marks a claimed job succeeded. It returns false when the
	// lease token is no longer current.
	CompleteJob(ctx context.Context, id, leaseToken uuid.UUID, result json.RawMessage) (bool, error)

	// FailJob records a handler failure. A positive retryDelay schedules a
	// retry; otherwise, or once attempts are exhausted, the job is dead-lettered.
	// It returns false when the lease token is no longer current.
	FailJob(ctx context.Context, id, leaseToken uuid.UUID, cause string, retryDelay time.Duration) (bool, error)

	// HeartbeatJob renews a running job's lease. It returns false when the
	// lease token is no longer current, which means the worker has lost
	// ownership and must stop attempting state changes.
	HeartbeatJob(ctx context.Context, id, leaseToken uuid.UUID, leaseDuration time.Duration) (bool, error)

	// RecoverExpiredJobs requeues or dead-letters running jobs whose lease has
	// expired. The operation is fenced by the scan itself and is safe to run
	// from several workers concurrently.
	RecoverExpiredJobs(ctx context.Context, limit int) (RecoveryResult, error)
}
