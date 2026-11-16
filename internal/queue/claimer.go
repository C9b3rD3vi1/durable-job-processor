package queue

import (
	"context"
	"encoding/json"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/domain"
	"github.com/google/uuid"
)

// HeartbeatResult reports the outcome of a lease renewal.
type HeartbeatResult struct {
	// Renewed is false when the lease token is no longer current, which means
	// the worker has lost ownership and must stop attempting state changes.
	Renewed bool
	// CancelRequested reports a durable cooperative cancellation request. The
	// worker should cancel the handler context and finalize the attempt as
	// cancelled instead of completed or failed.
	CancelRequested bool
}

// Store is the persistence contract used by the worker runtime. PostgreSQL is
// the source of truth; claims and transitions are fenced by the lease token.
type Store interface {
	// ClaimJob atomically claims one due job, or returns nil when none is
	// available. A fresh lease token is assigned to the claimed job.
	ClaimJob(ctx context.Context, workerID string, leaseDuration time.Duration) (*domain.Job, error)

	// CompleteJob marks a claimed job succeeded. It returns false when the
	// lease token is no longer current or cancellation was requested first.
	CompleteJob(ctx context.Context, id, leaseToken uuid.UUID, result json.RawMessage) (bool, error)

	// FailJob records a handler failure. A positive retryDelay schedules a
	// retry; otherwise, or once attempts are exhausted, the job is dead-lettered.
	// It returns false when the lease token is no longer current or cancellation
	// was requested first.
	FailJob(ctx context.Context, id, leaseToken uuid.UUID, cause string, retryDelay time.Duration) (bool, error)

	// HeartbeatJob renews a running job's lease and reports whether a
	// cooperative cancellation has been requested while it runs.
	HeartbeatJob(ctx context.Context, id, leaseToken uuid.UUID, leaseDuration time.Duration) (HeartbeatResult, error)

	// CancelRunningJob finalizes a running job as cancelled when cancellation
	// was requested and the caller still holds the lease. It returns false when
	// the lease is no longer current or no cancellation was requested.
	CancelRunningJob(ctx context.Context, id, leaseToken uuid.UUID) (bool, error)

	// RecoverExpiredJobs requeues, cancels or dead-letters running jobs whose
	// lease has expired. The operation is fenced by the scan itself and is safe
	// to run from several workers concurrently.
	RecoverExpiredJobs(ctx context.Context, limit int) (RecoveryResult, error)
}
