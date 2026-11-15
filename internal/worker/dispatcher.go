package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/domain"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/handlers"
)

// process executes one claimed job and records its terminal or retry outcome.
// The handler runs under a per-job timeout, while persistence deliberately uses
// a context detached from shutdown cancellation so an interrupted handler can
// still be fenced and scheduled for retry.
func (r *Runner) process(ctx context.Context, job *domain.Job) {
	if job == nil {
		return
	}

	logger := r.logger.With(
		"worker_id", r.workerID,
		"job_id", job.ID,
		"job_type", job.JobType,
		"tenant_id", job.TenantID,
		"attempt", job.Attempts,
		"max_attempts", job.MaxAttempts,
	)

	if job.LeaseToken == nil {
		logger.Error("claimed job has no lease token")
		return
	}

	handler, ok := r.registry.Handler(job.JobType)
	if !ok {
		// A job can outlive a handler that was removed from the binary. Retrying
		// will not help, so dead-letter it immediately.
		r.fail(ctx, job, logger, fmt.Errorf("no handler registered for job type %q", job.JobType), 0)
		return
	}

	started := time.Now()
	handlerCtx, cancel := context.WithTimeout(ctx, r.handlerTimeout)
	result, err := invokeHandler(handlerCtx, handler, *job)
	timedOut := handlerCtx.Err() == context.DeadlineExceeded
	cancel()
	elapsed := time.Since(started)

	if err == nil && timedOut {
		err = fmt.Errorf("handler timed out after %s", r.handlerTimeout)
	}
	if err != nil {
		if timedOut {
			err = fmt.Errorf("handler timed out after %s: %w", r.handlerTimeout, context.DeadlineExceeded)
		}
		r.fail(ctx, job, logger, err, r.retryBaseDelay)
		return
	}

	persistCtx, cancelPersist := context.WithTimeout(context.WithoutCancel(ctx), persistTimeout)
	defer cancelPersist()

	completed, completeErr := r.store.CompleteJob(persistCtx, job.ID, *job.LeaseToken, result)
	if completeErr != nil {
		logger.Error("complete job", "error", completeErr)
		return
	}
	if !completed {
		logger.Warn("job completion fenced; lease is no longer held")
		return
	}
	logger.Info("job succeeded", "duration", elapsed.String())
}

// fail records a handler failure. A positive retryDelay schedules a retry until
// attempts are exhausted; a zero delay dead-letters the job immediately.
func (r *Runner) fail(ctx context.Context, job *domain.Job, logger *slog.Logger, cause error, retryDelay time.Duration) {
	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), persistTimeout)
	defer cancel()

	message := truncateError(cause.Error())
	failed, err := r.store.FailJob(persistCtx, job.ID, *job.LeaseToken, message, retryDelay)
	if err != nil {
		logger.Error("fail job", "error", err)
		return
	}
	if !failed {
		logger.Warn("job failure fenced; lease is no longer held")
		return
	}

	if retryDelay <= 0 || job.Attempts >= job.MaxAttempts {
		logger.Error("job dead-lettered", "error", message)
		return
	}
	logger.Warn("job failed; retry scheduled",
		"error", message,
		"retry_in", retryDelay.String(),
	)
}

// invokeHandler isolates the runner from handler panics.
func invokeHandler(ctx context.Context, handler handlers.Handler, job domain.Job) (result json.RawMessage, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = nil
			err = fmt.Errorf("handler panicked: %v", recovered)
		}
	}()
	return handler.Execute(ctx, job)
}

func truncateError(message string) string {
	message = strings.TrimSpace(message)
	if message == "" {
		return "handler failed"
	}
	if len(message) > maxErrorLength {
		return message[:maxErrorLength] + "..."
	}
	return message
}
