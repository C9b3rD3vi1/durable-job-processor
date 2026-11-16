package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/domain"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/handlers"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/retry"
)

// process executes one claimed job and records its terminal or retry outcome.
// The handler runs under a per-job timeout, while a heartbeat renews the lease
// for as long as the handler is running. Persistence deliberately uses a
// context detached from shutdown cancellation so an interrupted handler can
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
		if !r.fail(ctx, job, logger,
			retry.Permanent(fmt.Errorf("no handler registered for job type %q", job.JobType)), 0) {
			if !r.cancelledAfterWrite(ctx, job, logger) {
				logger.Warn("job failure fenced; lease is no longer held")
			}
		}
		return
	}

	handlerCtx, cancelHandler := context.WithTimeout(ctx, r.handlerTimeout)
	defer cancelHandler()

	heartbeatCtx, stopHeartbeat := context.WithCancel(ctx)
	defer stopHeartbeat()

	var leaseLost atomic.Bool
	var cancellationRequested atomic.Bool
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		r.heartbeat(heartbeatCtx, job,
			func() {
				leaseLost.Store(true)
				cancelHandler()
			},
			func() {
				if cancellationRequested.CompareAndSwap(false, true) {
					cancelHandler()
				}
			},
		)
	}()

	started := time.Now()
	result, err := invokeHandler(handlerCtx, handler, *job)
	timedOut := errors.Is(handlerCtx.Err(), context.DeadlineExceeded)
	elapsed := time.Since(started)

	stopHeartbeat()
	<-heartbeatDone
	cancelHandler()

	// A worker that lost its lease must not write state for this attempt. The
	// fencing check in every write is the final guard; this avoids pointless
	// writes and makes the ownership loss explicit in the logs.
	if leaseLost.Load() {
		logger.Warn("job lease lost; abandoning handler result", "duration", elapsed.String())
		return
	}

	// A durable cancellation request is authoritative. Persist the cancelled
	// state with the same lease fence and discard whatever the handler returned.
	if cancellationRequested.Load() {
		if !r.cancelledAfterWrite(ctx, job, logger) {
			logger.Warn("job cancellation fenced; lease is no longer held")
		}
		return
	}

	if err == nil && timedOut {
		err = fmt.Errorf("handler timed out after %s", r.handlerTimeout)
	}
	if err != nil {
		if timedOut {
			err = fmt.Errorf("handler timed out after %s: %w", r.handlerTimeout, context.DeadlineExceeded)
		}
		retryDelay := time.Duration(0)
		if retry.ShouldRetry(retry.Classify(err), job.Attempts, job.MaxAttempts) {
			retryDelay = r.retryDelay(job.Attempts)
		}
		if !r.fail(ctx, job, logger, err, retryDelay) {
			if !r.cancelledAfterWrite(ctx, job, logger) {
				logger.Warn("job failure fenced; lease is no longer held")
			}
		}
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
		// Completion is rejected when the lease was lost or a cancellation
		// request landed first. Try to finalize the cancellation durably.
		if !r.cancelledAfterWrite(ctx, job, logger) {
			logger.Warn("job completion fenced; lease is no longer held")
		}
		return
	}
	logger.Info("job succeeded", "duration", elapsed.String())
}

// heartbeat renews the lease until the handler finishes or ownership is lost.
// A database error is retried on the next tick; a fenced update means another
// worker owns the job, so the handler is cancelled and its result is discarded.
// A durable cancellation request also cancels the handler but keeps renewing the
// lease so this worker can finalize the attempt as cancelled.
func (r *Runner) heartbeat(ctx context.Context, job *domain.Job, onLost, onCancel func()) {
	ticker := time.NewTicker(r.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			heartbeatCtx, cancel := context.WithTimeout(ctx, persistTimeout)
			result, err := r.store.HeartbeatJob(heartbeatCtx, job.ID, *job.LeaseToken, r.leaseDuration)
			cancel()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				r.logger.Error("heartbeat job",
					"worker_id", r.workerID,
					"job_id", job.ID,
					"error", err,
				)
				continue
			}
			if !result.Renewed {
				onLost()
				return
			}
			if result.CancelRequested {
				onCancel()
			}
		}
	}
}

// cancelledAfterWrite finalizes a cooperative cancellation after a heartbeat
// observed the request, or after a completion/failure write was rejected because
// a cancellation request landed first. It returns true when the job was
// durably marked cancelled.
func (r *Runner) cancelledAfterWrite(ctx context.Context, job *domain.Job, logger *slog.Logger) bool {
	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), persistTimeout)
	defer cancel()

	cancelled, err := r.store.CancelRunningJob(persistCtx, job.ID, *job.LeaseToken)
	if err != nil {
		logger.Error("cancel running job", "error", err)
		return false
	}
	if !cancelled {
		return false
	}
	logger.Info("job cancelled")
	return true
}

// fail records a handler failure. A positive retryDelay schedules a retry until
// attempts are exhausted; a zero delay dead-letters the job immediately. It
// returns false when the lease is no longer current or cancellation won the
// race, so the caller can try to finalize a cancellation instead.
func (r *Runner) fail(ctx context.Context, job *domain.Job, logger *slog.Logger, cause error, retryDelay time.Duration) bool {
	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), persistTimeout)
	defer cancel()

	message := truncateError(cause.Error())
	failed, err := r.store.FailJob(persistCtx, job.ID, *job.LeaseToken, message, retryDelay)
	if err != nil {
		logger.Error("fail job", "error", err)
		return false
	}
	if !failed {
		return false
	}

	if retryDelay <= 0 || job.Attempts >= job.MaxAttempts {
		logger.Error("job dead-lettered", "error", message)
		return true
	}
	logger.Warn("job failed; retry scheduled",
		"error", message,
		"retry_in", retryDelay.String(),
	)
	return true
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
