package worker

import (
	"context"
	"time"
)

// recoveryLoop periodically resolves running jobs whose lease expired because a
// worker crashed or stopped without finishing. Recovery is a single fenced SQL
// statement, so several workers can run it concurrently without double-claiming
// the same abandoned row.
func (r *Runner) recoveryLoop(ctx context.Context) {
	ticker := time.NewTicker(r.recoveryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.recoverExpired(ctx)
		}
	}
}

// recoverExpired performs one bounded recovery pass and logs the outcome.
// Errors are not fatal: the next pass retries, and claim loops keep running.
func (r *Runner) recoverExpired(ctx context.Context) {
	recoverCtx, cancel := context.WithTimeout(ctx, persistTimeout)
	defer cancel()

	result, err := r.store.RecoverExpiredJobs(recoverCtx, recoveryBatch)
	if err != nil {
		if ctx.Err() == nil {
			r.logger.Error("recover expired jobs", "worker_id", r.workerID, "error", err)
		}
		return
	}
	if result.Requeued > 0 || result.Dead > 0 {
		r.logger.Warn("recovered expired job leases",
			"worker_id", r.workerID,
			"requeued", result.Requeued,
			"dead", result.Dead,
		)
	}
}
