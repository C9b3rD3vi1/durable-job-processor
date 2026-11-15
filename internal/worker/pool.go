package worker

import (
	"context"
	"time"
)

// loop is one bounded claim slot. It claims and executes at most one job at a
// time, so the number of running loops is exactly the job concurrency.
func (r *Runner) loop(ctx, handlerCtx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		job, err := r.claim(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			r.logger.Error("claim job", "worker_id", r.workerID, "error", err)
			if !wait(ctx, r.pollInterval) {
				return
			}
			continue
		}
		if job == nil {
			if !wait(ctx, r.pollInterval) {
				return
			}
			continue
		}

		r.process(handlerCtx, job)
	}
}

// wait sleeps for d, returning false when the runner is shutting down.
func wait(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
