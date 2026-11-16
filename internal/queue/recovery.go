package queue

// RecoveryResult reports how expired leases were resolved.
type RecoveryResult struct {
	// Requeued counts jobs returned to retry_wait for another attempt.
	Requeued int
	// Dead counts jobs dead-lettered because attempts were exhausted.
	Dead int
	// Cancelled counts jobs with a pending cancellation request that were
	// finalized as cancelled after their lease expired.
	Cancelled int
}
