package queue

// Stats is a point-in-time view of work ready for a worker to claim. Values are
// computed by PostgreSQL so all workers observe the same backlog and age.
type Stats struct {
	// Depth counts queued and retry_wait jobs that are due, have attempts left
	// and are not awaiting cancellation.
	Depth int64

	// OldestDueSeconds is the age of the oldest due job according to the
	// database clock. It is zero when the queue is empty.
	OldestDueSeconds float64
}
