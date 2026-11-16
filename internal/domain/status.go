package domain

// JobStatus is the persisted lifecycle state of a job.
type JobStatus string

const (
	StatusQueued    JobStatus = "queued"
	StatusRunning   JobStatus = "running"
	StatusRetryWait JobStatus = "retry_wait"
	StatusSucceeded JobStatus = "succeeded"
	StatusDead      JobStatus = "dead"
	StatusCancelled JobStatus = "cancelled"
)

// Valid reports whether s is a known job status.
func (s JobStatus) Valid() bool {
	switch s {
	case StatusQueued, StatusRunning, StatusRetryWait, StatusSucceeded, StatusDead, StatusCancelled:
		return true
	default:
		return false
	}
}

// IsTerminal reports whether no automatic transition may leave s.
// A dead job is terminal for automatic processing but may be replayed by an operator.
func (s JobStatus) IsTerminal() bool {
	switch s {
	case StatusSucceeded, StatusDead, StatusCancelled:
		return true
	default:
		return false
	}
}

func (s JobStatus) String() string { return string(s) }

// allowedTransitions describes the lifecycle documented in the architecture.
var allowedTransitions = map[JobStatus]map[JobStatus]struct{}{
	StatusQueued: {
		StatusRunning:   {},
		StatusCancelled: {},
	},
	StatusRetryWait: {
		StatusRunning:   {},
		StatusCancelled: {},
	},
	StatusRunning: {
		StatusSucceeded: {},
		StatusRetryWait: {},
		StatusDead:      {},
		StatusCancelled: {},
	},
	StatusDead: {
		StatusRetryWait: {},
	},
	StatusSucceeded: {},
	StatusCancelled: {},
}

// CanTransition reports whether from may legally move to to.
func CanTransition(from, to JobStatus) bool {
	if !from.Valid() || !to.Valid() {
		return false
	}
	_, ok := allowedTransitions[from][to]
	return ok
}
