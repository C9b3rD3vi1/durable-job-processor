package domain

import "errors"

var (
	// ErrNotFound indicates that the requested job does not exist for the tenant.
	ErrNotFound = errors.New("job not found")

	// ErrInvalidJob indicates that a job failed domain validation.
	ErrInvalidJob = errors.New("invalid job")

	// ErrInvalidArgument indicates that a caller supplied an invalid argument.
	ErrInvalidArgument = errors.New("invalid argument")

	// ErrInvalidTransition indicates an illegal job lifecycle transition.
	ErrInvalidTransition = errors.New("invalid job status transition")
)
