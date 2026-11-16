package retry

import (
	"context"
	"errors"
)

// Class is the retry treatment for a handler failure.
type Class int

const (
	// ClassUnknown is a failure without an explicit classification. It is
	// treated conservatively: retried until max_attempts is reached.
	ClassUnknown Class = iota
	// ClassTransient is a failure that may succeed on another attempt.
	ClassTransient
	// ClassPermanent is a failure that retrying cannot fix.
	ClassPermanent
	// ClassCancelled is a failure caused by context cancellation. Until a
	// cancellation API exists, it is retried like a transient failure so a
	// graceful shutdown does not discard work.
	ClassCancelled
)

func (c Class) String() string {
	switch c {
	case ClassTransient:
		return "transient"
	case ClassPermanent:
		return "permanent"
	case ClassCancelled:
		return "cancelled"
	default:
		return "unknown"
	}
}

type classifiedError struct {
	class Class
	err   error
}

func (e *classifiedError) Error() string { return e.err.Error() }
func (e *classifiedError) Unwrap() error { return e.err }

// Transient marks err as retryable.
func Transient(err error) error { return classify(ClassTransient, err) }

// Permanent marks err as not retryable.
func Permanent(err error) error { return classify(ClassPermanent, err) }

// Cancelled marks err as cancellation-related.
func Cancelled(err error) error { return classify(ClassCancelled, err) }

func classify(class Class, err error) error {
	if err == nil {
		return nil
	}
	return &classifiedError{class: class, err: err}
}

// Classify returns the treatment for err. Explicit wrappers win; otherwise
// context cancellation is cancelled, a deadline is transient, and every other
// error is unknown.
func Classify(err error) Class {
	if err == nil {
		return ClassUnknown
	}
	var classified *classifiedError
	if errors.As(err, &classified) {
		return classified.class
	}
	switch {
	case errors.Is(err, context.Canceled):
		return ClassCancelled
	case errors.Is(err, context.DeadlineExceeded):
		return ClassTransient
	default:
		return ClassUnknown
	}
}
