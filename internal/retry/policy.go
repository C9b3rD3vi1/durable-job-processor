package retry

// ShouldRetry reports whether a failure of class should be attempted again.
// Permanent failures never retry; every other class is bounded by maxAttempts.
func ShouldRetry(class Class, attempt, maxAttempts int) bool {
	if class == ClassPermanent {
		return false
	}
	return attempt < maxAttempts
}
