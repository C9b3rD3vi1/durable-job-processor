package handlers

import (
	"sort"
	"strings"
)

// Registry is an allow-list of supported job types. Phase 1 uses it to reject
// unknown job types before a row is written. Concrete handlers will be bound to
// these type names in Phase 2.
type Registry struct {
	allowed map[string]struct{}
}

// NewRegistry builds an allow-list from the supplied job type names.
func NewRegistry(jobTypes ...string) *Registry {
	r := &Registry{allowed: make(map[string]struct{}, len(jobTypes))}
	for _, jobType := range jobTypes {
		jobType = strings.TrimSpace(jobType)
		if jobType != "" {
			r.allowed[jobType] = struct{}{}
		}
	}
	return r
}

// DefaultRegistry returns the job types supported by this implementation.
func DefaultRegistry() *Registry {
	return NewRegistry(
		"report.generate",
		"email.send",
		"webhook.deliver",
		"data.import",
	)
}

// Allows reports whether jobType is on the allow-list.
func (r *Registry) Allows(jobType string) bool {
	if r == nil {
		return false
	}
	_, ok := r.allowed[jobType]
	return ok
}

// Types returns the allowed job types in sorted order.
func (r *Registry) Types() []string {
	if r == nil {
		return nil
	}
	types := make([]string, 0, len(r.allowed))
	for jobType := range r.allowed {
		types = append(types, jobType)
	}
	sort.Strings(types)
	return types
}
