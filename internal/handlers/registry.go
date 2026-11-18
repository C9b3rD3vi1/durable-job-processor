// Package handlers maps allow-listed job types to execution handlers.
package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/domain"
)

// ErrHandlerRegistered indicates that a job type already has a handler.
var ErrHandlerRegistered = errors.New("handler already registered")

// Handler executes one job type. Handlers must be safe for at-least-once
// execution and must honour context cancellation.
type Handler interface {
	Execute(ctx context.Context, job domain.Job) (json.RawMessage, error)
}

// PayloadValidator is implemented by handlers that can check a job payload
// before submission. It lets the API reject malformed jobs with 400 instead of
// accepting them and dead-lettering them later at execution time.
type PayloadValidator interface {
	ValidatePayload(payload json.RawMessage) error
}

// HandlerFunc adapts a function to the Handler interface.
type HandlerFunc func(ctx context.Context, job domain.Job) (json.RawMessage, error)

// Execute calls f.
func (f HandlerFunc) Execute(ctx context.Context, job domain.Job) (json.RawMessage, error) {
	return f(ctx, job)
}

// Registry is both the submission allow-list and the worker dispatch table.
// A job type is accepted by the API only when a handler is registered for it.
type Registry struct {
	handlers map[string]Handler
}

// New creates an empty registry.
func New() *Registry {
	return &Registry{handlers: make(map[string]Handler)}
}

// Register binds a job type to its handler.
func (r *Registry) Register(jobType string, handler Handler) error {
	if r == nil {
		return errors.New("handler registry is nil")
	}
	jobType = strings.TrimSpace(jobType)
	if jobType == "" {
		return errors.New("job type is required")
	}
	if len(jobType) > domain.MaxJobTypeLength {
		return fmt.Errorf("job type exceeds %d characters", domain.MaxJobTypeLength)
	}
	if handler == nil {
		return fmt.Errorf("handler for %q is nil", jobType)
	}
	if _, exists := r.handlers[jobType]; exists {
		return fmt.Errorf("%w: %s", ErrHandlerRegistered, jobType)
	}
	r.handlers[jobType] = handler
	return nil
}

// Allows reports whether jobType has a registered handler.
func (r *Registry) Allows(jobType string) bool {
	if r == nil {
		return false
	}
	_, ok := r.handlers[jobType]
	return ok
}

// Handler returns the handler registered for jobType.
func (r *Registry) Handler(jobType string) (Handler, bool) {
	if r == nil {
		return nil, false
	}
	handler, ok := r.handlers[jobType]
	return handler, ok
}

// ValidatePayload applies a handler's submission-time schema when it has one.
// Handlers without a validator accept any structurally valid JSON object.
func (r *Registry) ValidatePayload(jobType string, payload json.RawMessage) error {
	if r == nil {
		return nil
	}
	handler, ok := r.handlers[strings.TrimSpace(jobType)]
	if !ok {
		return fmt.Errorf("unsupported job type %q", jobType)
	}
	validator, ok := handler.(PayloadValidator)
	if !ok {
		return nil
	}
	return validator.ValidatePayload(payload)
}

// Types returns registered job types in sorted order.
func (r *Registry) Types() []string {
	if r == nil {
		return nil
	}
	types := make([]string, 0, len(r.handlers))
	for jobType := range r.handlers {
		types = append(types, jobType)
	}
	sort.Strings(types)
	return types
}
